import sys
import pydata_google_auth
import re
import json
import logging as logger
from google.cloud import bigquery_datatransfer
from google.cloud import bigquery
from google.api_core import exceptions
from pathlib import Path
from datetime import datetime


def file_setup(files_name, files_status):
    """
    Process the input file names and statuses, split them into lists, and return two lists:
    1. config_files_list: A list of file names with a '.config' extension that are not marked as 'removed'.
    2. sql_files_list: A list of file names with a '.sql' extension that are not marked as 'removed'.
    """

    # Split the file names and statuses strings into lists
    file_names, file_status = files_name.split(' '), files_status.split(' ')

    # Initialize empty lists to store config and SQL files
    config_files_list, sql_files_list = [], []

    # Loop over file names and their corresponding statuses
    for key, value in zip(file_names, file_status):
        # If the file status is not 'removed', add the file name to the appropriate list
        if value != 'removed':
            # If the file is a config file, append the file name to config_files_list
            if re.search('.config$', key, flags=re.IGNORECASE):
                config_files_list.append(key)
            # If the file is an SQL file, append the file name to sql_files_list
            elif re.search('.sql$', key, flags=re.IGNORECASE):
                sql_files_list.append(key)

    # Return the config and SQL files lists separately
    return config_files_list, sql_files_list


def delete_dataset(client, datasets, dataset_ids):
    """Deletes any dataset older than 60 days excluding permanent datasets that should never be dropped (has label expiry:never)"""

    # Obtain today's date
    current_date=datetime.utcnow().day

    # Iterates over the names and objects of all datasets
    for dataset_id, dataset in zip(dataset_ids, datasets):

        # Excluding permanent datasets that should never be dropped (has label expiry:never)
        if not(client.get_dataset(dataset.dataset_id).labels.get('expiry')=='never'):

            # Capture age of dataset using dataset created date and current date
            age_of_dataset = (current_date - client.get_dataset(dataset.dataset_id).created.day)

            # If the dataset is older than 60 days, delete it
            if age_of_dataset>60:
                client.delete_dataset(client.get_dataset(dataset.dataset_id), delete_contents=True)
                print(f"Deleted dataset '{dataset.dataset_id}'")
    return


def dataset_check(client, branch_name, expiration_duration, location):
    """
    Check the presence of a dataset in the project, create one if it doesn't exist based on branch name,
    and return the dataset name.
    """
    
    # List all datasets in the project and extract their IDs
    datasets = list(client.list_datasets())
    dataset_ids = [dataset.dataset_id for dataset in datasets]

    # Determine the default dataset name based on the branch name
    if re.search(r'feature/(.+)', branch_name, re.IGNORECASE):
        # Extracting part after 'feature/'
        temp_name = re.search(r'feature/(.+)', branch_name, re.IGNORECASE).group(1)
        # Replacing dashes with underscores and ensuring valid BQ dataset name format
        default_dataset_name = re.sub('[^a-zA-Z0-9_]', '', re.sub('-', '_', temp_name)).lower()
    elif branch_name == 'main':
        default_dataset_name = 'production'
        # Delete temporary datasets if branch is 'main'
        delete_dataset(client, datasets, dataset_ids)
    else:
        # If the branch name doesn't match any known patterns, raise an error
        raise ValueError("The branch name does not match the expected patterns.")

    print(f"The '{branch_name}' branch will create or modify BQ assets in the '{default_dataset_name}' dataset")

    # Check for the existence of the determined dataset name
    if default_dataset_name not in dataset_ids:
        print(f"'{default_dataset_name}' does not exist. Creating '{default_dataset_name}'")
        
        # Create the dataset with the specified properties
        dataset = bigquery.Dataset(client.dataset(default_dataset_name))
        dataset.default_table_expiration_ms = expiration_duration
        dataset.location = location
        
        client.create_dataset(dataset)
        print(f"Dataset '{default_dataset_name}' created with a default table expiration of one month.")
    else:
        # If the dataset already exists, print a message
        print(f"Dataset '{default_dataset_name}' already exists.")

    # Return the name (ID) of the dataset
    return default_dataset_name


def create_asset(project_id, location, files_list, client, default_dataset_name):
    """Function to create an asset based on the file type (SQL or config)"""

    # Printing the list of files to process
    print(files_list)
    
    # Iterating over each file in the list
    for file in files_list:
        
        # Setting the proper path for the file
        proper_path = "/workspace/" + file
        print("Processing File: ", proper_path)

        # If the file is a SQL file
        if re.search('.sql$', file, flags=re.IGNORECASE):
            
            # Reading the SQL query from the file and replacing parameterized dataset_name in the file 
            # with the default_dataset_name obtained from dataset_check
            query = open(proper_path, 'r').read().replace('${dataset_name}', default_dataset_name)
            print(f"Executing query - {query}")
            
            # Running the SQL query
            bq_routine = client.query(query)

            # Checking the result of the query
            results = bq_routine.result()
            if bq_routine.state != 'DONE':
                e = "Query failed: {}".format(query)
                logger.error(e)
                raise RuntimeError(e)
            print(f"The results of the asset - {results}")
            
        # If the file is a config file
        elif re.search('.config$', file, flags=re.IGNORECASE):
            
            # Loading the JSON config data from the file
            f = open(str(proper_path))
            data = json.load(f)
            
            # Extracting required data from the JSON and replacing parameterized dataset_name in the file 
            # with the default_dataset_name obtained from dataset_check
            display_name = data["display_name"].replace('${dataset_name}', default_dataset_name)
            query = data["query"].replace('${dataset_name}', default_dataset_name)
            schedule = data["schedule"]
            
            # Setting the parent path for the project and location
            parent = f"projects/{project_id}/locations/{location.lower()}"
            
            # Fetching existing transfer configurations
            transfer_configs = client.list_transfer_configs(parent=parent)     
            
            # Defining the transfer configuration for the schedule
            transfer_config = bigquery_datatransfer.TransferConfig(
                display_name=display_name,
                data_source_id="scheduled_query",
                params={"query": query},
                schedule=schedule
            )
            
            # Checking if a schedule with the given display name exists, and if so, deleting it
            for config in transfer_configs:
                if config.display_name == display_name:
                    try:
                        client.delete_transfer_config(name=config.name)
                    except google.api_core.exceptions.NotFound:
                        print("Scheduled query not found.")
                    else:
                        print(f"Deleted Scheduled query: {config.display_name}")
            
            # Creating a new schedule with the transfer configuration
            transfer_config = client.create_transfer_config(
                bigquery_datatransfer.CreateTransferConfigRequest(
                    parent=parent, 
                    transfer_config=transfer_config
                )
            )
            print("Created scheduled query '{}'".format(transfer_config.name))

        # If the file is neither SQL nor config
        else:
            print("Not a config or sql file")  
    
    # Returning a completion message
    return "asset created"


if __name__ == "__main__":
    
    # Gather input arguments for the script
    project_id = sys.argv[1]  # The GCP project ID, e.g., "skyuk-uk-decis-models-01-dev"
    branch_name = sys.argv[2]  # The git branch name, either "main" or starting with "feature/"
    files_name = sys.argv[3]   # List of file names obtained from the commit
    files_status = sys.argv[4]  # Corresponding status of each file obtained from the commit

    # Define constants and settings
    expiration_duration = 30 * 24 * 60 * 60 * 1000  # Expiration duration for datasets, set to 1 month in milliseconds
    location = 'EU'  # All datasets are located in the multi-region EU
  ##  credentials = pydata_google_auth.load_user_credentials("../../shared/credentials-ref.json")  # Load cross-project credentials

    # Returns a list of config and SQL files based on files passed
    config_files_list, sql_files_list = file_setup(files_name, files_status)

    # Create a BigQuery client
    bq_client = bigquery.Client(project=project_id) ## credentials=credentials

    # Check the datasets in the project and decide on the dataset name based on the branch
    # For a feature branch, it ensures that a corresponding dataset exists, and if not, it creates one.
    default_dataset_name = dataset_check(bq_client, branch_name, expiration_duration, location)

    # Check if there are config files to process
    if len(config_files_list) > 0:
        
        # Create a BigQuery Data Transfer Service client
        transfer_client = bigquery_datatransfer.DataTransferServiceClient()## credentials= credentials
        
        # Check if there are SQL files to process
        if len(sql_files_list) > 0:
            
            # Create assets for SQL and configuration files
            create_asset(project_id, location, sql_files_list, bq_client, default_dataset_name)
            create_asset(project_id, location, config_files_list, transfer_client, default_dataset_name)

        else:

            # Create assets for configuration files only
            create_asset(project_id, location, config_files_list, transfer_client, default_dataset_name)

    # Check if there are SQL files to process
    elif len(sql_files_list) > 0:
        # Create assets for SQL files
        create_asset(project_id, location, sql_files_list, bq_client, default_dataset_name)

    # If no config or SQL files were found, print a message
    else:
        print("No config or SQL files have been added or modified")