try:
    import sys
    import boto3
    import pandas as pd
    from datetime import datetime
    import uuid
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    print("All imports ok ...")
except Exception as e:
    print(f"Error Imports : {e}")

# Initialize the DMS client
dms_client = boto3.client('dms')


def get_dms_task_arn_from_id(dms_task_id):
    """
    Get the ARN for a given DMS task ID.

    :param dms_task_id: The identifier of the DMS task
    :return: The ARN of the DMS task or None if not found
    """

    try:
        # Get all DMS tasks
        response = dms_client.describe_replication_tasks()
        # Iterate through the tasks to find the one that matches the given task_id
        for task in response['ReplicationTasks']:
            if task['ReplicationTaskIdentifier'] == dms_task_id:
                return task['ReplicationTaskArn']

        # If no matching task is found, return None
        return None
    except dms_client.exceptions.ResourceNotFoundFault:
        print(f"No task found with ID: {dms_task_id}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def get_table_statistics(dms_task_arn):
    """
    Get table statistics for a specified DMS task ARN.

    :param dms_task_arn: ARN of the DMS task
    :return: List of table statistics or None if not found
    """
    try:
        # Call the describe_table_statistics API
        response = dms_client.describe_table_statistics(
            ReplicationTaskArn=dms_task_arn
        )

        # Extract the table statistics
        table_statistics = response.get('TableStatistics', [])
        return table_statistics
    except dms_client.exceptions.ResourceNotFoundFault:
        print(f"No statistics found for the DMS task with ARN: {dms_task_arn}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def update_table_load_statistics(table_statistics, metadata_output_path, ingestion_date):
    """
    Converts table statistics to a DataFrame, adds ingestion and processing dates,
    generates a unique file name, and saves the data to a Parquet file. Returns a
    boolean indicating success or failure of the operation.

    :param table_statistics: List of dictionaries with table statistics
    :param metadata_output_path: Base path for the output Parquet files
    :param ingestion_date: The date when data ingestion happened : format expected :yyyy-MM-dd
    :return: True if the operation is successful, False otherwise
    """
    try:
        # Convert the list of dictionaries to a Pandas DataFrame
        df = pd.DataFrame(table_statistics)

        # Add an ingestion date column
        processed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        year = datetime.now().strftime("%Y")
        month = datetime.now().strftime("%m")
        day = datetime.now().strftime("%d")

        df['IngestionDate'] = ingestion_date
        df['ProcessedDate'] = processed_date

        # Generate a unique ID using UUID
        unique_id = str(uuid.uuid4())
        # Get the current timestamp
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

        # Check if metadata_output_path ends with '/' and remove it if true
        if metadata_output_path.endswith('/'):
            metadata_output_path = metadata_output_path.rstrip('/')

        metadata_filename = f"{metadata_output_path}/year={year}/month={month}/day={day}/{unique_id}_{timestamp}.parquet"

        # Save the DataFrame to a Parquet file
        df.to_parquet(metadata_filename, index=False)

        print(f"Metadata saved to {metadata_filename}")
        return True

    except Exception as e:
        print(f"An error occurred while updating table load statistics: {e}")
        return False


# expected argument names
args_names = ['DMS_TASK_ID', 'INGESTION_DATE', 'METADATA_BUCKET', 'METADATA_KEY']
# Retrieve the DMS task ARN from Glue job parameters
args = getResolvedOptions(sys.argv, args_names)

dms_task_id = args['DMS_TASK_ID']
ingestion_date = args['INGESTION_DATE']
metadata_bucket = args['METADATA_BUCKET']
metadata_key = args['METADATA_KEY']

dms_task_arn = get_dms_task_arn_from_id(dms_task_id)
if dms_task_arn is not None:
    print(f"The ARN for task ID '{dms_task_id}' is '{dms_task_arn}'")
else:
    raise ValueError(f"No task found with ID '{dms_task_id}'")

# Extract the table statistics
table_statistics = get_table_statistics(dms_task_arn)
if table_statistics is None:
    raise Exception("Table Statistics could not be retrieved.")

# Write out the data to Parquet
metadata_output_path = f"s3://{metadata_bucket}/{metadata_key}"
if update_table_load_statistics(table_statistics, metadata_output_path, ingestion_date):
    print("Successfully updated table load statistics.")
else:
    print("Failed to update table load statistics.")
