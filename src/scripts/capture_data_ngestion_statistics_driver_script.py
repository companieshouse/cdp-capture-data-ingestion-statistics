try:
    from src.modules.helpers.logging_helpers import *
    from src.modules.helpers.datetime_helpers import *
    import sys
    import boto3
    import pandas as pd
    from datetime import datetime
    import uuid
    import logging
    from awsglue.utils import getResolvedOptions

    print("All imports ok ...")
except Exception as e:
    print(f"Error Imports : {e}")
    raise Exception(f"Error Imports : {e}")

# Set up logging
setup_loggings()
# Initialize logger
logger = logging.getLogger()

# Initialize the DMS client
dms_client = boto3.client('dms', region_name='eu-west-2')


def strip_slash_if_exists_at_end(input_string):
    """
    Removes a trailing slash from the input string if it exists.

    :param input_string: The string from which to potentially remove a trailing slash.
    :return: The input string without a trailing slash at the end.
    """
    if input_string.endswith('/'):
        return input_string.rstrip('/')
    return input_string


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
        logger.error(f"No task found with ID: {dms_task_id}")
        return None
    except Exception as e:
        logger.error(f"An error occurred: {e}")
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
        logger.error(f"No statistics found for the DMS task with ARN: {dms_task_arn}")
        return None
    except Exception as e:
        logger.error(f"An error occurred while getting table statistics: {e}")
        return None


def save_table_statistics_to_parquet(table_statistics, metadata_output_path, ingestion_date):
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
        table_statistics_df = pd.DataFrame(table_statistics)

        # Add an ingestion date column
        processed_date = get_formatted_current_datetime()
        if processed_date is None:
            logger.error("Error formatting current datetime")
            return False

        year, month, day = extract_date_parts(ingestion_date)
        if year is None or month is None or day is None:
            logger.error("Error extracting date parts from date")
            return False
        table_statistics_df['IngestionDate'] = ingestion_date
        table_statistics_df['ProcessedDate'] = processed_date

        # Generate a unique ID using UUID
        unique_id = str(uuid.uuid4())
        # Get the current timestamp
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

        # Check if metadata_output_path ends with '/' and remove it if true
        striped_metadata_output_path = strip_slash_if_exists_at_end(metadata_output_path)
        metadata_filename = f"{striped_metadata_output_path}/year={year}/month={month}/day={day}/{unique_id}-{timestamp}.parquet"

        # Save the DataFrame to a Parquet file
        table_statistics_df.to_parquet(metadata_filename, index=False)

        logger.info(f"Metadata saved to {metadata_filename}")
        return True

    except Exception as e:
        logger.info(f"An error occurred while saving the metadata to Parquet: {e}")
        return False


try:
    # expected argument names
    args_names = ['DMS_TASK_ID', 'INGESTION_DATE', 'METADATA_BUCKET', 'METADATA_KEY']
    # Retrieve the DMS task ARN from Glue job parameters
    args = getResolvedOptions(sys.argv, args_names)

    dms_task_id = args['DMS_TASK_ID']
    ingestion_date = args['INGESTION_DATE']
    metadata_bucket = args['METADATA_BUCKET']
    metadata_key = args['METADATA_KEY']

    # Extract the DMS task ARN from the DMS task ID
    dms_task_arn = get_dms_task_arn_from_id(dms_task_id)
    if dms_task_arn is None:
        logger.info(f"The ARN for task ID '{dms_task_id}' is '{dms_task_arn}'")
        raise ValueError(f"No task found with ID '{dms_task_id}'")

    logger.info(f"The ARN for task ID '{dms_task_id}' is '{dms_task_arn}'")

    # Extract the table statistics from the DMS task ARN
    table_statistics = get_table_statistics(dms_task_arn)
    if table_statistics is None:
        logger.info("Table Statistics could not be retrieved.")
        raise Exception("Table Statistics could not be retrieved.")

    # Write the table statistics to a Parquet file
    metadata_output_path = f"s3://{metadata_bucket}/{metadata_key}"
    if save_table_statistics_to_parquet(table_statistics, metadata_output_path, ingestion_date):
        logger.info("Successfully updated table load statistics.")
    else:
        logger.info("Failed to update table load statistics.")
        raise Exception("Failed to update table load statistics.")
except Exception as e:
    logger.error(f"Error processing Glue job: {str(e)}")
    raise Exception(f"Error processing Glue job: {str(e)}")
