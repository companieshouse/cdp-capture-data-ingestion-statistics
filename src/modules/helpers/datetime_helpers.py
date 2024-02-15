from datetime import datetime


def get_current_datetime():
    """Returns the current datetime."""
    return datetime.now()


def format_datetime(dt, format_string="%Y-%m-%d %H:%M:%S"):
    """Returns a formatted string for a datetime object."""
    return dt.strftime(format_string)


def parse_datetime(date_string, format_string="%Y-%m-%d %H:%M:%S"):
    """Parses a string into a datetime object based on the given format."""
    return datetime.strptime(date_string, format_string)


# Add more datetime-related utility functions as needed
def get_formatted_current_datetime(format_string="%Y-%m-%d %H:%M:%S"):
    """Returns a formatted string for a datetime object."""
    try:
        return datetime.now().strftime(format_string)
    except Exception as e:
        return None


def extract_date_parts(ingest_date):
    """
    Extracts the year, month, and day from a given date string.

    :param ingest_date: A date string in the format "yyyy-MM-dd".
    :return: A tuple containing the year, month, and day as strings.
    """
    # Parse the ingest_date string into a datetime object
    try:
        ingest_datetime = datetime.strptime(ingest_date, "%Y-%m-%d")

        # Extract year, month, and day from the datetime object
        year = ingest_datetime.strftime("%Y")
        month = ingest_datetime.strftime("%m")
        day = ingest_datetime.strftime("%d")

        return year, month, day
    except Exception as e:
        return None, None, None
