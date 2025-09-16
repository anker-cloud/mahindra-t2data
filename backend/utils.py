import logging
from google.cloud import bigquery
from data_agent.constants import PROJECT_ID, DATASET_NAME, TABLE_NAMES

def get_table_description(table_name: str) -> str:
    """Fetches the description for a given table from BigQuery."""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_NAME}.{table_name}"
        table = client.get_table(table_id)  # Make an API request.
        return table.description if table.description else ""
    except Exception as e:
        logging.error(f"Error fetching table description for {table_name}: {e}")
        return ""

def get_table_ddl_strings() -> list[dict]:
    """
    Fetches the DDL strings, table type, and creation time
    for specified base tables, or all base tables if no list is provided,
    in the given Google BigQuery project and dataset.

    Args:
        project_id: The Google Cloud project ID.
        dataset_name: The BigQuery dataset ID.
        table_names_list: An optional list of table names. If provided, DDLs
                          will be fetched only for these tables. If None or empty,
                          DDLs for all base tables in the dataset will be fetched.

    Returns:
        A list of dictionaries, where each dictionary contains:
            - 'table_type': The type of the table (e.g., 'BASE TABLE').
            - 'creation_time': The timestamp of when the table was created.
            - 'ddl': The raw DDL string for the table.
        Returns an empty list if no tables are found or if an error occurs.
    """
    all_table_ddls = []
    client = bigquery.Client(project=PROJECT_ID)

    # Base SQL query to fetch basic table information including the DDL string
    # We still select table_name for logging and potential internal use,
    # but it won't be in the final returned dictionary per user request.
    base_query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name, 
            table_type,
            creation_time,
            ddl  -- The DDL string for the table
        FROM
            `{PROJECT_ID}.{DATASET_NAME}.INFORMATION_SCHEMA.TABLES`
        WHERE
            table_type = 'BASE TABLE'  -- Target only base tables (excluding views, etc.)
    """

    # Add condition for specific table names if provided
    if TABLE_NAMES and len(TABLE_NAMES) > 0:
        # Format table names for the IN clause: ('table1', 'table2', ...)
        formatted_table_names = ", ".join([f"'{name}'" for name in TABLE_NAMES])
        base_query += f" AND table_name IN ({formatted_table_names})"

    base_query += " ORDER BY table_name;"
    final_query = base_query

    try:
        query_job = client.query(final_query)
        results = query_job.result()  # Wait for the query to complete

        for row in results:
            if not row.ddl:
                continue

            # Construct the dictionary with only the requested fields
            table_info = {
                "table_catalog": row.table_catalog,
                "table_schema": row.table_schema,
                "table_name": row.table_name,
                "table_type": row.table_type,
                "creation_time": row.creation_time,
                "ddl": row.ddl
            }
            all_table_ddls.append(table_info)

        return all_table_ddls

    except Exception as e:
        print(
            f"--- Failed to fetch DDL strings ---",
            exc_info=True  # Automatically add exception info (like traceback)
        )
        return []

def get_total_rows(table_name: str) -> int:
    """Fetches the total number of rows for a given table from BigQuery."""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query = f"""
            SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_NAME}.{table_name}`
        """
        query_job = client.query(query)  # Make an API request.
        results = query_job.result()  # Waits for the query to finish.
        for row in results:
            return row[0]
    except Exception as e:
        logging.error(f"Error fetching total rows for {table_name}: {e}")
        return 0

def get_total_column_count() -> int:
    """Fetches the total number of columns across all tables in the dataset from BigQuery."""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query = f"""
            SELECT
                count(*) as total_columns
            FROM
                `{PROJECT_ID}.{DATASET_NAME}.INFORMATION_SCHEMA.COLUMNS`
        """
        query_job = client.query(query)  # Make an API request.
        results = query_job.result()  # Waits for the query to finish.
        for row in results:
            return row.total_columns
    except Exception as e:
        logging.error(f"Error fetching total column count: {e}")
        return 0

def fetch_sample_data_for_single_table(table_name: str, num_rows: int = 3) -> list[dict]:
    """
    Fetches a few sample rows from a specific table in the dataset.

    Args:
        table_name: The name of the table to fetch data from.
        num_rows: The number of sample rows to fetch.

    Returns:
        A list of dictionaries representing the sample rows.
        Returns an empty list if no data can be fetched or an error occurs.
    """
    if not PROJECT_ID or not DATASET_NAME:
        logging.error("PROJECT_ID and DATASET_NAME must be configured in constants.py to fetch sample data.")
        return []
    try:
        client = bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        logging.error(f"Failed to create BigQuery client for project {PROJECT_ID}: {e}", exc_info=True)
        return []

    full_table_name = f"{PROJECT_ID}.{DATASET_NAME}.{table_name}"
    try:
        logging.info(f"Fetching sample data for table (using list_rows): {full_table_name}")

        table_reference = bigquery.table.TableReference.from_string(full_table_name, default_project=PROJECT_ID)

        # Fetch rows using list_rows.
        rows_iterator = client.list_rows(table_reference, max_results=num_rows)

        table_sample_rows = [dict(row.items()) for row in rows_iterator]

        if not table_sample_rows:
            logging.info(f"No sample data found for table '{full_table_name}' using list_rows.")

        return table_sample_rows

    except Exception as e:
        logging.error(f"Error fetching sample data for table {full_table_name} using list_rows: {e}", exc_info=True)
        return []