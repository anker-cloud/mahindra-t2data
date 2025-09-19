import collections
from google.cloud import bigquery, dataplex_v1
from google.cloud.bigquery.table import TableReference
from .constants import PROJECT_ID, DATASET_NAME, TABLE_NAMES, DATA_PROFILES_TABLE_FULL_ID, LOCATION
import time
import logging
from proto.marshal.collections.repeated import RepeatedComposite
from proto.marshal.collections.maps import MapComposite
from decimal import Decimal # Import the Decimal type

# --- CORRECT LOGGING SETUP ---
# Get a logger instance for this module. It will inherit its configuration
# (level, format, stream) from the central setup in app.py.
# The incorrect logging.basicConfig() call has been removed.
logger = logging.getLogger(__name__)


# --- HELPER FUNCTION TO FIX THE DECIMAL-to-JSON ERROR ---
def _convert_decimals(obj):
    """Recursively traverses a data structure to convert Decimal objects to floats."""
    if isinstance(obj, list):
        return [_convert_decimals(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _convert_decimals(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def fetch_bigquery_data_profiles() -> list[dict]:
    """
    Fetches data profile information from a BigQuery table specified in constants.
    """
    start_time = time.time()
    # Use constants for dataset_name, table_names, and data_profiles_table_full_id
    dataset_name_to_filter = DATASET_NAME
    target_table_names = TABLE_NAMES
    profiles_table_id = DATA_PROFILES_TABLE_FULL_ID

    if not profiles_table_id: # Check if the ID is None or an empty string
        logger.info(
            "DATA_PROFILES_TABLE_FULL_ID is not configured. Skipping data profile fetching."
        )
        return [] # Return an empty list immediately

    # Check if table_names list has elements to determine logging message
    if target_table_names and len(target_table_names) > 0:
        logger.info(
            f"Starting to fetch data profiles for tables {target_table_names} in dataset '{dataset_name_to_filter}' "
            f"from '{profiles_table_id}'."
        )
    else:
        logger.info(
            f"Starting to fetch data profiles for all tables in dataset '{dataset_name_to_filter}' "
            f"from '{profiles_table_id}'."
        )

    client = bigquery.Client(project=PROJECT_ID)

    # Construct the SELECT clause with specific columns
    # Aliasing data_source fields for clarity in the output
    select_clause = """
        SELECT
            CONCAT(data_source.table_project_id, '.', data_source.dataset_id, '.', data_source.table_id) AS source_table_id,
            column_name, 
            percent_null,
            percent_unique,
            min_string_length,
            max_string_length,
            min_value,
            max_value,
            top_n
    """

    from_clause = f"FROM `{profiles_table_id}`"

    # Initialize WHERE conditions and query parameters
    where_conditions = ["data_source.dataset_id = @dataset_name_param"]
    query_params = [
        bigquery.ScalarQueryParameter("dataset_name_param", "STRING", dataset_name_to_filter)
    ]

    # Add condition for specific table names if the list has elements
    if target_table_names and len(target_table_names) > 0:
        where_conditions.append("data_source.table_id IN UNNEST(@table_names_param)")
        query_params.append(
            bigquery.ArrayQueryParameter("table_names_param", "STRING", target_table_names)
        )

    where_clause = "WHERE " + " AND ".join(where_conditions)
    order_by_clause = "ORDER BY source_table_id, column_name" # Consistent ordering

    final_query = f"{select_clause}\n{from_clause}\n{where_clause}\n{order_by_clause};"

    logger.debug(f"Executing BigQuery data profiles query:\n{final_query}")

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)

    try:
        query_job = client.query(final_query, job_config=job_config)
        results = query_job.result()  # Wait for the query to complete
        # Convert all rows to dictionaries first
        raw_profiles_data = [dict(row.items()) for row in results]

        # --- FIX #1 APPLIED HERE ---
        # Clean the data by converting all Decimal objects to standard floats
        cleaned_profiles_data = _convert_decimals(raw_profiles_data)
        
        profiles_data = []  # Initialize the final list for filtered profiles
        # Use the cleaned data for the rest of the function
        for profile in cleaned_profiles_data:
            percent_null_value = profile.get('percent_null')

            # Check condition: percent_null > 90
            if isinstance(percent_null_value, (float, int)) and percent_null_value > 90:
                continue  # Skip adding this profile to the final list

            # Add profile if it doesn't meet any removal condition
            profiles_data.append(profile)

        num_profiles_fetched = len(profiles_data)
        duration = time.time() - start_time
        logger.info(
            f"--- Successfully fetched {num_profiles_fetched} column profiles "
            f"(Duration: {duration:.2f} seconds) ---"
        )
        return profiles_data

    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            f"--- Failed to fetch data profiles after {duration:.2f} seconds ---",
            exc_info=True  # Automatically add exception info (like traceback)
        )
        return []

def fetch_sample_data_for_tables(num_rows: int = 3) -> list[dict]:
    """
    Fetches a few sample rows from tables defined in constants.
    """
    start_time = time.time()
    sample_data_results: list[dict] = []

    project_id = PROJECT_ID
    dataset_id = DATASET_NAME
    table_names_list = TABLE_NAMES

    if not project_id or not dataset_id:
        logger.error("PROJECT_ID and DATASET_NAME must be configured in constants.py to fetch sample data.")
        return sample_data_results
    try:
        client = bigquery.Client(project=project_id)
    except Exception as e:
        logger.error(f"Failed to create BigQuery client for project {project_id}: {e}", exc_info=True)
        return sample_data_results

    tables_to_fetch_samples_from_ids: list[str] = []

    if table_names_list and len(table_names_list) > 0: # If specific table names are provided
        tables_to_fetch_samples_from_ids = table_names_list
        logger.info(f"Fetching sample data for specified tables in {project_id}.{dataset_id}: {table_names_list}")
    else: # If TABLE_NAMES is empty, fetch for all tables in the dataset
        logger.info(f"Fetching sample data for all tables in dataset: {project_id}.{dataset_id}")
        try:
            dataset_ref = client.dataset(dataset_id, project=project_id)
            bq_tables = client.list_tables(dataset_ref)
            tables_to_fetch_samples_from_ids = [bq_table.table_id for bq_table in bq_tables if bq_table.table_type == 'TABLE']
        except Exception as e:
            logger.error(f"Error listing tables for {project_id}.{dataset_id}: {e}", exc_info=True)
            return sample_data_results

    if not tables_to_fetch_samples_from_ids:
        logger.info(f"No tables identified to fetch samples from in {project_id}.{dataset_id}.")
        return sample_data_results

    for table_id_str in tables_to_fetch_samples_from_ids:
        full_table_name = f"{project_id}.{dataset_id}.{table_id_str}"
        try:
            logger.info(f"Fetching sample data for table: {full_table_name}")
            table_reference = TableReference.from_string(full_table_name, default_project=project_id)
            rows_iterator = client.list_rows(table_reference, max_results=num_rows)
            table_sample_rows_raw = [dict(row.items()) for row in rows_iterator]

            # --- FIX #2 APPLIED HERE ---
            # Clean the sample data by converting all Decimal objects to floats
            table_sample_rows = _convert_decimals(table_sample_rows_raw)

            if table_sample_rows:
                sample_data_results.append({
                    "table_name": full_table_name,
                    "sample_rows": table_sample_rows
                })
            else:
                logger.info(f"No sample data found for table '{full_table_name}'.")
        except Exception as e:
            logger.error(f"Error fetching sample data for table {full_table_name}: {e}", exc_info=True)
            continue

    duration = time.time() - start_time
    logger.info(
        f"--- Successfully fetched {len(sample_data_results)} sample data sets "
        f"(Duration: {duration:.2f} seconds) ---"
    )
    return sample_data_results


def convert_proto_to_dict(obj):
    if isinstance(obj, MapComposite):
        return {k: convert_proto_to_dict(v) for k, v in obj.items()}
    elif isinstance(obj, RepeatedComposite):
        return [convert_proto_to_dict(elem) for elem in obj]
    else:
        return obj

def fetch_table_entry_metadata() -> list[dict]:
    """
    Fetches complete metadata for table entries from Dataplex Catalog.
    """
    start_time = time.time()
    logger.info(
        f"Fetching Dataplex entry metadata for project='{PROJECT_ID}', location='{LOCATION}', "
        f"dataset='{DATASET_NAME}', tables='{TABLE_NAMES if TABLE_NAMES else 'All'}'"
    )
    all_entry_metadata: list[dict] = []

    try:
        client = dataplex_v1.CatalogServiceClient()
    except Exception as e:
        logger.error(f"Failed to create Dataplex CatalogServiceClient: {e}", exc_info=True)
        return all_entry_metadata

    target_entry_names: list[str] = []

    if TABLE_NAMES:
        entry_group_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/@bigquery"
        for table_name in TABLE_NAMES:
            entry_id_for_bq = f"bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_NAME}/tables/{table_name}"
            target_entry_names.append(f"{entry_group_name}/entries/{entry_id_for_bq}")
    else:
        logger.info(f"Listing all entries in project '{PROJECT_ID}' to find tables in dataset '{DATASET_NAME}'.")
        try:
            search_request = dataplex_v1.SearchEntriesRequest(
                name=f"projects/{PROJECT_ID}/locations/global",
                scope=f"projects/{PROJECT_ID}",
                query=f"name:projects/{PROJECT_ID}/datasets/{DATASET_NAME}/tables/",
            )
            for entry in client.search_entries(request=search_request):
                target_entry_names.append(entry.dataplex_entry.name)
        except Exception as e:
            logger.error(f"Error listing Dataplex entries: {e}", exc_info=True)

    if not target_entry_names:
        logger.info("No target entries identified for fetching entry metadata.")
        return all_entry_metadata

    for entry_name in target_entry_names:
        try:
            logger.debug(f"Getting entry: {entry_name} with EntryView.ALL")
            get_request = dataplex_v1.GetEntryRequest(name=entry_name, view=dataplex_v1.EntryView.ALL)
            entry = client.get_entry(request=get_request)
            
            aspects_data = {
                aspect_key: {
                    key: convert_proto_to_dict(value_proto)
                    for key, value_proto in aspect.data.items()
                }
                for aspect_key, aspect in entry.aspects.items() if hasattr(aspect, 'data') and aspect.data
            }

            all_entry_metadata.append({
                'table_name': entry_name.split('/')[-1],
                'aspects': aspects_data
            })
            logger.debug(f"Fetched ALL entry metadata for '{entry_name}'")
        except Exception as e:
            logger.error(f"Error fetching FULL entry metadata for entry {entry_name}: {e}", exc_info=True)
            continue

    duration = time.time() - start_time
    logger.info(
        f"--- Successfully fetched {len(all_entry_metadata)} entry metadata sets "
        f"(Duration: {duration:.2f} seconds) ---"
    )
    return all_entry_metadata