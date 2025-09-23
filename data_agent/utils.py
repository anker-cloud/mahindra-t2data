import collections
from google.cloud import bigquery, dataplex_v1
from google.cloud.bigquery.table import TableReference
from .constants import PROJECT_ID, DATASET_NAME, TABLE_NAMES, DATA_PROFILES_TABLE_FULL_ID, LOCATION
import time
import logging
from proto.marshal.collections.repeated import RepeatedComposite
from proto.marshal.collections.maps import MapComposite
from decimal import Decimal
import pprint
from google.protobuf.json_format import MessageToDict

# Get a logger instance for this module, inheriting from the central app config.
logger = logging.getLogger(__name__)

def _convert_decimals(obj):
    """
    Recursively traverses a data structure to convert Decimal objects to floats.
    This is a helper function to prevent JSON serialization errors.

    Args:
        obj: The Python object (e.g., list, dict) to traverse.

    Returns:
        The object with all Decimal instances converted to floats.
    """
    if isinstance(obj, list):
        return [_convert_decimals(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _convert_decimals(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

def fetch_bigquery_data_profiles() -> list[dict]:
    """
    Fetches column data profiles from a specified BigQuery table.

    This function queries a pre-populated data profiles table to get statistics
    (like null percentage, min/max values, top values) for columns in the target
    dataset. It filters out columns that are more than 90% null to reduce noise.
    The data is cleaned to handle Decimal-to-float conversions.

    Returns:
        A list of dictionaries, where each dictionary is the data profile for a column.
        Returns an empty list if an error occurs or no profiles are found.
    """
    start_time = time.time()
    dataset_name_to_filter = DATASET_NAME
    target_table_names = TABLE_NAMES
    profiles_table_id = DATA_PROFILES_TABLE_FULL_ID

    if not profiles_table_id:
        logger.info("DATA_PROFILES_TABLE_FULL_ID is not configured. Skipping data profile fetching.")
        return []

    logger.info(f"Starting to fetch data profiles from '{profiles_table_id}'.")
    client = bigquery.Client(project=PROJECT_ID)

    select_clause = """
        SELECT
            CONCAT(data_source.table_project_id, '.', data_source.dataset_id, '.', data_source.table_id) AS source_table_id,
            column_name, percent_null, percent_unique, min_string_length,
            max_string_length, min_value, max_value, top_n
    """
    from_clause = f"FROM `{profiles_table_id}`"
    where_conditions = ["data_source.dataset_id = @dataset_name_param"]
    query_params = [bigquery.ScalarQueryParameter("dataset_name_param", "STRING", dataset_name_to_filter)]

    if target_table_names:
        where_conditions.append("data_source.table_id IN UNNEST(@table_names_param)")
        query_params.append(bigquery.ArrayQueryParameter("table_names_param", "STRING", target_table_names))

    final_query = f"{select_clause}\n{from_clause}\nWHERE {' AND '.join(where_conditions)}\nORDER BY source_table_id, column_name;"
    logger.debug(f"Executing BigQuery data profiles query:\n{final_query}")
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)

    try:
        query_job = client.query(final_query, job_config=job_config)
        raw_profiles_data = [dict(row.items()) for row in query_job.result()]
        cleaned_profiles_data = _convert_decimals(raw_profiles_data)
        
        profiles_data = [
            p for p in cleaned_profiles_data 
            if not (isinstance(p.get('percent_null'), (float, int)) and p.get('percent_null') > 90)
        ]

        duration = time.time() - start_time
        logger.info(f"--- Successfully fetched {len(profiles_data)} column profiles (Duration: {duration:.2f} seconds) ---")
        return profiles_data
    except Exception:
        logger.error("--- Failed to fetch data profiles ---", exc_info=True)
        return []

def fetch_sample_data_for_tables(num_rows: int = 3) -> list[dict]:
    """
    Fetches a small sample of rows from the target BigQuery tables.

    If TABLE_NAMES is specified in constants, it fetches samples only for those tables.
    Otherwise, it lists all tables in the dataset and fetches samples for each.
    The data is cleaned to handle Decimal-to-float conversions.

    Args:
        num_rows: The number of sample rows to fetch for each table.

    Returns:
        A list of dictionaries, each containing a 'table_name' and a list of 'sample_rows'.
    """
    start_time = time.time()
    sample_data_results: list[dict] = []
    client = bigquery.Client(project=PROJECT_ID)

    tables_to_fetch = TABLE_NAMES
    if not tables_to_fetch:
        logger.info(f"No specific tables listed; fetching samples for all tables in dataset '{DATASET_NAME}'.")
        try:
            tables_to_fetch = [t.table_id for t in client.list_tables(DATASET_NAME) if t.table_type == 'TABLE']
        except Exception:
            logger.error(f"Could not list tables for dataset '{DATASET_NAME}'", exc_info=True)
            tables_to_fetch = []

    for table_id in tables_to_fetch:
        full_table_name = f"{PROJECT_ID}.{DATASET_NAME}.{table_id}"
        try:
            rows_iterator = client.list_rows(full_table_name, max_results=num_rows)
            raw_rows = [dict(row.items()) for row in rows_iterator]
            cleaned_rows = _convert_decimals(raw_rows)
            if cleaned_rows:
                sample_data_results.append({"table_name": full_table_name, "sample_rows": cleaned_rows})
        except Exception:
            logger.error(f"Error fetching sample data for table {full_table_name}", exc_info=True)
            continue

    duration = time.time() - start_time
    logger.info(f"--- Successfully fetched {len(sample_data_results)} sample data sets (Duration: {duration:.2f} seconds) ---")
    return sample_data_results

def convert_proto_to_dict(obj):
    """
    Recursively converts complex Dataplex proto objects into simple Python dicts and lists.

    Args:
        obj: A Dataplex proto object (e.g., MapComposite, RepeatedComposite).

    Returns:
        A cleaned, simple Python dictionary or list.
    """
    if isinstance(obj, MapComposite):
        return {k: convert_proto_to_dict(v) for k, v in obj.items()}
    elif isinstance(obj, RepeatedComposite):
        return [convert_proto_to_dict(elem) for elem in obj]
    return obj

def fetch_table_entry_metadata() -> list[dict]:
    """
    Fetches complete metadata entries for tables using a hybrid approach.
    It discovers tables using the Dataplex Catalog and enriches them with
    real-time descriptions directly from the BigQuery API to ensure accuracy.

    Returns:
        A list of dictionaries, where each dictionary contains the metadata for one table.
    """
    start_time = time.time()
    logger.info(f"Fetching Dataplex entry metadata for tables='{TABLE_NAMES if TABLE_NAMES else 'All'}'")
    all_entry_metadata: list[dict] = []
    dataplex_client = dataplex_v1.CatalogServiceClient()
    bq_client = bigquery.Client(project=PROJECT_ID) # Initialize BigQuery client

    target_entry_names: list[str] = []
    if TABLE_NAMES:
        for table_name in TABLE_NAMES:
            entry_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/@bigquery/entries/bigquery.googleapis.com%2Fprojects%2F{PROJECT_ID}%2Fdatasets%2F{DATASET_NAME}%2Ftables%2F{table_name}"
            target_entry_names.append(entry_name)
    else:
        try:
            search_request = dataplex_v1.SearchEntriesRequest(name=f"projects/{PROJECT_ID}/locations/global", query=f"name:projects/{PROJECT_ID}/datasets/{DATASET_NAME}/tables/")
            target_entry_names = [entry.dataplex_entry.name for entry in dataplex_client.search_entries(request=search_request)]
        except Exception:
            logger.error(f"Error listing Dataplex entries", exc_info=True)

    for entry_name in target_entry_names:
        try:
            get_request = dataplex_v1.GetEntryRequest(name=entry_name, view=dataplex_v1.EntryView.ALL)
            entry = dataplex_client.get_entry(request=get_request)
            
            aspects_data = {k: convert_proto_to_dict(v.data) for k, v in entry.aspects.items() if hasattr(v, 'data') and v.data}
            
            # --- FINAL FIX: Correctly parse the table name from the full resource string ---
            short_table_name = entry_name.split('/')[-1]

            # --- HYBRID FIX: Get description directly from BigQuery ---
            table_description = ''
            try:
                full_bq_table_id = f"{PROJECT_ID}.{DATASET_NAME}.{short_table_name}"
                bq_table = bq_client.get_table(full_bq_table_id)
                table_description = bq_table.description or ''
                logger.info(f"Successfully fetched description for '{short_table_name}' from BigQuery.")
            except Exception:
                logger.warning(f"Could not fetch description for '{short_table_name}' from BigQuery.", exc_info=True)


            logger.info(f"Found aspects for table '{short_table_name}': {list(aspects_data.keys())}")

            all_entry_metadata.append({
                'table_name': short_table_name,
                'description': table_description, # Use the description from BigQuery
                'aspects': aspects_data
            })
            
        except Exception:
            logger.error(f"Error processing Dataplex entry {entry_name}", exc_info=True)
            continue

    duration = time.time() - start_time
    logger.info(f"--- Successfully fetched {len(all_entry_metadata)} entry metadata sets (Duration: {duration:.2f} seconds) ---")
    return all_entry_metadata

def log_startup_kpis(metadata: list[dict], profiles: list[dict], token_count: int, load_time: float):
    """
    Calculates and logs a summary of KPIs after the initial data fetch.
    """
    if metadata:
        first_table_metadata = pprint.pformat(metadata[0])
        #logger.debug(f"\n--- Raw Metadata for First Table ---\n{first_table_metadata}\n---------------------------------")

    num_tables = len(metadata)
    total_columns = 0
    tables_with_desc = 0
    cols_with_desc = 0
    cols_without_desc_samples = []

    for table_meta in metadata:
        schema_aspect = {}
        for key, value in table_meta.get('aspects', {}).items():
            if key.endswith('.schema'):
                schema_aspect = value
                break
        
        # This check will now work correctly with the description from BigQuery
        if table_meta.get('description'):
            tables_with_desc += 1
        
        schema_cols = schema_aspect.get('fields', [])
        total_columns += len(schema_cols)
        for col in schema_cols:
            if col.get('description'):
                cols_with_desc += 1
            elif len(cols_without_desc_samples) < 5:
                if col.get('name'):
                    cols_without_desc_samples.append(f"{table_meta.get('table_name', 'UNKNOWN')}.{col['name']}")

    num_profiles = len(profiles)

    tables_desc_percent = f"({tables_with_desc/num_tables:.0%})" if num_tables > 0 else "(N/A)"
    cols_desc_percent = f"({cols_with_desc/total_columns:.0%})" if total_columns > 0 else "(N/A)"

    log_summary = f"""
    \n======================================================================
    ==           Application Load Log Summary KPIs                      ==
    ======================================================================
    [Performance]
      - Total Application Load Time: {load_time:.2f} seconds
      - Initial Prompt Token Count: {token_count:,} tokens

    [Table Metadata]
      - Tables Found: {num_tables}
      - Total Columns Found: {total_columns}
      - Tables with Descriptions: {tables_with_desc} / {num_tables} {tables_desc_percent}
      - Columns with Descriptions: {cols_with_desc} / {total_columns} {cols_desc_percent}
      - Columns Missing Description (Sample): {cols_without_desc_samples if cols_without_desc_samples else 'None'}

    [Data Profiles]
      - Column Profiles Fetched: {num_profiles} (out of {total_columns} total columns)

    ======================================================================
    """
    logger.info(log_summary)

