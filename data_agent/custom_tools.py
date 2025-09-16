# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
from google.cloud import bigquery, dataplex_v1
from google.cloud.bigquery.table import TableReference 
from .constants import PROJECT_ID, DATASET_NAME, TABLE_NAMES, DATA_PROFILES_TABLE_FULL_ID, LOCATION
import time
import logging
from proto.marshal.collections.repeated import RepeatedComposite
from proto.marshal.collections.maps import MapComposite
from .utils import fetch_bigquery_data_profiles, fetch_sample_data_for_tables, convert_proto_to_dict, fetch_table_entry_metadata


# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def execute_bigquery_query(sql_query: str) -> list[dict]:
    """
    Executes a given SQL query on Google BigQuery and returns the results as a list of dictionaries.
    Uses the globally (or module-level) defined PROJECT_ID constant.
    Includes detailed logging using the 'logging' module.

    Args:
        sql_query: The SQL query string to execute.

    Returns:
        A list of dictionaries representing the query results.
        Returns an empty list if the query returns no results or if an error occurs.
    """
    logger.info("--- Starting BigQuery query execution ---")
    start_time = time.time()

    try:
        client = bigquery.Client(project=PROJECT_ID)
        logger.info("BigQuery client created successfully.")

        # Execute the query
        logger.info(f"Submitting query to BigQuery...") # Replaced print, added truncation
        query_job = client.query(sql_query)

        # Waits for the query to finish and get results
        results = query_job.result()

        # Convert the results to a list of dictionaries
        logger.info("Processing results...")
        data = [dict(row.items()) for row in results]
        num_rows = len(data)

        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"--- BigQuery query execution successful (Duration: {duration:.2f} seconds) ---")

        return data

    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        # Log the error with traceback information using exc_info=True
        logger.error(
            f"--- BigQuery query execution failed after {duration:.2f} seconds ---",
            exc_info=True # This automatically adds exception info (like traceback)
        )

        return []