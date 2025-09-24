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

import os
import datetime
import logging
import json
import yaml
import time
import tempfile
import google.generativeai as genai

# GCP Imports
from google.cloud import storage

# Import your project's modules
from .utils import (
    fetch_table_entry_metadata,
    fetch_bigquery_data_profiles,
    fetch_sample_data_for_tables,
    log_startup_kpis
)
from .constants import MODEL, GCS_BUCKET_FOR_DEBUGGING

logger = logging.getLogger(__name__)

def json_serial_default(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def _log_prompt_for_debugging(prompt_content: str):
    """Logs the entire prompt as a single, structured JSON payload for Cloud Logging."""
    try:
        log_payload = {
            "severity": "INFO",
            "message": "Complete agent instructions for debugging. Expand the jsonPayload to view.",
            "full_prompt": prompt_content
        }
        print(json.dumps(log_payload))
    except Exception as e:
        logger.warning(f"Could not create or log the structured debug prompt: {e}")

def _save_instructions_for_debugging(prompt_content: str):
    """
    Saves the final generated prompt to a file for easier debugging.

    - If running in Cloud Run (K_SERVICE env var is set), it saves to a GCS bucket.
    - Otherwise, it saves to the local system's temporary directory.
    - All operations are wrapped in a try/except block to prevent crashes.
    """
    try:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"prompt_{timestamp}.txt"
        
        # Check if running in a Google Cloud Run environment
        is_cloud_run = os.environ.get('K_SERVICE')

        if is_cloud_run:
            # Save to GCS
            client = storage.Client()
            bucket = client.bucket(GCS_BUCKET_FOR_DEBUGGING)
            blob = bucket.blob(filename)
            blob.upload_from_string(prompt_content)
            logger.info(f"Successfully saved full prompt to GCS: gs://{GCS_BUCKET_FOR_DEBUGGING}/{filename}")
        else:
            # Save to local temporary directory
            temp_dir = tempfile.gettempdir()
            file_path = os.path.join(temp_dir, filename)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(prompt_content)
            logger.info(f"Running locally. Saved full prompt to temporary file: {file_path}")

    except Exception as e:
        # Log the error but do not raise it, so the application can continue.
        logger.warning(f"Could not save prompt for debugging. This will not affect the application's functionality. Error: {e}")

def _build_master_instructions() -> str:
    """
    (Internal Helper) Fetches, formats, and combines all context for the agent.
    This function runs only once at server startup and logs KPIs.
    """
    app_start_time = time.time()
    logger.info("Building master agent instructions... (This should only run once!)")
    
    # 1. Fetch all dynamic data from utils
    table_metadata = fetch_table_entry_metadata()
    data_profiles = fetch_bigquery_data_profiles()
    samples = []
    if not data_profiles:
        logger.info("Data profiles not found. Fetching sample data as a fallback.")
        samples = fetch_sample_data_for_tables()

    # 2. Format data into strings for the prompt
    table_metadata_str = json.dumps(table_metadata, indent=2, default=json_serial_default)
    data_profiles_str = json.dumps(data_profiles, indent=2, default=json_serial_default)
    samples_str = json.dumps(samples, indent=2, default=json_serial_default)
    
    # 3. Load the static instruction template from the YAML file
    script_dir = os.path.dirname(__file__)
    yaml_file_path = os.path.join(script_dir, 'instructions.yaml')
    with open(yaml_file_path, 'r', encoding='utf-8') as f:
        instructions_yaml = yaml.safe_load(f)
    
    instruction_template = "\n---\n".join(instructions_yaml.values())

    # 4. Inject dynamic data into the final prompt
    final_prompt = instruction_template.format(
        table_metadata=table_metadata_str,
        data_profiles=data_profiles_str,
        samples=samples_str
    )
    
    # 5. Log and save the final prompt for debugging purposes
    logger.info("\n--- START: FINAL POPULATED AGENT INSTRUCTIONS (DEBUG VIEW) ---\n\n")
    _log_prompt_for_debugging(final_prompt)
    logger.info("---\n\n END: FINAL POPULATED AGENT INSTRUCTIONS (DEBUG VIEW) ---\n")
    
    # --- NEW: Save the instructions to a file ---
    _save_instructions_for_debugging(final_prompt)
    
    # --- KPI Calculation and Logging ---
    try:
        model_for_token_count = genai.GenerativeModel(MODEL)
        token_count = model_for_token_count.count_tokens(final_prompt).total_tokens
    except Exception as e:
        token_count = 0
        logging.warning(f"Could not calculate token count: {e}")
    total_load_time = time.time() - app_start_time
    
    log_startup_kpis(
        metadata=table_metadata, 
        profiles=data_profiles,
        token_count=token_count,
        load_time=total_load_time
    )
    # --- End KPI Logic ---

    logger.info(f"[AGENT_INSTRUCTIONS] Caching complete. Final prompt length: {len(final_prompt)} characters.")
    return final_prompt

# This variable is populated only ONCE when the module is first imported.
CACHED_INSTRUCTIONS = _build_master_instructions()

def return_instructions_bigquery() -> str:
    """Returns the pre-cached master instructions instantly from a module-level variable."""
    logger.info(f"Returning CACHED_INSTRUCTIONS. Length: {len(CACHED_INSTRUCTIONS)} characters.")
    logger.debug("[AGENT_INSTRUCTIONS] CACHED_INSTRUCTIONS requested.")
    return CACHED_INSTRUCTIONS
