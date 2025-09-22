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
import google.generativeai as genai

# Import your project's modules
from .utils import (
    fetch_table_entry_metadata,
    fetch_bigquery_data_profiles,
    fetch_sample_data_for_tables,
    log_startup_kpis
)
from .constants import MODEL

logger = logging.getLogger(__name__)

def json_serial_default(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def _log_prompt_for_debugging(prompt_content: str):
    """
    (NEW) Logs the entire prompt as a single, structured JSON payload.
    This allows for easy viewing and copying from the Cloud Logging UI.
    """
    try:
        # Create a dictionary payload. This is the standard for structured logging.
        log_payload = {
            "severity": "INFO",
            "message": "Complete agent instructions for debugging. Expand the jsonPayload to view.",
            "full_prompt": prompt_content
        }
        # Print the dictionary as a single-line JSON string.
        # Cloud Logging will automatically parse this.
        print(json.dumps(log_payload))

    except Exception as e:
        logger.warning(f"Could not create or log the structured debug prompt: {e}")


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
    
    # 5. Check if in debug mode and log the final prompt as a structured JSON object
    if os.getenv('FLASK_DEBUG') == '1' or os.getenv('DEBUG'):
        _log_prompt_for_debugging(final_prompt)

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
    logger.debug("[AGENT_INSTRUCTIONS] CACHED_INSTRUCTIONS requested.")
    return CACHED_INSTRUCTIONS

