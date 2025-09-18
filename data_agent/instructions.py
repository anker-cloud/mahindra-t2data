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
import logging, json, yaml
from .custom_tools import execute_bigquery_query
from .utils import fetch_bigquery_data_profiles, fetch_sample_data_for_tables, fetch_table_entry_metadata
from .constants import PROJECT_ID, DATASET_NAME, TABLE_NAMES

# --- Logging Configuration ---
# Set to DEBUG to see the detailed context logs for metadata, profiles, etc.
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)



def json_serial_default(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError (f"Type {type(obj)} not serializable")


# --- ORIGINAL FUNCTION (Commented out for caching refactor) ---
# The logic below was moved into the `_build_master_instructions` helper function.
# This original function was inefficient because it would re-fetch and re-format
# all the database schema and profile data on every single user message.
"""
def return_instructions_bigquery() -> str:
    \"\"\"
    Fetches table metadata, data profiles (conditionally sample data), formats them,
    and injects them into the main instruction template.
    \"\"\"
    # ... (original function code) ...
"""


# --- REFACTORED CODE FOR CACHING (Executes only once on server start) ---

def _build_master_instructions() -> str:
    """
    (Internal Helper) Fetches and formats all the static context data for the agent.
    This expensive function is designed to run only ONCE when the server starts.
    """
    logger.info("Building master agent instructions... (This should only run once!)")
    
    # 1. Fetch Table Metadata
    table_metadata_raw = fetch_table_entry_metadata()
    
    # --- Formatting logic for table metadata ---
    if not table_metadata_raw:
        table_metadata_string_for_prompt = "Table metadata information is not available."
    else:
        formatted_metadata = []
        for metadata in table_metadata_raw:
            try:
                metadata_str = json.dumps(metadata, indent=2, ensure_ascii=False, default=json_serial_default)
                formatted_metadata.append(f"**Table Entry Metadata:**\n```json\n{metadata_str}\n```")
            except TypeError as e:
                logger.warning(f"Could not serialize table metadata: {e}")
                formatted_metadata.append("Table metadata contains non-serializable data.")
        table_metadata_string_for_prompt = "\n\n---\n\n".join(formatted_metadata)

    # 2. Fetch Data Profiles
    data_profiles_raw = fetch_bigquery_data_profiles()

    data_profiles_string_for_prompt = ""
    samples_string_for_prompt = ""

    if data_profiles_raw: # If data profiles are available
        logger.info(f"Data profiles found ({len(data_profiles_raw)} entries). Formatting for prompt.")
        # --- Formatting logic for data profiles ---
        formatted_profiles = []
        for profile in data_profiles_raw:
            try:
                profile_str = json.dumps(profile, indent=2, ensure_ascii=False, default=json_serial_default)
            except TypeError as e: 
                logger.warning(f"Could not serialize profile part: {e}. Profile: {profile}")
                profile_str = f"Profile for column '{profile.get('source_column_name', profile.get('column_name'))}' in table '{profile.get('source_table_id')}' contains non-serializable data."

            column_key = profile.get('source_column_name', profile.get('column_name'))
            table_key = profile.get('source_table_id')
            formatted_profiles.append(f"Data profile for column '{column_key}' in table '{table_key}':\n{profile_str}")
        
        data_profiles_string_for_prompt = "\n\n---\n\n".join(formatted_profiles) if formatted_profiles else "Data profiles were processed but no displayable content was generated."
        
        samples_string_for_prompt = "Full data profiles are provided; sample data section is omitted for brevity in this context. If needed, sample data can be fetched for specific tables based on constants."
    else: # If data profiles are not available, fetch sample data based on constants
        logger.info("Data profiles not found. Attempting to fetch sample data based on constants...")
        data_profiles_string_for_prompt = "Data profile information is not available. Please refer to the sample data below."
        
        sample_data_raw = fetch_sample_data_for_tables(num_rows=3) 
        
        # --- Formatting logic for sample data ---
        if sample_data_raw:
            logger.info(f"Sample data fetched ({len(sample_data_raw)} tables). Formatting for prompt.")
            formatted_samples = []
            for item in sample_data_raw:
                try:
                    sample_rows_str = json.dumps(item['sample_rows'], indent=2, ensure_ascii=False, default=json_serial_default)
                except TypeError as e: 
                    logger.warning(f"Could not serialize sample_rows for table {item.get('table_name')}: {e}. Sample rows: {item.get('sample_rows')}")
                    sample_rows_str = f"Sample rows for table {item.get('table_name')} contain non-serializable data."

                formatted_samples.append(
                    f"**Sample Data for table `{item['table_name']}` (first {len(item.get('sample_rows',[]))} rows):**\n"
                    f"```json\n{sample_rows_str}\n```"
                )
            samples_string_for_prompt = "\n\n---\n\n".join(formatted_samples)
        else:
            logger.warning(f"Could not fetch sample data for the target scope: {PROJECT_ID}.{DATASET_NAME} (Tables: {TABLE_NAMES if TABLE_NAMES else 'All'}).")
            samples_string_for_prompt = f"Could not fetch sample data for the target scope: {PROJECT_ID}.{DATASET_NAME} (Tables: {TABLE_NAMES if TABLE_NAMES else 'All'})."
    
    # 3. Format the final instruction string
    script_dir = os.path.dirname(os.path.abspath(__file__))
    yaml_file_path = os.path.join(script_dir, 'instructions.yaml')
    try:
        with open(yaml_file_path, 'r') as f:
            instructions_yaml = yaml.safe_load(f)
            instruction_template_from_yaml = "\n".join([
                instructions_yaml.get('overall_workflow', ''),
                instructions_yaml.get('bigquery_data_schema_and_context', ''),
                instructions_yaml.get('table_schema_and_join_information', ''),
                instructions_yaml.get('critical_joining_logic_and_context', ''),
                instructions_yaml.get('data_profile_information', ''),
                instructions_yaml.get('sample_data', ''),
                instructions_yaml.get('usecase_specific_table_information', ''),
                instructions_yaml.get('few_shot_examples', '')
            ])
            if not instruction_template_from_yaml.strip():
                raise ValueError("Instruction template loaded from YAML is empty.")
    except Exception as e:
        logger.error(f"Error loading or processing instructions.yaml: {e}")
        raise

    # --- NEW DEBUG LOGS ---
    # These logs will show the exact context being fed into the prompt template.
    # They are set to DEBUG to avoid cluttering the main INFO logs.
    logger.debug(
        f"Formatted Table Metadata for prompt:\n---\n{table_metadata_string_for_prompt}\n---"
    )
    logger.debug(
        f"Formatted Data Profiles for prompt:\n---\n{data_profiles_string_for_prompt}\n---"
    )
    logger.debug(
        f"Formatted Sample Data for prompt:\n---\n{samples_string_for_prompt}\n---"
    )

    final_instruction = instruction_template_from_yaml.format(
        table_metadata=table_metadata_string_for_prompt,
        data_profiles=data_profiles_string_for_prompt,
        samples=samples_string_for_prompt
    )
    
    logger.info(
        f"[AGENT_INSTRUCTIONS] Caching complete. Final prompt length: {len(final_instruction)} characters."
    )
    return final_instruction

# This variable is populated only ONCE when the Python module is first imported.
CACHED_INSTRUCTIONS = _build_master_instructions()

def return_instructions_bigquery() -> str:
    """
    Returns the pre-cached master instructions instantly from a module-level variable.
    """
    return CACHED_INSTRUCTIONS