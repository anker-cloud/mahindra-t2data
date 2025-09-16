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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)



def json_serial_default(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    # Add other custom serializers here if needed, e.g., for Decimal
    # elif isinstance(obj, decimal.Decimal):
    #     return float(obj)
    raise TypeError (f"Type {type(obj)} not serializable")

def return_instructions_bigquery() -> str:
    """
    Fetches table metadata, data profiles (conditionally sample data), formats them,
    and injects them into the main instruction template.
    """
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
                # Use json_serial_default to handle date/datetime objects
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
        
        # fetch_sample_data_for_tables uses constants (PROJECT_ID, DATASET_NAME, TABLE_NAMES) internally
        sample_data_raw = fetch_sample_data_for_tables(num_rows=3) 
        
        # --- Formatting logic for sample data ---
        if sample_data_raw:
            logger.info(f"Sample data fetched ({len(sample_data_raw)} tables). Formatting for prompt.")
            formatted_samples = []
            for item in sample_data_raw:
                try:
                    # Use json_serial_default to handle date/datetime objects
                    sample_rows_str = json.dumps(item['sample_rows'], indent=2, ensure_ascii=False, default=json_serial_default)
                except TypeError as e: 
                    logger.warning(f"Could not serialize sample_rows for table {item.get('table_name')}: {e}. Sample rows: {item.get('sample_rows')}")
                    sample_rows_str = f"Sample rows for table {item.get('table_name')} contain non-serializable data."

                formatted_samples.append(
                    f"**Sample Data for table `{item['table_name']}` (first {len(item.get('sample_rows',[]))} rows):**\n" # Dynamically show row count
                    f"```json\n{sample_rows_str}\n```"
                )
            samples_string_for_prompt = "\n\n---\n\n".join(formatted_samples)
        else:
            logger.warning(f"Could not fetch sample data for the target scope: {PROJECT_ID}.{DATASET_NAME} (Tables: {TABLE_NAMES if TABLE_NAMES else 'All'}).")
            samples_string_for_prompt = f"Could not fetch sample data for the target scope: {PROJECT_ID}.{DATASET_NAME} (Tables: {TABLE_NAMES if TABLE_NAMES else 'All'})."
      
    # 3. Format the final instruction string
    # Load instruction template sections from YAML
    script_dir = os.path.dirname(os.path.abspath(__file__))
    yaml_file_path = os.path.join(script_dir, 'instructions.yaml')
    try:
        with open(yaml_file_path, 'r') as f: #use absolute path 
            instructions_yaml = yaml.safe_load(f)
            overall_workflow = instructions_yaml.get('overall_workflow', '')
            bigquery_data_schema_and_context = instructions_yaml.get('bigquery_data_schema_and_context', '')
            table_schema_and_join_information = instructions_yaml.get('table_schema_and_join_information', '')
            critical_joining_logic_and_context = instructions_yaml.get('critical_joining_logic_and_context', '')
            data_profile_information = instructions_yaml.get('data_profile_information', '')
            sample_data = instructions_yaml.get('sample_data', '')
            usecase_specific_table_information = instructions_yaml.get('usecase_specific_table_information', '')
            few_shot_examples = instructions_yaml.get('few_shot_examples', '')

            # Concatenate sections in the desired order
            instruction_template_from_yaml = "\n".join([
                overall_workflow,
                bigquery_data_schema_and_context,
                table_schema_and_join_information,
                critical_joining_logic_and_context,
                data_profile_information,
                sample_data,
                usecase_specific_table_information,
                few_shot_examples
            ])

            if not instruction_template_from_yaml.strip():
                 logger.error("Instruction template loaded from YAML is empty.")
                 raise ValueError("Instruction template loaded from YAML is empty.")

    except FileNotFoundError:
        logger.error("instructions.yaml not found.")
        raise FileNotFoundError("instructions.yaml not found.")
    except yaml.YAMLError as e:
        logger.error(f"Error loading instructions.yaml: {e}")
        raise yaml.YAMLError(f"Error loading instructions.yaml: {e}")

    # 3. Format the final instruction string
    final_instruction = instruction_template_from_yaml.format(
        table_metadata=table_metadata_string_for_prompt,
        data_profiles=data_profiles_string_for_prompt,
        samples=samples_string_for_prompt
    )
    
    return final_instruction