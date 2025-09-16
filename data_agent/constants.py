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

MODEL="gemini-2.5-pro-preview-03-25" # Identifier for the specific generative model to be used by the agent. (e.g., "gemini-2.5-pro-preview-03-25")
# MODEL="gemini-2.5-flash-preview-04-17"
PROJECT_ID="agentic-data" # The Google Cloud Project ID that contains the BigQuery datasets and tables to be analyzed. (e.g., "my-gcp-project-123")
LOCATION="asia-south1" # The geographical location of the BigQuery datasets and tables to be analyzed (e.g., "US", "asia-northeast3")
DATASET_NAME="thelook_manufacturing" # The target BigQuery dataset name to be analyzed or for which data profiles are fetched. (e.g., "sales_data")
TABLE_NAMES=[] # Optional list of specific table names within DATASET_NAME. If empty, operations might apply to all tables in the dataset. (e.g., ["orders", "customers"] or [])
DATA_PROFILES_TABLE_FULL_ID="" # Optional: Full BigQuery table ID where data profiling results are stored. Set to None or an empty string if not used. (e.g., "my_project.profiling_dataset.all_profiles", None, "")

