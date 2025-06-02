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

from google.adk.agents import Agent
from .constants import MODEL
from .custom_tools import execute_bigquery_query
from .instructions import return_instructions_bigquery
from dotenv import load_dotenv


load_dotenv('env')

root_agent = Agent(
    model=MODEL,
    name="Data_Agent",
    description="Converts natural language questions about provided BigQuery data into executable BigQuery SQL queries and runs them.",
    instruction=return_instructions_bigquery(),
    tools=[execute_bigquery_query]
) 