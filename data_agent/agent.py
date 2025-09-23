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

import logging
import os
from google.adk.models.google_llm import Gemini
from google.adk.agents import Agent
from .constants import MODEL
from .custom_tools import execute_bigquery_query
from .instructions import return_instructions_bigquery
from dotenv import load_dotenv

load_dotenv()

logging.info(f"[AGENT_INIT] Attempting to initialize agent with MODEL constant: '{MODEL}'")

# --- Configuration Section ---
USE_TUNED_CONFIG = True
tuned_generation_config = {
    "temperature": 0.1,
    "top_p": 0.95,
    "top_k": 40,
    "max_output_tokens": 4096
}

# --- Dynamic Authentication and Initialization ---
api_key = os.getenv("GOOGLE_API_KEY")
project_id = os.getenv("PROJECT_ID")
location = os.getenv("LOCATION")

llm_init_kwargs = { "model_name": MODEL }

# If an API key is provided, use the Google AI (Generative Language) API.
if api_key:
    logging.info("[AGENT_INIT] Found GOOGLE_API_KEY. Initializing for Google AI API.")
    llm_init_kwargs["api_key"] = api_key
# If no API key, assume Application Default Credentials (ADC) and use Vertex AI.
elif project_id and location:
    logging.info(f"[AGENT_INIT] No API Key found. Initializing for Vertex AI with Project: '{project_id}' and Location: '{location}'.")
    # +++ FINAL FIX: Explicitly tell the client to use the Vertex AI API +++
    llm_init_kwargs["vertexai"] = True
    llm_init_kwargs["project"] = project_id
    llm_init_kwargs["location"] = location
else:
    raise ValueError("Missing authentication. Please provide either GOOGLE_API_KEY or both PROJECT_ID and LOCATION in your .env file.")

if USE_TUNED_CONFIG:
    logging.info("AGENT_CONFIG: Using TUNED generation configuration.")
    llm_init_kwargs["generation_config"] = tuned_generation_config
else:
    logging.info("AGENT_CONFIG: Using DEFAULT generation configuration.")

# Instantiate the final model object
llm = Gemini(**llm_init_kwargs)

# --- Agent Definition ---
root_agent = Agent(
    model=llm,
    name="Data_Agent",
    description="Converts natural language questions about provided BigQuery data into executable BigQuery SQL queries and runs them.",
    instruction=return_instructions_bigquery(),
    tools=[execute_bigquery_query]
)

