import logging
import time
from google.cloud import bigquery

# It's good practice to get the logger at the module level
logger = logging.getLogger(__name__)

def execute_bigquery_query(sql_query: str) -> str:
    """
    Executes a read-only (SELECT) GoogleSQL query on BigQuery and returns the result.

    This tool is designed to be called by an AI agent. It returns data formatted
    as a Markdown string for easy interpretation by the LLM. It also handles
    cases where a query runs successfully but returns no data.

    Args:
        sql_query (str): The GoogleSQL query string to be executed. This must
                         be a valid and complete SQL statement.

    Returns:
        str: A string containing the query results in a Markdown table format,
             a message indicating no results were found, or a detailed error message.
    """
    logger.info("--- Starting BigQuery query execution ---")
    start_time = time.time()
    
    # Log the exact query the LLM is attempting to run
    logger.info(f"[AGENT_TOOL] Executing LLM-generated query:\n---\n{sql_query}\n---")

    try:
        client = bigquery.Client()
        logger.info("BigQuery client created successfully.")

        query_job = client.query(sql_query)
        results = query_job.result()  # Waits for the job to complete.

        if results.total_rows > 0:
            logger.info(f"[AGENT_TOOL] Query successful. Fetched {results.total_rows} rows.")
            df = results.to_dataframe()
            
            # Return results as a Markdown string for easy processing
            return df.to_markdown(index=False, tablefmt="pipe")
        else:
            # This clear message prevents the LLM from getting confused by an empty result
            logger.info("[AGENT_TOOL] Query successful but returned no results.")
            return "The query executed successfully but returned no matching data."

    except Exception as e:
        logger.error(
            "--- BigQuery query execution failed ---",
            exc_info=True # Provides the full traceback in your logs for debugging
        )
        # This provides a clear error message back to the LLM
        return f"An error occurred while executing the BigQuery query: {str(e)}"
    
    finally:
        duration = time.time() - start_time
        logger.info(f"--- BigQuery query execution finished (Duration: {duration:.2f} seconds) ---")