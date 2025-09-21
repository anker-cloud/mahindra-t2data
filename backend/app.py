import os
import logging
import sys
import functools
import time
import sqlalchemy
from flask import Flask, send_from_directory, abort, jsonify, request, current_app
from dotenv import load_dotenv
import uuid

# --- Configure Logging to DEBUG Level ---
# This is set to DEBUG to ensure all detailed logs are captured.
logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Import other modules after logging is set up ---
from backend.utils import get_table_description, get_table_ddl_strings, get_total_rows, get_total_column_count, fetch_sample_data_for_single_table
try:
    from data_agent.agent import root_agent
    from google.adk.runners import Runner
    from google.adk.sessions.database_session_service import DatabaseSessionService
    from google.adk.sessions.in_memory_session_service import InMemorySessionService
    from google.genai import types as genai_types
except ImportError as e:
    logging.critical(f"A critical module could not be imported. The app cannot start. Error: {e}")
    root_agent = Runner = DatabaseSessionService = InMemorySessionService = genai_types = None

# Load environment variables from a .env file if it exists
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

def create_app():
    """Application Factory Function"""
    app = Flask(__name__, static_folder='../frontend/build', static_url_path='/')

    # --- RESTORED: Caching Decorator ---
    def cache(timeout=3600):
        def decorator(f):
            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                cache_key = request.url
                if cache_key in wrapper.cache:
                    result, timestamp = wrapper.cache[cache_key]
                    if (time.time() - timestamp) < timeout:
                        logging.debug(f"Returning cached result for {cache_key}")
                        return result
                result = f(*args, **kwargs)
                wrapper.cache[cache_key] = (result, time.time())
                return result
            wrapper.cache = {}
            return wrapper
        return decorator

    # --- Initialization Logic Inside the Factory ---
    APP_NAME = "data_agent_chatbot"
    if all([Runner, InMemorySessionService, root_agent]):
        try:
            db_url = "sqlite:///./my_agent_data.db"
            engine = sqlalchemy.create_engine(db_url)
            engine.connect()
            logging.info("Database connection successful.")
            session_service = DatabaseSessionService(db_url=db_url)
        except Exception as e:
            logging.warning(f"Failed to connect to the database, falling back to in-memory session: {e}")
            session_service = InMemorySessionService()

        try:
            runner = Runner(
                app_name=APP_NAME,
                agent=root_agent,
                session_service=session_service,
            )
            app.runner = runner
            app.session_service = session_service
            app.genai_types = genai_types
            logging.info("ADK Runner initialized successfully and attached to app.")
        except Exception as e:
            logging.critical(f"FATAL: Could not initialize ADK Runner: {e}", exc_info=True)
            app.runner = None
    else:
        logging.critical("ADK Runner could not be initialized due to missing components.")
        app.runner = None

    frontend_build_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'frontend', 'build'))
    if not os.path.isdir(frontend_build_path):
        logging.warning(f"React build directory not found at {frontend_build_path}.")
    app.config['FRONTEND_BUILD_DIR'] = frontend_build_path

    # --- API Routes ---

    @app.route("/api/login", methods=["POST"])
    def login():
        """Creates a new session for a user and returns the session ID."""
        session_service = current_app.session_service
        runner = current_app.runner
        if not all([session_service, runner]):
            return jsonify({"error": "Session service not initialized."}), 500
        
        req_data = request.get_json()
        user_id = req_data.get('user_id')
        if not user_id:
            return jsonify({"error": "user_id is required"}), 400

        try:
            session = session_service.create_session(app_name=runner.app_name, user_id=user_id)
            logging.info(f"New session created for user '{user_id}' with session_id: {session.id}")
            return jsonify({"session_id": session.id, "user_id": user_id}), 200
        except Exception as e:
            logging.error(f"Failed to create session for user '{user_id}': {e}", exc_info=True)
            return jsonify({"error": "Could not create session."}), 500

    @app.route("/api/logout", methods=["POST"])
    def logout():
        """Handles user logout."""
        req_data = request.get_json()
        user_id = req_data.get('user_id')
        session_id = req_data.get('session_id')
        logging.info(f"User '{user_id}' logged out of session '{session_id}'.")
        return jsonify({"message": "Logout successful"}), 200

    @app.route("/api/chat", methods=["POST"])
    async def chat_handler():
        """Handles a chat turn with the ADK agent."""
        runner = current_app.runner
        session_service = current_app.session_service
        genai_types = current_app.genai_types
        if not all([runner, session_service, genai_types]):
            return jsonify({"error": "Chat components not initialized on the server."}), 500
        
        session_id = None
        try:
            req_data = request.get_json()
            logging.debug(f"[BACKEND] Received raw request on /api/chat: {req_data}")
            user_id = req_data.get('user_id')
            session_id = req_data.get('session_id')
            message_text = req_data.get('message', {}).get('message')
            if not all([user_id, session_id, message_text]):
                return jsonify({"error": "user_id, session_id, and message are required"}), 400
            
            final_response_parts = []
            async for event in runner.run_async(
                user_id=user_id,
                session_id=session_id,
                new_message=genai_types.Content(parts=[genai_types.Part(text=message_text)], role='user')
            ):
                logging.debug(f"[AGENT_EVENT] Received event: {event}")
                if event.error_code == 'MALFORMED_FUNCTION_CALL':
                    final_response_parts.append({
                        "role": "assistant", 
                        "content": "I'm sorry, I encountered a technical issue while processing your request. Please try rephrasing it."
                    })
                    break 
                if hasattr(event, 'content') and event.content and hasattr(event.content, 'parts'):
                    for part in event.content.parts:
                        if hasattr(part, 'text') and part.text:
                            final_response_parts.append({"role": event.content.role or "model", "content": part.text})
            
            return jsonify({"session_id": session_id, "messages": final_response_parts}), 200

        except Exception as e:
            logging.error(f"Error during chat processing: {str(e)}", exc_info=True)
            return jsonify({"session_id": session_id or "", "messages": [], "error": f"Internal server error: {str(e)}"}), 500

    @app.route("/api/tables", methods=["GET"])
    @cache(timeout=3600)
    def list_tables():
        """Returns a list of tables and their aggregate stats from BigQuery."""
        try:
            tables = get_table_ddl_strings()
            
            # --- This is the restored logic that calculates the totals ---
            num_tables = len(tables)
            total_rows = 0
            table_names = []
            for table in tables:
                table_name = table["table_name"]
                table_names.append(table_name)
                # This helper function calculates rows per table
                total_rows += get_total_rows(table_name)

            # This helper function calculates the total columns
            total_columns = get_total_column_count()

            return jsonify({
                "tables": table_names,
                "num_tables": num_tables,
                "total_columns": total_columns,
                "total_rows": total_rows
            }), 200
            # ----------------------------------------------------------------

        except Exception as e:
            logging.error(f"Error listing tables: {str(e)}", exc_info=True)
            return jsonify({"error": f"Internal server error: {str(e)}"}), 500
    

    @app.route("/api/test_query", methods=["GET"])
    async def test_query():
        """
        An unauthenticated endpoint for internal testing.
        Takes a user_id and question, runs a stateless chat turn,
        and returns the generated SQL, a clarification question, or an error.
        URL - http://127.0.0.1:8080/api/test_query?user_id=internal_tester&question=What were the top 5 vehicle models by part consumption last month
        """
        user_id = request.args.get("user_id")
        question = request.args.get("question")

        if not all([user_id, question]):
            return jsonify({"error": "Both 'user_id' and 'question' URL parameters are required."}), 400

        runner = current_app.runner
        genai_types = current_app.genai_types
        session_service = current_app.session_service

        if not all([runner, genai_types, session_service]):
            return jsonify({"error": "Chat components not initialized on the server."}), 500

        generated_sql = None
        agent_error = None
        llm_response = "" # --- NEW: Variable to capture text responses ---
        try:
            temp_session = session_service.create_session(app_name=runner.app_name, user_id=user_id)
            logging.debug(f"[TEST_ENDPOINT] Created temporary session_id: {temp_session.id}")
            
            new_message = genai_types.Content(parts=[genai_types.Part(text=question)], role='user')

            async for event in runner.run_async(
                user_id=user_id,
                session_id=temp_session.id,
                new_message=new_message
            ):
                logging.debug(f"[TEST_ENDPOINT] Received event: {event}")
                
                if event.error_code:
                    agent_error = { "code": event.error_code, "message": event.error_message }
                    break

                if hasattr(event, 'content') and event.content:
                    for part in event.content.parts:
                        # Capture the SQL if a tool call is present
                        if hasattr(part, 'function_call') and part.function_call and part.function_call.name == 'execute_bigquery_query':
                           raw_sql = part.function_call.args.get('sql_query')
                           if raw_sql:
                               generated_sql = ' '.join(line.strip() for line in raw_sql.splitlines())
                        
                        # --- NEW: Always capture any text content ---
                        if hasattr(part, 'text') and part.text:
                            llm_response += part.text

                if generated_sql or agent_error:
                    break
            
            if agent_error:
                return jsonify({ "status": "AgentError", "error": agent_error }), 400
            
            # --- NEW: Check if the LLM asked a question instead of generating SQL ---
            if not generated_sql and llm_response:
                return jsonify({
                    "status": "ClarificationNeeded",
                    "user_id": user_id,
                    "question": question,
                    "clarification_question": llm_response.strip()
                }), 200

            return jsonify({
                "status": "Success",
                "user_id": user_id,
                "question": question,
                "generated_sql": generated_sql or "No SQL was generated for this query."
            }), 200

        except Exception as e:
            logging.error(f"Error in test_query endpoint: {e}", exc_info=True)
            return jsonify({"error": f"An internal server error occurred: {str(e)}"}), 500
        
    @app.route("/api/table_data", methods=["GET"])
    @cache(timeout=3600)
    def get_table_data():
        """Returns sample data and a description for a specific table."""
        table_name = request.args.get("table_name")
        if not table_name:
            return jsonify({"error": "Table name is required"}), 400
        try:
            sample_rows = fetch_sample_data_for_single_table(table_name=table_name)
            description = get_table_description(table_name)
            return jsonify({"data": sample_rows, "description": description}), 200
        except Exception as e:
            logging.error(f"Error getting table data for {table_name}: {str(e)}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500

    @app.route("/api/code", methods=["GET"])
    def get_code_file():
        filepath = request.args.get("filepath")
        if not filepath:
            return jsonify({"error": "Filepath is required"}), 400
        if not filepath.startswith("data_agent/"):
            return jsonify({"error": "Invalid filepath"}), 400
        
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        abs_filepath = os.path.normpath(os.path.join(project_root, filepath))
        if not abs_filepath.startswith(project_root):
            return jsonify({"error": "Invalid filepath"}), 400
        try:
            with open(abs_filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            return jsonify({"content": content}), 200
        except FileNotFoundError:
            return jsonify({"error": "File not found"}), 404
        except Exception as e:
            logging.error(f"Error reading code file {filepath}: {str(e)}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500

    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def serve_react_app(path):
        """Serves the React app and its assets."""
        build_dir = current_app.config.get('FRONTEND_BUILD_DIR')
        if not build_dir:
            return abort(404, description="React application build directory not found.")
        
        if path != "" and os.path.exists(os.path.join(build_dir, path)):
            return send_from_directory(build_dir, path)
        else:
            return send_from_directory(build_dir, 'index.html')

    return app

app = create_app()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    debug_mode = os.environ.get('FLASK_DEBUG', 'True').lower() in ['true', '1', 't']
    logging.info(f"Starting Flask development server on http://0.0.0.0:{port} (debug={debug_mode})...")
    app.run(debug=debug_mode, host='0.0.0.0', port=port)