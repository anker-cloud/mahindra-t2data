import os
import logging
from flask import Flask, send_from_directory, abort, jsonify, request, current_app
from dotenv import load_dotenv
import functools
import time

# --- NEW: Centralized Logging Configuration ---
# Moved from create_app() and set to DEBUG to see all detailed logs.
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

from backend.utils import get_table_description, get_table_ddl_strings, get_total_rows, get_total_column_count

# Load environment variables from .env file in the project root
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'env')
load_dotenv(dotenv_path)

# Assuming data_agent is in the parent directory and accessible in PYTHONPATH
try:
    from data_agent.agent import root_agent
except ImportError:
    logging.error("Could not import root_agent from data_agent.agent. Ensure data_agent is in PYTHONPATH.")
    root_agent = None

# ADK components
try:
    from google.adk.runners import Runner
    from google.adk.sessions.database_session_service import DatabaseSessionService
    from google.adk.sessions.in_memory_session_service import InMemorySessionService
    from google.genai import types as genai_types
    import sqlalchemy
except ImportError:
    logging.error("Could not import ADK components. Ensure 'google-adk' is installed.")
    Runner = None
    InMemorySessionService = None
    genai_types = None

# Define APP_NAME, USER_ID
APP_NAME = "data_agent_chatbot"
USER_ID = "user_1"

# Initialize ADK services and Runner globally
session_service = None
runner = None
if Runner and InMemorySessionService and root_agent:
    try:
        db_url = "sqlite:///./my_agent_data.db"
        try:
            engine = sqlalchemy.create_engine(db_url)
            engine.connect()
            print("Database connection successful.")
            session_service = DatabaseSessionService(db_url=db_url)
        except Exception as e:
            print(f"Failed to connect to the database: {e}")
            session_service = InMemorySessionService()
        
        runner = Runner(
            app_name=APP_NAME,
            agent=root_agent,
            session_service=session_service,
        )
        logging.info("ADK Runner initialized successfully.")
    except Exception as e:
        logging.error(f"Error initializing ADK Runner: {e}")
        runner = None
        session_service = None
else:
    logging.error("ADK Runner could not be initialized due to missing components or root_agent.")

def create_app():
    """Application Factory Function"""
    app = Flask(__name__, static_folder='../frontend/build/static', static_url_path='/static')

    frontend_build_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'frontend', 'build'))
    app.config['FRONTEND_BUILD_DIR'] = frontend_build_path
    logging.info(f"Frontend build directory: {frontend_build_path}")

    if not os.path.isdir(frontend_build_path):
        logging.warning(f"React build directory not found at {frontend_build_path}. Run 'npm run build' in the 'frontend' directory.")
        app.config['FRONTEND_BUILD_DIR'] = None

    def cache(timeout=3600):
        """Simple in-memory cache decorator."""
        def decorator(f):
            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                cache_key = request.url
                if cache_key in wrapper.cache:
                    result, timestamp = wrapper.cache[cache_key]
                    if (time.time() - timestamp) < timeout:
                        logging.info(f"Returning cached result for {cache_key}")
                        return result
                    else:
                        logging.info(f"Cache expired for {cache_key}, recomputing.")
                result = f(*args, **kwargs)
                wrapper.cache[cache_key] = (result, time.time())
                return result
            wrapper.cache = {}
            return wrapper
        return decorator

    # --- API Routes ---

    @app.route("/api/chat", methods=["POST"])
    async def chat_handler():
        """Handles a chat turn with the ADK agent."""
        if not runner or not genai_types or not session_service:
            return jsonify({"error": "Chat runner, GenAI types, or Session service not initialized"}), 500

        session_id = None
        try:
            req_data = request.get_json()
            logging.debug(f"[BACKEND] Received raw request on /api/chat: {req_data}")
            user_id = req_data.get('user_id') or USER_ID
            session_id = req_data.get('session_id')
            message_text = req_data.get('message', {}).get('message')

            if not user_id or not message_text:
                return jsonify({"error": "user_id and message with 'message' key are required"}), 400

            if session_id:
                logging.debug(f"Attempting to retrieve session with ID: {session_id} for user_id: {user_id}")
                session = session_service.get_session(app_name=runner.app_name, user_id=user_id, session_id=session_id)
                if not session:
                    logging.warning(f"Session with ID: {session_id} not found for user_id: {user_id}")
                    return jsonify({"session_id": session_id, "messages": [], "error": "Session not found"}), 404
                else:
                    logging.debug(f"Session with ID: {session_id} retrieved successfully.")
            else:
                logging.info(f"No session ID provided, creating a new session for user_id: {user_id}")
                session = session_service.create_session(app_name=runner.app_name, user_id=user_id)
                session_id = session.id
                logging.info(f"New session created with ID: {session_id}")

            new_message_content = genai_types.Content(parts=[genai_types.Part(text=message_text)], role='user')
            logging.debug(f"[BACKEND] Calling ADK runner with session_id='{session_id}', user_id='{user_id}', message='{message_text}'")

            agent_responses = []
            async for event in runner.run_async(user_id=user_id, session_id=session_id, new_message=new_message_content):
                logging.debug(f"[AGENT_EVENT] Received event: {event}")
                if hasattr(event, 'content') and event.content and hasattr(event.content, 'parts'):
                    for part in event.content.parts:
                        if hasattr(part, 'text') and part.text:
                            agent_responses.append({"role": event.content.role or "model", "content": part.text})

            final_payload = {"session_id": session_id, "messages": agent_responses}
            logging.debug(f"[BACKEND] Sending final response to frontend: {final_payload}")
            return jsonify(final_payload), 200

        except Exception as e:
            logging.error(f"Error during chat processing: {str(e)}", exc_info=True)
            return jsonify({"session_id": session_id if session_id is not None else "", "messages": [], "error": f"Internal server error: {str(e)}"}), 500

    @app.route("/api/tables", methods=["GET"])
    @cache(timeout=3600)
    def list_tables():
        """Returns a list of tables from BigQuery."""
        try:
            tables = get_table_ddl_strings()
            num_tables = len(tables)
            total_rows = 0
            table_names = []
            for table in tables:
                table_name = table["table_name"]
                table_names.append(table_name)
                total_rows += get_total_rows(table_name)
            total_columns = get_total_column_count()
            return jsonify({"tables": table_names, "num_tables": num_tables, "total_columns": total_columns, "total_rows": total_rows}), 200
        except Exception as e:
            logging.error(f"Error listing tables: {str(e)}", exc_info=True)
            return jsonify({"error": f"Internal server error: {str(e)}"}), 500

    @app.route("/api/table_data", methods=["GET"])
    @cache(timeout=3600)
    def get_table_data():
        """Returns data for a specific table from BigQuery."""
        table_name = request.args.get("table_name")
        if not table_name:
            return jsonify({"error": "Table name is required"}), 400
        try:
            from backend.utils import get_table_description, fetch_sample_data_for_single_table
            sample_rows = fetch_sample_data_for_single_table(table_name=table_name)
            table_description = get_table_description(table_name)
            return jsonify({"data": sample_rows, "description": table_description}), 200
        except Exception as e:
            logging.error(f"Error getting table data: {str(e)}", exc_info=True)
            return jsonify({"error": f"Internal server error: {str(e)}"}), 500

    @app.route("/api/code", methods=["GET"])
    def get_code_file():
        """Returns the content of specified code files."""
        filepath = request.args.get("filepath")
        if not filepath:
            return jsonify({"error": "Filepath is required"}), 400
        if not filepath.startswith("data_agent/"):
            logging.warning(f"Access to disallowed file attempted: {filepath}")
            return jsonify({"error": "Invalid filepath"}), 400
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        abs_filepath = os.path.normpath(os.path.join(project_root, filepath))
        if not abs_filepath.startswith(project_root):
            logging.warning(f"Attempted directory traversal: {filepath}")
            return jsonify({"error": "Invalid filepath"}), 400
        try:
            with open(abs_filepath, 'r') as f:
                content = f.read()
            return jsonify({"content": content}), 200
        except FileNotFoundError:
            logging.error(f"Code file not found: {filepath}")
            return jsonify({"error": "File not found"}), 404
        except Exception as e:
            logging.error(f"Error reading code file {filepath}: {str(e)}", exc_info=True)
            return jsonify({"error": f"Internal server error: {str(e)}"}), 500

    # --- REMOVED: logging.basicConfig(level=logging.INFO) ---
    # This is now handled globally at the top of the file.
    app.logger.info("Flask app created and configured.")

    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def serve_react_app(path):
        build_dir = app.config.get('FRONTEND_BUILD_DIR')
        if not build_dir:
            logging.error("React build directory is not configured or not found.")
            return abort(404, description="React application not found. Build the frontend first.")
        index_html_path = os.path.join(build_dir, 'index.html')
        if os.path.exists(index_html_path):
            return send_from_directory(build_dir, 'index.html')
        else:
            logging.error(f"index.html not found in React build directory: {build_dir}")
            return abort(404, description="Application entry point (index.html) not found.")

    return app

app = create_app()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    debug_mode = os.environ.get('FLASK_DEBUG', 'True').lower() in ['true', '1', 't']
    print(f"Starting Flask development server on http://0.0.0.0:{port} (debug={debug_mode})...")
    app.run(debug=debug_mode, host='0.0.0.0', port=port)

# - Demo changes