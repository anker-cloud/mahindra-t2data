import os
import logging
from flask import Flask, send_from_directory, abort, jsonify, request, current_app
from dotenv import load_dotenv
import functools
import time

from backend.utils import get_table_description, get_table_ddl_strings, get_total_rows, get_total_column_count
# Removed cachetools imports

# Load environment variables from .env file in the project root
# Use an absolute path to ensure it's loaded correctly regardless of the current working directory
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'env')
load_dotenv(dotenv_path)

# Assuming data_agent is in the parent directory and accessible in PYTHONPATH
try:
    # Import the root_agent instance directly
    from data_agent.agent import root_agent
except ImportError:
    logging.error("Could not import root_agent from data_agent.agent. Ensure data_agent is in PYTHONPATH.")
    root_agent = None # Set to None if import fails

# ADK components
try:
    from google.adk.runners import Runner
    from google.adk.sessions.database_session_service import DatabaseSessionService
    from google.adk.sessions.in_memory_session_service import InMemorySessionService # Keep this import for conditional usage
  # Removed InMemoryArtifactService import
    from google.genai import types as genai_types # Re-added genai_types import
    import sqlalchemy
except ImportError:
    logging.error("Could not import ADK components. Ensure 'google-adk' is installed.")
    # Define placeholders if ADK is not available, to prevent app crash at import time
    Runner = None
    InMemorySessionService = None
    # Removed InMemoryArtifactService placeholder
    genai_types = None # Re-added genai_types placeholder

# Define APP_NAME, USER_ID
APP_NAME = "data_agent_chatbot"
USER_ID = "user_1"

# Initialize ADK services and Runner globally
session_service = None
runner = None
if Runner and InMemorySessionService and root_agent: # Removed InMemoryArtifactService check
    try:
        db_url = "sqlite:///./my_agent_data.db"
        try:
            engine = sqlalchemy.create_engine(db_url)
            # Test the database connection
            engine.connect()
            print("Database connection successful.")
            session_service = DatabaseSessionService(db_url=db_url)
        except Exception as e:
            print(f"Failed to connect to the database: {e}")
            session_service = InMemorySessionService()
        # Removed artifact_service initialization
        runner = Runner(
            app_name=APP_NAME,  # Choose an appropriate app name
            agent=root_agent,
            # Removed artifact_service parameter
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
    # Configure static files to be served from /static URL path, pointing to frontend/build/static directory
    app = Flask(__name__, static_folder='../frontend/build/static', static_url_path='/static')

    frontend_build_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'frontend', 'build'))
    app.config['FRONTEND_BUILD_DIR'] = frontend_build_path
    logging.info(f"Frontend build directory: {frontend_build_path}")

    if not os.path.isdir(frontend_build_path):
        logging.warning(f"React build directory not found at {frontend_build_path}. "
                        "Run 'npm run build' in the 'frontend' directory. "
                        "Static file serving might not work correctly.")
        app.config['FRONTEND_BUILD_DIR'] = None

    # --- Caching Decorator (Restored Original) ---
    def cache(timeout=3600):
        """Simple in-memory cache decorator."""
        def decorator(f):
            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                # Use request.url as a simple cache key
                # More sophisticated key generation might be needed for POST or complex args
                cache_key = request.url
                if cache_key in wrapper.cache:
                    result, timestamp = wrapper.cache[cache_key]
                    if (time.time() - timestamp) < timeout:
                        logging.info(f"Returning cached result for {cache_key}")
                        return result
                    else:
                        # Cache expired
                        logging.info(f"Cache expired for {cache_key}, recomputing.")

                # Compute the result if not cached or expired
                result = f(*args, **kwargs)
                wrapper.cache[cache_key] = (result, time.time())
                return result
            wrapper.cache = {} # Initialize cache dict on the wrapper
            return wrapper
        return decorator

    # --- API Routes ---

    @app.route("/api/chat", methods=["POST"])
    async def chat_handler():
        """Handles a chat turn with the ADK agent."""
        if not runner or not genai_types or not session_service: # Re-added genai_types check
            return jsonify({"error": "Chat runner, GenAI types, or Session service not initialized"}), 500

        session_id = None # Initialize session_id to None
        try:
            req_data = request.get_json()
            user_id = req_data.get('user_id') or USER_ID # Use default user ID if not provided
            session_id = req_data.get('session_id') # Get session_id from request
            message_text = req_data.get('message', {}).get('message') # Get message text

            if not user_id or not message_text:
                 return jsonify({"error": "user_id and message with 'message' key are required"}), 400

            # Get or create session using the globally initialized session_service
            if session_id:
                logging.info(f"Attempting to retrieve session with ID: {session_id} for user_id: {user_id}")
                session = session_service.get_session(
                    app_name=runner.app_name,
                    user_id=user_id,
                    session_id=session_id,
                )
                if not session:
                    logging.warning(f"Session with ID: {session_id} not found for user_id: {user_id}")
                    return jsonify({"session_id": session_id, "messages": [], "error": "Session not found"}), 404
                else:
                    logging.info(f"Session with ID: {session_id} retrieved successfully.")
            else:
                logging.info(f"No session ID provided, creating a new session for user_id: {user_id}")
                session = session_service.create_session(
                    app_name=runner.app_name,
                    user_id=user_id,
                )
                session_id = session.id # Set session_id to the newly created session
                logging.info(f"New session created with ID: {session_id}")


            # Create content for the new message using genai_types
            new_message_content = genai_types.Content(
                parts=[genai_types.Part(text=message_text)],
                role='user' # Assuming user role for incoming messages
            )

            agent_responses = []
            # Use the globally initialized runner
            async for event in runner.run_async(
                user_id=user_id,
                session_id=session_id,
                new_message=new_message_content,
            ):
                if hasattr(event, 'content') and event.content and hasattr(event.content, 'parts'):
                    for part in event.content.parts:
                        if hasattr(part, 'text') and part.text:
                            agent_responses.append({
                                "role": event.content.role or "model",
                                "content": part.text
                            })

            if not agent_responses:
                print(f"No text response parts found in ADK events for session {session_id}")

            # Return the session_id and messages
            return jsonify({"session_id": session_id, "messages": agent_responses}), 200

        except Exception as e:
            logging.error(f"Error during chat processing: {str(e)}")
            current_app.logger.exception("Chat processing error details:")
            # Include session_id in error response if available
            return jsonify({"session_id": session_id if session_id is not None else "", "messages": [], "error": f"Internal server error: {str(e)}"}), 500

    @app.route("/api/tables", methods=["GET"])
    @cache(timeout=3600) # Restored original cache decorator
    def list_tables():
        """Returns a list of tables from BigQuery."""
        # Removed TTLCache specific logging
        try:
            tables = get_table_ddl_strings()
            num_tables = len(tables)
            total_rows = 0
            table_names = []
            for table in tables:
                table_name = table["table_name"]
                table_names.append(table_name)
                total_rows += get_total_rows(table_name)

            # Use the new function from utils.py to get the total column count
            total_columns = get_total_column_count()

            return jsonify({
                "tables": table_names,
                "num_tables": num_tables,
                "total_columns": total_columns,
                "total_rows": total_rows
            }), 200
        except Exception as e:
            logging.error(f"Error listing tables: {str(e)}")
            return jsonify({"error": f"Internal server error: {str(e)}"}), 500

    @app.route("/api/table_data", methods=["GET"])
    @cache(timeout=3600) # Restored original cache decorator
    def get_table_data():
        """Returns data for a specific table from BigQuery."""
        table_name = request.args.get("table_name")
        # Removed TTLCache specific logging
        if not table_name:
            return jsonify({"error": "Table name is required"}), 400
        try:
            # Import the new function from backend.utils
            from backend.utils import get_table_description, fetch_sample_data_for_single_table

            # Fetch sample data using the new utility function
            sample_rows = fetch_sample_data_for_single_table(table_name=table_name)

            # Fetch table description
            table_description = get_table_description(table_name)

            # Return the fetched data and description
            return jsonify({
                "data": sample_rows,
                "description": table_description
            }), 200

        except Exception as e:
            logging.error(f"Error getting table data: {str(e)}")
            return jsonify({"error": f"Internal server error: {str(e)}"}), 500

    # --- Route to serve specific code files ---
    @app.route("/api/code", methods=["GET"])
    def get_code_file():
        """Returns the content of specified code files."""
        filepath = request.args.get("filepath")

        if not filepath:
            return jsonify({"error": "Filepath is required"}), 400

        # Ensure the requested filepath is within the allowed directory
        if not filepath.startswith("data_agent/"):
            logging.warning(f"Access to disallowed file attempted: {filepath}")
            return jsonify({"error": "Invalid filepath"}), 400

        # Construct the absolute path to the file relative to the project root
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        abs_filepath = os.path.join(project_root, filepath)

        # Normalize the path to prevent directory traversal attacks
        abs_filepath = os.path.normpath(abs_filepath)

        # Security check: Ensure the normalized path is within the project root
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
            logging.error(f"Error reading code file {filepath}: {str(e)}")
            return jsonify({"error": f"Internal server error: {str(e)}"}), 500


    logging.basicConfig(level=logging.INFO)
    app.logger.info("Flask app created and configured.")

    # --- Catch-all Route to Serve React App's index.html ---
    # This serves index.html for the root path and any other path not matched by API or specific static file routes.
    # This must be the LAST route defined.
    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def serve_react_app(path): # path variable is captured but not used if always serving index.html
        build_dir = app.config.get('FRONTEND_BUILD_DIR')
        if not build_dir:
            logging.error("React build directory is not configured or not found.")
            return abort(404, description="React application not found. Build the frontend first.")

        index_html_path = os.path.join(build_dir, 'index.html')
        if os.path.exists(index_html_path):
            logging.info(f"Serving index.html from: {index_html_path}")
            return send_from_directory(build_dir, 'index.html')
        else:
            logging.error(f"index.html not found in React build directory: {build_dir}")
            return abort(404, description="Application entry point (index.html) not found.")

    return app

# --- Gunicorn Entry Point ---
# Gunicorn will look for this 'app' object.
app = create_app()

# --- Direct Execution Entry Point (for local development) ---
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    debug_mode = os.environ.get('FLASK_DEBUG', 'True').lower() in ['true', '1', 't']
    print(f"Starting Flask development server on http://0.0.0.0:{port} (debug={debug_mode})...")
    app.run(debug=debug_mode, host='0.0.0.0', port=port)