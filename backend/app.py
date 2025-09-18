import os
import logging
from flask import Flask, send_from_directory, abort, jsonify, request, current_app
from dotenv import load_dotenv
import functools
import time
import sys
import sqlalchemy

# --- FIX 1: Configure Logging to DEBUG Level ---
# This is set to DEBUG to ensure all detailed logs are captured in GCP.
logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Import other modules after logging is set up ---
from backend.utils import get_table_description, get_table_ddl_strings, get_total_rows, get_total_column_count
try:
    from data_agent.agent import root_agent
    from google.adk.runners import Runner
    from google.adk.sessions.database_session_service import DatabaseSessionService
    from google.adk.sessions.in_memory_session_service import InMemorySessionService
    from google.genai import types as genai_types
except ImportError as e:
    logging.critical(f"A critical module could not be imported. The app cannot start. Error: {e}")
    # Set placeholders to prevent further errors
    root_agent = Runner = DatabaseSessionService = InMemorySessionService = genai_types = None

# Load environment variables from a .env file if it exists
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

def create_app():
    """Application Factory Function"""
    app = Flask(__name__, static_folder='../frontend/build/static', static_url_path='/static')

    # --- FIX 2: Move All Initialization Logic Inside the Factory ---
    APP_NAME = "data_agent_chatbot"
    USER_ID = "user_1" # Default user_id

    # Initialize ADK services and Runner within the app context
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
            # Attach the runner and other components to the app object for access in routes
            app.runner = runner
            app.session_service = session_service
            app.genai_types = genai_types
            app.default_user_id = USER_ID
            logging.info("ADK Runner initialized successfully and attached to app.")
        except Exception as e:
            logging.critical(f"FATAL: Could not initialize ADK Runner: {e}", exc_info=True)
            app.runner = None # Ensure runner is None if initialization fails
    else:
        logging.critical("ADK Runner could not be initialized due to missing components.")
        app.runner = None

    frontend_build_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'frontend', 'build'))
    if not os.path.isdir(frontend_build_path):
        logging.warning(f"React build directory not found at {frontend_build_path}.")
    app.config['FRONTEND_BUILD_DIR'] = frontend_build_path

    # --- API Routes ---
    @app.route("/api/chat", methods=["POST"])
    async def chat_handler():
        """Handles a chat turn with the ADK agent."""
        # Access components from the application context
        runner = current_app.runner
        session_service = current_app.session_service
        genai_types = current_app.genai_types

        if not all([runner, session_service, genai_types]):
            return jsonify({"error": "Chat components not initialized on the server."}), 500

        session_id = None
        try:
            req_data = request.get_json()
            logging.debug(f"[BACKEND] Received raw request on /api/chat: {req_data}")
            user_id = req_data.get('user_id') or current_app.default_user_id
            session_id = req_data.get('session_id')
            message_text = req_data.get('message', {}).get('message')

            if not user_id or not message_text:
                return jsonify({"error": "user_id and message with 'message' key are required"}), 400

            if session_id:
                session = session_service.get_session(app_name=runner.app_name, user_id=user_id, session_id=session_id)
                if not session:
                    return jsonify({"session_id": session_id, "messages": [], "error": "Session not found"}), 404
            else:
                session = session_service.create_session(app_name=runner.app_name, user_id=user_id)
                session_id = session.id
                logging.info(f"New session created with ID: {session_id}")

            new_message_content = genai_types.Content(parts=[genai_types.Part(text=message_text)], role='user')
            agent_responses = []
            async for event in runner.run_async(user_id=user_id, session_id=session_id, new_message=new_message_content):
                logging.debug(f"[AGENT_EVENT] Received event: {event}")
                if hasattr(event, 'content') and event.content and hasattr(event.content, 'parts'):
                    for part in event.content.parts:
                        if hasattr(part, 'text') and part.text:
                            agent_responses.append({"role": event.content.role or "model", "content": part.text})

            return jsonify({"session_id": session_id, "messages": agent_responses}), 200

        except Exception as e:
            logging.error(f"Error during chat processing: {str(e)}", exc_info=True)
            return jsonify({"session_id": session_id or "", "messages": [], "error": f"Internal server error: {str(e)}"}), 500
    
    # Other API routes like /api/tables, /api/table_data, etc. would go here...

    # --- React Frontend Serving ---
    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def serve_react_app(path):
        build_dir = current_app.config.get('FRONTEND_BUILD_DIR')
        if not build_dir:
            return abort(404, description="React application build directory not found.")
        
        # If the path points to an existing file, serve it.
        if path != "" and os.path.exists(os.path.join(build_dir, path)):
            return send_from_directory(build_dir, path)
        
        # Otherwise, serve the index.html for client-side routing.
        return send_from_directory(build_dir, 'index.html')

    return app

# Create the app instance using the factory
app = create_app()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    debug_mode = os.environ.get('FLASK_DEBUG', 'True').lower() in ['true', '1', 't']
    logging.info(f"Starting Flask development server on http://0.0.0.0:{port} (debug={debug_mode})...")
    app.run(debug=debug_mode, host='0.0.0.0', port=port)