#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Load Environment Variables ---
# Load variables from .env file if it exists
if [ -f .env ]; then
  echo "Loading environment variables from .env file..."
  set -a # Automatically export all variables
  . ./.env # Source the .env file
  set +a # Turn off auto-export
else
  echo "Warning: .env file not found. Ensure necessary environment variables are set."
fi

# --- Database Initialization ---
echo "Initializing SQLite database..."
if [ ! -f my_agent_data.db ]; then
  echo "Creating SQLite database file..."
  touch my_agent_data.db
  echo "SQLite database file created."
else
  echo "SQLite database file already exists."
fi

# --- Backend Server Start ---
echo "Starting backend server with Gunicorn..."
# Navigate to the backend directory (optional, if app is specified relative to root)
# cd backend

# Start Gunicorn
# -w: number of worker processes (adjust based on your server's cores)
# -b: bind address and port
# app:app: module_name:flask_app_instance_name (referring to backend/app.py and the 'app' instance in it)
# --chdir: change directory to 'backend' before running, so app:app resolves correctly
# --log-level: set logging level for Gunicorn
# --access-logfile: where to log access requests
# --error-logfile: where to log Gunicorn errors

# Ensure the backend directory exists
if [ ! -d "backend" ]; then
  echo "ERROR: 'backend' directory not found. Cannot start Gunicorn."
  exit 1
fi

# Check if backend/app.py exists
if [ ! -f "backend/app.py" ]; then
  echo "ERROR: 'backend/app.py' not found. Cannot start Gunicorn."
  exit 1
fi

# Default port, can be overridden by PORT environment variable
PORT="${PORT:-8080}"

echo "Attempting to start Gunicorn on port $PORT..."
exec gunicorn --chdir backend -w 4 -b 0.0.0.0:$PORT --timeout 300 --preload app:app --log-level info --access-logfile - --error-logfile -
# Using '-' for logfiles sends them to stdout/stderr, which is common for containerized apps.
# If you prefer files: --access-logfile ./logs/gunicorn_access.log --error-logfile ./logs/gunicorn_error.log
# (Ensure ./logs directory exists and Gunicorn has write permissions)

echo "Gunicorn started."