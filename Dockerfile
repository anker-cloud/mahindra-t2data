# --- Stage 1: FE Build ---
  FROM node:lts-alpine AS builder
  WORKDIR /app/frontend
  COPY frontend/package.json frontend/yarn.lock* frontend/package-lock.json* ./
  RUN if [ -f yarn.lock ]; then yarn install --frozen-lockfile; \
      else npm install --legacy-peer-deps; fi
  COPY frontend/ ./
  RUN npm run build
  
  # --- Stage 2: Setup Python Runtime Environment ---
  FROM python:3.12.7-slim AS python-base
  ENV PYTHONDONTWRITEBYTECODE 1
  ENV PYTHONUNBUFFERED 1
  ENV FLASK_APP=backend/app.py
  
  ENV FLASK_PORT=8080
  ENV STREAMLIT_PORT=8501
  ENV APP_ENV=docker
  
  # Set working directory for the backend/final app
  WORKDIR /app
  
  
  COPY requirements.txt .
  RUN pip install --no-cache-dir -r requirements.txt
  
  # Copy the built frontend from the builder stage
  COPY --from=builder /app/frontend/build ./frontend/build
  
  # Copy the backend Flask application code
  COPY backend ./backend
  
  # Copy the Streamlit application code
  COPY ./data_agent ./data_agent
  
  # Copy the entrypoint script and make it executable
  COPY entrypoint.sh .
  RUN chmod +x ./entrypoint.sh
  
  EXPOSE 8080
  
  # Run the entrypoint script when the container starts
  CMD ["./entrypoint.sh"]