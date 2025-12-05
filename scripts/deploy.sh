#!/bin/bash

# === Configuration ===
# Define App identifiers and corresponding Cloud Run Service Names
APP_IDENTIFIER="talk-to-data-app" # Argument for Flask app

APP_SERVICE_NAME="talk-to-data-main-app"   # Cloud Run service name for the Flask app
# Define components of the image name
GCP_REGION="asia-south1"
ARTIFACT_REGISTRY_REPO="talk-to-data-repo" # CHANGE IF YOUR REPO NAME IS DIFFERENT
IMAGE_NAME="talk-to-data-main-app" # Name of the image itself
TAG="latest"
# Note: MAIN_IMAGE_NAME is now constructed later, after PROJECT_ID is confirmed

# === Script Setup ===
# Go to root directory relative to the script
cd "$(dirname "$0")/.."

# Source initialization variables (like PROJECT_ID)
# Ensure init.sh sets the PROJECT_ID variable
if [ -f "./scripts/init.sh" ]; then
    echo "Sourcing configuration from init.sh..."
    . ./scripts/init.sh
else
    echo "Error: ./scripts/init.sh not found!"
    exit 1
fi

# Check if PROJECT_ID is set
if [ -z "$PROJECT_ID" ]; then
    echo "Error: PROJECT_ID environment variable is not set after sourcing init.sh."
    exit 1
fi
echo "Using PROJECT_ID: $PROJECT_ID"

# === Construct Full Image Name (AFTER PROJECT_ID is known) ===
MAIN_IMAGE_NAME="${GCP_REGION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REGISTRY_REPO}/${IMAGE_NAME}:${TAG}"

# === Argument Parsing ===
DEPLOY_TARGET=$1

if [ -z "$DEPLOY_TARGET" ]; then
  echo "Usage: $0 <$APP_IDENTIFIER | $DNA_IDENTIFIER>"
  echo "  $APP_IDENTIFIER: Deploys the Flask application service ($APP_SERVICE_NAME)"
  echo "  $DNA_IDENTIFIER: Deploys the Streamlit/KG application service ($DNA_SERVICE_NAME)"
  exit 1
fi

# === Set Target Specific Variables ===
CLOUDRUN_SERVICE_NAME=""
APP_MODE_VAR=""
# Add other specific env vars here if needed, e.g.
# OTHER_ENV_VARS="KEY1=VALUE1,KEY2=VALUE2"

if [ "$DEPLOY_TARGET" = "$APP_IDENTIFIER" ]; then
  CLOUDRUN_SERVICE_NAME="$APP_SERVICE_NAME"
  APP_MODE_VAR="$APP_IDENTIFIER"
  # OTHER_ENV_VARS="FLASK_SPECIFIC_KEY=flask_value" # Example
  echo "Deploying target: Flask App (Service: $CLOUDRUN_SERVICE_NAME, Mode: $APP_MODE_VAR)"
elif [ "$DEPLOY_TARGET" = "$DNA_IDENTIFIER" ]; then
  CLOUDRUN_SERVICE_NAME="$DNA_SERVICE_NAME"
  APP_MODE_VAR="$DNA_IDENTIFIER"
  # OTHER_ENV_VARS="STREAMLIT_SPECIFIC_KEY=dna_value" # Example
  echo "Deploying target: Streamlit/KG App (Service: $CLOUDRUN_SERVICE_NAME, Mode: $APP_MODE_VAR)"
else
  echo "Error: Invalid deployment target '$DEPLOY_TARGET'. Must be '$APP_IDENTIFIER' or '$DNA_IDENTIFIER'."
  exit 1
fi

# === GCP Setup ===
echo "Setting GCP project to: $PROJECT_ID"
gcloud config set project "$PROJECT_ID"

# === Build Docker Image (using Cloud Build) ===
# This builds the *single* image containing both apps.
# Ensure cloudbuild.yaml uses the _IMAGE_NAME substitution variable.
echo "Submitting build to Cloud Build..."
echo "Image to be built: $MAIN_IMAGE_NAME" # This should now print the correct path

# Define the VPC-SC compliant log bucket here
GCS_LOG_DIR="gs://${PROJECT_ID}-cloudbuild-logs"

gcloud builds submit \
  --config cloudbuild.yaml \
  --substitutions=_IMAGE_NAME="$MAIN_IMAGE_NAME" \
  --timeout=1200s \
  --gcs-log-dir="$GCS_LOG_DIR" \
  . # Build context is current directory

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Error: Cloud Build failed."
    exit 1
fi
echo "Cloud Build finished successfully."

# === Deploy to Cloud Run ===
echo "Deploying image $MAIN_IMAGE_NAME to Cloud Run service $CLOUDRUN_SERVICE_NAME..."

# Prepare environment variables string for Cloud Run
ENV_VARS="APP_MODE=$APP_MODE_VAR,GOOGLE_GENAI_USE_VERTEXAI=0,GOOGLE_API_KEY=AIzaSyBOxFPWXJwN2Wtti6RV6Q-XTsa3ASXd6uk"
# Add secrets if needed, e.g. --set-secrets=GOOGLE_API_KEY=your-secret-name:latest

gcloud run deploy "$CLOUDRUN_SERVICE_NAME" \
    --image "$MAIN_IMAGE_NAME" \
    --region "$GCP_REGION" \
    --platform managed \
    --port 8080 `# Port your container listens on (defined in entrypoint)` \
    --set-env-vars "$ENV_VARS" \
    --min-instances 1 `# Or adjust as needed` \
    --max-instances 1 \
    --timeout=900s \
    --memory=4Gi \
    --ingress internal \
    --vpc-connector projects/host-project-dev-env-mum/locations/asia-south1/connectors/serverless-vpc-connector \
    --vpc-egress private-ranges-only \
    --no-allow-unauthenticated


# Check if deployment was successful
if [ $? -ne 0 ]; then
    echo "Error: Cloud Run deployment failed for $CLOUDRUN_SERVICE_NAME."
    exit 1
fi

echo "Successfully deployed $CLOUDRUN_SERVICE_NAME to Cloud Run in region $GCP_REGION."
SERVICE_URL=$(gcloud run services describe "$CLOUDRUN_SERVICE_NAME" --platform managed --region "$GCP_REGION" --format 'value(status.url)')
echo "Service URL: $SERVICE_URL"
