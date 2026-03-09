#!/bin/bash
# YouTube Bulk Uploader - Quick Test Script for Cloud Shell
# This script tests your deployed Cloud Function

set -e

echo "=========================================="
echo "YouTube Bulk Uploader - Function Test"
echo "=========================================="
echo ""

# Get project info
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
REGION="us-east1"
FUNCTION_NAME="youtube-bulk-uploader"

echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Function: $FUNCTION_NAME"
echo ""

# Get function URL
echo "Getting function URL..."
FUNCTION_URL=$(gcloud functions describe $FUNCTION_NAME \
  --region=$REGION \
  --gen2 \
  --format="value(serviceConfig.uri)" 2>/dev/null)

if [ -z "$FUNCTION_URL" ]; then
  echo "❌ Error: Function not found!"
  echo "Please deploy first with: ./setup.sh deploy_all"
  exit 1
fi

echo "Function URL: $FUNCTION_URL"
echo ""

# Create test payload
PAYLOAD='{
  "drive_root_folder_id": "1cMlIsNLn7KoaMjHEZYjn8pavtQF43xAM",
  "youtube_channel_id": "UCDhxapLoA0eGl7PJa4ZtTiw"
}'

echo "Triggering function with payload:"
echo "$PAYLOAD"
echo ""
echo "Please wait... (this may take a few minutes)"
echo ""

# Get service account for authentication
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
SERVICE_ACCOUNT="$PROJECT_NUMBER-compute@developer.gserviceaccount.com"

# Make authenticated request
RESPONSE=$(curl -X POST "$FUNCTION_URL" \
  -H "Content-Type: application/json" \
  -H "Authorization: bearer $(gcloud auth print-identity-token --impersonate-service-account=$SERVICE_ACCOUNT 2>/dev/null || gcloud auth print-identity-token)" \
  -d "$PAYLOAD" \
  -s -w "\n%{http_code}" 2>&1)

# Parse response
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

echo "=========================================="
echo "Response Status: $HTTP_CODE"
echo "=========================================="
echo ""

if [ "$HTTP_CODE" = "200" ]; then
  echo "✅ Success!"
  echo ""
  echo "Response:"
  echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"
else
  echo "❌ Error"
  echo ""
  echo "Response:"
  echo "$BODY"
fi

echo ""
echo "=========================================="
echo "View detailed logs:"
echo "  gcloud functions logs read $FUNCTION_NAME --region=$REGION --gen2 --limit=50"
echo ""
echo "Or visit:"
echo "  https://console.cloud.google.com/functions/details/$REGION/$FUNCTION_NAME?project=$PROJECT_ID"
echo "=========================================="
