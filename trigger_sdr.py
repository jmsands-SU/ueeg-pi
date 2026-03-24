# main.py - The CORRECT version for Cloud Run Functions

import json
import os
from google.cloud import pubsub_v1

def trigger_sdr(request):
    """
    HTTP Cloud Function to receive SDR commands and publish them to Pub/Sub.
    The Functions Framework will automatically wrap this in a web server.
    """
    
    # --- Set CORS headers for browser compatibility ---
    headers = {'Access-Control-Allow-Origin': '*'}
    if request.method == 'OPTIONS':
        headers.update({
            'Access-Control-Allow-Methods': 'GET, POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        })
        return ('', 204, headers)
    
    # --- Security Check: Validate the Secret Key from the URL Path (ROBUST VERSION) ---
    try:
        EXPECTED_SECRET = os.environ.get("SECRET_KEY")
        if not EXPECTED_SECRET:
            print("CRITICAL ERROR: SECRET_KEY environment variable is not set.")
            return ("Server configuration error", 500, headers)

        # The full path, including the leading slash, is what we need.
        # e.g., '/c7e4a2d8-b8f1-4e7f-8d2a-9c7a6e1d3b0f'
        submitted_path = request.path
        
        # For debugging, let's see what path the function is actually receiving.
        print(f"Received request path: '{submitted_path}'")

        # We check if the expected secret is a substring of the path.
        # This is more robust than splitting.
        # We add a '/' to the start of the secret to match the full path.
        expected_path_segment = f"/{EXPECTED_SECRET}"

        if expected_path_segment not in submitted_path:
            # If the check fails, print what we were expecting vs what we got.
            print(f"Secret key check failed. Expected segment '{expected_path_segment}' not in path '{submitted_path}'.")
            return ("Forbidden: Invalid secret key or URL format", 403, headers)
            
    except Exception as e:
        # This is a fallback for any other unexpected error during the check.
        print(f"An unexpected error occurred during security check: {e}")
        return ("Forbidden", 403, headers)

    # --- Parse URL Parameters ---
    request_args = request.args
    action = request_args.get("action", "start")
    
    if action == "start":
        blob_name = request_args.get("blob")
        try:
            duration_seconds = int(request_args.get("duration", 0))
        except (ValueError, TypeError):
            return ("Invalid 'duration' parameter.", 400, headers)
        overwrite = request_args.get("overwrite", "false").lower() == "true"
        
        if not blob_name or duration_seconds <= 0:
            return ("'blob' and a positive 'duration' are required for start action.", 400, headers)
        
        payload = { "action": "start", "blob": blob_name, "duration_seconds": duration_seconds, "overwrite": overwrite }

    elif action == "stop":
        payload = { "action": "stop" }
    else:
        return (f"Invalid action: '{action}'. Must be 'start' or 'stop'.", 400, headers)

    # --- Publish the Payload to Pub/Sub ---
    try:
        PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
        TOPIC_ID = os.environ.get("TOPIC_ID") # This is 'sdr-commands'
        
        if not PROJECT_ID or not TOPIC_ID:
            print("CRITICAL ERROR: GCP_PROJECT_ID or TOPIC_ID environment variables are not set.")
            return ("Server configuration error", 500, headers)

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
        data = json.dumps(payload).encode("utf-8")
        
        future = publisher.publish(topic_path, data)
        future.result()
        
        success_message = f"✅ Success! Sent '{action}' command."
        print(f"Published payload: {payload}")
        return (success_message, 200, headers)
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to publish to Pub/Sub: {e}")
        return ("Error sending command to device.", 500, headers)
