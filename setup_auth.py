#!/usr/bin/env python3
"""
Script to authenticate with Strava API and save the refresh token.
This is a one-time setup script to get the initial refresh token.
"""

import os
import sys
import json
import argparse
import webbrowser
import http.server
import socketserver
import sqlite3
import logging
import urllib.parse
from urllib.parse import urlencode
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Default redirect URI for local OAuth callback
DEFAULT_REDIRECT_URI = "http://localhost:8000/callback"

# Global variable to store the auth code
auth_code = None

class OAuthCallbackHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler for OAuth callback"""
    
    def do_GET(self):
        """Handle GET request to the callback URL"""
        global auth_code
        
        # Parse the URL and get the query parameters
        query = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(query)
        
        # Check if we have an authorization code
        if 'code' in params:
            auth_code = params['code'][0]
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            
            # Send a success message to the user
            self.wfile.write(b"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Strava Authentication Successful</title>
                <style>
                    body { font-family: Arial, sans-serif; text-align: center; padding: 40px; }
                    h1 { color: #FC4C02; } /* Strava orange */
                    .success { background-color: #dff0d8; padding: 15px; border-radius: 5px; }
                </style>
            </head>
            <body>
                <h1>Strava Authentication Successful!</h1>
                <div class="success">
                    <p>Authorization successful! You can now close this window and return to the terminal.</p>
                </div>
            </body>
            </html>
            """)
            logger.info("Received authorization code")
        else:
            # Handle error
            self.send_response(400)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"Authorization failed. Please try again.")
            logger.error("No authorization code received")
    
    def log_message(self, format, *args):
        """Disable default log messages"""
        return


def load_env(file_path='.env'):
    """Load environment variables from .env file"""
    if not os.path.exists(file_path):
        logger.error(f".env file not found at {file_path}")
        return {}
        
    env_vars = {}
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            key, value = line.split('=', 1)
            env_vars[key.strip()] = value.strip()
            
    return env_vars


def save_refresh_token(db_path, refresh_token):
    """Save the refresh token to the database"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tokens table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tokens (
        name TEXT PRIMARY KEY,
        value TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Save the refresh token
    cursor.execute("""
    INSERT OR REPLACE INTO tokens (name, value, updated_at)
    VALUES ('refresh_token', ?, datetime('now'))
    """, (refresh_token,))
    
    conn.commit()
    conn.close()
    
    logger.info(f"Saved refresh token to database: {db_path}")


def save_tokens_to_file(tokens, file_path='tokens.json'):
    """Save tokens to a JSON file"""
    with open(file_path, 'w') as f:
        json.dump(tokens, f)
    
    logger.info(f"Saved tokens to file: {file_path}")


def authenticate_with_strava(client_id, client_secret, redirect_uri, db_path, save_to_file=True):
    """Complete OAuth flow with Strava and save refresh token"""
    global auth_code
    
    # Construct the authorization URL
    auth_url = "https://www.strava.com/oauth/authorize"
    params = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
        'scope': 'activity:read,activity:read_all'
    }
    auth_url = f"{auth_url}?{urlencode(params)}"
    
    # Start a local web server to receive the callback
    port = int(redirect_uri.split(':')[2].split('/')[0])
    httpd = socketserver.TCPServer(("", port), OAuthCallbackHandler)
    
    logger.info(f"Starting local server on port {port}")
    logger.info("Opening web browser for Strava authorization...")
    
    # Open the browser for user to authorize
    webbrowser.open(auth_url)
    
    # Wait for the callback
    try:
        while auth_code is None:
            httpd.handle_request()
    finally:
        httpd.server_close()
    
    if not auth_code:
        logger.error("Failed to get authorization code")
        return False
    
    # Exchange the authorization code for tokens
    token_url = "https://www.strava.com/oauth/token"
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'code': auth_code,
        'grant_type': 'authorization_code'
    }
    
    logger.info("Exchanging authorization code for tokens...")
    response = requests.post(token_url, data=data)
    
    if response.status_code != 200:
        logger.error(f"Token exchange failed: {response.text}")
        return False
    
    tokens = response.json()
    
    # Save the refresh token
    if 'refresh_token' not in tokens:
        logger.error("No refresh token in response")
        return False
    
    # Save to database
    save_refresh_token(db_path, tokens['refresh_token'])
    
    # Optionally save to file
    if save_to_file:
        save_tokens_to_file(tokens)
    
    return True


def main():
    parser = argparse.ArgumentParser(description='Authenticate with Strava API and save refresh token')
    parser.add_argument('--env', type=str, default='.env',
                        help='Path to the .env file with Strava credentials')
    parser.add_argument('--db', type=str, default='data/segments.db',
                        help='Path to the SQLite database to store the refresh token')
    parser.add_argument('--port', type=int, default=8000,
                        help='Port to use for the local callback server')
    parser.add_argument('--no-file', action='store_true',
                        help='Do not save tokens to tokens.json file (only to database)')
    
    args = parser.parse_args()
    
    # Load environment variables
    env_vars = load_env(args.env)
    
    # Get Strava credentials
    client_id = env_vars.get('STRAVA_CLIENT_ID')
    client_secret = env_vars.get('STRAVA_CLIENT_SECRET')
    
    if not client_id or not client_secret:
        logger.error("Missing Strava credentials. Check your .env file.")
        logger.error("Required variables: STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET")
        return 1
    
    # Create database directory if needed
    db_dir = os.path.dirname(args.db)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    # Set up the redirect URI
    redirect_uri = f"http://localhost:{args.port}/callback"
    
    # Run the authentication flow
    logger.info("Starting Strava OAuth authentication process...")
    
    try:
        if authenticate_with_strava(client_id, client_secret, redirect_uri, args.db, not args.no_file):
            logger.info("Authentication successful!")
            logger.info("You can now run the backfill process.")
            return 0
        else:
            logger.error("Authentication failed.")
            return 1
    except Exception as e:
        logger.error(f"Error during authentication: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == '__main__':
    exit(main())
