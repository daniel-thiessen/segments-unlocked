import requests  # type: ignore[import]
import time
import webbrowser
from http.server import HTTPServer as BaseHTTPServer, BaseHTTPRequestHandler
import urllib.parse
import threading
import socket
import json
from typing import Optional, Dict, Any, Union, cast

# Custom HTTP server with auth_code attribute
class HTTPServer(BaseHTTPServer):
    """Extended HTTP server with auth_code attribute"""
    auth_code: Optional[str] = None

from src.settings import (
    STRAVA_CLIENT_ID,
    STRAVA_CLIENT_SECRET,
    STRAVA_AUTH_URL,
    STRAVA_TOKEN_URL,
    STRAVA_REDIRECT_URI,
    save_tokens,
    load_tokens
)

class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """Simple HTTP server to handle OAuth callback"""
    
    def do_GET(self):
        """Handle GET requests to the callback URL"""
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        
        # Parse query parameters
        query_components = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        
        # Check if there's an authorization code
        if 'code' in query_components:
            auth_code = query_components['code'][0]
            # Set auth_code on server (with type assertion)
            server = cast(HTTPServer, self.server)
            server.auth_code = auth_code
            self.wfile.write(b"<html><body><h1>Authentication successful!</h1>")
            self.wfile.write(b"<p>You can close this window now.</p></body></html>")
        else:
            self.wfile.write(b"<html><body><h1>Authentication failed!</h1>")
            self.wfile.write(b"<p>Error: No authorization code received.</p></body></html>")
    
    def log_message(self, format, *args):
        """Silence the server logs"""
        return

def get_auth_url() -> str:
    """Generate the authorization URL for Strava OAuth"""
    params = {
        'client_id': STRAVA_CLIENT_ID,
        'response_type': 'code',
        'redirect_uri': STRAVA_REDIRECT_URI,
        'approval_prompt': 'force',
        'scope': 'activity:read_all'
    }
    
    auth_url = f"{STRAVA_AUTH_URL}?"
    auth_url += "&".join([f"{k}={v}" for k, v in params.items()])
    return auth_url

def get_server_port() -> int:
    """Extract port from redirect URI"""
    parsed_uri = urllib.parse.urlparse(STRAVA_REDIRECT_URI)
    return parsed_uri.port or 8000

def exchange_code_for_token(code: str) -> Optional[Dict[str, Any]]:
    """Exchange authorization code for access token"""
    payload = {
        'client_id': STRAVA_CLIENT_ID,
        'client_secret': STRAVA_CLIENT_SECRET,
        'code': code,
        'grant_type': 'authorization_code'
    }
    
    response = requests.post(STRAVA_TOKEN_URL, data=payload)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error exchanging code for token: {response.text}")
        return None

def refresh_access_token(refresh_token: str) -> Optional[Dict[str, Any]]:
    """Refresh the access token using the refresh token"""
    payload = {
        'client_id': STRAVA_CLIENT_ID,
        'client_secret': STRAVA_CLIENT_SECRET,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token'
    }
    
    response = requests.post(STRAVA_TOKEN_URL, data=payload)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error refreshing token: {response.text}")
        return None

def authenticate() -> Dict[str, Any]:
    """Run the full OAuth flow to authenticate with Strava"""
    # Check if we already have tokens
    tokens = load_tokens()
    
    if tokens and 'refresh_token' in tokens:
        # Check if access token is expired
        if 'expires_at' in tokens and tokens['expires_at'] < time.time():
            print("Access token expired, refreshing...")
            new_tokens = refresh_access_token(tokens['refresh_token'])
            
            if new_tokens:
                save_tokens(new_tokens)
                return new_tokens
        else:
            return tokens
    
    # No valid tokens, start OAuth flow
    auth_url = get_auth_url()
    
    # Start a local web server to handle the callback
    server_port = get_server_port()
    server_address = ('', server_port)
    
    httpd = HTTPServer(server_address, OAuthCallbackHandler)
    # auth_code already set to None in class definition
    
    # Start the server in a separate thread
    server_thread = threading.Thread(target=httpd.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    
    print(f"Opening browser to authenticate with Strava...")
    webbrowser.open(auth_url)
    
    # Wait for the callback to set the authorization code
    while httpd.auth_code is None:
        time.sleep(1)
    
    # Shutdown the server
    httpd.shutdown()
    server_thread.join()
    
    # Get the authorization code (we know it's not None now)
    auth_code = cast(str, httpd.auth_code)
    
    # Exchange the authorization code for tokens
    tokens = exchange_code_for_token(auth_code)
    
    if tokens:
        save_tokens(tokens)
        return tokens
    else:
        raise Exception("Failed to get access token")

def get_access_token() -> str:
    """Get a valid access token, refreshing if necessary"""
    tokens = load_tokens()
    
    if not tokens:
        return authenticate()['access_token']
    
    # Check if token is expired
    if 'expires_at' in tokens and tokens['expires_at'] < time.time():
        refresh_token = tokens.get('refresh_token')
        if refresh_token:
            new_tokens = refresh_access_token(refresh_token)
            if new_tokens:
                save_tokens(new_tokens)
                return new_tokens['access_token']
        # Fall back to re-authentication if refresh fails or no refresh token
        return authenticate()['access_token']
    else:
        return tokens['access_token']

if __name__ == '__main__':
    # If run directly, authenticate with Strava
    if not STRAVA_CLIENT_ID or not STRAVA_CLIENT_SECRET:
        print("Error: STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET must be set")
        print("Please create a .env file with these variables")
        exit(1)
        
    print("Starting authentication flow...")
    tokens = authenticate()
    if tokens:
        print("Authentication successful!")
        print(f"Access token will expire at: {time.ctime(tokens['expires_at'])}")
    else:
        print("Authentication failed!")
