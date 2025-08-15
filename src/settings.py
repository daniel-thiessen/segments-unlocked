from dotenv import load_dotenv
import os
import json

# Load environment variables from .env file
load_dotenv()

# Strava API credentials
STRAVA_CLIENT_ID = os.getenv('STRAVA_CLIENT_ID')
STRAVA_CLIENT_SECRET = os.getenv('STRAVA_CLIENT_SECRET')
STRAVA_REDIRECT_URI = os.getenv('STRAVA_REDIRECT_URI', 'http://localhost:8000/callback')

# API endpoints
STRAVA_AUTH_URL = 'https://www.strava.com/oauth/authorize'
STRAVA_TOKEN_URL = 'https://www.strava.com/oauth/token'
STRAVA_API_BASE = 'https://www.strava.com/api/v3'

# Database settings
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'segments.db')

# Token storage path
TOKEN_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'tokens.json')

# Rate limiting settings
RATE_LIMIT_REQUESTS = 100  # Strava API allows 100 requests
RATE_LIMIT_PERIOD = 900  # per 15 minutes (900 seconds)

# Application settings
DEFAULT_ACTIVITY_LIMIT = 50  # Number of activities to retrieve by default

def save_tokens(tokens):
    """Save tokens to file"""
    os.makedirs(os.path.dirname(TOKEN_PATH), exist_ok=True)
    with open(TOKEN_PATH, 'w') as f:
        json.dump(tokens, f)

def load_tokens():
    """Load tokens from file"""
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'r') as f:
            return json.load(f)
    return None
