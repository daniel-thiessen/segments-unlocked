# Segments Unlocked

A personal Strava segment tracker that helps you analyze and visualize your performance on segments over time.

## Features

- OAuth2 authentication with Strava
- Retrieve your activities and segment efforts
- Store performance data locally
- Analyze segment performance trends over time
- Visualize your progress with charts and maps

## Project Structure

```
segments-unlocked/
├── src/
│   ├── auth.py           # Strava OAuth2 authentication
│   ├── data_retrieval.py # Functions to fetch data from Strava API
│   ├── storage.py        # Database handling and storage operations
│   ├── analysis.py       # Data processing and analytics
│   └── visualization.py  # Plotting and dashboard components
├── config/
│   └── settings.py       # App configuration (tokens, IDs, etc.)
├── data/
│   └── segments.db       # SQLite database (gitignored)
└── app.py                # Main application entry point
```

## Setup

1. Register a Strava API application at https://developers.strava.com
2. Copy your Client ID and Secret to `config/settings.py`
3. Install dependencies: `pip install -r requirements.txt`
4. Run the authentication flow: `python src/auth.py`
5. Start analyzing your segments: `python app.py`

## Dependencies

- requests: HTTP requests to Strava API
- pandas: Data manipulation and analysis
- matplotlib/plotly: Data visualization
- sqlite3: Local data storage
- flask: (Optional) For web dashboard
