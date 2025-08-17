# Segments Unlocked

A personal Strava segment tracker that helps you analyze and visualize your performance on segments over time.

## Features

- OAuth2 authentication with Strava
- Retrieve your activities and segment efforts
- Smart activity fetching (only new activities since last pull)
- Import data from Strava activity archives to avoid API rate limiting
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
│   ├── visualization.py  # Plotting and dashboard components
│   ├── archive_import.py # Import data from Strava activity archives
│   └── timestamp_utils.py # Utilities for timestamp operations
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

## Usage

### Basic Usage

```bash
# Generate visualizations (default action)
python app.py

# Fetch latest activities from Strava API
python app.py --fetch

# Fetch only new activities since the last pull
python app.py --fetch-new

# Fetch a specific number of activities
python app.py --fetch --limit 100

# Generate visualizations after fetching data
python app.py --fetch --visualize

# View recent activities and their segments
python app.py --recent-activities

# View activities from the last 60 days
python app.py --recent-activities --recent-days 60
```

### Import from Strava Archive

You can import activities from a Strava archive export to avoid API rate limiting:

1. Request your data archive from Strava (Profile > Settings > My Account > Download or Delete Your Account)
2. Once downloaded, use the import feature:

```bash
# Import from a Strava archive ZIP file
python app.py --import-archive path/to/your/strava_archive.zip

# Import from an already extracted archive directory
python app.py --import-archive path/to/extracted/archive/directory

# Import archive and fetch additional segment details during import
python app.py --import-archive path/to/your/strava_archive.zip --fetch-segment-details

# Import archive first, then backfill segment details incrementally later
python app.py --import-archive path/to/your/strava_archive.zip
python app.py --fetch-segment-details  # Run this separately to backfill segment data

# Import archive and generate visualizations
python app.py --import-archive path/to/your/strava_archive.zip --visualize
```

### Incremental Segment Data Backfilling

When importing data from a Strava archive, you have the option to incrementally backfill segment details to enhance your local dataset:

1. **Understanding Strava Archives**:
   - Strava archives may contain limited segment data in their export files.
   - Full segment details (like exact coordinates, elevation profiles) may need to be fetched separately.

2. **Two-phase approach**:
   - **Initial Import**: Import activities and basic segment data from the archive.
   - **Incremental Backfill**: Fetch detailed segment information from the Strava API.

3. **Usage Options**:
   - **Combined Operation**: `--import-archive path/to/archive --fetch-segment-details`  
     Imports archive and immediately begins fetching segment details.
   
   - **Separated Operation**: 
     ```
     # First import the archive
     python app.py --import-archive path/to/archive
     
     # Later, fetch segment details incrementally
     python app.py --fetch-segment-details
     ```
     This approach is recommended for large archives to respect API rate limits.

4. **Benefits**:
   - Respects Strava API rate limits (200 requests per 15 minutes)
   - Picks up where it left off if interrupted
   - Only processes segments with incomplete data
   - Enables better visualizations with complete segment information

## Dependencies

- requests: HTTP requests to Strava API
- pandas: Data manipulation and analysis
- matplotlib/plotly: Data visualization
- sqlite3: Local data storage
- flask: (Optional) For web dashboard
