# Strava Segment Backfill

This package provides tools for efficiently backfilling segment efforts and segment details from Strava while respecting API rate limits.

## Overview

The backfill process is split into two main phases:
1. **Segment Efforts**: Fetch segment efforts for activities where this data is missing
2. **Segment Details**: Fetch detailed information for segments that appear in segment efforts

This two-phase approach allows for prioritization and efficient use of API calls.

## API Rate Limits

Strava API has the following rate limits:
- 100 requests every 15 minutes (short-term)
- 1,000 requests per day (long-term)

The backfill tools implement automatic rate limiting with a conservative buffer (90% of the limit) to avoid hitting these limits.

## Scripts

### 1. `update_schema.py`

Updates the database schema to support the incremental backfill process by:
- Creating tables if they don't exist (activities, segments, segment_efforts)
- Adding a `segment_efforts_processed` flag to the activities table
- Creating appropriate indices for efficient querying

Usage:
```
python update_schema.py --db data/segments.db --create
```

The `--create` flag allows the script to create a new database if it doesn't exist.

### 2. `incremental_backfill.py`

The core backfill tool that fetches segment efforts and segment details from Strava.

Usage:
```
python incremental_backfill.py [--mode {both|efforts|segments}] [--activities N] [--segments N] [--db DB_PATH]
```

Options:
- `--mode`: Choose to backfill segment efforts, segment details, or both
- `--activities`: Maximum number of activities to process in one run
- `--segments`: Maximum number of segments to process in one run
- `--db`: Path to the SQLite database

### 3. `manage_backfill.py`

High-level script to manage different backfill strategies.

Usage:
```
python manage_backfill.py [--mode {one-time|continuous|stats}] [OPTIONS]
```

Options:
- `--mode`: Choose between one-time backfill, continuous incremental backfill, or view stats
- `--activities`: Number of activities to process per batch
- `--segments`: Number of segments to process per batch
- `--interval`: Seconds between backfill cycles (continuous mode)
- `--max-runs`: Maximum number of runs (0 for unlimited, continuous mode only)
- `--db`: Path to the SQLite database
- `--env`: Path to the .env file with Strava credentials (default: .env)
- `--state`: Path to the state file to track progress (default: backfill_state.json)

## Authentication

There are two ways to authenticate with the Strava API:

### 1. Direct Access Token

You can set a Strava API access token as an environment variable:

```
export STRAVA_ACCESS_TOKEN=your_access_token
```

This approach is simple but requires manually refreshing the token when it expires.

### 2. OAuth 2.0 (Recommended)

The preferred approach is to use OAuth with refresh tokens, which allows the system to automatically refresh access tokens as needed.

#### Step 1: Create `.env` file with your credentials
```
STRAVA_CLIENT_ID=your_client_id
STRAVA_CLIENT_SECRET=your_client_secret
```

#### Step 2: Run the authentication setup script
```
python setup_auth.py
```

This script will:
1. Open a browser window for you to authorize the app with Strava
2. Save the refresh token to the database
3. Create a tokens.json file with all token information

#### Token Storage Locations
The refresh token can be stored in:
- `.env` file as `STRAVA_REFRESH_TOKEN=your_refresh_token`
- Database `tokens` table (automatically handled by setup_auth.py)
- A `tokens.json` file in the project root (automatically created by setup_auth.py)

## Best Practices

1. **Start with segment efforts**: Populate segment efforts first, then segment details
2. **Batch sizes**: Choose batch sizes based on your API usage patterns and total data volume
3. **Continuous mode**: For large datasets, use continuous mode with longer intervals
4. **Monitoring**: Check the backfill.log file for progress and any errors

## Example Workflows

### View Current Backfill Status

```bash
python manage_backfill.py --mode stats --db data/segments.db
```

### Complete One-time Backfill (with OAuth)

```bash
python manage_backfill.py --mode one-time --activities 10 --segments 20 --db data/segments.db
```

### Continuous Backfill

```bash
python manage_backfill.py --mode continuous --activities 5 --segments 10 --interval 300 --db data/segments.db
```

### Backfill Segment Efforts Only (with direct access token)

```bash
export STRAVA_ACCESS_TOKEN=your_access_token
python incremental_backfill.py --mode efforts --activities 20 --db data/segments.db
```

## Efficiency Optimizations

1. **Selective Backfill**: Only processes activities and segments that need data
2. **Batch Processing**: Processes data in configurable batches to optimize API usage
3. **Rate Limiting**: Automatic rate limiting to respect Strava's API limits
4. **Database Indices**: Creates appropriate indices for efficient querying
5. **Error Handling**: Robust error handling to continue processing despite individual failures

## Troubleshooting

### "No refresh token found" Error

If you see an error like:
```
ERROR - No refresh token found. Please authenticate with Strava first.
```

Run the setup_auth.py script to complete the OAuth authorization process:
```
python setup_auth.py
```

### Token Expiration Issues

If your refresh token becomes invalid:
1. Delete any existing tokens.json file
2. Run `python setup_auth.py` to generate a new refresh token

### Import Issues with Strava Archive

If you have issues importing from a Strava data export:
1. Ensure the zip file is a complete Strava data export
2. Check that it contains an activities.csv file
3. Try extracting it manually and importing the CSV directly

### Database Schema Issues

If you encounter schema-related errors:
1. Check your database schema with: `sqlite3 data/segments.db ".schema table_name"`
2. The code adapts to different schema structures, but if you're getting errors, you might need to modify the database columns
3. Common error types:
   - "no such column": The column doesn't exist in your database
   - "type X is not supported": Need to convert the data type before insertion
