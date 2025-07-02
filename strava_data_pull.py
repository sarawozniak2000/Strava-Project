import os
import requests
import pandas as pd
import sqlite3
import json
from dotenv import load_dotenv
from google.cloud import bigquery

# Load environment variables
load_dotenv()

CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")
DB_FILE = "strava_activities.db"
BQ_TABLE_ID = "vast-cogency-464203-t0.strava_activity_upload.strava_activity_upload" 

# Write credentials if provided
if "GCP_CREDENTIALS_JSON" in os.environ:
    with open("credentials.json", "w") as f:
        f.write(os.environ["GCP_CREDENTIALS_JSON"])
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

# Refresh Strava access token
def refresh_access_token():
    response = requests.post(
        url="https://www.strava.com/oauth/token",
        data={
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'grant_type': 'refresh_token',
            'refresh_token': REFRESH_TOKEN
        }
    )
    response.raise_for_status()
    return response.json()["access_token"]

# Pull all activities
def get_activities(access_token, per_page=200):
    activities = []
    page = 1
    while True:
        url = "https://www.strava.com/api/v3/athlete/activities"
        headers = {'Authorization': f'Bearer {access_token}'}
        params = {'per_page': per_page, 'page': page}
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        if not data:
            break
        activities.extend(data)
        page += 1
    return activities

# Store only new records in raw table
def store_new_activities(df):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS strava_data_allactivities (
            id INTEGER PRIMARY KEY,
            name TEXT,
            type TEXT,
            sport_type TEXT,
            moving_time INTEGER,
            elapsed_time INTEGER,
            distance REAL,
            total_elevation_gain REAL,
            start_date_local TEXT,
            timezone TEXT,
            kudos_count INTEGER,
            start_latlng TEXT,
            end_latlng TEXT,
            average_speed REAL,
            max_speed REAL,
            average_heartrate REAL,
            max_heartrate REAL,
            average_cadence REAL,
            average_watts REAL,
            kilojoules REAL,
            elev_high REAL,
            elev_low REAL
        )
    ''')

    df.columns = df.columns.str.replace('.', '_')
    allowed_columns = [
        'id', 'name', 'type', 'sport_type', 'moving_time', 'elapsed_time', 'distance',
        'total_elevation_gain', 'start_date_local', 'timezone', 'kudos_count',
        'start_latlng', 'end_latlng', 'average_speed', 'max_speed',
        'average_heartrate', 'max_heartrate', 'average_cadence', 'average_watts',
        'kilojoules', 'elev_high', 'elev_low'
    ]
    df = df[[col for col in allowed_columns if col in df.columns]]

    columns = df.columns.tolist()
    placeholders = ','.join(['?'] * len(columns))
    columns_sql = ','.join(columns)

    new_rows = 0
    for _, row in df.iterrows():
        cur.execute("SELECT 1 FROM strava_data_allactivities WHERE id = ?", (row["id"],))
        if not cur.fetchone():
            row_values = []
            for col in columns:
                val = row[col]
                if isinstance(val, list):
                    val = str(val)
                row_values.append(val)
            cur.execute(
                f"INSERT INTO strava_data_allactivities ({columns_sql}) VALUES ({placeholders})",
                tuple(row_values)
            )
            new_rows += 1

    conn.commit()
    conn.close()
    print(f"Inserted {new_rows} new records into strava_data_allactivities.")

# Transform and store new records in cleaned table
def transform_and_insert_cleaned():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS strava_data_cleaned (
            id INTEGER PRIMARY KEY,
            name TEXT,
            type TEXT,
            subtype TEXT,
            moving_time_mins REAL,
            elapsed_time_mins REAL,
            distance_miles REAL,
            total_elevation_gain REAL,
            local_start_date TEXT,
            local_start_time TEXT,
            timezone TEXT,
            timezone_name TEXT,
            kudos_count INTEGER,
            start_latlng TEXT,
            end_latlng TEXT,
            start_latitude TEXT,
            start_longitude TEXT,
            end_latitude TEXT,
            end_longitude TEXT,
            average_speed_mph REAL,
            max_speed_mph REAL,
            average_heartrate REAL,
            max_heartrate REAL,
            average_cadence REAL,
            average_watts REAL,
            kilojoules REAL,
            elevation_high REAL,
            elevation_low REAL,
            elevation_gain REAL
        )
    ''')

    cur.execute('DELETE FROM strava_data_cleaned')

    cur.execute('''
        INSERT INTO strava_data_cleaned (
            id, name, type, subtype,
            moving_time_mins, elapsed_time_mins, distance_miles, total_elevation_gain,
            local_start_date, local_start_time, timezone, timezone_name, kudos_count,
            start_latlng, end_latlng, start_latitude, start_longitude, end_latitude, end_longitude,
            average_speed_mph, max_speed_mph, average_heartrate, max_heartrate,
            average_cadence, average_watts, kilojoules,
            elevation_high, elevation_low, elevation_gain
        )
        SELECT
            id,
            name,
            type,
            sport_type,
            ROUND(moving_time / 60.0, 2),
            ROUND(elapsed_time / 60.0, 2),
            ROUND(distance / 1609.344, 2),
            total_elevation_gain,
            DATE(start_date_local),
            TIME(start_date_local),
            timezone,
            SUBSTR(timezone, INSTR(timezone, ')') + 2),
            kudos_count,
            start_latlng,
            end_latlng,
            TRIM(REPLACE(SUBSTR(start_latlng, 2, INSTR(start_latlng, ',') - 1), ']', '')),
            TRIM(REPLACE(SUBSTR(start_latlng, INSTR(start_latlng, ',') + 2), ']', '')),
            TRIM(REPLACE(SUBSTR(end_latlng, 2, INSTR(end_latlng, ',') - 1), ']', '')),
            TRIM(REPLACE(SUBSTR(end_latlng, INSTR(end_latlng, ',') + 2), ']', '')),
            ROUND(average_speed * 2.23694, 2),
            ROUND(max_speed * 2.23694, 2),
            average_heartrate,
            max_heartrate,
            average_cadence,
            average_watts,
            kilojoules,
            ROUND(elev_high / 3.280839, 2),
            ROUND(elev_low / 3.280839, 2),
            ROUND(total_elevation_gain / 3.280839, 2)
        FROM strava_data_allactivities
    ''')

    conn.commit()
    conn.close()
    print("Cleaned table rebuilt successfully.")

# Upload cleaned table to BigQuery
def upload_to_bigquery():
    conn = sqlite3.connect(DB_FILE)
    df_cleaned = pd.read_sql_query("SELECT * FROM strava_data_cleaned", conn)
    conn.close()

    client = bigquery.Client()
    job = client.load_table_from_dataframe(
        df_cleaned,
        BQ_TABLE_ID,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()
    print(f"Uploaded {len(df_cleaned)} rows to BigQuery table {BQ_TABLE_ID}.")

# Run everything
if __name__ == "__main__":
    print("Starting Strava data sync...")
    token = refresh_access_token()
    data = get_activities(token)
    df = pd.json_normalize(data)
    store_new_activities(df)
    transform_and_insert_cleaned()
    upload_to_bigquery()
    print("All data processed and synced to BigQuery.")
