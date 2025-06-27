# Strava Data Automation Script

import os
import requests
import pandas as pd
import sqlite3
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env
load_dotenv()

CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")

DB_FILE = "strava_activities.db"

print(f"Refreshing Token...")

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

print(f"Pulling Strava activities...")

# Get all Strava activities
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

print(f"Saving new activities...")

# Save new activities to SQLite
def store_new_activities(df):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # Ensure base table exists
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

    # Insert only new records
    for _, row in df.iterrows():
        cur.execute("SELECT 1 FROM strava_data_allactivities WHERE id = ?", (row["id"],))
        if not cur.fetchone():
            cur.execute('''
                INSERT INTO strava_data_allactivities VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''', tuple(row[col] for col in df.columns))

    conn.commit()
    conn.close()

print(f"Cleaning data...")

# Apply transformation to new records only
def transform_and_insert_cleaned():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute('''
        INSERT INTO strava_data_cleaned (
            name, type, subtype, moving_time_mins, elapsed_time_mins, distance_miles,
            total_elevation_gain, local_start_date, local_start_time, timezone, timezone_name,
            kudos_count, start_latlng, end_latlng, start_latitude, start_longitude,
            end_latitude, end_longitude, average_speed_mph, max_speed_mph,
            average_heartrate, max_heartrate, average_cadence, average_watts,
            kilojoules, elevation_high, elevation_low, elevation_gain
        )
        SELECT
            name,
            type,
            sport_type,
            ROUND(moving_time / 60,2),
            ROUND(elapsed_time / 60,2),
            ROUND(distance / 1609.344, 2),
            total_elevation_gain,
            DATE(start_date_local),
            TIME(start_date_local),
            timezone,
            SUBSTR(timezone, INSTR(timezone, ')') + 2),
            kudos_count,
            start_latlng,
            end_latlng,
            TRIM(REPLACE(SUBSTR(start_latlng,2,INSTR(start_latlng, ',') - 1), ']','')),
            TRIM(REPLACE(SUBSTR(start_latlng,INSTR(start_latlng, ',') + 2), ']','')),
            TRIM(REPLACE(SUBSTR(end_latlng,2,INSTR(end_latlng, ',') - 1), ']','')),
            TRIM(REPLACE(SUBSTR(end_latlng,INSTR(end_latlng, ',') + 2), ']','')),
            average_speed * 2.23694,
            max_speed * 2.23694,
            average_heartrate,
            max_heartrate,
            average_cadence,
            average_watts,
            kilojoules,
            ROUND(elev_high / 3.280839 ,2),
            ROUND(elev_low / 3.280839,2),
            ROUND(total_elevation_gain / 3.280839,2)
        FROM strava_data_allactivities
        WHERE id NOT IN (SELECT id FROM strava_data_cleaned)
    ''')

    conn.commit()
    conn.close()
print(f"Writing new records to file...")

if __name__ == "__main__":
    token = refresh_access_token()
    data = get_activities(token)
    df = pd.json_normalize(data)
    store_new_activities(df)
    transform_and_insert_cleaned()
    print(f"Imported and cleaned {len(df)} Strava records successfully.")
