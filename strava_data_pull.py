# strava_data_pull.py
import os
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

# Load environment variables
load_dotenv()

CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")
BQ_TABLE_ID = "vast-cogency-464203-t0.strava_activity_upload.strava_data_allactivities"

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

# Upload all activity data to BigQuery (truncate and replace)
def upload_to_bigquery(df):
    client = bigquery.Client()
    job = client.load_table_from_dataframe(
        df,
        BQ_TABLE_ID,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()
    print(f"Uploaded {len(df)} rows to BigQuery table {BQ_TABLE_ID}.")

# Run pipeline
if __name__ == "__main__":
    print("Starting Strava data sync...")
    token = refresh_access_token()
    data = get_activities(token)
    df = pd.json_normalize(data)
    upload_to_bigquery(df)
    print("Raw activity data uploaded to BigQuery.")
