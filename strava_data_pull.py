# strava_data_pull.py
import os
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

# Optional: Drive upload
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

load_dotenv()

CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")

# BigQuery table for TRANSFORMED data
BQ_TABLE_ID = "vast-cogency-464203-t0.strava_activity_upload.strava_data_cleaned"

# Optional Drive target (set as a GitHub secret -> env)
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")

# Write GCP credentials file if provided (so both BQ + Drive can auth)
if "GCP_CREDENTIALS_JSON" in os.environ:
    with open("credentials.json", "w") as f:
        f.write(os.environ["GCP_CREDENTIALS_JSON"])
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"


def refresh_access_token():
    r = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": REFRESH_TOKEN,
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def get_activities(access_token, per_page=200):
    activities, page = [], 1
    while True:
        resp = requests.get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"per_page": per_page, "page": page},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        activities.extend(data)
        page += 1
    return activities


def _safe_offset(lst, i):
    return lst[i] if isinstance(lst, (list, tuple)) and len(lst) > i else None


def transform_like_sql(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()

    # flatten dotted cols from json_normalize earlier
    df.columns = df.columns.str.replace(".", "_", regex=False)

    # make sure columns exist even if missing from some rows
    for col in [
        "id", "name", "type", "sport_type", "moving_time", "elapsed_time", "distance",
        "total_elevation_gain", "start_date_local", "timezone", "start_latlng", "end_latlng",
        "average_speed", "max_speed", "average_heartrate", "max_heartrate",
        "average_cadence", "average_watts", "kilojoules", "elev_high", "elev_low"
    ]:
        if col not in df:
            df[col] = None

    # Timestamp parse
    # start_date_local example format: '2024-01-01T08:30:00Z'
    ts = pd.to_datetime(df["start_date_local"], errors="coerce", utc=True)

    # Build output columns to match your SQL SELECT list
    out = pd.DataFrame({
        "id": df["id"],
        "name": df["name"],
        "type": df["type"],
        "subtype": df["sport_type"],
        "moving_time_mins": (df["moving_time"] / 60.0).round(2),
        "elapsed_time_mins": (df["elapsed_time"] / 60.0).round(2),
        "distance_miles": (df["distance"] / 1609.344).round(2),
        "total_elevation_gain": df["total_elevation_gain"],
        "local_start_date": ts.dt.tz_convert(None).dt.date,
        "local_start_time": ts.dt.tz_convert(None).dt.time,
        "timezone": df["timezone"],
        # after ') ' e.g. "(GMT-07:00) America/Denver" -> "America/Denver"
        "timezone_name": df["timezone"].astype(str).str.split(r"\)\s+", n=1, regex=True).str[-1],
        "kudos_count": df.get("kudos_count"),
        "start_latlng": df["start_latlng"],
        "end_latlng": df["end_latlng"],
        "start_latitude": df["start_latlng"].apply(lambda x: _safe_offset(x, 0)),
        "start_longitude": df["start_latlng"].apply(lambda x: _safe_offset(x, 1)),
        "end_latitude": df["end_latlng"].apply(lambda x: _safe_offset(x, 0)),
        "end_longitude": df["end_latlng"].apply(lambda x: _safe_offset(x, 1)),
        "average_speed_mph": (df["average_speed"] * 2.23694).round(2),
        "max_speed_mph": (df["max_speed"] * 2.23694).round(2),
        "average_heartrate": df["average_heartrate"],
        "max_heartrate": df["max_heartrate"],
        "average_cadence": df["average_cadence"],
        "average_watts": df["average_watts"],
        "kilojoules": df["kilojoules"],
        # Mirrors your SQL (divide by 3.280839). Change to *3.280839 if you intended feet.
        "elevation_high": (df["elev_high"] / 3.280839).round(2) if "elev_high" in df else None,
        "elevation_low": (df["elev_low"] / 3.280839).round(2) if "elev_low" in df else None,
        "elevation_gain": (df["total_elevation_gain"] / 3.280839).round(2),
    })

    # BQ-safe column names
    out.columns = (
        out.columns.str.strip()
        .str.replace(r"[^0-9a-zA-Z_]", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
    )
    return out


def upload_to_bigquery(df: pd.DataFrame, table_id: str):
    client = bigquery.Client()
    job = client.load_table_from_dataframe(
        df, table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    print(f"Uploaded {len(df)} rows to BigQuery table {table_id}.")


def upload_csv_to_drive(local_csv_path: str, folder_id: str):
    if not folder_id:
        print("DRIVE_FOLDER_ID not set; skipping Drive upload.")
        return
    creds = service_account.Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        scopes=["https://www.googleapis.com/auth/drive.file"],
    )
    service = build("drive", "v3", credentials=creds)
    meta = {"name": os.path.basename(local_csv_path), "parents": [folder_id]}
    media = MediaFileUpload(
        local_csv_path, mimetype="text/csv", resumable=True)
    file = service.files().create(body=meta, media_body=media, fields="id,name").execute()
    print(f"Uploaded to Drive: {file.get('name')} (id: {file.get('id')})")


if __name__ == "__main__":
    print("Starting Strava data sync...")
    token = refresh_access_token()
    data = get_activities(token)

    raw = pd.json_normalize(data)
    raw.columns = raw.columns.str.replace(".", "_", regex=False)

    print("Transforming in Python...")
    df_clean = transform_like_sql(raw)

    print("Uploading transformed data to BigQuery...")
    upload_to_bigquery(df_clean, BQ_TABLE_ID)

    # Save & upload the same transformed extract to Drive
    out_name = f"strava_transformed_{pd.Timestamp.utcnow():%Y%m%d}.csv"
    df_clean.to_csv(out_name, index=False)
    upload_csv_to_drive(out_name, DRIVE_FOLDER_ID)

    print("Done.")
