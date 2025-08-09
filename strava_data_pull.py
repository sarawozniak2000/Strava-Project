# strava_data_pull.py
import os
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

# Drive API (OAuth as YOU)
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

load_dotenv()

# ----- Strava auth -----
CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")

# ----- BigQuery target -----
BQ_TABLE_ID = "vast-cogency-464203-t0.strava_activity_upload.strava_data_cleaned"

# ----- Google Drive (OAuth as your personal account) -----
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")          # folder ID from URL
OAUTH_CLIENT_ID = os.getenv("DRIVE_CLIENT_ID")
OAUTH_CLIENT_SECRET = os.getenv("DRIVE_CLIENT_SECRET")
OAUTH_REFRESH_TOKEN = os.getenv("DRIVE_REFRESH_TOKEN")
DRIVE_SCOPE = ["https://www.googleapis.com/auth/drive"]

# Fixed filename to overwrite each run
DRIVE_FILE_NAME = "strava_transformed.csv"

# ----------------------- Strava helpers -----------------------


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

# ----------------------- Transform -----------------------


def _safe_offset(lst, i):
    return lst[i] if isinstance(lst, (list, tuple)) and len(lst) > i else None


def transform_like_sql(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    df.columns = df.columns.str.replace(".", "_", regex=False)

    expected_cols = [
        "id", "name", "type", "sport_type", "moving_time", "elapsed_time", "distance",
        "total_elevation_gain", "start_date_local", "timezone", "start_latlng", "end_latlng",
        "average_speed", "max_speed", "average_heartrate", "max_heartrate",
        "average_cadence", "average_watts", "kilojoules", "elev_high", "elev_low", "kudos_count"
    ]
    for col in expected_cols:
        if col not in df:
            df[col] = None

    ts = pd.to_datetime(df["start_date_local"], errors="coerce", utc=True)

    out = pd.DataFrame({
        "id": df["id"],
        "name": df["name"],
        "type": df["type"],
        "subtype": df["sport_type"],
        "moving_time_mins": (df["moving_time"] / 60.0).round(2),
        "elapsed_time_mins": (df["elapsed_time"] / 60.0).round(2),
        "distance_miles": (df["distance"] / 1609.344).round(2),
        "total_elevation_gain": df["total_elevation_gain"],  # meters (raw)
        "local_start_date": ts.dt.tz_convert(None).dt.date,
        "local_start_time": ts.dt.tz_convert(None).dt.time,
        "timezone": df["timezone"],
        "timezone_name": df["timezone"].astype(str).str.split(r"\)\s+", n=1, regex=True).str[-1],
        "kudos_count": df["kudos_count"],
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
        # meters → feet
        "elevation_high": (df["elev_high"] * 3.280839).round(2) if "elev_high" in df else None,
        "elevation_low": (df["elev_low"] * 3.280839).round(2) if "elev_low" in df else None,
        "elevation_gain": (df["total_elevation_gain"] * 3.280839).round(2),
    })

    out["pace_min_per_mile"] = out.apply(
        lambda r: round(60 / r["average_speed_mph"], 2)
        if r["average_speed_mph"] and r["average_speed_mph"] > 0 else None,
        axis=1
    )

    out.columns = (
        out.columns.str.strip()
        .str.replace(r"[^0-9a-zA-Z_]", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
    )
    return out

# ----------------------- BigQuery -----------------------


def upload_to_bigquery(df: pd.DataFrame, table_id: str):
    client = bigquery.Client()  # uses GOOGLE_APPLICATION_CREDENTIALS
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    print(f"Uploaded {len(df)} rows to BigQuery table {table_id}.")

# ----------------------- Drive (OAuth) -----------------------


def _build_drive_service_with_oauth():
    client_id = os.getenv("DRIVE_CLIENT_ID")
    client_secret = os.getenv("DRIVE_CLIENT_SECRET")
    refresh_token = os.getenv("DRIVE_REFRESH_TOKEN")
    if not (client_id and client_secret and refresh_token):
        raise RuntimeError(
            "Missing DRIVE OAuth secrets (client id/secret/refresh token).")
    creds = UserCredentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=DRIVE_SCOPE,
    )
    creds.refresh(Request())  # fetch access token
    return build("drive", "v3", credentials=creds)


def _resolve_folder(service, fid: str) -> str:
    """Resolve real folder ID (handles shortcuts)."""
    meta = service.files().get(
        fileId=fid,
        fields="id,name,mimeType,shortcutDetails",
    ).execute()
    mt = meta.get("mimeType")
    if mt == "application/vnd.google-apps.shortcut":
        target = meta.get("shortcutDetails", {}).get("targetId")
        if not target:
            raise RuntimeError(
                f"Folder ID {fid} is a shortcut without targetId.")
        return _resolve_folder(service, target)
    if mt != "application/vnd.google-apps.folder":
        raise RuntimeError(f"ID {fid} is not a folder (mimeType={mt}).")
    return meta["id"]


def upload_csv_to_drive_overwrite(local_csv_path: str, folder_id: str, file_name: str):
    if not folder_id:
        print("DRIVE_FOLDER_ID not set; skipping Drive upload.")
        return

    service = _build_drive_service_with_oauth()
    resolved_folder_id = _resolve_folder(service, folder_id)

    # Look for an existing file with the same name in the target folder
    query = (
        f"'{resolved_folder_id}' in parents and "
        f"name='{file_name}' and trashed=false"
    )
    results = service.files().list(
        q=query,
        spaces="drive",
        fields="files(id,name)",
    ).execute()
    files = results.get("files", [])

    media = MediaFileUpload(
        local_csv_path, mimetype="text/csv", resumable=True)

    if files:
        # Overwrite existing file content
        file_id = files[0]["id"]
        updated = service.files().update(
            fileId=file_id,
            media_body=media,
        ).execute()
        print(
            f"Overwritten file on Drive: {updated.get('name')} (id: {updated.get('id')})")
    else:
        # Create new file
        file_metadata = {"name": file_name, "parents": [resolved_folder_id]}
        created = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id,name,parents",
        ).execute()
        print(
            f"Uploaded new file to Drive: {created.get('name')} (id: {created.get('id')})")


# ----------------------- Main -----------------------
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

    # Save fixed-name CSV (no date suffix)
    out_name = DRIVE_FILE_NAME
    df_clean.to_csv(out_name, index=False)

    print("Uploading CSV to Google Drive (overwrite mode)…")
    upload_csv_to_drive_overwrite(out_name, DRIVE_FOLDER_ID, DRIVE_FILE_NAME)

    print("Done.")
