# strava_data_pull.py
from google.oauth2 import service_account
import os
import requests
import time
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from datetime import datetime

# Drive API (OAuth as YOU)
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# Reverse geocoding (OpenStreetMap / Nominatim)
from geopy.geocoders import Nominatim

load_dotenv()

# ----- Strava auth (env from GitHub Secrets or .env locally) -----
CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")

# ----- BigQuery target (service account via GOOGLE_APPLICATION_CREDENTIALS) -----
BQ_TABLE_ID = "vast-cogency-464203-t0.strava_activity_upload.strava_data_cleaned"

# ----- Google Drive target (OAuth as your personal account) -----
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
OAUTH_CLIENT_ID = os.getenv("DRIVE_CLIENT_ID")
OAUTH_CLIENT_SECRET = os.getenv("DRIVE_CLIENT_SECRET")
OAUTH_REFRESH_TOKEN = os.getenv("DRIVE_REFRESH_TOKEN")
DRIVE_SCOPE = ["https://www.googleapis.com/auth/drive"]

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

    # Drop original lat/lon columns
    out = out.drop(columns=["start_latlng", "end_latlng"])

    return out

# ----------------------- Reverse Geocoding (OSM/Nominatim) -----------------------


def _round_key(lat, lon, places=3):
    return f"{round(float(lat), places)}|{round(float(lon), places)}"


def add_reverse_geocode_columns(df: pd.DataFrame,
                                lat_col: str = "start_latitude",
                                lon_col: str = "start_longitude",
                                places: int = 3) -> pd.DataFrame:
    """
    Adds 'city', 'state', 'country' columns by reverse-geocoding unique (lat, lon).
    Uses OpenStreetMap Nominatim, respects ~1 req/sec, and caches by rounded coords.
    """
    # Prepare keys for rows that have coordinates
    def make_key(row):
        lat, lon = row.get(lat_col), row.get(lon_col)
        if pd.notnull(lat) and pd.notnull(lon):
            try:
                return _round_key(lat, lon, places)
            except Exception:
                return None
        return None

    keys_series = df.apply(make_key, axis=1)
    unique_keys = sorted({k for k in keys_series.dropna().unique()})

    if not unique_keys:
        # No coordinates present – just add empty cols if missing
        for c in ["city", "state", "country"]:
            if c not in df.columns:
                df[c] = None
        return df

    geolocator = Nominatim(user_agent="strava_city_lookup")
    mapping = {}
    last_call = 0.0

    for k in unique_keys:
        lat_s, lon_s = k.split("|")
        lat, lon = float(lat_s), float(lon_s)

        # rate limit: ~1 request/sec
        now = time.time()
        delta = now - last_call
        if delta < 1.05:
            time.sleep(1.05 - delta)

        loc = geolocator.reverse((lat, lon), language="en", zoom=10)
        last_call = time.time()

        if not loc or not getattr(loc, "raw", None):
            mapping[k] = (None, None, None)
            continue

        addr = loc.raw.get("address", {})
        city = addr.get("city") or addr.get(
            "town") or addr.get("village") or addr.get("hamlet")
        state = addr.get("state")
        country = addr.get("country")
        mapping[k] = (city, state, country)

    # Map back to rows
    vals = keys_series.map(lambda k: mapping.get(k, (None, None, None)))
    city_state_country = pd.DataFrame(
        vals.tolist(), columns=["city", "state", "country"])
    for col in ["city", "state", "country"]:
        df[col] = city_state_country[col]

    return df

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


# ----------------------- Drive (Service Account + Overwrite) -----------------------


def _build_drive_service_with_service_account():
    # Load credentials from GOOGLE_APPLICATION_CREDENTIALS (set in GitHub Actions)
    sa_path = os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
    creds = service_account.Credentials.from_service_account_file(
        sa_path,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)


def upload_csv_to_drive_overwrite(local_csv_path: str, folder_id: str, target_file_name: str):
    """
    Uploads a CSV to Google Drive. If a file with the same name exists in the folder,
    it will be overwritten. Otherwise, a new file is created.
    """
    if not folder_id:
        print("DRIVE_FOLDER_ID not set; skipping Drive upload.")
        return

    service = _build_drive_service_with_service_account()

    # Resolve folder (handle shortcuts)
    def resolve_folder(fid: str) -> str:
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
            return resolve_folder(target)
        if mt != "application/vnd.google-apps.folder":
            raise RuntimeError(f"ID {fid} is not a folder (mimeType={mt}).")
        return meta["id"]

    resolved_folder_id = resolve_folder(folder_id)

    # Search for existing file with the same name in the folder
    query = f"'{resolved_folder_id}' in parents and name = '{target_file_name}' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    media = MediaFileUpload(
        local_csv_path, mimetype="text/csv", resumable=True)

    if files:
        # Overwrite existing file
        file_id = files[0]["id"]
        updated = service.files().update(
            fileId=file_id,
            media_body=media,
        ).execute()
        print(
            f"Overwritten Drive file: {updated.get('name')} (id: {updated.get('id')})")
    else:
        # Create new file
        file_metadata = {"name": target_file_name,
                         "parents": [resolved_folder_id]}
        created = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id,name,parents",
        ).execute()
        print(
            f"Created new Drive file: {created.get('name')} (id: {created.get('id')})")


# ----------------------- Main -----------------------
if __name__ == "__main__":
    print("Starting Strava data sync...")
    token = refresh_access_token()
    data = get_activities(token)

    raw = pd.json_normalize(data)
    raw.columns = raw.columns.str.replace(".", "_", regex=False)

    print("Transforming in Python...")
    df_clean = transform_like_sql(raw)

    print("Reverse geocoding start coordinates with OSM/Nominatim…")
    df_clean = add_reverse_geocode_columns(
        df_clean, "start_latitude", "start_longitude", places=3)

    print("Uploading transformed data to BigQuery...")
    upload_to_bigquery(df_clean, BQ_TABLE_ID)

    out_name = f"strava_transformed_{pd.Timestamp.utcnow():%Y%m%d}.csv"
    df_clean.to_csv(out_name, index=False)

    print("Uploading CSV to Google Drive (overwrite mode)…")
    # Always overwrite with the same name in Drive
    DRIVE_FILE_NAME = "strava_transformed.csv"
    upload_csv_to_drive_overwrite(out_name, DRIVE_FOLDER_ID, DRIVE_FILE_NAME)

    print("Done.")
