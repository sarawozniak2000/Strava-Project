import os
import pandas as pd
import streamlit as st
from datetime import date, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account


PROJECT = "vast-cogency-464203-t0"
TABLE = "strava_activity_upload.strava_data_cleaned"
FULL_TABLE = f"`{PROJECT}.{TABLE}`"

st.set_page_config(page_title="Strava Explorer", layout="wide")
st.title("Strava Insights")

# ---------- BigQuery client (works locally and on Streamlit Cloud) ----------


def make_bq_client():
    # Streamlit Community Cloud-  service account JSON in st.secrets["gcp_service_account"]
    if "gcp_service_account" in st.secrets:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(credentials=creds, project=creds.project_id)

    # Local: set GOOGLE_APPLICATION_CREDENTIALS to a key file (or be gcloud-auth'd)
    return bigquery.Client(project=PROJECT)


client = make_bq_client()

# ---------- Sidebar filters ----------
st.sidebar.header("Filters")

# fetch quick domains for filters


@st.cache_data(ttl=3600)
def fetch_domains():
    q = f"""
    SELECT
      ARRAY_AGG(DISTINCT COALESCE(subtype,'')) AS sports,
      ARRAY_AGG(DISTINCT COALESCE(city,'')) AS cities,
      MIN(local_start_date) AS min_d,
      MAX(local_start_date) AS max_d
    FROM {FULL_TABLE}
    """
    row = client.query(q).result().to_dataframe().iloc[0]
    return sorted([s for s in row["sports"] if s]), sorted([c for c in row["cities"] if c]), row["min_d"], row["max_d"]


sports, cities, min_d, max_d = fetch_domains()

# Add "(All)" option to both lists
sports_options = ["(All)"] + sports
cities_options = ["(All)"] + cities

# Sidebar multiselects
sel_sports = st.sidebar.multiselect(
    "Activity Type",
    sports_options,
    default=["(All)"]  # default is All
)

sel_cities = st.sidebar.multiselect(
    "City",
    cities_options,
    default=["(All)"]  # default is All
)

# Handle logic: if All is selected, treat it as everything
if "(All)" in sel_sports:
    sel_sports = sports  # use the full sports list

if "(All)" in sel_cities:
    sel_cities = cities  # use the full cities list


# ---------- Query data ----------
@st.cache_data(ttl=600, show_spinner=False)
def load_data(sport, cities, start_d, end_d):
    params = [
        bigquery.ScalarQueryParameter("start_d", "DATE", start_d),
        bigquery.ScalarQueryParameter("end_d", "DATE", end_d),
        bigquery.ScalarQueryParameter(
            "sport", "STRING", "" if sport == "(All)" else sport),
    ]
    cities_cond = "TRUE"
    if cities:
        # Use UNNEST for IN-list safely
        params.append(bigquery.ArrayQueryParameter("cities", "STRING", cities))
        cities_cond = "city IN UNNEST(@cities)"

    sql = f"""
    WITH b AS (
      SELECT
        local_start_date AS d,
        subtype,
        name,
        distance_miles,
        elevation_gain,
        pace_min_per_mile,
        city,
        start_latitude, start_longitude
      FROM {FULL_TABLE}
      WHERE local_start_date BETWEEN @start_d AND @end_d
        AND (@sport = '' OR subtype = @sport)
        AND {cities_cond}
    )
    SELECT * FROM b ORDER BY d DESC
    """
    job = client.query(
        sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    return job.result().to_dataframe()


df = load_data(sport, sel_cities, start_d, end_d)

# ---------- KPIs ----------
c1, c2, c3, c4 = st.columns(4)
c1.metric("Activities", len(df))
c2.metric("Total miles", round(
    df["distance_miles"].sum(), 1) if not df.empty else 0)
c3.metric("Median pace (min/mi)",
          round(df["pace_min_per_mile"].median(), 2) if not df.empty else 0)
c4.metric("Total elevation (ft)", int(
    df["elevation_gain"].sum()) if not df.empty else 0)

st.divider()

# ---------- Charts ----------
if df.empty:
    st.info("No data for the current filters.")
else:
    # Trends
    t1, t2 = st.columns(2)
    with t1:
        miles_by_day = df.groupby("d", as_index=False)["distance_miles"].sum()
        st.subheader("Miles over time")
        st.line_chart(miles_by_day.set_index("d"))

    with t2:
        pace_by_day = df.groupby("d", as_index=False)[
            "pace_min_per_mile"].median()
        st.subheader("Median pace over time")
        st.line_chart(pace_by_day.set_index("d"))

    # Breakdowns
    b1, b2 = st.columns(2)
    with b1:
        st.subheader("Miles by city")
        by_city = df.groupby("city", as_index=False)["distance_miles"].sum(
        ).sort_values("distance_miles", ascending=False).head(10)
        st.bar_chart(by_city, x="city", y="distance_miles")

    with b2:
        st.subheader("Elevation by day")
        elev_by_day = df.groupby("d", as_index=False)["elevation_gain"].sum()
        st.area_chart(elev_by_day.set_index("d"))

    # Map
    st.subheader("Start locations")
    map_df = df.dropna(subset=["start_latitude", "start_longitude"])[
        ["start_latitude", "start_longitude"]]
    map_df = map_df.rename(
        columns={"start_latitude": "lat", "start_longitude": "lon"})
    if not map_df.empty:
        st.map(map_df, size=3)
    else:
        st.caption("No coordinates to map for the selected filters.")

    # Table
    st.subheader("Activities")
    st.dataframe(
        df[["d", "subtype", "city", "name", "distance_miles",
            "pace_min_per_mile", "elevation_gain"]],
        use_container_width=True,
        hide_index=True,
    )
