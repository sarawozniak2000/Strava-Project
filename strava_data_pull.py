import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")
print(f"Variables loaded")

# Refresh access token
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

print(f"Access token refreshed")

# Pull data
def get_activities(access_token, per_page=200): #200 is the most Strava API allows
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
print(f"Activity data pulled")

# Run and export to CSV
if __name__ == "__main__":
    token = refresh_access_token()
    activities = get_activities(token)
    df = pd.json_normalize(activities)  # Flatten nested JSON
    df.to_csv("strava_data_allactivities.csv", index=False)
    print(f"Exported {len(df)}  activities to strava_data_allactivities.csv")
