# Strava Data Analytics Project

This project automates the process of pulling, transforming, and analyzing data from Strava, with results uploaded to Google BigQuery and Google Drive. It is designed to help you familiarize yourself with the end-to-end workflow of a data analytics project.

## Features

- **Automated Data Sync:** Weekly GitHub Actions workflow to fetch and process Strava data.
- **Data Transformation:** SQL-based cleaning and transformation of raw data.
- **Cloud Integration:** Uploads processed data to Google BigQuery and Google Drive.
- **Configurable & Secure:** Uses GitHub Secrets for credentials and tokens.

## Repository Structure

```
.
├── .github/workflows/strava_sync.yml   # GitHub Actions workflow for automation
├── strava_data_pull.py                 # Main script for pulling and processing Strava data
├── transform_cleaned_table.sql         # SQL script for data transformation
├── requirements.txt                    # Python dependencies
├── README.md                           # Project documentation
└── Idea doc                            # (Your project notes/ideas)
```

## Getting Started

### Prerequisites

- Python 3.10+
- Access to Strava API credentials
- Google Cloud Platform service account (for BigQuery)
- Google Drive API credentials

### Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/Strava-Project.git
    cd Strava-Project
    ```

2. Install dependencies:
    ```sh
    pip install -r requirements.txt
    pip install google-auth-oauthlib pandas-gbq>=0.26.1
    ```

### Configuration

Set up the following secrets in your GitHub repository for the workflow to function:

- `STRAVA_CLIENT_ID`
- `STRAVA_CLIENT_SECRET`
- `STRAVA_REFRESH_TOKEN`
- `GCP_CREDENTIALS_B64` (base64-encoded GCP service account JSON)
- `DRIVE_FOLDER_ID`
- `DRIVE_CLIENT_ID`
- `DRIVE_CLIENT_SECRET`
- `DRIVE_REFRESH_TOKEN`

### Usage

- To run the data pull and transformation manually:
    ```sh
    python strava_data_pull.py
    ```
- The workflow in [.github/workflows/strava_sync.yml](.github/workflows/strava_sync.yml) will run automatically every Monday at 12:00 UTC.

## Files

- [`strava_data_pull.py`](strava_data_pull.py): Main script for data extraction and upload.
- [`transform_cleaned_table.sql`](transform_cleaned_table.sql): SQL script for cleaning and transforming the data.
- [`requirements.txt`](requirements.txt): Python dependencies.
- [`.github/workflows/strava_sync.yml`](.github/workflows/strava_sync.yml): GitHub Actions workflow for automation.

---

*Dipping my toes into some data analytics projects to familiarize myself with the start-to-finish process of anDipping my toes into some data analytics projects to familiarize myself with the start-to-finish process of an insightful project.
