from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from airflow.models import Variable
import requests
import json

# Define the datasets you want to pull for each year
DATASETS = [
    {"endpoint": "games", "s3_key_template": "raw/games/{year}/week_{week}.json"},
    {"endpoint": "games/teams", "s3_key_template": "raw/team_stats/{year}/week_{week}.json"},
    {"endpoint": "games/players", "s3_key_template": "raw/player_stats/{year}/week_{week}.json"},
	{"endpoint": "rankings", "s3_key_template": "raw/rankings/{year}/week_{week}.json"},
    {"endpoint": "stats/game/advanced", "s3_key_template": "raw/advanced_stats/{year}/week_{week}.json"},
    {"endpoint": "drives", "s3_key_template": "raw/drives/{year}/week_{week}.json"},
	{"endpoint": "recruiting/teams", "s3_key_template": "raw/recruiting_teams/{year}/week_{week}.json"},
    # Add more datasets as needed
]

# Function to pull data from the College Football Data API and store in S3
def fetch_data_from_cfb_api(year, week, dataset, **kwargs):
    api_key = Variable.get("cfb_api_key")
    url = f"https://api.collegefootballdata.com/{dataset['endpoint']}"
    params = {"year": year, "week": week, "seasonType": "regular"}
    headers = {"Authorization": f"Bearer {api_key}"}  # Replace with your actual API key
    
    # Make API request
    response = requests.get(url, params=params, headers=headers)
    #data = response.json()
	
    if response.status_code == 200:
        try:
            data = response.json()  # Attempt to parse the response as JSON
        except ValueError as e:
            # Log error and raise if JSON parsing fails
            raise ValueError(f"Error decoding JSON from {url}: {e}")
    else:
        # Log the failed request details and raise an exception
        raise ValueError(f"Failed to fetch data from {url}. Status code: {response.status_code}, Response: {response.text}")
    
    
    # Define the S3 bucket and key
    s3_hook = S3Hook(aws_conn_id='aws-s3-conn')
    s3_bucket = 'dds-college-football-data'  # Replace with your bucket name
    s3_key = dataset['s3_key_template'].format(year=year, week=week)
    
    # Upload the data to S3
    s3_hook.load_string(
        string_data=json.dumps(data), 
        key=s3_key, 
        bucket_name=s3_bucket, 
        replace=True
    )

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
with DAG('collect_cfb_data',
         default_args=default_args,
         schedule=None,
         catchup=False) as dag:

    # Iterate through the years (2022 and 2023)
    for year in [2022, 2023]:
        # Iterate through the weeks of the season (e.g., 1 to 15)
        for week in range(1, 16):
            # Iterate through each dataset in the list
            for dataset in DATASETS:
                # Create a task for each dataset, year, and week
                task = PythonOperator(
                    task_id=f'fetch_{dataset["endpoint"].replace("/", "_")}_{year}_week_{week}',
                    python_callable=fetch_data_from_cfb_api,
                    op_args=[year, week, dataset]

                )
