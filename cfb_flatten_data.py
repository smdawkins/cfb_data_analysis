from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from airflow.models import Variable
import requests
import json
import boto3
import pandas as pd
from io import StringIO
from airflow.utils.dates import days_ago
from pandas import json_normalize

# Define the datasets you want to pull for each year
DATASETS = [
    {"endpoint": "games", "s3_key_template": "games", "normalization_method": "default"},
    {"endpoint": "games/teams", "s3_key_template": "team_stats" , "normalization_method": "team_stats"},
    {"endpoint": "games/players", "s3_key_template": "player_stats", "normalization_method": "player_stats"},
	{"endpoint": "rankings", "s3_key_template": "rankings", "normalization_method": "rankings"},
    {"endpoint": "stats/game/advanced", "s3_key_template": "advanced_stats", "normalization_method": "default"},
    {"endpoint": "drives", "s3_key_template": "drives", "normalization_method": "default"},
	{"endpoint": "recruiting/teams", "s3_key_template": "recruiting_teams", "normalization_method": "default"},
    # Add more datasets as needed
]

# Define the function to fetch the raw JSON data from S3 using S3Hook
def fetch_raw_data_from_s3(s3_conn_id, bucket_name, raw_key):
    # Initialize the S3 hook
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    print(f'reading from {raw_key} in {bucket_name}')
    # Read the raw JSON file from S3
    raw_data = s3_hook.read_key(key=raw_key, bucket_name=bucket_name)
    
    # Return the JSON data as a Python object
    return json.loads(raw_data)

# Define the function to flatten the JSON, drop null values, and return the cleaned DataFrame
def flatten_and_clean_data(raw_data, normalization_method):
    # Step 1: Flatten the JSON structure
	object_size = len(raw_data)
	print(f'there are {object_size} items that have been pulled')
	if normalization_method == 'team_stats':
		df = pd.json_normalize(raw_data, record_path=['teams', 'stats'], meta=['id', ['teams', 'schoolId'], ['teams', 'school'], ['teams', 'conference'], ['teams', 'homeAway'], ['teams', 'points']])

	elif normalization_method == 'player_stats':
		df = pd.json_normalize(
					raw_data,
					record_path=['teams', 'categories', 'types', 'athletes'],  # Deepest path (athletes)
					meta=[
						'id',  # Root level ID
						['teams', 'school'],  # Team-level data
						['teams', 'conference'],
						['teams', 'homeAway'],
						['teams', 'points'],
						['teams', 'categories', 'name'],  # Category name
						['teams', 'categories', 'types', 'name']  # Type name
					],
					meta_prefix='meta_'
				)
	elif normalization_method == 'rankings':
		df = pd.json_normalize(
					raw_data,
					record_path=['polls', 'ranks'],  # Path to ranks, which is inside polls
					meta=[
						'season',  # Root-level metadata
						'seasonType',
						'week',
						['polls', 'poll']  # Poll-level metadata
					]
				) 
	else:
		df = pd.json_normalize(raw_data)
	object_size = len(df)
	print(f'there are {object_size} items after normalizing')
    # Step 2: Drop records with null values
	df_cleaned = df.fillna(0)
	
	object_size = len(df_cleaned)
	print(f'there are {object_size} items after dropping empty records')
	return df_cleaned

# Define the function to upload the cleaned CSV to S3 using S3Hook
def upload_cleaned_data_to_s3(df_cleaned, s3_conn_id, bucket_name, flattened_key):
    # Step 1: Convert the DataFrame to CSV in-memory
    csv_buffer = StringIO()
    df_cleaned.to_csv(csv_buffer, index=False)

    # Initialize the S3 hook
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    
    # Step 2: Upload the CSV to S3
    s3_hook.load_string(
        csv_buffer.getvalue(),  # CSV content in string form
        key=flattened_key,  # S3 key for the output
        bucket_name=bucket_name,  # S3 bucket name
        replace=True  # Replace existing object with the same key
    )

    print(f"Successfully uploaded cleaned data to {flattened_key}")

# Define the main function to tie it all together (fetch, clean, and upload)
def process_and_upload_flattened_data(**kwargs):
    # Fetch the S3 details from the DAG run context
	bucket_name = kwargs['bucket_name']
	raw_key = kwargs['raw_key']
	flattened_key = kwargs['flattened_key']
	s3_conn_id = kwargs['s3_conn_id']
	normalization_method = kwargs['normalization_method']
    
    # Fetch raw data from S3
	raw_data = fetch_raw_data_from_s3(s3_conn_id, bucket_name, raw_key)
    
    # Flatten and clean the data
	df_cleaned = flatten_and_clean_data(raw_data, normalization_method)
    
    # Upload the cleaned, flattened data back to S3
	upload_cleaned_data_to_s3(df_cleaned, s3_conn_id, bucket_name, flattened_key)


# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0
}

with DAG(
    dag_id='cfb_process_flattened_json',
    default_args=default_args,
    schedule=None,  # Run manually or set a schedule interval as needed
    catchup=False
) as dag:

	for year in [2022,2023]:
        # Iterate through the weeks of the season (e.g., 1 to 15)
		for week in range(1, 16):
            # Iterate through each d
			for dataset in DATASETS:
                # Create a task for each dataset, year, and week
    # Define the task to process and upload the flattened data
				print("running dataset")
				task = PythonOperator(
					task_id=f'process_{dataset["endpoint"].replace("/", "_")}_{year}_week_{week}',
					python_callable=process_and_upload_flattened_data,
					op_kwargs={
						'bucket_name': 'dds-college-football-data',  # S3 bucket name
						'raw_key': f'raw/{dataset["s3_key_template"]}/{year}/week_{week}.json',  # Path to the raw JSON file in S3
						'flattened_key': f'flattened/{dataset["s3_key_template"]}/{year}/week_{week}_cleaned.csv',  # Path to save the flattened CSV
						's3_conn_id': 'aws-s3-conn',  # Airflow S3 connection ID
						'normalization_method': dataset["normalization_method"]
					}
				)

	


