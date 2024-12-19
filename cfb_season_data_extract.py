from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import json
import os

# Function to fetch data from the College Football API
def fetch_and_upload_to_s3(endpoint, year, bucket_name, s3_key_prefix, aws_conn_id):
	"""
	Fetch data from the College Football API and upload it to S3.
	
	Args:
		endpoint (str): The API endpoint (e.g., 'teams', 'stats').
		year (int): The year of data to fetch.
		bucket_name (str): S3 bucket name for uploading the data.
		s3_key_prefix (str): S3 key prefix (folder structure in the bucket).
		aws_conn_id (str): Airflow AWS connection ID.
	"""
	# Fetch the API base URL from Airflow variables
	api_base_url = Variable.get("cfb_base_url")
	api_key = Variable.get("cfb_api_key")  # Optionally, store the API key in a Variable
	
	# Construct the API URL
	endpoint = endpoint.replace("_","/")
	url = f"{api_base_url}/{endpoint}?year={year}"
	headers = {"Authorization": f"Bearer {api_key}"}
	endpoint = endpoint.replace("/","_")
	
	try:
		# Fetch data from the API
		response = requests.get(url, headers=headers)
		response.raise_for_status()
		data = response.json()
		# Save data locally as a JSON file
		local_file = f"{endpoint}_{year}.json"
		with open(local_file, "w") as f:
			json.dump(data, f, indent=4)
		
		# Upload the file to S3
		s3 = S3Hook(aws_conn_id=aws_conn_id)
		s3_key = f"{s3_key_prefix}/{endpoint}/{year}/{endpoint}_{year}.json"
		s3.load_file(
			filename=local_file,
			key=s3_key,
			bucket_name=bucket_name,
			replace=True
		)
		
		# Clean up local file
		os.remove(local_file)
		print(f"Data uploaded to S3: s3://{bucket_name}/{s3_key}")
	except requests.exceptions.RequestException as e:
		print(f"Error fetching data from {url}: {e}")
		raise
	except Exception as e:
		print(f"Error uploading to S3: {e}")
		raise


# Define the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cfb_season_extract",
    default_args=default_args,
    description="Fetch raw data from College Football API and upload to S3",
    schedule_interval=None,  # Run manually or set a schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    def create_task(endpoint, year):
        return PythonOperator(
            task_id=f"fetch_and_upload_{endpoint}_{year}",
            python_callable=fetch_and_upload_to_s3,
            op_kwargs={
                "endpoint": endpoint,
                "year": year,
                "bucket_name": "dds-college-football-data",  # Replace with your S3 bucket name
                "s3_key_prefix": "raw",  # Folder in S3 bucket
                "aws_conn_id": "aws_default",  # Airflow AWS connection ID
            },
        )

    # List of endpoints and years to process
    endpoints = ["records", "stats_season", "player_returning", "talent"]
    years = [2022, 2023]

    # Dynamically create tasks for each endpoint and year
    tasks = []
    for endpoint in endpoints:
        for year in years:
            task = create_task(endpoint, year)
            tasks.append(task)

    # Set task dependencies if needed
    # For example, ensure all tasks run independently
    for task in tasks:
        task
