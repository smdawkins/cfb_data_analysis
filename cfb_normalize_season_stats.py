from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# Define S3 bucket and paths
BUCKET_NAME = "dds-college-football-data"
RAW_FOLDER = "raw/"
CLEAN_FOLDER = "cleaned-data/"

# Function to pull file from S3 and load into DataFrame
def load_s3_json(s3_key):
	hook = S3Hook(aws_conn_id='aws_default')
	file_obj = hook.download_file(key=f"{RAW_FOLDER}{s3_key}", bucket_name=BUCKET_NAME)
	
	# Ensure correct reading of the JSON file
	with open(file_obj, "r") as f:
		data = json.load(f)
	
	# Normalize JSON if necessary
	if isinstance(data, list):
		return pd.json_normalize(data)  # Direct list of records
	elif isinstance(data, dict):
		return pd.json_normalize(data)  # Flatten nested structures
	else:
		raise ValueError("Unsupported JSON format. Must be list or dict.")
		
def handle_missing_data(df):
	#df.dropna(subset=['team'], inplace=True)
	for col in df.select_dtypes(include=[np.number]):
		df[col] = df[col].fillna(0)
	return df
	
	# Step 2: Standardize Column Names
def standardize_columns(df):
	df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("%", "percent")
	return df

		
def process_college_football_data(**kwargs):
	
	next_year = year+1
	data_dict = {}
	
	for endpoint in endpoints:
		if endpoint == 'player_returning':
			df = load_s3_json(f"{endpoint}/{next_year}/{endpoint}_{next_year}.json")
		elif endpoint == 'records':
			next_year_df = load_s3_json(f"{endpoint}/{next_year}/{endpoint}_{next_year}.json")
			next_year_df = handle_missing_data(next_year_df)
			next_year_df = standardize_columns(next_year_df)
			new_records_df = next_year_df
			
			df = load_s3_json(f"{endpoint}/{year}/{endpoint}_{year}.json")
		else: 
			df = load_s3_json(f"{endpoint}/{year}/{endpoint}_{year}.json")
			
		df = handle_missing_data(df)
		df = standardize_columns(df)
		data_dict[endpoint] = df 
		
	records_df = data_dict['records']
	talent_df = data_dict['talent']
	stats_df = data_dict['stats_season']
	player_df = data_dict['player_returning']
	
	data_dict = {}
		
	# Step 3: Convert Data Types
	talent_df['talent'] = pd.to_numeric(talent_df['talent'], errors='coerce').fillna(0)
	stats_df['statvalue'] = pd.to_numeric(stats_df['statvalue'], errors='coerce').fillna(0)
	
	# Step 4: Pivot stats_df
	stats_pivot = stats_df.pivot_table(index=['team', 'season'], columns='statname', values='statvalue', aggfunc='sum').reset_index()
	stats_pivot.columns.name = None
	
	# Step 5: Merge datasets
	merged_df = pd.merge(talent_df, player_df, left_on='school', right_on='team', how='inner')
	merged_df = pd.merge(merged_df, stats_pivot, left_on=['school', 'year'], right_on=['team', 'season'], how='inner')
	
	# Merge records data for 2022 and 2023, preventing duplicate columns
	#records_df.rename(columns=lambda x: f"2022_{x}" if x not in ['team', 'year'] else x, inplace=True)
	new_records_df = new_records_df.add_prefix('new_')
	
	merged_df = pd.merge(merged_df, records_df, left_on=['school', 'year'], right_on=['team', 'year'], how='inner')
	final_df = pd.merge(merged_df, new_records_df, left_on=['school'], right_on=['new_team'], how='inner')
	
	final_df.drop(columns=['team_x', 'team_y'], inplace=True)
	
	# Step 6: Save to S3
	output_file = "/tmp/normalized_college_football_data.csv"
	final_df.to_csv(output_file, index=False)
	
	hook = S3Hook(aws_conn_id='aws_default')
	hook.load_file(
		filename=output_file,
		key=f"{CLEAN_FOLDER}{year}_normalized_college_football_data.csv",
		bucket_name=BUCKET_NAME,
		replace=True
	)
	print("Normalized data uploaded to S3.")
			
	
	




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
    dag_id="cfb_normalize_season_data",
    default_args=default_args,
    description="Normalize College Football Data from S3 and Save Cleaned Data",
    schedule_interval=None,  # Run manually or set a schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

	def create_task(endpoints, year):
		return PythonOperator(
			task_id=f"normalize_cfb_season_data_{year}",
			python_callable=process_college_football_data,
			op_kwargs={
				"endpoints": endpoints,
				"year": year,
				"bucket_name": "dds-college-football-data",  # Replace with your S3 bucket name
				"s3_key_prefix": RAW_FOLDER,  # Folder in S3 bucket
				"aws_conn_id": "aws_default",  # Airflow AWS connection ID
			},
		)
	
	# List of endpoints and years to process
	endpoints = ["records", "stats_season", "player_returning", "talent"]
	years = range(2021,2022)
	
	# Dynamically create tasks for each endpoint and year
	tasks = []
	#for endpoint in endpoints:
	for year in years:
		task = create_task(endpoints, year)
		tasks.append(task)
	
	# Set task dependencies if needed
	# For example, ensure all tasks run independently
	for task in tasks:
		task

