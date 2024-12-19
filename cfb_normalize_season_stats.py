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
	
def process_college_football_data(**kwargs):
	# Load datasets from S3
	talent_df = load_s3_json('talent/2022/talent_2022.json')
	player_df = load_s3_json('player_returning/2023/player_returning_2023.json')
	stats_df = load_s3_json('stats_season/2022/stats_season_2022.json')
	records_df = load_s3_json('records/2022/records_2022.json')
	new_records_df = load_s3_json('records/2023/records_2023.json')
	
	# Step 1: Handle Missing Data
	def handle_missing_data(df):
		#df.dropna(subset=['team'], inplace=True)
		for col in df.select_dtypes(include=[np.number]):
			df[col] = df[col].fillna(0)
		return df
	
	talent_df = handle_missing_data(talent_df)
	player_df = handle_missing_data(player_df)
	stats_df = handle_missing_data(stats_df)
	records_df = handle_missing_data(records_df)
	new_records_df = handle_missing_data(new_records_df)
		
	
	# Step 2: Standardize Column Names
	def standardize_columns(df):
		df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("%", "percent")
		return df
	
	talent_df = standardize_columns(talent_df)
	player_df = standardize_columns(player_df)
	stats_df = standardize_columns(stats_df)
	records_df = standardize_columns(records_df)
	new_records_df = standardize_columns(new_records_df)
	
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
		key=f"{CLEAN_FOLDER}2022_normalized_college_football_data.csv",
		bucket_name=BUCKET_NAME,
		replace=True
	)
	print("Normalized data uploaded to S3.")
	
# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id="cfb_normalize_season_data",
    default_args=default_args,
    description='Normalize College Football Data from S3 and Save Cleaned Data',
    schedule_interval=None,
    start_date=datetime(2024, 6, 1),
    catchup=False
)

process_task = PythonOperator(
    task_id='process_college_football_data',
    python_callable=process_college_football_data,
    provide_context=True,
    dag=dag
)

process_task
