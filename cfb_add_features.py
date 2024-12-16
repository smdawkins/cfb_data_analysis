import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO
from datetime import datetime
from pandas import json_normalize

# Function to load weekly data from S3
def load_weekly_data_from_s3(s3_conn_id, bucket_name, key):
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    csv_data = s3_hook.read_key(key=key, bucket_name=bucket_name)
    df = pd.read_csv(StringIO(csv_data))
    return df

# Function to upload the processed data back to S3
def upload_processed_data_to_s3(df, s3_conn_id, bucket_name, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    s3_hook.load_string(csv_buffer.getvalue(), key=key, bucket_name=bucket_name, replace=True)
    print(f"Uploaded transformed data to {key}")

# Function to process a single week's data
def process_weekly_data(year, week, s3_conn_id, bucket_name, **kwargs):
    # Define the S3 keys for games, teams, and players for a given year and week
    games_key = f'flattened/games/{year}/week_{week}_cleaned.csv'
    teams_key = f'flattened/team_stats/{year}/week_{week}_cleaned.csv'
    players_key = f'flattened/player_stats/{year}/week_{week}_cleaned.csv'

    # Load the data from S3
    df_games = load_weekly_data_from_s3(s3_conn_id, bucket_name, games_key)
    df_teams = load_weekly_data_from_s3(s3_conn_id, bucket_name, teams_key)
    df_players = load_weekly_data_from_s3(s3_conn_id, bucket_name, players_key)

    # Merge the datasets (games, teams, and players) by 'game_id' and 'team'
    df_merged = pd.merge(df_games, df_teams, on=['game_id', 'team'], how='inner')
    df_merged = pd.merge(df_merged, df_players, on=['game_id', 'team'], how='inner')

    # Sort and group by team and week
    df_merged = df_merged.sort_values(by=['team', 'week'])
    grouped = df_merged.groupby('team')

    # Feature Engineering: Rolling and Lagged Features
    df_merged['avg_points_scored'] = grouped['points'].apply(lambda x: x.shift().expanding().mean())
    df_merged['avg_points_allowed'] = grouped['opponent_points'].apply(lambda x: x.shift().expanding().mean())
    df_merged['cumulative_wins'] = grouped['win'].apply(lambda x: x.shift().cumsum())
    df_merged['points_last_week'] = grouped['points'].shift(1)
    df_merged['points_2_weeks_ago'] = grouped['points'].shift(2)
    df_merged['rolling_3wk_points'] = grouped['points'].apply(lambda x: x.shift().rolling(3, min_periods=1).mean())
    df_merged['rolling_5wk_wins'] = grouped['win'].apply(lambda x: x.shift().rolling(5, min_periods=1).sum())

    # Fill NaN values with 0
    df_merged.fillna(0, inplace=True)

    # Upload the processed data to S3
    processed_key = f'transformed/{year}/week_{week}_transformed.csv'
    upload_processed_data_to_s3(df_merged, s3_conn_id, bucket_name, processed_key)

    print(f"Processed data for year {year}, week {week} and uploaded to S3.")

# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'cfb_weekly_data_processing',
    default_args=default_args,
    description='Process weekly college football data and upload to S3',
    schedule_interval=None,
    catchup=False,
) as dag:

    # Loop through years and weeks to create tasks dynamically
    for year in [2022, 2023]:
        for week in range(1, 17):  # Weeks 1 through 16
            process_task = PythonOperator(
                task_id=f'process_year_{year}_week_{week}',
                python_callable=process_weekly_data,
                op_kwargs={
                    'year': year,
                    'week': week,
                    's3_conn_id': 'aws-s3-conn',
                    'bucket_name': 'your-bucket-name',
                },
            )

            # You can add dependencies between tasks if needed, e.g., sequential processing
            # (Optional: Set dependencies if needed)
            # if prev_task:
            #     prev_task >> process_task
            # prev_task = process_task
