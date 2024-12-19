from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.linear_model import LogisticRegression
from datetime import datetime, timedelta
import os

# Define S3 bucket and paths
BUCKET_NAME = "dds-college-football-data"
CLEAN_FOLDER = "cleaned-data/"
RESULTS_FOLDER = "results/"

# Function to pull file from S3 and load into DataFrame
def load_s3_csv(s3_key):
    hook = S3Hook(aws_conn_id='aws_default')
    local_path = hook.download_file(key=f"{CLEAN_FOLDER}{s3_key}", bucket_name=BUCKET_NAME)
    return pd.read_csv(local_path)
	
def save_results_to_s3(filename, content):
	local_path = f"/tmp/{filename}"
	with open(local_path, "w") as f:
		f.write(content)
	hook = S3Hook(aws_conn_id='aws_default')
	hook.load_file(
		filename=local_path,
		key=f"{RESULTS_FOLDER}{filename}",
		bucket_name=BUCKET_NAME,
		replace=True
	)
	
def output_results(r2, mse,X, model, filename):
	# Prepare results as a string
	results = []
	results.append(f"Mean Squared Error (MSE): {mse}\n")
	results.append(f"R² Score: {r2}\n\n")
	
	# Step 7: Print model coefficients for interpretation
	
	coefficients = pd.DataFrame({"Feature": X.columns, "Coefficient": model.coef_})
	results.append("Model Coefficients:\n")
	results.append(coefficients.sort_values(by="Coefficient", ascending=False).to_string(index=False))
	
	# Save results to a file
	save_results_to_s3(filename, "".join(results))

def train_linear_model(**kwargs):
    # Step 1: Load the dataset from S3
	data = load_s3_csv("2022_normalized_college_football_data.csv")
	
	data['improvement'] = (data['new_total.wins'] > data['total.wins']).astype(int)
	
	# Step 2: Drop columns with 'new_' prefix
	features = data.drop(columns=[col for col in data.columns if col.startswith("new_")])
	features = features.select_dtypes(include=['number'])
	
	features = features.drop(columns=[
								'totalpassingppa',
	                            'totalreceivingppa',
	                            'totalrushingppa',
								'totalppa',
								'awaygames.ties',
								'conferencegames.ties',
								'total.ties',
								'homegames.ties',
								'year',
								'season_x',
								'games',
								'total.games'])
								
	# Ensure the target column 'new_total.wins' exists and is numeric
	#if 'new_total.wins' not in data.columns:
	#	raise ValueError("Target column 'new_total.wins' is missing from the dataset.")
	#data['new_total.wins'] = pd.to_numeric(data['new_total.wins'], errors='coerce')
	
	# Step 3: Split features (X) and target (y)
	X = features.drop(columns=['improvement'], errors='ignore')  # Exclude target from features
	y = data['improvement']
	
	# Step 4: Train-test split
	X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
	
	# Step 5: Train the Linear Regression model
	model = LinearRegression()
	model.fit(X_train, y_train)
	
	# Step 6: Evaluate the model
	y_pred = model.predict(X_test)
	
	mse = mean_squared_error(y_test, y_pred)
	r2 = r2_score(y_test, y_pred)
	
	# Prepare results as a string
	output_results(r2, mse,X, model, "normal_results.txt")
	
	#Scaling data
	scaler = StandardScaler()
	X_train_scaled = scaler.fit_transform(X_train)
	X_test_scaled = scaler.transform(X_test)

	# Step 4: Train the Linear Regression model
	model = LinearRegression()
	model.fit(X_train_scaled, y_train)

	# Step 5: Evaluate the model
	y_pred = model.predict(X_test_scaled)
	mse = mean_squared_error(y_test, y_pred)
	r2 = r2_score(y_test, y_pred)
	
	output_results(r2, mse,X, model, "scaled_results.txt")
	
	model = Ridge(alpha=1.0)
	model.fit(X_train_scaled, y_train)# L2 regularization
	y_pred = model.predict(X_test_scaled)
	mse = mean_squared_error(y_test, y_pred)
	r2 = r2_score(y_test, y_pred)
	
	output_results(r2, mse,X, model, "ridge_results.txt")
	
	
	# or
	model = Lasso(alpha=0.1)  # L1 regularization
	model.fit(X_train_scaled, y_train)
	y_pred = model.predict(X_test_scaled)
	mse = mean_squared_error(y_test, y_pred)
	r2 = r2_score(y_test, y_pred)
	
	output_results(r2, mse,X, model, "lasso_results.txt")
	
	model = LogisticRegression()
	model.fit(X_train, y_train)
	
	y_pred = model.predict(X_test)
	y_prob = model.predict_proba(X_test)[:, 1] 
	
	print(classification_report(y_test, y_pred))
	print(f"AUC-ROC: {roc_auc_score(y_test, y_prob)}")
	
	'''
	# Step 3: Train-test split
	X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

	# Step 4: Train Random Forest Model
	rf_model = RandomForestRegressor(random_state=42, n_estimators=100)
	rf_model.fit(X_train, y_train)

	# Predict and evaluate Random Forest
	rf_y_pred = rf_model.predict(X_test)
	rf_mse = mean_squared_error(y_test, rf_y_pred)
	rf_r2 = r2_score(y_test, rf_y_pred)
	
	output_results(rf_r2, rf_mse, X, rf_model, "rf_results.txt")

	# Step 5: Train Gradient Boosting Model
	gb_model = GradientBoostingRegressor(random_state=42, n_estimators=100, learning_rate=0.1)
	gb_model.fit(X_train, y_train)

	# Predict and evaluate Gradient Boosting
	gb_y_pred = gb_model.predict(X_test)
	gb_mse = mean_squared_error(y_test, gb_y_pred)
	gb_r2 = r2_score(y_test, gb_y_pred)

	output_results(gb_r2, gb_mse, X, gb_model, "gb_results.txt")'''
		
	'''param_grid = {
		'n_estimators': [100, 200, 500],
		'max_depth': [None, 10, 20, 30],
		'min_samples_split': [2, 5, 10],
		'min_samples_leaf': [1, 2, 4],
		'bootstrap': [True, False]
	}

	grid_search = GridSearchCV(
		RandomForestRegressor(random_state=42),
		param_grid,
		scoring='neg_mean_squared_error',
		cv=3
	)
	grid_search.fit(X_train, y_train)

	print("Best Parameters:", grid_search.best_params_)
	print("Best MSE:", -grid_search.best_score_)'''
	
	
	
# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'cfb_linear_regression_model',
    default_args=default_args,
    description='Train a linear regression model to predict new_total.wins',
    schedule_interval=None,
    start_date=datetime(2024, 6, 1),
    catchup=False
)

train_model_task = PythonOperator(
    task_id='cfb_train_linear_model',
    python_callable=train_linear_model,
    provide_context=True,
    dag=dag
)

train_model_task
