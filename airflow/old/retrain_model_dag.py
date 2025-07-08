from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_model_retraining',
    default_args=default_args,
    description='Retrains ML model daily using retrain_model.py',
    schedule_interval='@daily',
    start_date=datetime(2025, 7, 8),
    catchup=False,
    tags=['ml', 'retraining'],
) as dag:

    retrain = BashOperator(
        task_id='run_retrain_script',
        bash_command='python3 /home/mle-user/mle_projects/mle-project-sprint-final/airflow/plugins/steps/retrain_model.py',
    )

    retrain