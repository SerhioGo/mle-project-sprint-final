import pendulum
from airflow.decorators import dag, task
import pandas as pd
import pickle
import os
import pandas as pd
from io import BytesIO
from catboost import CatBoostClassifier, Pool
import boto3
import pyarrow.parquet as pq

BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
KEY = 'finale/retrain/candidates_for_train.parquet'
PATH_MODELS = '../../models'
MODEL_FILE = 'retrained_model_2.pkl'
RANDOM_STATE = 8310

@dag(
    dag_id='retrained_model',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['ml', 'retraining'],
)
def train_model():
    @task()
    def load_and_train():
        """Настройки S3"""
        session = boto3.session.Session()
        client_s3 = session.client(
            service_name='s3',
            endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
        )
        """Загружает данные из S3"""
        response = client_s3.get_object(Bucket=BUCKET_NAME, Key=KEY)
        parquet_file = BytesIO(response['Body'].read())
        candidates_for_train = pq.read_table(parquet_file).to_pandas()
            
        """Заново обучаем модель"""
        features = ['categoryid_enc','parentid_enc', 'available', 'istransaction', 'day_of_week', 'day', 'hour', 'rating', 'item_id_week', 'item_viewed', 'rating_avg', 'rating_std']
        cat_features = ['categoryid_enc', 'parentid_enc', 'available', 'istransaction','day_of_week', 'day', 'hour']
        target = ["target"]

        train_data = Pool(
            data=candidates_for_train[features],
            label=candidates_for_train[target],
            cat_features=cat_features,
        )

        cb_model = CatBoostClassifier(iterations=3000,
                                learning_rate=0.1,
                                loss_function='Logloss',
                                auto_class_weights='Balanced',
                                verbose=500,
                                random_seed=RANDOM_STATE)

        cb_model.fit(train_data)

        with open(f"{PATH_MODELS}/{MODEL_FILE}", "wb") as f:
            pickle.dump(cb_model, f)
        load_and_train()
    
    train_model()