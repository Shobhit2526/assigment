from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from DataExtraction import FacebookExtractor, GoogleExtractor
from DataTransformation import DataTransformer
from DataLoader import DataLoader

# Spark Session
spark = SparkSession.builder.appName('FacebookGoogleETL').getOrCreate()

def extract_facebook_data():
    facebook_extractor = FacebookExtractor(access_token='your_token', ad_account_id='your_account_id', spark_session=spark)
    return facebook_extractor.extract()

def extract_google_data():
    google_extractor = GoogleExtractor(credentials='your_credentials', client_id='your_client_id', customer_id='your_customer_id', spark_session=spark)
    return google_extractor.extract()

def transform_data(facebook_data, google_data):
    transformer = DataTransformer()
    return transformer.transform(facebook_data, google_data)

def load_data(data, table_name):
    db_loader = DataLoader(db_url='jdbc:postgresql://localhost:5432/database', db_properties={'user': 'username', 'password': 'password'})
    db_loader.load(data, table_name)

with DAG(
    'facebook_google_etl',
    default_args={'owner': 'airflow', 'retries': 1},
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    extract_facebook_task = PythonOperator(
        task_id='extract_facebook',
        python_callable=extract_facebook_data
    )

    extract_google_task = PythonOperator(
        task_id='extract_google',
        python_callable=extract_google_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_args=[
            '{{ task_instance.xcom_pull(task_ids="extract_facebook") }}',
            '{{ task_instance.xcom_pull(task_ids="extract_google") }}'
        ]
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_args=['{{ task_instance.xcom_pull(task_ids="transform_data") }}', 'ads_data']
    )

    [extract_facebook_task, extract_google_task] >> transform_task >> load_task
