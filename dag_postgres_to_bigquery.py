from datetime import datetime, timezone
from datetime import date, timedelta

from airflow import configuration
from airflow import models
from airflow.contrib.hooks import gcs_hook
from airflow.contrib.operators import mssql_to_gcs
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.gcs import GCDDeleteObjetsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

BQ_DS = models.Variable.get('dataset_rz_projuris')
BQ_PROJECT = models.Variable.get('gcs_project')
GCS_BUCKET = models.Variable.get('postgres_export_bucket')
GCS_OBJECT_PATH = models.Variable.get('posta_projuris_dev')
SOURCE_TABLE_NAME = 'tabela'
POSTGRESS_CONNECTION_ID = 'postgres'

today = date.today()
today = datetime.strftime(str(today), '%Y-%m-%d')

sql_query = '''SELECR * FROM tabela'''

with models.DAG(
    dag_id = 'load_postgres_into_bq_tabela',
    start_date = days_ago(1),
    schedule_interval = '10 11 * * *',
    defaut_args = {
        'owner': 'airflow',
        'retries_delays': timedelta(minutes=5),
        'email': ['weberv.souza@gmail.com'],
        'email_on_failure': True,
    },

    max_active_runs = 1,
    tags = ["datalakers", "raw zone"],
) as dag:
    postgres_to_gcs_task = PostgresToGCSOperator(
        task_id = f'postgres_to_gcs',
        postgres_cocn_id = 'postgres_projuris',
        sql = sql_query,
        bucket = GCS_BUCKET,
        filename = f'input/{today}/{SOURCE_TABLE_NAME}.csv',
        export_format = 'csv',
        gzip = False,
        use_server_side_cursor = False,
    )
    gcs_to_bq_task = GCSToBigQueryOperator(
        task_id = f'gcs_to_bq',
        bucket = GCS_BUCKET,
        source_object =[f'processing/{today}/{SOURCE_TABLE_NAME}.csv'],
        destination_prorject_dataset_table = '.'.join(
            [BQ_PROJECT, BQ_DS, SOURCE_TABLE_NAME]),
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE',
        skip_leading_rows = 1,
        allow_quoted_newlines = True,     
    )
    copy_files_input_processing = GCSToBigQueryOperator(
        task_id = "copy_files_input_processing",
        source_bucket = GCS_BUCKET,
        source_object = f'input/{today}/{SOURCE_TABLE_NAME}.csv',
        destination_bucket = GCS_BUCKET,
        destination_object = f'processing/{today}/{SOURCE_TABLE_NAME}.csv'
    )
    cleanup_task_input = GCDDeleteObjetsOperator(
        task_id = 'cleanup_task_input',
        bucket_name = GCS_BUCKET,
        objects = [f'input/{today}/{SOURCE_TABLE_NAME}.csv'],
    )
    copy_files_processing_output = GCSToGCSOperator(
        task_id = "copy_files_processing_output",
        source_bucket = GCS_BUCKET,
        source_object = f'processing/{today}/{SOURCE_TABLE_NAME}.csv',  
        destination_bucket = GCS_BUCKET,
        destination_object = f'output/{today}/{SOURCE_TABLE_NAME}.csv' 
    )
    cleanup_task_processing = GCDDeleteObjetsOperator(
        task_id = 'cleanup_task_processing',
        bucket_name = GCS_BUCKET,
        objects = [f'processing/{today}/{SOURCE_TABLE_NAME}.csv'],
    )

    postgres_to_gcs_task >> copy_files_input_processing >> cleanup_task_input >> gcs_to_bq_task >> copy_files_processing_output >> cleanup_task_processing