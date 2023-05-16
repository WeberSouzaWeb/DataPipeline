from __future__ import annotations
import os
from datetime import datetime, timezone
from datetime import date, timedelta
import requests
import json
import pandas as pd
from google.cloud import storage

from airflow import models
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToGCSOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

url_oauth2 = models.Variable.get('url_oauth2')
client_id = models.Variable.get('client_id')
user_id = models.Variable.get('user_id')
token_url = models.Variable.get('token_url')
private_key = models.Variable.get('private_key')
company_id = models.Variable.get('company_id')
grant_type = models.Variable.get('grant_type')
user_id_token = models.Variable.get('user_id_token')
client_id_token = models.Variable.get('client_id_token')
BQ_DS = models.Variable.get('dataset_rz_rh')
BQ_PROJECT = models.Variable.get('gcp_project')
GCS_BUCKET = models.Variable.get('rh_bucket')
SOURCE_TABLE_NAME = 'ft_arquivo' 

client = storage.Client()
storage_client = storage.Client()
v_bucket = GCS_BUCKET
bucket = client.get_bucket(v_bucket)

presentday = datetime.now()
yesterday = presentday - timedelta(1)
data_referencia = str(yesterday.strftime('%Y-%m-%d'))

## function to request in url. Using id valid
def request_sf_oauth2_idp():

    url = 'https://url.requests.com '

    data = {
        'client_id': client_id,
        'user_id': user_id,
        'token_url': token_url,
        'private_key': private_key,
    }
    headers = {'Content-type': 'application'}
    response = requests.post(url, json=data, headers=headers, data=data)

    if  response.status_code == 200:
        assertion = response.text

        return assertion
    else:
        return 'ERROR'
    
def request_sf_token(assertion):
    url = 'https://url.requests.com/token'

    data = {
        'company_id': company_id,
        'client_id': client_id_token,
        'grant_type': grant_type,
        'assertion': assertion,
        'user_id': user_id_token
    }
    headers = {'Content-type': 'application'}
    response = requests.post(url, json=data, headers=headers, data=data)

    if  response.status_code == 200:
        response_json = json.loads(response.text)
        token = response_json['access_token']

        return token
    else:
        return 'ERROR'
    
def get_sap_select(token):
    results_list_users = []

    url = 'url'
    headers = {
        'Authorization': f'Bearer{token}',
        'Content-type': 'application'
        }
    params = {
        'format': 'json'
    }
    response = requests.get(url, params=params , headers=headers)

    print(response.status_code)

    if response.status_code == 200:
        response_json = json.loads(response.text)

        results_list = response_json['d']['result']

        try:
            next_url = response_json['d']['_next']
            print(f'next_url - {next_url}')

        except:
            next_url = ''
        
        print(f'result_list - {len(results_list)}')

        for i in range(len(results_list)):

            results_list[i].pop("_metadata")
            results_list_users.append(results_list[i])

        if next_url != '':
            get_sap_select(token, next_url,results_list_users)
        return results_list_users
    else:
        return[]


def orc_request_main():

    assertion = request_sf_oauth2_idp()

    if assertion != 'ERRO':
        token = request_sf_token(assertion)
        if token != 'ERRO':
            results_list_users = get_sap_select(token)
            if len(results_list_users) > 0:
                df = pd.DataFrame(results_list_users)
                bucket.blob(f'input/{data_referencia}/arquivo.csv').upload_from_string(df.to_csv(index=))
            else:
                False
        else:
            return False
    else:
        return False

def continue_func(**kwargs):
    print('continue')

#####     AIRFLOW    ##########################################################################      
with models.DAG(
    dag_id = "load_data_into_api",
    start_date = days_ago(1),
    default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['weberv.souza@gmail.com'],
    'email_on_failure': True,    
    },
    schedule_interval = '0 10 * * *',
    max_active_runs = 1,
    tags = ["api", "raw zone"],

) as dag:

    begin = DummyOperator(
        talk_id = 'begin'
    )

    run_orc_request_main = ShortCircuitOperator(
        task_id = "postgres_task",
        provide_context = True,
        python_callable = orc_request_main,
        op_kwargs = {},
        )
    
    continue_op = PythonOperator(
        task_id = "continue_task",
        provide_context = True, 
        python_callable = continue_func,
        op_kwargs = {},
    )

    copy_files_input_processing = GCSToGCSOperator(
        task_id = "copy_files_input_processing",
        source_bucket = GCS_BUCKET, 
        source_object = f'input/{data_referencia}/arquivo.csv',
        destination_bucket = GCS_BUCKET,
        destination_object = f'processing/{data_referencia}/arquivo.csv',  
    )

    gcs_to_bq_task_pc = GCSToBigQueryOperator(
        task_id = f'gcs_to_bq',
        bucket = GCS_BUCKET,
        source_objects = [f'processing/{data_referencia}/arquivo.csv'],
        destination_project_dataset_table = '.'.join(
            [BQ_PROJECT, BQ_DS, SOURCE_TABLE_NAME]),
        create_disposition = 'CREATE_IF_NEEDED', #CREATE_NEVER
        write_disposition = 'WRITE_APPEND',  # WRITE_TRUNCATE  WRITE_EMPTY
        skip_leading_rows = 1,
        allow_quoted_newlines = True,
    )

    copy_files_processing_output = GCSToGCSOperator(
        task_id =   "copy_files_processing_output",
        source_bucket = GCS_BUCKET,
        source_object = f'processing/{data_referencia}/arquivo.csv',
        destination_bucket = GCS_BUCKET,
        destination_object = f'output/{data_referencia}/arquivo.csv',
    )

    end = DummyOperator(
        task_id = 'end'
    )

    begin >> run_orc_request_main >> continue_op >> copy_files_input_processing >> gcs_to_bq_task_pc >> end