import airflow
import csv
import errno
import json
import logging
import random
import requests
import time
import os
import os.path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
# from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator



default_args = {
    'owner': 'airflow'
    ,'depends_on_past': False
    ,'start_date': airflow.utils.dates.days_ago(25)
    # All the parameters below are BigQuery specific and will be available to all the tasks
    ,'bigquery_conn_id': 'bigquery_default'
    ,'create_disposition': 'CREATE_IF_NEEDED'
    ,'google_cloud_storage_conn_id': 'google_cloud_storage_default'
    ,'write_disposition': 'WRITE_TRUNCATE'
    ,'retries': 12
    ,'retry_delay': timedelta(minutes = 5)
    ,'email': ['admin1@admin.com', 'admin2@admin.com']
    ,'email_on_failure': True
    ,'email_on_retry': False
}

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def safe_open_w_plus(path):
    ''' Open "path" for writing, creating any parent directories as needed.
    '''
    mkdir_p(os.path.dirname(path))
    return open(path, 'w+')

with DAG('download_scheduled_callrail_data_v2'
        ,schedule_interval="30 0 * * *"
        ,catchup=True
        ,default_args=default_args) as dag:

        # Just 1 Database call
        config = Variable.get('callrail_to_bq_config', deserialize_json=True)

        working_directory = '/home/airflow/gcs/data'

        sec_to_sleep = 300

        def download_callrail_calls_data(**kwargs):

            dt = kwargs["execution_date"]
            # dt = dt - timedelta(days=1)
            str_start_time = dt.strftime('%Y-%m-%d')
            str_start_time_nodash = dt.strftime('%Y%m%d')
            str_end_time = str_start_time

            logging.info(json.dumps(config))

            account_number = config['callrail_account_number']
            records_per_page = config['api_results_per_page']
            gcp_project_id = config['gcp_client_project_id']

            authorization_header_value = config['callrail_api_auth_token']
            headers = {
                'Authorization': authorization_header_value 
                ,'Content-Type': 'application/json'
            }
            
            companies_url = config['callrail_api_for_companies'].format(account_number=account_number,per_page=records_per_page)

            resp = requests.get(companies_url, headers=headers)
            logging.info('Url: {} Status code:  {} returned '.format(companies_url, resp.status_code))
            if resp.status_code != 200:
                if resp.status_code == 429 or resp.status_code == 503:
                    # Going to wait because the quota limit is reached
                    logging.info('Status code {} returned '.format(resp.status_code))
                    logging.info('Sleeping for {} seconds ...'.format(sec_to_sleep))
                    time.sleep(sec_to_sleep)
                    sec_to_sleep *= 2
                    raise Exception('GET /companies.json/ {}'.format(resp.status_code))
                    # continue
                elif resp.status_code == 400:
                    pass
                else:
                    # This means something went wrong.
                    raise Exception('GET /companies.json/ {}'.format(resp.status_code))
            try:
                for company in resp.json()['companies']:

                    company_id = company['id']
                    company_name = company['name']

                    calls_url = config['callrail_api_for_calls'].format(account_number=account_number,company_id=company_id,
                        start_date=str_start_time,end_date=str_end_time,per_page=records_per_page)
                    
                    resp_inner = requests.get(calls_url, headers=headers)
                    if resp_inner.status_code != 200:
                        if resp_inner.status_code == 429 or resp_inner.status_code == 503:
                            # Going to wait because the quota limit is reached
                            logging.info('Status code {} returned '.format(resp_inner.status_code))
                            logging.info('Sleeping for {} seconds ...'.format(sec_to_sleep))
                            time.sleep(sec_to_sleep)
                            sec_to_sleep *= 2
                            continue
                        elif resp_inner.status_code == 400:
                            pass
                        else:
                            # This means something went wrong.
                            raise Exception('GET /calls.json/ {}'.format(resp_inner.status_code))
                    try:

                        csv_filename = '{0}/callrail_data/{1}/{2}_{3}.csv'.format(
                            working_directory
                            ,account_number
                            ,str_start_time_nodash
                            ,company_id
                        )

                        json_content = resp_inner.json()
                        
                        logging.info(json.dumps(json_content['calls']))

                        with safe_open_w_plus(csv_filename) as fd:
                            csvwriter = csv.writer(fd)

                            count = 0
                            for call in json_content['calls']:
                                logging.info(json.dumps(call))
                                if count == 0:
                                    csv_header = []
                                    csv_header.append('company_id')
                                    csv_header.append('company_name')
                                    for c in call.keys():
                                        csv_header.append(c)
                                    logging.info(json.dumps(csv_header))
                                    csvwriter.writerow(csv_header)
                                    count += 1

                                csv_row = []
                                csv_row.append(company_id)
                                csv_row.append(company_name)
                                for v in call.values():
                                    csv_row.append(v)
                                logging.info(csv_row)
                                csvwriter.writerow(csv_row)

                        sec_to_sleep = 5
                    except ValueError:
                          pass

                sec_to_sleep = 5
            except ValueError:
                  pass


        download_data = PythonOperator(
            task_id='task_download_callrail_calls_data'
            ,provide_context=True
            ,python_callable = download_callrail_calls_data
        )

        finish_download = DummyOperator(task_id = 'task_finish_download')
        finish_move_to_client_gcs = DummyOperator(task_id = 'task_finish_move_to_client_gcs')
        finish_upload_to_bq = DummyOperator(task_id = 'task_finish_upload_to_bq')
        complete = DummyOperator(task_id = 'task_complete')

        execution_date = '{{ ds_nodash }}'

        move_to_client_gcs = BashOperator(
            task_id = 'task_move_to_client_gcs'
            ,bash_command='gsutil -m mv ' 
                + 'gs://' + config['gcp_composer_gcs_bucket'] + '/data/callrail_data/{0}/{1}*'.format(config['callrail_account_number'],execution_date) + ' ' 
                + 'gs://' + config['gcp_client_gcs_bucket'] + '/data/callrail_data/{0}'.format(config['callrail_account_number'])
        )

        # move_to_client_gcs = GoogleCloudStorageToGoogleCloudStorageOperator(
        #     task_id = 'task_move_to_client_gcs'
        #     ,source_bucket = config['gcp_composer_gcs_bucket']
        #     ,source_object = ['data/callrail_data/{0}/{1}*'.format(config['callrail_account_number'],execution_date)]
        #     ,destination_bucket = config['gcp_client_gcs_bucket']
        #     ,destination_object = ['data/callrail_data/{0}/'.format(config['callrail_account_number'])]
        #     ,move_object = True
        # )

        import_to_bq = GoogleCloudStorageToBigQueryOperator(
            task_id = 'task_import_to_bq'
            ,bucket = config['gcp_client_gcs_bucket']
            ,source_objects = ['data/callrail_data/{0}/{1}*'.format(config['callrail_account_number'],execution_date)]
            ,destination_project_dataset_table = config['gcp_client_project_id'] + ':' + config['gcp_client_bq_dataset'] + '.' + config['gcp_client_bq_tablename']
            ,create_disposition = 'CREATE_IF_NEEDED'
            ,schema_object = config['callrail_table_schema']
            ,write_disposition = 'WRITE_APPEND'
            ,field_delimiter = ','
            ,skip_leading_rows = 1
            ,max_bad_records = 0
            ,quote_character = '"'
            ,allow_jagged_rows = False
            ,on_retry_callback = lambda context: download_data.clear(
                start_date=execution_date,
                end_date=execution_date)
        )

        download_data >> finish_download >> move_to_client_gcs >> finish_move_to_client_gcs >> import_to_bq >> finish_upload_to_bq


        send_success_email = EmailOperator(
            task_id = 'task_send_success_email'
            ,to = dag.default_args.get('email', [])
            ,subject = 'SUCCESS - DAG_ID = {{ dag.dag_id }}'
            ,html_content = "DAG_ID = {{ dag.dag_id }} has completed successfully on {{ ti.hostname }}"
        )

        finish_upload_to_bq >> send_success_email >> complete