import airflow
import calendar 
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
from io import BytesIO, StringIO
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
    ,'start_date': airflow.utils.dates.days_ago(24)
    # All the parameters below are BigQuery specific and will be available to all the tasks
    ,'bigquery_conn_id': 'bigquery_default'
    ,'google_cloud_storage_conn_id': 'google_cloud_storage_default'
    ,'retries': 10
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

with DAG('download_scheduled_rtbiq_data_v2'
        ,schedule_interval="15 0 * * *"
        ,catchup=True
        ,default_args=default_args) as dag:

        # Just 1 Database call
        config = Variable.get('rtbiq_to_bq_config', deserialize_json=True)

        working_directory = '/home/airflow/gcs/data'

        sec_to_sleep = 300

        def download_rtbiq_impressions_data(**kwargs):

            dt = kwargs["execution_date"];
            dt_next = dt + timedelta(days=1);
            
            str_start_time = dt.strftime('%Y-%m-%d');
            str_end_time = dt_next.strftime('%Y-%m-%d');

            tm = calendar.timegm(time.strptime(str_start_time, '%Y-%m-%d')) - 1;
            # tm = tm + 14400; # UTC-4
            tm_next = calendar.timegm(time.strptime(str_end_time, '%Y-%m-%d')) - 1;
            # tm_next = tm_next + 14400; # UTC-4

            str_start_time_nodash = dt.strftime('%Y%m%d')
            
            gcp_project_id = config['gcp_client_project_id']

            authorization_header_key_name = config['rtbiq_api_auth_key_name']
            authorization_header_key_value = config['rtbiq_api_auth_key_value']
            headers = {
                authorization_header_key_name: authorization_header_key_value 
                ,'Content-Type': 'application/json'
            }
            
            campaigns_url = config['rtbiq_api_for_campaigns']

            resp = requests.get(campaigns_url, headers=headers)

            if resp.status_code != 200:
                if resp.status_code == 429 or resp.status_code == 503:
                    # Going to wait because the quota limit is reached
                    logging.info('Status code {} returned '.format(resp.status_code))
                    logging.info('Sleeping for {} seconds ...'.format(sec_to_sleep))
                    time.sleep(sec_to_sleep)
                    sec_to_sleep *= 2
                    raise Exception('GET /campaigns/ {}'.format(resp.status_code))
                    # continue
                elif resp.status_code == 400:
                    pass
                else:
                    # This means something went wrong.
                    raise Exception('GET /campaigns/ {}'.format(resp.status_code))

            try:
                campaigns_json_content = resp.json();

                for campaign in campaigns_json_content['campaigns']:
                    try:
                        campaign_data = [];

                        groups_content = [];
                        placements_content = [];

                        campaign_id = campaign['id'];
                        campaign_name = campaign['name'];
                        campaign_end_time = campaign['end'];

                        # if campaign is not over yet or if it ended in last 24 hrs
                        if campaign_end_time >= tm_next or (campaign_end_time <= tm_next and campaign_end_time > tm):

                            placement_groups = campaign['groups'];
                            placements = campaign['placements'];

                            if len(placement_groups) > 0 and len(groups_content) == 0:

                                groups_url = config['rtbiq_api_for_placement_groups'];
                                
                                resp_groups = requests.get(groups_url, headers=headers);

                                if resp_groups.status_code != 200:
                                    if resp_groups.status_code == 429 or resp_groups.status_code == 503:
                                        # Going to wait because the quota limit is reached
                                        logging.info('Status code {} returned '.format(resp_groups.status_code))
                                        logging.info('Sleeping for {} seconds ...'.format(sec_to_sleep))
                                        time.sleep(sec_to_sleep)
                                        sec_to_sleep *= 2
                                        continue
                                    elif resp_groups.status_code == 400:
                                        pass
                                    else:
                                        # This means something went wrong.
                                        raise Exception('GET /placementGroups/ {}'.format(resp_groups.status_code))
                                try:

                                    json_content = resp_groups.json();
                                    
                                    logging.info(json.dumps(json_content['placementGroups']))

                                    groups_content = json_content['placementGroups'];

                                except ValueError:
                                      pass

                            if len(placements) > 0 and len(placements_content) == 0:

                                placements_url = config['rtbiq_api_for_placements'];
                                
                                resp_placements = requests.get(placements_url, headers=headers);

                                if resp_placements.status_code != 200:
                                    if resp_placements.status_code == 429 or resp_placements.status_code == 503:
                                        # Going to wait because the quota limit is reached
                                        logging.info('Status code {} returned '.format(resp_placements.status_code))
                                        logging.info('Sleeping for {} seconds ...'.format(sec_to_sleep))
                                        time.sleep(sec_to_sleep)
                                        sec_to_sleep *= 2
                                        continue
                                    elif resp_placements.status_code == 400:
                                        pass
                                    else:
                                        # This means something went wrong.
                                        raise Exception('GET /placements/ {}'.format(resp_placements.status_code))
                                try:

                                    json_content = resp_placements.json();
                                    
                                    logging.info(json.dumps(json_content['placements']))

                                    placements_content = json_content['placements'];

                                except ValueError:
                                      pass

                            # iterate over placement IDs to grab the stats frm query/v2 end point.
                            queries_url = config['rtbiq_api_for_queries']

                            for placement in placements:

                                query = {
                                    "format": "csv"
                                    ,"style": "flat"
                                    ,"placements": [placement]
                                    ,"filters": [
                                        {"dimension": "time", "include": "True", "values": [[tm,tm_next]]}
                                    ]
                                    ,"dimensions": [
                                        {"id": "time", "precision": 86400}
                                        
                                    ]
                                };
                                
                                resp_query = requests.post(queries_url, headers=headers, json=query);

                                if resp_query.status_code != 200:
                                    if resp_query.status_code == 429 or resp_query.status_code == 503:
                                        # Going to wait because the quota limit is reached
                                        logging.info('Status code {} returned '.format(resp_query.status_code))
                                        logging.info('Sleeping for {} seconds ...'.format(sec_to_sleep))
                                        time.sleep(sec_to_sleep);
                                        sec_to_sleep *= 2;

                                    elif resp_query.status_code == 400:
                                        pass
                                    else:
                                        # This means something went wrong.
                                        raise Exception('POST /query/v2/ {}'.format(resp_query.status_code))

                                try:
                                    logging.info(placement);
                                    filtered_placement_itr = filter(lambda x : x['id'] == placement, placements_content);
                                    
                                    filtered_placement = list(filtered_placement_itr)[0];

                                    placement_id = filtered_placement['id'];
                                    placement_name = filtered_placement['name'];

                                    filtered_group_itr = filter(lambda x : x['id'] == filtered_placement['group'], groups_content);
                                    filtered_group = list(filtered_group_itr)[0];

                                    group_id = filtered_group['id'];
                                    group_name = filtered_group['name'];
                                    
                                    query_resp_content = resp_query.content.decode();
                                    
                                    logging.info(query_resp_content);

                                    if query_resp_content:

                                        content_lines = query_resp_content.splitlines();

                                        if content_lines and len(content_lines) == 7:
                                            csv_row = [campaign_id,campaign_name,group_id,group_name,placement_id,placement_name, str_start_time];
                                            
                                            for csv_row_values in csv.DictReader(StringIO(content_lines[5] + os.linesep + content_lines[6])
                                                # ,['time','impressions','media spend','total spend','contract CPM','contract spend'
                                                # ,'spend margin','platform fee','ShareThis Segments fee','banner fee','click']
                                            ):
                                                
                                                if 'impressions' in csv_row_values:
                                                    csv_row.append(csv_row_values['impressions']);
                                                else:
                                                    csv_row.append('');
                                                
                                                if 'media spend' in csv_row_values:
                                                    csv_row.append(csv_row_values['media spend']);
                                                else:
                                                    csv_row.append('');
                                                
                                                if 'total spend' in csv_row_values:
                                                    csv_row.append(csv_row_values['total spend']);
                                                else:
                                                    csv_row.append('');
                                                
                                                if 'contract CPM' in csv_row_values:
                                                    csv_row.append(csv_row_values['contract CPM']);
                                                else:
                                                    csv_row.append('');
                                                
                                                if 'contract spend' in csv_row_values:
                                                    csv_row.append(csv_row_values['contract spend']);
                                                else:
                                                    csv_row.append('');
                                                
                                                if 'spend margin' in csv_row_values:
                                                    csv_row.append(csv_row_values['spend margin']);
                                                else:
                                                    csv_row.append('');
                                                
                                                if 'platform fee' in csv_row_values:
                                                    csv_row.append(csv_row_values['platform fee']);
                                                else:
                                                    csv_row.append('');
                                                
                                                if 'ShareThis Segments fee' in csv_row_values:
                                                    csv_row.append(csv_row_values['ShareThis Segments fee']);
                                                else:
                                                    csv_row.append('');
                                                
                                                if 'banner fee' in csv_row_values:
                                                    csv_row.append(csv_row_values['banner fee']);
                                                else:
                                                    csv_row.append('');
                                                
                                                if 'click' in csv_row_values:
                                                    csv_row.append(csv_row_values['click']);
                                                else:
                                                    csv_row.append('');
                                                
                                                campaign_data.append(csv_row);

                                except ValueError:
                                      continue

                            csv_filename = '{0}/rtbiq_data/{1}_{2}.csv'.format(
                                working_directory
                                ,str_start_time_nodash
                                ,campaign_id
                            )

                            with safe_open_w_plus(csv_filename) as fd:
                                csvwriter = csv.writer(fd);

                                csv_header = ['campaign_id','xvid','group_id','group_name','placement_id','placement_name'
                                            ,'date','impressions','media_spend','total_spend','contract_cpm','contract_spend','spend_margin'
                                            ,'platform_fee','share_this_segments_fee','banner_fee','click'];
                                
                                csvwriter.writerow(csv_header);

                                for row in campaign_data:
                                    logging.info(row);

                                    csvwriter.writerow(row);
                    except:
                        pass

            except ValueError:
                  pass


        download_data = PythonOperator(
            task_id='task_download_rtbiq_impressions_data'
            ,provide_context=True
            ,python_callable = download_rtbiq_impressions_data
        )

        finish_download = DummyOperator(task_id = 'task_finish_download')
        finish_move_to_client_gcs = DummyOperator(task_id = 'task_finish_move_to_client_gcs')
        finish_upload_to_bq = DummyOperator(task_id = 'task_finish_upload_to_bq')
        complete = DummyOperator(task_id = 'task_complete')

        execution_date = '{{ ds_nodash }}'

        move_to_client_gcs = BashOperator(
            task_id = 'task_move_to_client_gcs'
            ,bash_command='gsutil -m mv ' 
                + 'gs://' + config['gcp_composer_gcs_bucket'] + '/data/rtbiq_data/{0}*'.format(execution_date) + ' ' 
                + 'gs://' + config['gcp_client_gcs_bucket'] + '/data/rtbiq_data/'
        )

        import_to_bq = GoogleCloudStorageToBigQueryOperator(
            task_id = 'task_import_to_bq'
            ,bucket = config['gcp_client_gcs_bucket']
            ,source_objects = ['data/rtbiq_data/{0}*'.format(execution_date)]
            ,destination_project_dataset_table = config['gcp_client_project_id'] + ':' + config['gcp_client_bq_dataset'] + '.' + config['gcp_client_bq_tablename']
            ,create_disposition = 'CREATE_IF_NEEDED'
            ,schema_object = config['rtbiq_table_schema']
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

