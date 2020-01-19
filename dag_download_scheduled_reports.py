import os
import pathlib
import requests
import json
import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator



default_args = {
    'owner': 'airflow'
    ,'depends_on_past': False
    ,'start_date': airflow.utils.dates.days_ago(2)
    # All the parameters below are BigQuery specific and will be available to all the tasks
    ,'bigquery_conn_id': 'bigquery_default'
    ,'create_disposition': 'CREATE_IF_NEEDED'
    ,'google_cloud_storage_conn_id': 'google_cloud_storage_default'
    ,'write_disposition': 'WRITE_TRUNCATE'
    ,'retries': 10
    ,'retry_delay': timedelta(minutes = 10)
    ,'email': ['admin1@admin.com', 'admin2@admin.com']
    ,'email_on_failure': True
    ,'email_on_retry': False
}


report_list = ['167139', '167141','181041']


with DAG('download_scheduled_simplifi_reports'
        ,schedule_interval="30 21 * * *"
        ,catchup=False
        ,default_args=default_args) as dag:


        working_directory = '/home/airflow/gcs/data'
        gcs_bucket_id = 'sample_bucket_id'


        #working_directory = '/Users/matt/Desktop/simplifi/'
        #working_directory = '/home/airflow/gcs/data/'

        #current_date = datetime.today().strftime('%Y-%m-%d')
        current_date = datetime.today().strftime('%m-%d-%Y')
        current_date_nodash = datetime.today().strftime('%Y%m%d')

        ApplicationAPIKey = 'ApplicationAPIKey'
        UserAPIKey = 'UserAPIKey'
        OrganizationID = 'OrganizationID'

        headers = {
            'X-App-Key': ApplicationAPIKey
            ,'X-User-key': UserAPIKey
            ,'X-Api-Version': '2018-06-14'
            ,'Content-Type': 'application/json'
        }


        def download_scheduled_reports():

            pathlib.Path('{0}/data_files/{1}'.format(working_directory, current_date_nodash)).mkdir(parents=True, exist_ok=True)

            reports = requests.get('https://app.simpli.fi/api/organizations/{0}/report_center/reports'.format(OrganizationID), headers=headers)


            print(json.loads(reports.content)['reports'])

            report_raw = json.loads(reports.content)
            report_json = report_raw['reports']
            result_list = [json_dict['id'] for json_dict in report_json]

            print(result_list)

            for report in result_list:



                report_url = 'https://app.simpli.fi/api/organizations/{0}/report_center/reports/{1}'.format(
                    OrganizationID
                    ,report
                )

                reports = requests.get(report_url, headers=headers)

                d = json.loads(reports.content)
                r = d['reports']



                # print(r)
                # print([json_dict['title'] for json_dict in r])



                report_schedules_all_url = 'https://app.simpli.fi/api/organizations/{0}/report_center/reports/{1}/schedules/'.format(
                    OrganizationID
                    ,report
                )

                report_schedule_all = requests.get(report_schedules_all_url, headers = headers)
                print('ReportID = {0}'.format(report))
                # title = [json_dict['title'] for json_dict in report]
                # print(title)
                # print(report_schedule_all.content)

                s = json.loads(report_schedule_all.content)
                s_id  = s['schedules']

                for id in [json_dict['id'] for json_dict in s_id]:
                    sched_url = "https://app.simpli.fi/api/organizations/{0}/report_center/reports/{1}/schedules/{2}".format(
                        OrganizationID
                        ,report
                        ,id
                    )
                    sched = requests.get(sched_url, headers = headers)

                    download = json.loads(sched.content)
                    download_id = download['schedules']
                    downloads = download_id[0]['downloads']


                    if len(sched.content) > 0:
                        for download_links in downloads:
                            download_link = download_links['download_link']
                            report_date = str(download_links['date']).replace('/', '-')
                            csv_filename = '{0}/data_files/{1}/{2}_{3}.csv'.format(
                                working_directory
                                ,current_date_nodash
                                ,report
                                ,report_date
                            )

                            print('The report date is {0}'.format(report_date))
                            print('The current_date is {0}'.format(current_date))
                            print('download link is {0}'.format(download_link))


                            if download_link != None and report_date == current_date:
                                print(csv_filename)
                                print(download_link)
                                csv_file = requests.get(download_link, stream = True)
                                csv_file.raise_for_status()
                                with open(csv_filename, 'wb') as fd:
                                    for chunk in csv_file.iter_content(chunk_size = 50000):
                                        print(' Receieved a Chunk for report {0} on {1} ...'.format(report
                                                                                                    ,report_date))
                                        fd.write(chunk)




                        with open(working_directory + '/reports_schedule_{0}.json'.format(report), 'w') as f:
                            f.write(json.dumps(json.loads(sched.content)['schedules']
                                               ,indent=4))




        download = PythonOperator(
            task_id = 'download_files'
            ,python_callable = download_scheduled_reports
        )

        finished_downloading = DummyOperator(task_id = 'finished_downloading')
        finished_uploading_to_gbq = DummyOperator(task_id = 'finished_uploading_to_gbq')
        complete = DummyOperator(task_id = 'complete')

        download >> finished_downloading

        for file in report_list:


            import_to_gcs = GoogleCloudStorageToBigQueryOperator(
                task_id = 'import_to_gbq_{0}'.format(file)
                ,bucket = gcs_bucket_id
                ,source_objects = ['data/data_files/{0}/{1}_{2}.csv'.format(current_date_nodash, file, current_date)]
                ,destination_project_dataset_table = "canopy-advertising.reporting_raw.report_{0}".format(file)
                # ,google_cloud_storage_conn_id = google_cloud_storage_default
                # ,bigquery_conn_id = SOURCE_CONNECTION_ID
                # ,write_disposition = 'WRITE_TRUNCATE'
                ,create_disposition = 'CREATE_IF_NEEDED'
                #,schema_object = 'data/table_definitions/report_{0}_table_definition.json'.format(file)
                ,schema_object = 'data/table_definitions/report_{0}_table_definition.json'.format(file)
                ,field_delimiter = ','
                ,skip_leading_rows = 1
                ,max_bad_records = 0
                ,quote_character = '"'
                ,allow_jagged_rows = False
                ,on_retry_callback = lambda context: download.clear(
                    start_date=context['execution_date'],
                    end_date=context['execution_date'])
            )


            finished_downloading >> import_to_gcs >> finished_uploading_to_gbq



        success_email = EmailOperator(
            task_id = 'success_email'
            ,to = dag.default_args.get('email', [])
            ,subject = 'SUCCESS - DAG_ID = {{ dag.dag_id }}'
            ,html_content = "DAG_ID = {{ dag.dag_id }} has completed successfully on {{ ti.hostname }}"
            #,provide_context = True
        )

        finished_uploading_to_gbq >> success_email >> complete

