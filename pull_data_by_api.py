import csv
import datetime
import json
import logging

import airflow
import pandas as pd
from airflow.contrib.operators import file_to_gcs, gcs_to_bq
from airflow.models import Variable
from airflow.operators import http_operator, python_operator
from pandas.io.json import json_normalize

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
USERNAME = Variable.get("username")
PASSWORD = Variable.get("password")
TABLE_LIST = ["sys_user", "cmn_location"]

default_args = {
    'owner': 'Aifei Lu',
    'depends_on_past': False,
    'email': ['lu.aifei@thoughtworks.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': YESTERDAY,
}

dag = airflow.DAG("pull_data_by_api",
                  default_args=default_args,
                  schedule_interval=datetime.timedelta(days=1))

t1 = http_operator.SimpleHttpOperator(task_id="query_data",
                                      http_conn_id="servicenow_connect",
                                      endpoint="/api/now/table/sys_user",
                                      data={"sysparm_limit": 10,
                                            "sysparm_exclude_reference_link": True,
                                            "sysparm_fields": "active,email,employee_number,name,sys_tags,location"},
                                      method="GET",
                                      xcom_push=True,
                                      headers={"Content-Type": "application/json",
                                               "Accept": "application/json"},
                                      dag=dag)


def handle_response(**context):
    ti = context['ti']
    query_data: str = ti.xcom_pull(key=None, task_ids='query_data')
    query_data = query_data.replace("@thoughtworks.com", "@test.com")

    logging.info("Desensitized Data: " + query_data)
    if query_data:
        data = json.loads(query_data)
        df = json_normalize(data["result"])
        df.to_csv("user.csv", index=False, header=False)


t2 = python_operator.PythonOperator(task_id="handle_response",
                                    python_callable=handle_response,
                                    provide_context=True,
                                    dag=dag)


t3 = file_to_gcs.FileToGoogleCloudStorageOperator(task_id="upload_raw_data",
                                                  src="user.csv",
                                                  dst="data/user.csv",
                                                  retry_delay=5,
                                                  retries=1,
                                                  bucket="asia-northeast1-example-env-c50e72d7-bucket",
                                                  dag=dag)


t4 = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(task_id="load_into_bq",
                                                    bucket="asia-northeast1-example-env-c50e72d7-bucket",
                                                    source_objects=["data/user.csv"],
                                                    schema_fields=[
                                                        {'name': 'active', 'type': 'BOOL'},
                                                        {'name': 'email', 'type': 'STRING'},
                                                        {'name': 'employee_number', 'type': 'STRING'},
                                                        {'name': 'name', 'type': 'STRING'},
                                                        {'name': 'sys_tags', 'type': 'STRING'},
                                                        {'name': 'location', 'type': 'STRING'}
                                                    ],
                                                    destination_project_dataset_table="composer_demo.user",
                                                    write_disposition='WRITE_TRUNCATE',
                                                    autodetect=True,
                                                    dag=dag)

t1 >> t2 >> t3 >> t4
