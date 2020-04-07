import datetime
import json

import airflow
from airflow.models import Variable
from airflow.operators import http_operator, python_operator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
USERNAME = Variable.get("username")
PASSWORD = Variable.get("password")

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
                                      data={"sysparm_limit": 10},
                                      method="GET",
                                      xcom_push=True,
                                      headers={"Content-Type": "application/json",
                                               "Accept": "application/json"},
                                      dag=dag)


def handler(**context):
    ti = context['ti']
    query_data = ti.xcom_pull(key=None, task_ids='query_data')
    if query_data:
        print("Rec Query Data" + query_data)


t2 = python_operator.PythonOperator(task_id="handle_response",
                                    python_callable=handler,
                                    provide_context=True,
                                    dag=dag)

t1.set_downstream(t2)
