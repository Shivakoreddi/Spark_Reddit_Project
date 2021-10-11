from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

import sys

sys.path.append('~srp_env/lib/python3.7/site-packages/')
import redditor_fetch

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 10, 6),
    "retires": 0

}

dag = DAG(
    dag_id="redditor_DAG",
    default_args=default_args,
    catchup=False,
    schedule_interval="@once"
)

start = DummyOperator(
    task_id="start",
    dag=dag
)

reddit_fetch = PythonOperator(
    task_id="reddit_fetch",
    dag=dag,
    python_callable=redditor_fetch.main
)