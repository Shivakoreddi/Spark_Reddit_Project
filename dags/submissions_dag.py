import sys

sys.path.append('~srp_env/lib/python3.7/site-packages/')
import submissions_fetch

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 10, 6),
    "retires": 0

}

dag = DAG(
    dag_id="submissions_DAG",
    default_args=default_args,
    catchup=False,
    schedule_interval="@once"
)

start = DummyOperator(
    task_id="start",
    dag=dag
)

submissions_fetch = PythonOperator(
    task_id="submissions_fetch",
    dag=dag,
    python_callable=submissions_fetch.main
)
