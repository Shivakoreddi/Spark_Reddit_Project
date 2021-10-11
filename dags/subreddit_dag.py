import sys

sys.path.append('~srp_env/lib/python3.7/site-packages/')
import subreddit_fetch

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 10, 6),
    "retires": 0

}

dag = DAG(
    dag_id="subreddit_DAG",
    default_args=default_args,
    catchup=False,
    schedule_interval="@once"
)

start = DummyOperator(
    task_id="start",
    dag=dag
)

subreddit_fetch = PythonOperator(
    task_id="subreddit_fetch",
    dag=dag,
    python_callable=subreddit_fetch.main
)
