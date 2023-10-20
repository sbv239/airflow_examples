from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def push_hidden():
    return "Airflow tracks everything"


def pull_hidden(ti):
    ti.xcom_pull(
        key="return_value",
        task_ids="push_hidden_xm"
    )


with DAG(
    'j-rzayev_task_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
   },
    description='task_10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 20),
    catchup=False,
    tags=['task_10', 'lesson_11', 'j-rzayev'],
) as dag:
    task_1 = PythonOperator(
        task_id='push_hidden_xm',
        python_callable=push_hidden
    )
    task_2 = PythonOperator(
        task_id="pull_hidden_xm",
        python_callable=pull_hidden
    )

    task_1 >> task_2
