from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def put_xcom(ti):
    return 'Airflow tracks everything'

def pull_n_print_xcom(ti):
    past_xcom = ti.xcom_pull(
        key='return_value',
        task_ids='python_task_push'
    )
    print(past_xcom)


with DAG(
    'lesson11_s-kosouhov_task_9',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 10),
    catchup=False,
    tags=['example_2'],
) as dag:

    task_1 = PythonOperator(
        task_id = f"python_task_push",
        python_callable=put_xcom,
    )

    task_2 = PythonOperator(
        task_id = f"python_task_pull",
        python_callable=pull_n_print_xcom,
    )

    task_1 >> task_2
    