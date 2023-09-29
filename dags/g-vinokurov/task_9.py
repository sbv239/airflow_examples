from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('hw_g-vinokurov_9',
    default_args=default_args,
        description='DAG in task_9',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 9, 28),
        catchup=False,
) as dag:

    def push_func():
        return "Airflow tracks everything"


    def get_func(ti):
        result = ti.xcom_pull(
            key='return_value',
            task_ids='get_task'
        )
        print(result)


    get_task = PythonOperator(
        task_id='get_task',
        python_callable=push_func,
    )
    return_task = PythonOperator(
        task_id='return_task',
        python_callable=get_func,
    )

    get_task >> return_task