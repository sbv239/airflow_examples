from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

with DAG(
        'task9NN',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='sample xcom',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 21),
        catchup=False,
        tags=['NNtask9'],
) as dag:
    def xcom_push(ti):
        ti.xcom_push(key="sample_xcom_key",
                     value='xcom test')


    task1 = PythonOperator(task_id='PushXcom',
                           python_callable=xcom_push
                           )


    def print_xcom(ti):
        ti.xcom_pull(key="sample_xcom_key",
                     task_ids='PushXcom')


    task2 = PythonOperator(task_id='PullXcom',
                           python_callable=print_xcom)

    task1 >> task2
