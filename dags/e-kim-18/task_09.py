from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def push_xcom(ti):
    sample_xcom_key = "xcom test"
    # в ti уходит task_instance, его передает Airflow под таким названием
    # когда вызывает функцию в ходе PythonOperator
    ti.xcom_push(
        key='sample_xcom_key',
        value=sample_xcom_key
    )


def xcom_pull(ti):
    testing_increases = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='push_xcom'
    )
    #print(testing_increases)

with DAG(
        'e-kim-18_task_09',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A DAG for task 02',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 20),
        catchup=False,
        tags=['e-kim-18-tag'],
) as dag:
    t1 = PythonOperator(
        task_id = 'push_xcom',
        python_callable=push_xcom,
    )
    t2 = PythonOperator(
        task_id = 'xcom_pull',
        python_callable=xcom_pull,
    )

    t1 >> t2