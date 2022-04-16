from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'G-Ivanov-task8',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    def push(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )

    def pull(ti):
        xcom_test = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='push_xcom'
        )
        print(xcom_test)


    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push,
    )
    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull,
    )

    t1 >> t2
