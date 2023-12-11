from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
    "hw_10_n_vojtova",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG with Xcom_2',
    schedule_interval= timedelta(days=1),
    start_date=datetime(2023,12,9),
    catchup=False,
    tags = ['hw_10','n_vojtova'],
) as dag:

    def push_xcom(ti):
        return "Airflow tracks everything"

    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push_xcom,
    )
    def receive_xcom(ti):
        print(ti.xcom_pull(key='return_value', task_ids='push_xcom'))

    t2 = PythonOperator(
        task_ids='receive_xcom',
        python_callable=receive_xcom,
    )

    t1 >> t2