from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator


def push_xcom():
    return "Airflow tracks everything"


def pull_xcom(ti):
    print(ti.xcom_pull(
        key="return_value",
        task_ids='push_xcom'
    ))


with DAG(
    'hw_e-zastrogina_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 15)
) as dag:
    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push_xcom,
        provide_context=True
    )
    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom,
        provide_context=True
    )
    t1 >> t2
