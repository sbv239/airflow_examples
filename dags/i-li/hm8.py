from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_airflow():
    return 'Airflow tracks everything'

def pull(ti):
    ti.xcom_pull(
        key='return_value',
        task_ids='hm_10_1'
    )

with DAG(
        'hm_9_i-li',
        default_args={
            'dependes_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_in_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        start_date=datetime(2023, 2, 15)
) as dag:
    t1 = PythonOperator(
        task_id='hm_10_1',
        python_callable=print_airflow
    )
    t2= PythonOperator(
        task_id='hm_10_2',
        python_callable=pull
    )
    t1>>t2