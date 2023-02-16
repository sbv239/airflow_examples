from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def push_xcom(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )


def pull_xcom(ti):
    result = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='hm_9_1'
    )
    print(result)


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
        task_id='hm_9_1',
        python_callable=push_xcom
    )

    t2 = PythonOperator(
        task_id='hm_9_2',
        python_callable=pull_xcom
    )

t1>>t2