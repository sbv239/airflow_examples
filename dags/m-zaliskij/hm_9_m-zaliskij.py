from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def xcom_push():
    return 'Airflow tracks everything'


def xcom_pull(ti):
    print(ti.xcom_pull(
        key='return_value',
        task_ids='xcom_push'
    ))


with DAG(
        'hw_9_m-zaliskij',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        start_date=datetime(2022, 1, 1),
        catchup=False

) as dag:
    t1 = PythonOperator(
        task_id='xcom_push',
        python_callable=xcom_push
    )

    t2 = PythonOperator(
        task_id='xcom_pull',
        python_callable=xcom_pull
    )

    t1 >> t2
