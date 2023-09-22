from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime


def return_text():
    return 'Airflow tracks everything'


def get_xcom(ti):
    value = ti.xcom_pull(
        key='return_value',
        task_ids='push_xcom_test'
    )
    print(value)


with DAG(
        'hw_n-knjazeva-24_10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 9, 21),
        schedule_interval=timedelta(days=1)
) as dag:

    t1 = PythonOperator(
        task_id='push_xcom_test',
        python_callable=return_text,
    )
    t2 = PythonOperator(
        task_id='get_xcom_test',
        python_callable=get_xcom,
    )

    t1 >> t2