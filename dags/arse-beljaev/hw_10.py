from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator


def put_data():
    return "Airflow tracks everything"


def pull_data(ti):
    value = ti.xcom_pull(
        key='return_value',
        task_ids='pulling_data'
    )
    print(f"{value}")


with DAG(
    'hw_arse-beljaev_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
        },
    description='hw_9_lesson_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 18),
    catchup=False,
    tags=['example']
) as dag:

    t1 = PythonOperator(
        task_id='pulling_data',
        python_callable=put_data,
    )

    t2 = PythonOperator(
        task_id='data',
        python_callable=pull_data,
    )

    t1 >> t2
