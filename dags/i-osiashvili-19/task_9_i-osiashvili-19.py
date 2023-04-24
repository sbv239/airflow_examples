from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator


def push_data(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test",
    )

def pull_data(ti):
    meaning = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids="push_data_id")
    print(meaning)

with DAG(
        'les_11_task_9_i-osiashvili-19',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 4, 20),
        schedule_interval=timedelta(days=1),

) as dag:
    pushing_data = PythonOperator(
        task_id="push_data_id",
        python_callable=push_data,
    )

    pulling_data = PythonOperator(
        task_id="pull_data_id",
        python_callable=pull_data,
    )

    pushing_data >> pulling_data