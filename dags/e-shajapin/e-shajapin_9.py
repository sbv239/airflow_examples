from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'hw_9_e-shajapin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 20),
    catchup=False,
) as dag:

    def get_data(ti):
        data = "xcom test"
        ti.xcom_push(
            key="sample_xcom_key",
            value=data,
        )

    def print_data(ti):
        data = ti.xcom_pull(
            key="sample_xcom_key",
            task_ids="get_data",
        )
        print(data)

    get = PythonOperator(
        task_id="get_data",
        python_callable=get_data,
    )

    pr = PythonOperator(
        task_id="print_data",
        python_callable=print_data,
    )

    get >> pr
