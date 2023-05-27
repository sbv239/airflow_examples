from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def return_str(ti):
    return "Airflow tracks everything"


def get_str(ti):
    print(ti.xcom_pull(
        key="return_value",
        task_ids='xcom_push')
    )


with DAG(
        "hw_10_n-efremov",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 22),
        catchup=False,
        tags=['n-efremov'],
) as dag:
    return_xcom_dag = PythonOperator(
        task_id="xcom_push",
        python_callable=return_str,
    )

    pull_xcom_dag = PythonOperator(
        task_id="xcom_pull",
        python_callable=get_str
    )

    return_xcom_dag >> pull_xcom_dag
