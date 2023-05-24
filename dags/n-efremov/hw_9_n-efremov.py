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

with DAG(
        "hw_9_n-efremov",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 22),
        catchup=False,
        tags=['n-efremov'],
) as dag:
    def push_xcom(ti):
        ti.xcom_push(
            key="sample_xcom_key",
            value="xcom test"
        )


    def pull_xcom(ti):
        testing_pull = ti.xcom_pull(
            key="sample_xcom_key",
            task_ids="push_xcom"
        )
        print(testing_pull)


    push_xcom_dag = PythonOperator(
        task_id="push_xcom",
        python_callable=push_xcom,
    )

    pull_xcom_dag = PythonOperator(
        task_id="xcom_data",
        python_callable=pull_xcom
    )

    push_xcom_dag >> pull_xcom_dag
