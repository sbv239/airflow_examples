from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# aleksandraleksand-ivanov
default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        dag_id="hw_aleksandraleksand-ivanov_8",
        default_args=default_args,
        start_date=datetime(2023, 9, 18),
        schedule_interval=timedelta(days=1)
) as dag:
    def push_xcom(ti):
        ti.xcom_push(key="sample_xcom_key", value="xcom test")


    def pull_xcom(ti):
        print(ti.xcom_pull(task_ids="push", key="sample_xcom_key"))

    push = PythonOperator(
        task_id="push",
        python_callable=push_xcom
    )

    pull = PythonOperator(
        task_id="pull",
        python_callable=pull_xcom
    )

    push >> pull