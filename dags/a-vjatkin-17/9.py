from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def xcom_push(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value="xcom test"
    )


def xcom_pull(ti):
    result = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids="xcom_example_first_python_operator"
    )

    print(result)


with DAG(
    'a-vjatkin-17_task_9',
    default_args=default_args,
    description='test DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 2, 12),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id=f"xcom_example_first_python_operator",
        python_callable=xcom_push
    )

    t2 = PythonOperator(
        task_id=f"xcom_example_second_python_operator",
        python_callable=xcom_pull
    )

    t1 >> t2
