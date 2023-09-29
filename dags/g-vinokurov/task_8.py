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

with DAG('hw_g-vinokurov_8',
    default_args=default_args,
        description='DAG in task_2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 9, 28),
        catchup=False,
) as dag:

    def push(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value="xcom test"
        )

    def pull(ti):
        testing = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='push'
        )
        print(testing)


    operator_1 = PythonOperator(
        task_id='push',
        python_callable=push,
    )

    operator_2 = PythonOperator(
        task_id='pull',
        python_callable=pull,
    )

    operator_1 >> operator_2