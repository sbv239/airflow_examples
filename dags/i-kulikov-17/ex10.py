from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


with DAG(
        'hw_10_i-kulikov-17',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='x-com',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 10),
        catchup=False,
        tags=['hw_10_i-kulikov-17']
) as dag:

    def push_value(ti):
        return "Airflow tracks everything"

    t1 = PythonOperator(
        task_id='push_value',
        python_callable=push_value,
    )

    def pull_xcom(ti):
        print(
            ti.xcom_pull(
                key="return_value",
                task_ids="push_value"
            )
        )

    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom,
    )

    t1 >> t2
