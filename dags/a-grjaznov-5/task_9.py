from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'task_8_grjaznov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
    description='task_8_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False,
    tags=['hw_8_a-grjaznov-5'],
) as dag:
    def func():
        return "Airflow tracks everything"

    t1 = PythonOperator(
        task_id='return',
        python_callable=func
    )

    def func_pull(ti):
        output = ti.xcom_pull(
            key='return_value',
            task_ids='pull'
        )
        print(output)

    t2 = PythonOperator(
        task_id='pull',
        python_callable=func_pull
    )

    t1 >> t2
