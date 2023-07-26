from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'i-mjasnikov-22_hw_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='10 hw',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 2, 10),
    catchup=False,

) as dag:

    def put_data():
        return "Airflow tracks everything"

    def get_data(ti):
        print(ti.xcom_pull(key="return_value", task_ids="i-mjasnikov-22_hw_10_put_data_task"))

    t1 = PythonOperator(
        task_id='i-mjasnikov-22_hw_10_put_data_task',
        python_callable=put_data
    )

    t2 = PythonOperator(
        task_id='i-mjasnikov-22_hw_10_get_data_task',
        python_callable=get_data
    )

    t1 >> t2
