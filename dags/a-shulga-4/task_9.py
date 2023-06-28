from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def push_xcom(ti):
        ti.xcom_push(
                key="sample_xcom_key",
                value="xcom test"
        )

def pull_xcom(ti):
        print(ti.xcom_pull(key="sample_xcom_key", task_ids='xcom_pusher'))


with DAG(
    'hw_a-shulga-4_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 27),
    catchup=False
    ) as dag:
        
        t1 = PythonOperator(
            task_id='xcom_pusher',
            python_callable=push_xcom
        )

        t2 = PythonOperator(
            task_id='xcom_puller',
            python_callable=pull_xcom
        )

        t1 >> t2