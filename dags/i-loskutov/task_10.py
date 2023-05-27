from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def push_xcom(ti):
    return "Airflow tracks everything"

def pull_xcom(ti):
    xcom = ti.xcom_pull(
        key='return_value',
        task_ids='push_xcom'
    )
    print(xcom)

with DAG(
    'hw_10_i-loskutov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
},

    description='task09',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 27),
    catchup=False

) as dag:
    t1 = PythonOperator(
        task_id='push_xcom',

        python_callable=push_xcom
    )

    t2 = PythonOperator(
        task_id='pull_xcom',
 
        python_callable=pull_xcom
    )

    t1 >> t2




