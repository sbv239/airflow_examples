from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
from datetime import datetime, timedelta


with DAG(
    'hw11_10_r-safarov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Task_10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hw_11_r-safarov']
) as dag:
    
    def push_data(ti):
        return "Airflow tracks everything"
    
    def pull_data(ti):
        testing_key_xcom = ti.xcom_pull(
            key="return_value",
            task_ids="push_data"
        )
        print(testing_key_xcom)
        
        
    t1 = PythonOperator(
        task_id = 'push_data',
        python_callable=push_data
    )

    t2 = PythonOperator(
        task_id = 'pull_data',
        python_callable=pull_data
    )


t1 >> t2