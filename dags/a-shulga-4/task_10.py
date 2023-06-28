from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def talk(ti):
        return "Airflow tracks everything"

def repeat(ti):
        print(
                ti.xcom_pull(
                key="return_value",
                task_ids='speaker'
                )
        )


with DAG(
    'hw_a-shulga-4_10',
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
                task_id = 'speaker',
                python_callable=talk,
        )

        t2 = PythonOperator(
                task_id="receiver",
                python_callable=repeat
        )

        t1 >> t2