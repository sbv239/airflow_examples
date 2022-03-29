from datetime import timedelta, datetime
from airflow import  DAG
from airflow.operators.bash import BashOperator



with DAG(
    'hw_5_a-vdovenko',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
},
    description="Lesson 11 home work 5",
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022,1,1),
    catchup=False,
    tags=['a-vdovenko'],
) as dag:
    for i in range(11):
        task = BashOperator(
            task_id = f'env_number_{i}',
            env = {"NUMBER": str(i)},
            bash_command = 'echo $NUMBER'
        )
    task