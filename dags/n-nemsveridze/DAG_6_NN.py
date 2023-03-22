from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG (
    'task6NN',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description = 'ten times bash twenty times py',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023,3,21),
    catchup=False,
    tags=['NNtask6'],
) as dag:

    for i in range(10):
        NUMBER=i
        task1=BashOperator(task_id='Number_' + str(NUMBER),
                           dag=dag,
                           bash_command="echo$NUMBER",
                           env={"NUMBER": NUMBER})