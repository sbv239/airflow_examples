from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_an-panfilov_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 27),
    catchup=False,
    tags=["hw_3"]
) as dag:

    def print_ds(task_number, **kwargs):
        print (f"task number is: {task_number}")
        return 'Whatever you return gets printed in the logs'



    for i in range(10):
        task = BashOperator(
            task_id= "command_" + str(i),
            bash_command = f"echo {i}"
        )


    for i in range(10, 30):

        task = PythonOperator(
            task_id="task_number_" + str(i),
            python_callable=print_ds,
            op_kwargs={'task_number': i},
        )
        


    