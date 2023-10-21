from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
with DAG('hw_d-shestak_3',
         default_args=default_args,
         description='hw_d-shestak_3',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 10, 21),
         tags=['hw_3_d-shestak']
         ) as dag:
    def print_task_number(task_number):
        return f'task number is: {task_number}'


    for i in range(30):
        if i < 10:
            BashOperator(
                task_id=f'task_{i}_BashOperator',
                bash_command=f"echo task_{i}_by_Bash"
            )
        else:
            PythonOperator(
                task_id='task_' + str(i),
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
            )
