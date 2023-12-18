from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG('hw-b-shramko-3',
         default_args={
             'depends_on_past': False,
             'email': ['airflow@example.com'],
             'email_on_failure': False,
             'email_on_retry': False,
             'retries': 1,
             'retry_delay': timedelta(minutes=5),
         },
         start_date=datetime(2022, 1, 1),
         catchup=False
         ) as dag:
    def print_task_number(task_number, **kwargs):
        print(f"task number is: {task_number}")


    for i in range(1, 31):
        if i <= 10:
            t1 = BashOperator(
                task_id='Bash_print_number_' + str(i),
                bash_command=f"echo {i}"
            )
        if i > 10:
            t2 = PythonOperator(
                task_id='Python_print_number_' + str(i),
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
            )
    t1 >> t2
