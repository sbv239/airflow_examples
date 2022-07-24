from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def task_number_printer(task_number):
    print(f"task number is: {task_number}") 

with DAG('hw_3_o_bulaeva_10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries':1,
            'retry_delay': timedelta(minutes=5),
            },
        description = 'Second DAG',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2022, 7, 24),
        catchup = False) as dag:

    for i in range(30):
    	if i < 10:
    		task = BashOperator(
    	task_id = f"bash_task_number{i}",
    	bash_command = f"echo {i}",
    	)
        else:
            task = PythonOperator(
        task_id='python_task_number_' + str(i),  
        python_callable=task_number_printer,
        op_kwargs={'task_number': i},
    )
