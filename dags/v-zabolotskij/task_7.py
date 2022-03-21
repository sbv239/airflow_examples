from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG\
    (
    "task_7_v_zabolotskij",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description = "DAG for task #3",
    ) as dag:

    for task in range(30):

        if task <= 10:
            bash_task = BashOperator(
                task_id = "BO_task_" + str(task),
                bash_command = f"echo {task}"            
            )
        else:
            def print_task_num(task_number, ts, run_id):
                print(ts,run_id, sep='\n')
                return f"task number is: {task_number}"
            py_task = PythonOperator(
                task_id = "PY_task_" + str(task),
                python_callable = print_task_num,
                op_kwargs = {"task_number": task}
                 )