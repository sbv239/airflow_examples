from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_11_ex_3-n-jazvinskij',
    default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
    description = 'hw_11_ex_3',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023, 10, 21),
    catchup = False,
    tags = ['ex_3']
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id = 'Bash_task_' + str(i),
            bash_command = f"echo {i}"
        )

    def python_20_tasks(task_number):
        return f"task number is: {task_number}"

    for i in range(20):
        t2 = PythonOperator(
            task_id='Python_task_'+str(i),
            python_callable= python_20_tasks,
            op_kwargs = {'task_number': i}
        )
    t1>>t2