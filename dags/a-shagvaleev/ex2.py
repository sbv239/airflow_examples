from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'a-shagvaleev_ex2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2022, 6, 19),
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id = "bash_task_"+str(i),
            bash_command = f"echo {i}"
        )


    def ds_func(task_number):
        """Simple example for PythonOperator"""

        return f"task number is: {task_number}"

    for i in range(20):
        t2 = PythonOperator(
            task_id = "python_task_"+str(i),
            python_callable = ds_func,
            op_kwargs = {"task_number": i}
        )

    t1 >> t2