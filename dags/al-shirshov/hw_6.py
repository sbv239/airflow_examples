from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_ds(ds, **kwargs):
    print(ds)
    print(**kwargs)
    return 'Logs for python function'


with DAG(
    "hw_al-shirshov_3",
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    start_date=datetime(2023,5,20),
    catchup=True   
) as dag:
    for i in range(30):
        if i < 10:
            NUMBER = i
            task = BashOperator(
                task_id = f"bash_task_{i}",
                bash_command = f"echo $NUMBER",
                dag=dag,
                env = {'NUMBER': NUMBER}
            )
        else:
            task = PythonOperator(
                task_id = f"python_task_{i}",
                python_callable = print_ds,
                op_kwargs = {'i': f'task_number is: {i}'}

            )
