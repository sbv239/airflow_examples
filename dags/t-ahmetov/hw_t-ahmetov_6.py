from datetime import datetime, timedelta, time

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def my_sleeping_function(task_number):
    """Заснуть на random_base секунд"""
    print(f"task number is: {task_number}")


with DAG(
    'task_3_ahmetov',
    # Параметры по умолчанию
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now(),
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='print_cur_dir' + str(i),
            bash_command="echo $NUMBER",
            env={'NUMBER': i},
        )
        t1


