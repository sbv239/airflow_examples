"""
PythonOperator and BashOperator test
"""
from datetime import datetime, timedelta
# from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



with DAG(
    'hw_6_i-daniljuk',
    # Параметры по умолчанию для тасок
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    },
    description='A cycle tasks DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_6_i-daniljuk'],
) as dag:
    
    i = 0
    while i < 10:
        task_bash = BashOperator(
            task_id='echo_for_' + str(i),
            bash_command="echo $NUMBER",
            env={"NUMBER": i}
        )
        i += 1

    def print_funcnum(task_number):
        """Заснуть на random_base секунд"""
        print(f"task number is: {task_number}")

    while i < 30:
        # Каждый таск будет спать некое количество секунд
        task_python = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_funcnum,
            op_kwargs={'task_number': i},
        )
        i += 1
        
    task_bash >> task_python