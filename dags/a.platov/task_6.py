from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'HW_7_a.platov',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='First task',
        start_date=datetime(2022, 7, 17),
        catchup=False,
        tags=['a.platov'],
    ) as dag:
        
    def print_date(ts, run_id, **kwargs):
        print(ts, run_id)
        print(f'task number: {task_number}')
    
    date = "{{ ds }}"

    def t_bash(task_number: int, date):
        return  BashOperator(
                task_id='run_bush_op_'+str(task_number),  # id, будет отображаться в интерфейсе
                bash_command='echo $NUMBER',
                env={"NUMBER": str(task_number), "DATA START": date},
                dag=dag,)

    def t_python(task_number: int, func):
        return PythonOperator(
                task_id='run_python_op_'+str(task_number),
                op_kwargs={"task_number": task_number,
                           'ts': "{{ ts }}",
                           'rub_id': "{{ run_id }}"},
                python_callable=func,
            )

    for task_number in range(30):
        if task_manager == 0:
            t = t_bash(task_number, date)
        if task_manager < 10:
            new_task = t_bash(task_number, date)
            t >> new_task
            t = new_task
        else:
            new_task = t_python(task_number, print_date)
            t >> new_task
            t = new_task

