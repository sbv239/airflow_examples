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
        tags=['first_task'],
    ) as dag:
        
    def print_data(ts, run_id, **kwargs):
        task_number = kwarg["task_number"]
        print(f'{ts}')
        print(f'{run_id}')
        print(f'task number: {task_number}')

    for task_number in range(10):
        t_bash = BashOperator(
                    task_id='run_bush_op_'+str(task_number),  # id, будет отображаться в интерфейсе
                bash_command='echo $NUMBER',
                env={"NUMBER": str(task_number)},
                dag=dag,
            )
        
    for task_number in range(20):
        t_python = PythonOperator(
                task_id='run_python_op_'+str(task_number),
                op_kwargs={"task_number", task_number},
                python_callable=print_data,
            )
    t_bash >> t_python
