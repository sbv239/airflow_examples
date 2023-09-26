from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('hw_3_d-korjakov',
         default_args=default_args,
         description='DAG with "for" BashOperator and PythonOperator',
         start_date=datetime(2023, 9, 24),
         schedule_interval=timedelta(days=1)
         ) as dag:


        def python_operator_func(task_number):
            return f'task number is: {task_number}'


        for i in range(30):
            if i < 10:
                BashOperator(
                    task_id=f'{i}_less_then_10',
                    bash_command=f'echo {i}',
                )
            else:
                PythonOperator(
                    task_id=f'{i}_grater_then_10',
                    python_callable=python_operator_func,
                    op_kwargs={'task_number': i}
                )
