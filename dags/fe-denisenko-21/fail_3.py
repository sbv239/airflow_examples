
"""
Test documentation
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_fe-denisenko-21_3_step',
        default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG',
    start_date=datetime(2023, 6, 24),
    catchup=False,
    tags=['example'],
) as dag:
    for i in range(9):
        t1 = BashOperator(
        task_id='echo_id',  # id, будет отображаться в интерфейсе
        bash_command=f'echo,{i}',  # какую bash команду выполнить в этом таске
        )
      # свойственен только для PythonOperator - передаем саму функцию
    for i in range(19):
        def print_context(task_number = i):
            print("task number is: {task_number}")
        run_this = PythonOperator(
        task_id='print_i',  # нужен task_id, как и всем операторам
        python_callable=print_context)
    t1 >> run_this

