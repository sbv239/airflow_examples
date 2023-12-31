from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_p-pertsov-36_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_2 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 21),
    catchup=False,
    tags=['pavelp_hw_3'],
) as dag:

    for i in range(1,31):  # 10 задач сделайте типа BashOperatort
        if i <= 10:
            t1 = BashOperator(
                task_id=f'hw_3_p-pertsov-36_{i}',  # id, будет отображаться в интерфейсе
                bash_command=f'echo {i} ',  # можете указать f"echo {i}"
                dag=dag
            )
        else:
            def print_task_number(i, **kwargs):
                return (f'task number is: {i}')  # Функция должна печатать "task number is: {task_number}"


            t2 = PythonOperator(
                task_id=f'hw_3_p-pertsov-36_{i}',  # id, как у всех операторов
                python_callable=print_task_number,  # передаем функцию
                op_kwargs={'number': i},
            )

            t1 >> t2

