from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_p-pertsov-36_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_6 Env test',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 21),
    catchup=False,
    tags=['pavelp_hw_6'],
) as dag:

    for i in range(1,31):  # 10 задач сделайте типа BashOperatort
        if i <= 10:
            t1 = BashOperator(
                task_id=f'hw_6_p-pertsov-36_{i}',  # id, будет отображаться в интерфейсе
                bash_command="echo $NUMBER",  # можете указать f"echo {i}"
                env = {"NUMBER": i},
                dag = dag
            )
        else:
            def print_task_number(**kwargs):
                return (f'task number is: {kwargs["number"]}')  # Функция должна печатать "task number is: {task_number}"


            t2 = PythonOperator(
                task_id=f'hw_6_p-pertsov-36_{kwargs["number"]}',  # id, как у всех операторов
                python_callable=print_task_number,  # передаем функцию
                op_kwargs={'number': i},
            )

t1 >> t2