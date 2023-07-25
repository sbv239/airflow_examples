from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_p-pertsov-36_4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_4 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 21),
    catchup=False,
    tags=['pavelp_hw_4'],
) as dag:

    for i in range(1,31):  # 10 задач сделайте типа BashOperatort
        if i <= 10:
            t1 = BashOperator(
                task_id=f'hw_4_p-pertsov-36_{i}',  # id, будет отображаться в интерфейсе
                bash_command=f'echo {i} ',  # можете указать f"echo {i}"
                dag=dag
            )
            t1.doc_md = dedent(
                """\
                #### Test task documentation
                `code`, *italics*, **bold**
                """
            )
        else:
            def print_task_number(i, **kwargs):
                return (f'task number is: {i}')  # Функция должна печатать "task number is: {task_number}"


            t2 = PythonOperator(
                task_id=f'hw_4_p-pertsov-36_{i}',  # id, как у всех операторов
                python_callable=print_task_number,  # передаем функцию
                op_kwargs={'number': i},
            )
            t2.doc_md = dedent(
                """\
                #### Test task documentation
                `code`, *italics*, **bold**
                """
            )

            t1 >> t2