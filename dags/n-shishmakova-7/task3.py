from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_2_n-shishmakova-7',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        # Описание DAG (не тасок, а самого DAG)
        description='DAG for 2 task in lesson 11',
        # Как часто запускать DAG
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 1),
        # Запустить за старые даты относительно сегодня
        catchup=False,
        # теги, способ помечать даги
        tags=['hw2'],
) as dag:
    def print_task_number(task_number):
        print(f"task number is: {task_number}")


    for i in range(1, 31):
        if i <= 10:
            t1 = BashOperator(
                task_id='its_task_' + str(i),  # id, будет отображаться в интерфейсе
                bash_command=f"echo {i}",  # какую bash команду выполнить в этом таске
            )
            t1.doc_md = dedent(
                """\
        # Task Documentation
        Try to create **DOCUMENTATION**.
        It's task use  *Bash Operator*.

        """
            )

        else:
            t2 = PythonOperator(
                task_id='its_py_task_' + str(i),  # нужен task_id, как и всем операторам
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
            )
            t2.doc_md = dedent(
                """\
        # Task Documentation
        Try to create **DOCUMENTATION**.
        It's task use  *Python Operator* and print task number with function
        `
        def print_task_number(task_number):
            print(f"task number is: {task_number}")
        `

        """
            )



    t1 >> t2
