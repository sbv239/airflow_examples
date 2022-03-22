from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        # Название таск-дага
        'hw_3_i-rjazanov-6',

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='This is from DAG for Ryazanov to Task 2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['task_2']


) as dag:
    t1 = BashOperator(
        task_id='print_pwd',
        bash_command = 'pwd'
    )

    t1.doc_md = dedent(
        '''
        ## Documentation
        Print directory `PWD` Airflow
        '''
    )

    def print_txt(ds):
        print(ds)
        return 'This is task print txt'

    t2 = PythonOperator(
        task_id='print_txt',
        python_callable=print_txt
    )

    def print_task_number(num):
        return print(f"task number is: {num}")

    for i in range(30):
        if i < 10:
            task = BashOperator(
            task_id='task_' + str(i),  # в id можно делать все, что разрешают строки в python
            bash_command = f"echo {i}"
            )
        else:
            task = PythonOperator(
            task_id='task_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=print_task_number,
            # передаем в аргумент с названием random_base значение float(i) / 10
            op_kwargs={'num': i},
            )


    t1 >> t2 >> task

