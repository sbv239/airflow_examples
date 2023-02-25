from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'task_7_y-volkova-22',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
        description='task 7',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['task7'],
) as dag:

    kwargs = {"arg1": "Geeks", "arg2": "for", "arg3": "Geeks"}
    def print_task_num(ts, run_id, **kwargs):
        print(f'task number is: {kwargs["task_number"]}')
        print(ts)
        print(run_id)


    for i in range(10,30):
        t2 = PythonOperator(
            python_callable=print_task_num,
            op_kwargs={'task_number': i},
            task_id = "python_operator_task_" + str(i)
            )


        t2.doc_md = dedent(
            """
            #### Task Documentation
            Documentation for `task2`.
            This is a **python perator** that prints a number of the _task_
            """
        )
