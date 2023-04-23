from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'hw_7_r-korchagin',
    # Параметры по умолчанию для тасок
    default_args={
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        # Кому писать при провале
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале?
        'email_on_failure': False,
        # Писать ли при автоматическом перезапуске по провалу
        'email_on_retry': False,
        # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },

    start_date=datetime(2023, 4, 21),

) as dag:

    def print_task_number(task_number, ts, run_id):
        print(ts)
        print(run_id)
        print(f'task number is: {task_number}')

    for i in range(20):

        task_python = PythonOperator(
            task_id = 'PythonOperator_' + str(i),
            python_callable = print_task_number,
            op_kwargs={'task_number' : i},
        )
