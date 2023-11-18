from datetime import datetime, timedelta

from airflow import DAG


from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_number_task(random_base):
    print(f"task number is: {random_base}")

with DAG(
    'hw_s-razumov_3',
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

    start_date=datetime(202, 1, 1),
    tags=['hw_s-razumov_3'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='print_i',
            bash_command=f'echo {i}',
        )
    for task_number in range(10, 30):
        t2 = PythonOperator(
            task_id='print_date',
            python_callable=print_number_task,
            op_kwargs={'random_base': task_number}
        )

    t1 >> t2
