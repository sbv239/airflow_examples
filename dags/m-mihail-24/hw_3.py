from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent

with DAG(
    'm-mihail-24_3',
    default_args={
        'depends_ons_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG hw 3',
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2023, 12, 3),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
) as dag:
    for i in range(10):
        b3 = BashOperator(
            task_id='task_number_bash_' + str(i),
            bash_command=f"echo {i}"
        )


    def print_num(task_number):
        print(f'task number is: {task_number}')

    for i in range(20):
        p3 = PythonOperator(
            task_id='task_number_python_' + str(i),
            python_callable=print_num,
            op_kwargs={'task_number': i}
        )

    b3 >> p3


