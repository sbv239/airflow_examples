
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'task_3_andreeva',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A DAG for task 3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 30),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    tags=['task3','task_3','andreeva'],
) as dag:
    t1 = DummyOperator(task_id='start_dag')
    t2 = DummyOperator(task_id='wait_for_all_bash_operators')
    t3 = DummyOperator(task_id="finish_dag")

    # 10 задач на BashOperator
    for i in range(10):
        task_bash = BashOperator(
            task_id=f'print_bash_{i}',
            bash_command=f"echo {i}"
        )
        t1 >> task_bash >> t2


    def print_task_number(task_number):
        print(f'task number is: {task_number}')

    for i in range (20):
        task_python = PythonOperator(
            task_id=f'print_python_{i}',
            python_callable=print_task_number,
            op_kwargs={'task_number':i},
        )

t2 >> task_python >> t3
#ОШИБКА!! надо, чтобы последовательность тасок была В цикле (оставлю основной код с косяком для наглядности, ошибка исправлена в task3_1.py):
#       t2 >> task_python >> t3


#def print_context(ts, run_id, **kwargs):
#    print(kwargs)  # kwargs будет словарем, содержащим все переданные именованные аргументы
#    print(f"task number is: {kwargs.get('task_number')}")  # из него сможем достать task_number, например
#    print(run_id)  # run_id - это еще одна переменная, которую передает Airflow в функцию
#    print(ts)  # равно как и ts - time string (логическая дата и время запуска оператора)
#    return 'Whatever you return gets printed in the logs'

