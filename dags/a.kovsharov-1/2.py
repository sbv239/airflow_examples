from datetime import timedelta, datetime
#from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
        'hw_2_a.kosharov',
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
    description='A cycle generated DAG of homework of Lesson 11',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example']
) as dag:
    
    for num in range(10):
        bash_task = BashOperator(
            task_id = f"print_{num}_in_terminal",
            bash_command = f"echo {num} ")
        
    
    def print_tsk_num(task_num):
        print(f"task number is: {task_num}")
        
    for n in range(20):
        python_task = PythonOperator(
            task_id = f"python_task_{n}",
            python_callable=print_tsk_num,
            op_kwargs = {"task_num": n}
        )
     
        
    bash_task >> python_task
    