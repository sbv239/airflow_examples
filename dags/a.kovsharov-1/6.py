from datetime import timedelta, datetime
#from textwrap import dedent
from airflow import DAG
#from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
        'hw_6_a.kosharov',
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
    tags=['HW_6', 'a.kovsharov']
) as dag:
    
        
    def print_ts_run_id(ts, run_id, kwargs):
        print(f"task number is: {kwargs.get('task_number', None)}")
        print(f"ts - {ts}")
        print(f"run_id - {run_id}")
        
    for n in range(20):
        python_task = PythonOperator(
            task_id = f"python_task_{n}",
            python_callable=print_ts_run_id,
            provide_context=True,    
            dag=dag,
            op_kwargs = {"kwargs": {"task_number": n}}
        )
        
        
        

    