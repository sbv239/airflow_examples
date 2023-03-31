"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'dag_7_mmartsinyuk',
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
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
) as dag:
    
    def print_task_number(task_number, ts, run_id, **kwargs):
        print(ts)
        print(run_id)
        print(f'task_number_{task_number}')

    for i in range(30):
        if i+1 <= 10:
            NUMBER = i
            t1 = BashOperator(
                task_id='print_i_'+str(i+1),  # id, будет отображаться в интерфейсе
                bash_command = "echo $NUMBER",
                env={"NUMBER": NUMBER},
            )
            t1.doc_md = dedent(
                """ \
                 #### Task Documentation
                `doc_md`
                **test**
                *cringe*
                asdsa
                
                asddsa
                """
            )
            t1
        else:
            t2 = PythonOperator(
                task_id='print_task_number_'+str(i+1),
                python_callable=print_task_number,
                op_kwargs = {'task_number': i+1 }
            ) 
            t2.doc_md = dedent(
                """ \
                #### Task Documentation
                `doc_md`
                **test**
                *cringe*
                asdsa
                
                asddsa
                """
            )
            t2
