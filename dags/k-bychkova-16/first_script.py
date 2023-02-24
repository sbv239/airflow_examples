from datetime import datetime, timedelta

from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'tutorial',
    default_args={
        'start_date': datetime(2017, 2, 1),
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

) as dag:
    def print_context(ds):
        #print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'
    # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)

    t1 = BashOperator(
        task_id='MyDAG_1',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='MyDAG_2',
        python_callable=print_context
    )

t1 >> t2