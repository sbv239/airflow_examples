from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'tutorial',
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
    description='v-susanin_DAG_task_2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 01, 26),
    catchup=False,
    tags=['task_2'],
) as dag:
    first=BashOperator(
        task_id='show_dir',
        bash_command='pwd')
    t1.doc_md = dedent(
        """\
        #### Task Documentation
        Первый DAG. 
        """
    )


    def print_ds(ds):
        print(ds)
        return 'печать в логах'

    second=PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
        )
    #порядок выполнения кода
    first >> second

