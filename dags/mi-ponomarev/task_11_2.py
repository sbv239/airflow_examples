from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_2_mi-ponomarev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='task_2',
    schedule_interval=timedelta(days=1),
    #     С какой даты начать запускать DAG
    #     каждый даг видит свою дату запуска
    #
    #
    start_date=datetime(2023, 4, 21),
    #     Запустить за старые даты относительно сегодня
    catchup=False,
    # Способ помечать даги
    tags=['task_2']

) as dag:
    pwd_command = BashOperator(
        task_id='pwd_command',
        bash_command= 'pwd'
    )

    def print_ds(ds):
        print(ds)

    run_this = PythonOperator(
        task_id= 'print_the_context',
        python_callable=print_ds,
    )