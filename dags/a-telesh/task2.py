from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG('HW_2_a-telesh',
         default_args={
             # if Failed waiting or no
             'depends_on_past': False,
             # e-mail
             'email': ['airflow@example.com'],
             # e-mail notification enable
             'email_on_failure': False,
             # alarming on Retry
             'email_on_retry': False,
             # Сколько раз пытаться запустить, далее помечать как failed
             'retries': 1,
             # Сколько ждать между перезапусками
             'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
         },
         # Description DAG(not task)
         description='simple BashOperator and PythonOperator',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2022, 5, 6),
         catchup=False,
         tags=['hw_1'], ) as dag:
    t1 = BashOperator(task_id='print_pwd',  # id, id in UI Airflow
                      bash_command='pwd',  # name bash command to execute
                      )


    def print_context(ds):
        print(ds)


    t2 = PythonOperator(
        task_id='print_the_context',  # name in UI Airfolw
        python_callable=print_context, )  # without ()

    t1 >> t2
