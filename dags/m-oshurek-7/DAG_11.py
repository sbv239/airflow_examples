from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
'''
Напишите DAG, состоящий из одного PythonOperator.
Этот оператор должен печатать значение Variable с названием is_startml
'''


with DAG(
    # имя дага, которое отразиться на сервере airflow
    'DAG_11_oshurek',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание самого дага
    description='Задание1. Напишите DAG, который будет содержать BashOperator и PythonOperator.'
                ' В функции PythonOperator примите аргумент ds и распечатайте его.',
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
    tags=['example_11_oshurek'],
) as dag:
    def get_var():
        from airflow.models import Variable

        is_startml = Variable.get('is_startml')
        print(is_startml)
    
    t1 = PythonOperator(
        task_id='get_is_startml',
        python_callable=get_var,
    )
    t1
