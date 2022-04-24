from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_9_a.sidorenko-4_xCom_DAG_implicit',
    # Параметры по умолчанию для тасок
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
    # # Описание DAG (не тасок, а самого DAG)
    description='a.sidorov HW 9 DAG xCom',
    # # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # # С какой даты начать запускать DAG
    # # Каждый DAG "видит" свою "дату запуска"
    # # это когда он предположительно должен был
    # # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 23),
    # # Запустить за старые даты относительно сегодня
    # # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # # теги, способ помечать даги
    # tags=['example'],
) as dag:



    def return_str(**kwargs):
        return "Airflow tracks everything"

    def xcom_pull(ti, **kwargs):
        value_ret = ti.xcom_pull(task_ids='push_xcom_implicitly', key='return_value')
        print (f'{value_ret}')
    
    push_implicit = PythonOperator(
            task_id=f'push_xcom_implicitly',
            python_callable=return_str
        )

    pull_xcom = PythonOperator(
            task_id=f'pull_xcom',
            python_callable=xcom_pull
        )
    
    push_implicit >> pull_xcom