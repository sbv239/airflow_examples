from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG # Для объявления DAG нужно импортировать класс из airflow

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# def put_xcom_test(ti):
#     """
#     Gets totalTestResultsIncrease field from Covid API for given state and returns value
#     """
#     ti.xcom_push(
#         key='sample_xcom_key',
#         value="xcom test"
#     )

def get_xcom_test(ti):
    """
    Evaluates testing increase results
    """
    ti.xcom_push(
        key='sample_xcom_key',
        value="Airflow tracks everything"
    )
    testing_increases = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='IM_t1_id11'
    )
    return testing_increases

with DAG(
    'IM_DAG_9',
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
        # Описание DAG (не тасок, а самого DAG)
        description='My (IM) first DAG',
        # Как часто запускать DAG
        schedule_interval=timedelta(days=1),
        # С какой даты начать запускать DAG
        # Каждый DAG "видит" свою "дату запуска"
        # это когда он предположительно должен был
        # запуститься. Не всегда совпадает с датой на вашем компьютере
        start_date=datetime(2023, 1, 1),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        # теги, способ помечать даги
        tags=['IM_DAG_9'],
) as dag:

    def return_str():
        return "Airflow tracks everything"

    t1 = PythonOperator(
        task_id='IM_t1_id10',
        python_callable=return_str,

    )

    t2 = PythonOperator(
        task_id='IM_t2_id11',
        python_callable=get_xcom_test,

    )

    t1 >> t2