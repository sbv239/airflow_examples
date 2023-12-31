"""
Test documentation
"""
from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator

def push_xcom(ti):
    """Pushes an XCom without a specific target"""
    ti.xcom_push(key='sample_xcom_key', value="xcom test")

def pull_xcom(ti):
    """Pull all previously pushed XComs and check if the pushed values match the pulled values."""
    pulled_value = ti.xcom_pull(key="sample_xcom_key", task_ids='push_xcom')
    print(pulled_value)

with DAG(
        dag_id = 'hw_9_o-tjurina',
        # Параметры по умолчанию для тасок
        default_args={
            # Если прошлые запуски упали, надо ли ждать их успеха
            'depends_on_past': False,
            # Кому писать при провале
            'email': ['olesia.tiurina@outlook.com'],
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
        description='A lesson11 task 9 DAG',
        # Как часто запускать DAG
        schedule_interval=timedelta(days=1),
        # С какой даты начать запускать DAG
        # Каждый DAG "видит" свою "дату запуска"
        # это когда он предположительно должен был
        # запуститься. Не всегда совпадает с датой на вашем компьютере
        start_date=datetime(2023, 1, 27),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        # теги, способ помечать даги
        tags=['example'],
) as dag:
    push_data = PythonOperator(
        # provide context is for getting the TI (task instance ) parameters
        task_id='push_xcom', provide_context=True,
        python_callable=push_xcom,
    )
    pull_and_print_data = PythonOperator(
        # provide context is for getting the TI (task instance ) parameters
        task_id='pull_xcom', provide_context=True,
        python_callable=pull_xcom,
    )
    push_data >> pull_and_print_data
