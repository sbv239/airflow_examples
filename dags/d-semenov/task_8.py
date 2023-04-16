from datetime import datetime, timedelta


# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.python import PythonOperator


def get_data(ts):
    ts.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )

def pull_data(ts):
    check = ts.xcom_pull(
        key='sample_xcom_key',
        task_ids='push_data'
    )
    print(check)



with DAG(
    'xcom_d_semenov',
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
    tags=['d-semenov'],
) as dag:

    task1 = PythonOperator(
        task_id='push_data',
        python_callable=get_data,
    )

    task2 = PythonOperator(
        task_id='test',
        python_callable=pull_data,
    )

    task1 >> task2