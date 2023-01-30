from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
# Для объявления DAG нужно импортировать класс из airflow


# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)

with DAG(
    'HW_ryabchenyuk_7',
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
    description='task_7',
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
    tags=['task_7'],
) as dag:
    def print_ds(ts, run_id, **kwargs):
        print(ts, run_id, sep='\n')
    for i in range(10):
        bash_t = BashOperator(task_id=f'bash_task_{i}', bash_command=f'echo {i}')
        bash_t.doc_md="""
        ## Bash task doc in *markdown*. 
        The **task** is created using `BashOperator` from the module *airflow*
        """
    for i in range(20):
        py_task = PythonOperator(
            task_id='DZ' + str(i + 11),
            python_callable=print_ds,
            op_kwargs={'task_number': i + 11}
        )

    bash_t >> py_task