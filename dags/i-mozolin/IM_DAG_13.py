from datetime import datetime, timedelta
from textwrap import dedent


from airflow import DAG # Для объявления DAG нужно импортировать класс из airflow

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator


with DAG(
    'IM_DAG_12',
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
        tags=['IM_DAG_12'],
) as dag:

    t1 = DummyOperator(
        task_id = 'before_branching'
    )

    def get_var():
        from airflow.models import Variable
        is_startml = Variable.get('False')
        return is_startml


    def decide_what_to_do(**kwargs):
        if get_var() == 'True':
            return 'startml_desc'
        else:
            return 'not_startml_desc'

    t2 = BranchPythonOperator(
        task_id = 'determine_course',
        python_callable = decide_what_to_do,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id = 'startml_desc',
        python_callable = lambda: print('StartML is a starter course for ambitious people'),
    )

    t4 = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable = lambda: print('Not a startML course, sorry'),
    )

    t5 = DummyOperator(
        task_id = 'after_branching'
    )

    t1 >> t2 >> [t3, t4] >> t5