from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG # Для объявления DAG нужно импортировать класс из airflow

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

with DAG(
    'IM_DAG_6',
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
        tags=['IM_DAG_6'],
) as dag:
    NUMBER = Variable.get("NUMBER")  # необходимо передать имя, заданное при создании Variable

    for i in range(10):
        NUMBER = i
        t1 = BashOperator(
            task_id='IM_t1_id6'+str(i),
            # bash_command=f"echo {i}",
            bash_command="echo $NUMBER",
        )

    def print_number(task_number):

        print(f"task number is: {task_number}")

    for i in range(10, 30):
        t2 = PythonOperator(
            task_id='IM_t2_id7'+str(i),
            python_callable=print_number,
            op_kwargs={'task_number': i}
        )

    t1.doc_md = dedent(
        """
    ### Task Documentation
    <p>This **task** use `echo` command for *writing* index</p>
      

    """
    )

    t1 >> t2