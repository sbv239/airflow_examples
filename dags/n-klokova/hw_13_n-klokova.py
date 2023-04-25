from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Variable

def get_task_id():
    var = Variable.get('is_startml')

    if var == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def print_startml_desc():
    print('StartML is a starter course for ambitious people')

def print_not_startml_desc():
    print('Not a startML course, sorry')

with DAG(
    'hw_13_n-klokova',
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
    description='XCom',
    # Как часто запускать DAG
    # schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2023, 4, 21),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['n-klokova'],
) as dag:

    opr_startml_desc= PythonOperator(
        task_id='startml_desc',
        python_callable=print_startml_desc
    )

    opr_not_startml_desc= PythonOperator(
        task_id='not_startml_desc',
        python_callable=print_not_startml_desc
    )

    opr_branch_python = BranchPythonOperator(
        task_id='choose_task',
        python_callable=get_task_id
    )
    opr_dummy_start = DummyOperator(
        task_id = 'dummy_operator_start'
    )

    opr_dummy_end = DummyOperator(
        task_id = 'dummy_operator_end'
    )


    opr_dummy_start >> opr_branch_python >> [opr_startml_desc, opr_not_startml_desc] >> opr_dummy_end