from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

data='xcom test'
def push_data(ti):
    # в ti уходит task_instance, его передает Airflow под таким названием
    # когда вызывает функцию в ходе PythonOperator
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

def put_data(ti):
    results = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='put_data'
    )
    print(results)

with DAG(
    'hw_9_n-klokova',
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


    '''

    Сделайте новый DAG, содержащий два Python оператора. Первый PythonOperator должен класть в XCom значение "xcom test" 
    по ключу "sample_xcom_key".
    Второй PythonOperator должен доставать это значение и печатать его. 
    Настройте правильно последовательность операторов.
    Посмотрите внимательно, какие аргументы мы принимали в функции, когда работали с XCom.
    '''


    opr_put_data = PythonOperator(
        task_id='put_data',
        python_callable=push_data
    )
    opr_get_data = PythonOperator(
        task_id='get_data',
        python_callable=put_data
    )

    opr_put_data >> opr_get_data