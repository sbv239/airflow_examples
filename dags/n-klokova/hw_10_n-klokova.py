from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

def push_data(ti):
    return 'Airflow tracks everything'

def put_data(ti):
    results = ti.xcom_pull(
        key='return_value',
        task_ids='return_str'
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
    В лекции говорилось, что любой вывод return уходит неявно в XCom. Давайте это проверим.
    Создайте новый DAG, содержащий два PythonOperator. Первый оператор должен вызвать функцию, 
    возвращающую строку "Airflow tracks everything".
    Второй оператор должен получить эту строку через XCom. Вспомните по лекции, какой должен быть ключ. 
    Настройте правильно последовательность операторов.
    '''

    opr_return_str = PythonOperator(
        task_id='return_str',
        python_callable=push_data
    )
    opr_get_data = PythonOperator(
        task_id='get_data',
        python_callable=put_data
    )

    opr_return_str >> opr_get_data