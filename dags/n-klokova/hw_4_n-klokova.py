from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'hw_4_n-klokova',
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
    description='Simple BashOperator and PythonOperator',
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
    Добавьте к вашим задачам из прошлого задания документацию. 
    В документации обязательно должны быть элементы кода (заключены в кавычки `code`), 
    полужирный текст и текст курсивом, а также абзац (объявляется через решетку).
    '''
    for i in range(10):

        task_bush = BashOperator(
            task_id = 'BashOperator_'+ str(i),
            bash_command= f'echo {i}'
        )
        task_bush.doc_md = dedent(
        '''
        Документация к задаче содержит `code`,  **полужирный текст**, _текст курсивом_, 
        # а также абзац (объявляется через решетку)
        '''
        )

    def print_task_number(task_number):
        return f'task number is: {task_number}'

    for i in range(20):

        task_python = PythonOperator(
            task_id = 'PythonOperator_' + str(i),
            python_callable = print_task_number,
            op_kwargs={'task_number' : i},
        )
        task_python.doc_md = dedent(
        '''
        Документация к задаче содержит `code`,  **полужирный текст**, _текст курсивом_, 
        # а также абзац (объявляется через решетку)
        '''
        )

    # А вот так в Airflow указывается последовательность задач
    task_bush >> task_python
