from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_j-miller_3',
        default_args={
            'depends_on_past': False,  # Если прошлые запуски упали, надо ли ждать их успеха
            'email': ['airflow@example.com'],  # Кому писать при провале
            'email_on_failure': False,  # А писать ли вообще при провале?
            'email_on_retry': False,  # Писать ли при автоматическом перезапуске по провалу
            'retries': 1,  # Сколько раз пытаться запустить, далее помечать как failed
            'retry_delay': timedelta(minutes=5),  # Сколько ждать между перезапусками# timedelta из пакета datetime
        },
        description='A 3 simple tutorial DAG',  # Описание DAG (не тасок, а самого DAG)
        schedule_interval=timedelta(days=1),  # Как часто запускать DAG
        start_date=datetime(2023, 5, 31),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        tags=['SimJul'],  # теги, способ помечать даги
) as dag:
    for i in range(30):  # 10 задач сделайте типа BashOperatort
        if i < 10:
            t1 = BashOperator(
                task_id=f'hw_3_j-miller-20_{i}',  # id, будет отображаться в интерфейсе
                bash_command=f'echo {i} ',  # можете указать f"echo {i}"
                dag=dag
            )
            #элементы кода (заключены в кавычки `code`), полужирный текст и текст курсивом, а также абзац
            t1.doc_md = dedent(
                """\
            `code`
            **text**
            *text*
            _text_
            #text
            """
            )
        else:
            def print_task_number(i, **kwargs):
                return (f'task number is: {i}')  # Функция должна печатать "task number is: {task_number}"


            t2 = PythonOperator(
                task_id=f'hw_3_j-miller-20_{i}',  # id, как у всех операторов
                python_callable=print_task_number,  # передаем функцию
                op_kwargs={'number': i},
            )
            t2.doc_md = dedent(
                """\
            `code`
            **text**
            *text*
            _text_
            #text
            """
            )

    t1 >> t2
