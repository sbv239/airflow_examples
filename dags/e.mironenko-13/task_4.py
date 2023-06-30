from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

from datetime import timedelta, datetime

with DAG(
    'hw_e.mironenko-13_4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
            },
    start_date=datetime(2023, 6, 26),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags = ['e.mironenko-13']
) as dag:

    for i in range(1, 11):
        t1 = BashOperator(
            task_id=f"task_id_{i}",
            depends_on_past=False,
            bash_command=f"echo {i}",
        )

    def print_11_30(ds, task_number):
        print(f"task number is: {task_number}")

    for i in range(11, 31):
        t2 = PythonOperator(
            task_id=f'task_id_{i}',
            python_callable=print_11_30,
            op_kwargs={'task_number': i},
        )

    t2.doc_md = dedent(
        """
        Добавьте к вашим задачам из прошлого задания документацию. В документации обязательно должны быть 
        элементы кода (заключены в кавычки `code`), **полужирный текст** и *текст курсивом*, а также абзац 
        (объявляется через решетку).
        # Абзац
        ```
        task_id=f'task_id_{i}',
        python_callable=print_11_30,
        op_kwargs={'task_number': i},
        ```
        """
    )

t1 >> t2