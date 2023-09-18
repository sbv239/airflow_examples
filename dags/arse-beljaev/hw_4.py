from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_arse-beljaev_4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
        },
    description='hw_4_lesson_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 17),
    catchup=False,
    tags=['example']
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='print_the_context_' + str(i),
            bash_command=f"echo {i}",
        )
        t1.doc_md = dedent(
            """\
            #### Task Documentation
            # Абзац
            _Циклическое выполнение тасок_
            BashOperator

            ```
            t1 = BashOperator(
                task_id='print_the_context_' + str(i),
                bash_command=f"echo {i}",
            )
            ```

            """
        )
        t1

    def print_numbers(task_number):
        print(f"task number is: {task_number}")

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_number_' + str(i),
            python_callable=print_numbers,
            op_kwargs={'task_number': i},
        )
        t2.doc_md = dedent(
            """\
            #### Task Documentation
            # Абзац
            _Циклическое выполнение тасок_
            __PythonOperator__

            ```
            t2 = PythonOperator(
                task_id='print_number_' + str(i),
                python_callable=print_numbers,
                op_kwargs={'task_number': i},
            )
            ```

            """
            )
        t2
