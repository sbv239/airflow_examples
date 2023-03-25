"""
Task 02 documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



with DAG(
    'task_02',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A DAG for task 02',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 20),
    catchup=False,
    tags=['task 2'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'echo_command_{i}',  # id, будет отображаться в интерфейсе
            bash_command=f"echo {i}",  # какую bash команду выполнить в этом таске
        )

    def print_ds(task_number):
        print(f'task number is: {task_number}')

    for i in range(20):
        t2 = PythonOperator (
            task_id=f'print_ds_{i}',  # нужен task_id, как и всем операторам
            python_callable=print_ds,  # свойственен только для PythonOperator - передаем саму функцию
            op_kwargs={'task_number': i},
        )

    t1.doc_md = dedent(
        """
            #### Task 03 Documentation
            You can document your task using the attributes `doc_md` (markdown),
            `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
            rendered in the UI's Task Instance Details page.
            ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
            
            # Task 03 specific tasks
            ```python
                def print_ds(task_number):
                    print(f'task number is: {task_number}')
            ```
            **bold text**
            *italic*
            
        """
        )  # dedent - это особенность Airflow, в него нужно оборачивать всю доку

    dag.doc_md = __doc__  # Можно забрать докстрингу из начала файла вот так
    dag.doc_md = """
    This is a documentation placed anywhere
    """
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ds }}"
            echo "{{ macros.ds_add(ds, 7)}}"
        {% endfor %}
    """
    )  # поддерживается шаблонизация через Jinja

    t1 >> t2