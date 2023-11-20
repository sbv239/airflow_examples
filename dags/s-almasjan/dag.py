from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
from datetime import timedelta, datetime

with DAG(
    'hw_2_s-almasjan',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    start_date=datetime(2022, 1, 1)

) as dag:
    
    def show_date(ds):
        print(ds)
    
    t1 = BashOperator(
        task_id = 'ls',
        bash_command = 'pwd'
    )

    t2 = PythonOperator(
        task_id = 'date',
        python_callable = show_date
    )

    t1.doc_md = dedent("""
        `print('hello world')`
        #Header
        **bold**
        **kursiv**
        """)
    
    t2.doc_md = dedent("""
        `print('hello world')`
        #Header
        **bold**
        **kursiv**
        """)

    t1 >> t2

with DAG(
    'hw_3_s-almasjan',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    start_date=datetime(2022, 1, 1)

) as dag:
    
    def print_task_num(task_number):
        print(f'task number is: {task_number}')

    for i in range(30):
        if i < 10:
            task1 = BashOperator(
                task_id = 'number_' + str(i),
                bash_command = f"echo {i}"
            )
        else:
            task2 = PythonOperator(
                task_id = 'task_num_almasjan' + str(i),
                python_callable = print_task_num,
                op_kwargs = {'task_number': i}
            )
    
    task1.doc_md = dedent("""
        `print('hello world')`
        #Header
        **bold**
        **kursiv**
        """)
    
    task2.doc_mc = dedent("""
        `print('hello world')`
        #Header
        **bold**
        **kursiv**
        """)
    
    task1>>task2

with DAG(
    'hw_5_s-almasjan',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    start_date=datetime(2022, 1, 1)

) as dag:
    templated_command = dedent("""
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
    """)
    task = BashOperator(
        task_id = 'templated_command',
        bash_command = templated_command
    )