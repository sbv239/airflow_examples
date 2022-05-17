from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'hw_4_e-demidov',
    default_args={
        'depends_on_past': False,
        'email': ['yevgeniy.demidov@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Lesson_11_task_1',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 17),
    catchup=False,
    tags=['e-demidov', 'hw_4_e-demidov', 'hw_4'],
) as dag:
    
    def print_task_no(task_number):
        print(f'task number is: {task_number}')

    for i in range(10):
        cyclic_task_1 = BashOperator(
        task_id = 'echo_' + str(i),
        bash_command = f'echo {i}')
    for i in range(20):
        cyclic_task_2 = PythonOperator(
        task_id = 'print_' + str(i),
        python_callable = print_task_no,
        op_kwargs = {'task_number': float(i) / 20},)
    cyclic_task_1.doc_md = dedent(
        '''
    # new line, `code`, *bold*, **bold**, _italic_
    ''')
    cyclic_task_2.doc_md = dedent(
        '''
    # new line, `code`, *bold*, **bold**, _italic_
    ''')

cyclic_task_1 >> cyclic_task_2