from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
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
    
    def print_task_num(i):
        print(f'task number is: {i}')

    for i in range(30):
        if i < 10:
            task = BashOperator(
                task_id = 'it_num_almasjan',
                bash_command = 'f"echo {i}"'
            )
        else:
            task = PythonOperator(
                task_id = 'task_num_almasjan',
                python_callable = print_task_num,
                op_kwargs = {'i': i}
            )