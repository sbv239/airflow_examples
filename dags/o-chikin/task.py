from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, timedelta, datetime


def print_ds_from_python(ds, **kwargs):
    print(ds)
    return 'Function to print ds date is working OK'

with DAG(
    'o-chikin_task3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    description='task3_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 16),
    catchup=False,
    tags=['Oleg_Chikin_DAG']
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='bash_operator' + str(i),
            bash_command=f'echo {i}'
        )

    def python_func(task_number):
        print(f"task number is: {task_number}")


    for i in range(20):
        t2 = PythonOperator(
            task_id='python_operator' + str(i),
            python_callable=python_func,
            op_kwargs={'task_number': i},
        )

    t1>>t2