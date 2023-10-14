from datetime import timedelta, datetime



from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator



def print_date(ds):
    print(ds)
    print("Hello, world!")



with DAG(
    'hw_d-oreshnikov_01',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    start_date=datetime(2023, 1, 1)
) as dag:
    
    t1 = BashOperator(
        task_id = 'print_pwd',
        bash_command = 'pwd'
    )

    t2 = PythonOperator(
        task_id = 'print_datetime',
        python_callable= print_date
    )
