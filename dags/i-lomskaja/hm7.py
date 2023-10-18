from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_context(ts, run_id, task_number, **kwargs):
    print(ts)
    print(run_id)
    print(task_number)

with DAG(
    'hw_7_i-lomskaya',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
},

    description='hw7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 17),
    catchup=False

) as dag:
    for i in range(10):
        t1 = BashOperator(
        task_id = f'hw7_BashOperator_{i}',      
        bash_command=f"echo $NUMBER", 
        env=({'NUMBER': i}),         
        )
    for i in range(20):
        t2 = PythonOperator(
        task_id = f'hw7_PythonOperator_{i}',
        python_callable = print_context,
        op_kwargs={'task_number': i}
        )
    t1 >> t2
