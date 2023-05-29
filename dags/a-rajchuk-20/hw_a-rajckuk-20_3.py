from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

with DAG(
    'tutorial', 
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},

    description = 'first DAG',

    start_date = datetime(2023, 1, 1)



) as dag:

    def print_i(i, **kwargs):
        return f'task number is: {i}'

for i in range(30):
    if i < 10:
        t1 = BashOperator(
            task_id = f'hw_a-rajchuk-20_2_{i}.py',
            bash_command = f'echo {i} ', 
            dag = dag
            
        )
    else:
        t2 = PythonOperator(
            task_id = 'print_' + str(i),

            python_callable = print_i,

            op_kwargs = {'i':i},

            dag = dag 
        )