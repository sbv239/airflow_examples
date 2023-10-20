from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta

with DAG(
    'hw_ale-turkin_1',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description = 'dag_ex_1'
    ) as dag:

    def print_ds(ds):
        print(ds)
        print('Hola')
    
    t1 = PythonOperator(
        task_id = 'print_ds_arg',
        python_callable = print_ds
    )
    
    t2 = BashOperator(
        task_id='pwd_command',
        bash_command='pwd',
    )

    t2 >> t1