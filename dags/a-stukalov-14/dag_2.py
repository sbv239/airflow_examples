from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'dag_2_st',

    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },

    # теги, способ помечать даги
    tags=['dag_2_st'],
) as dag:
    def print_context(i, **kwargs):
        print(f"task number is: {i}")

    for i  in range(30):
        if i<10:
            t1 = BashOperator(
                task_id = 'pwd',
                bash_command = f'excho{i}',
            )
        else:
            t1 = PythonOperator(
                task_id = 'ds',
                python_callable = print_context,
                op_kwargs = {'i':i},
            )

t1