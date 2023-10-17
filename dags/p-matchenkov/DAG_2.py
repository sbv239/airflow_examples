from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta



def get_ds(ds="{{ds}}"):
    print(ds)


with DAG(
    ' hw_p-matchenkov_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
) as dag:

    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id='print_execution_time',
        python_callable=get_ds
    )

    t1 >> t2