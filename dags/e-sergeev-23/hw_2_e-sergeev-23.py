from datetime import datetime, timedelta
#from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
def print_ds(ds):
    print(ds)
    return ds
with DAG(
    "hw_2_e-sergeev-23",
    default_args={

    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

    },
    schedule_interval=timedelta(days=1),
    start_date=datetime.now()-timedelta(days=1),
    catchup=False



) as dag:
    t1=PythonOperator(
        task_id='print_ds',
        python_callable=print_ds
    )
    t2=BashOperator(
        task_id="show_dir",
        bash_command="pwd"
    )
