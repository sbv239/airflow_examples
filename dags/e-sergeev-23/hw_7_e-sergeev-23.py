from datetime import datetime, timedelta
#from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
def pythtask(ts,run_id,**kwargs):
    task_number=kwargs.get("task_number")
    print(ts)
    print(run_id)
with DAG(
    "hw_7_e-sergeev-23",
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
    for i in range(10):

        t1=BashOperator(
            task_id=f'Bash{i}',
            bash_command=f"echo {i}"
            )
    for i in range(20):
        t2=PythonOperator(
            task_id='pythtask'+ str(i),
            python_callable=pythtask,
            op_kwargs={"task_number":int(i)},
            dag=dag
        )