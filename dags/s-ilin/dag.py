"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_smt(ts, run_id, **kwargs):
    print(f"task number is: {kwargs['task_number']}")
    print(ts, run_id)


with DAG(
    'first_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 26),
    catchup=False,
    tags=['example'],
) as dag:
    for i in range(10):
        s_ilin_1 = BashOperator(
            task_id=f"ilin_curdir_{i}",
            bash_command='echo $NUMBER',
            env={'NUMBER': str(i)}
        )
        s_ilin_1.doc_md = dedent(
            """#### **Doc** for dag ilin_curdir_`i`
            *enjoy*
            """
        )
    for i in range(20):
        s_ilin_2 = PythonOperator(
            task_id=f"ilin_print_smt_{i}",
            python_callable=print_smt,
            op_kwargs={'task_number': i}
        )
        s_ilin_2.doc_md = dedent(
            """#### **Doc** for dag ilin_print_smt_`i`
            *enjoy*
            """
        )

s_ilin_1 >> s_ilin_2
