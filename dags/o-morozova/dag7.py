"""
 Test documentation
 `code 2`
 `Monospace`
 # bold text 2
 **bold text 2**
 ## italicized text 2
 *italicized text 2*
 _italicized text 2_
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.operators.bash import BashOperator

with DAG(
    '7o-morozova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='7o-morozova',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['7o-morozova'],
) as dag:
        date = "{{ ds }}"
        for i in range(10):
                t_bash = BashOperator(
                        task_id="echo_" + str(i),
                        bash_command='echo ' + str(i),
                        dag=dag,  # говорим, что таска принадлежит дагу из переменной dag
                        env={"DATA_INTERVAL_START": date},
                )
                if i == 0:
                        t = t_bash
                else:
                        t >> t_bash
                        t = t_bash

        dag.doc_md = __doc__

        def print_str(task_number, ts, run_id):
                print("task number is:", task_number)
                print(ts, run_id)

        for j in range(10, 30):
                t_pth = PythonOperator(
                        task_id='print_str_' + str(j),
                        python_callable=print_str,
                        op_kwargs={'task_number': j, 'ts': "{{ ts }}", 'run_id': "{{ run_id }}"},
                )
                if j == 0:
                        t = t_bash
                else:
                        t >> t_pth
                        t = t_pth