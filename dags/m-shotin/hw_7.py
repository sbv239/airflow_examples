from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
        'hw_m-shotin_7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False, 'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_2 DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 6, 28),
        catchup=False,
        tags=['example'],
) as dag:
    for i in range(30):
        if i < 10:
            NUMBER = i
            t1 = BashOperator(
                task_id='print_echo' + str(i),
                bash_command="echo $NUMBER",
                env={'NUMBER': i}
            )
        else:
            def print_context(ts, run_id, task_number, **kwargs):
                print(kwargs)
                print(f"task number is: {task_number}", ts, run_id)

            t2 = PythonOperator(
                task_id='task_namber' + str(i),
                python_callable=print_context,
                op_kwargs={'task_number': i}
            )
    t1 >> t2
