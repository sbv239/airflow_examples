from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
with DAG('hw_d-shestak_4',
         default_args=default_args,
         description='hw_d-shestak_3',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 10, 21),
         tags=['hw_4_d-shestak']
         ) as dag:
    def print_task_number(task_number):
        return f'task number is: {task_number}'


    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id=f'task_{i}_BashOperator',
                bash_command=f"echo task_{i}_by_Bash"
            )
            t1.doc_md = dedent(
                '''
                # t1 doc:
                `Bash_operator` ***makes*** `f'echo task_{i}_by_Bash`
                '''
            )
        else:
            t2 = PythonOperator(
                task_id='task_' + str(i),
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
            )
            t2.doc_md = dedent(
                '''
                # t2 doc:
                `Python_operator` ***print*** `f'task number is: {task_number}'`
                '''
            )
