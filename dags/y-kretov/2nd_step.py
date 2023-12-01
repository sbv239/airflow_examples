from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_y-kretov_1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 20),
    catchup=False,
    tags=["hw_4"]
) as dag:

    def print_task_no(task_number, ts, run_id, **kwargs):
        print(f"task number is: {task_number}, ts is: {ts}, run_id is: {run_id}")

    for i in range(20):
        task2 = PythonOperator(
            task_id='print_task'+str(i),
            python_callable=print_task_no,
            op_kwargs={'task_number': i},
            provide_context=True
        )

        task2.doc_md=dedent(
        """
        #### Task Documentation
        `code`
        _текст_
        **полужирный**
        *курсив*
        """
        )

    for i in range(10):
        task1 = BashOperator(
            task_id='Bash'+str(i),
            bash_command=f"echo $NUMBER",
            env={'NUMBER': str(i)}
        )
        
    task2 >> task1