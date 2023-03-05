from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
from datetime import datetime, timedelta

with DAG(
    'hw11_4_r-safarov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Task_3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hw_11_r-safarov']
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f"print_echo_{i}",
            bash_command=f"echo {i}"
        )
    t1.doc_md = dedent(
    """\
    #Абзац
    `code`
    **жирный текст**
    *курсив*
    """
    )
    def print_context(task_number):
        print(f"task number is: {task_number}")

    for i in range(20):
        run_this = PythonOperator(
            task_id='print_the_context' + str(i),
            python_callable=print_context,
            op_kwargs={'task_number': i}
        )


t1 >> run_this
