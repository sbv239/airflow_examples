from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
    'hw_d-ivashkin-23_4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework 4 step DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 21),
    catchup=False,
    tags=['homework', 'di']
) as dag:

        for i in range(10):
                task = BashOperator(
                        task_id='echo_' + str(i),
                        bash_command=f"echo {i}"
                )


        def print_task(task_number):
                return f"task number is: {task_number}"


        for i in range(11, 31):
                task = PythonOperator(
                        task_id="print_" + str(i),
                        python_callable=print_task,
                        op_kwargs={'task_number': i}
                )

        task.doc_md = dedent(
        """\
            # Task Documentation
            Первые **10** тасков выводят *номер* это таска обращаясь к `BashOperator` и `echo`.
            Остальные **20** тасков явно выводят *сообщение с номером* этого таска, обращаясь к `PythonOperator` и `print`
        """
    )

task