from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'task_r-izutov_3',

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },

        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 6, 20),
        catchup=False
) as dag:
    def print_task_number(task_number):
        print(f"task number is: {task_number}")


    for i in range(30):
        if i < 10:
            bash_task = BashOperator(
                task_id=f'BashOperator_{i}',
                bash_command=f'echo {i}'
            )
            bash_task.doc_md = dedent("""
                #### Task documentation
                This is the task for `bash` *scripting*
                #### end
            """)

        else:
            python_task = PythonOperator(
                task_id=f'PythonOperator_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
            )

            python_task.doc_md = dedent("""
                ### Task documentation
                This is the task for **python** scripting
                ## end
            """)

    # bash_task >> python_task
