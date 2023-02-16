from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'hw_4_a-tjurin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },

        description='Task 4',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 16),
        catchup=False,

        tags=['Task_4'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='print_in_bash_' + str(i),
            bash_command=f"echo line number {i}",
        )

    def task_number(ds, task_number, **kwargs):
        print(kwargs)
        print(ds)
        return f"task number is: {task_number}"

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_task_num_' + str(i),
            python_callable=task_number,
            op_kwargs={'task_number': i}
        )
    t2.doc_md = dedent(
        """\
        # Task documentations
        This *task_4* prints a line 
        with the task **number**,
        example `op_kwargs={'task_number': i}  \ return f"task number is: {task_number}"`
        ![image.png](attachment:image.png)
        """
    )

    t1 >> t2
