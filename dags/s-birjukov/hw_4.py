# испортирую нужные библиотеки и методы
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# описываю DAG
with DAG(
    # уникальное название DAG
    'hw_4_s-birjukov',
    # аргументы по умолчанию
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_4_s-birjukov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

    for task_number in range(10):
        t1 = BashOperator(
            task_id=f'bash_cycle_number_{task_number}',
            bash_command=f"echo{task_number}",
            )

    t1.doc_md = dedent(
    """
    ### Task1 Documentation
    **Task1 (t1)** comprises 10 cycles made with the help of `for` cycle.
    The *task id* of each **task** includes the *task number*,
    which is the same as the number of *iteration*
    The *output* of the **Task1** is command `ecco{task_number}`.
    """
    )

    def print_task_number(task_number):
        return f'task number is: {task_number}'

    for task_n in range(20):
        t2 = PythonOperator(
            task_id=f'python_cycle_number_{task_n}',
            python_callable=print_task_number,
            op_kwargs={'task_number': task_n}
        )


    t2.doc_md = dedent(
    """
    ##### **Task2 (t2)*** includes *20 cycles*.
    Furthermore, it utilizes `PythonOperator`, whereas **t1** uses `BashOperator`.
    In order to utilize the *task number*, `op_kwargs` is implemented in the `PythonOperator`.
    `op_kwargs` is a *dictionary*.
    """
    )

    t1 >> t2
