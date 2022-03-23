from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'n-anufrieva_task4',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
        # Ждем 5 минут между перезапусками
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG n-anufrieva_task4',
        # Запускаем DAG каждый день
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['n-anufrieva_task4'],
) as dag:
    def task(task_number):
        print(f"task number is: {task_number}")


    for i in range(30):
        if i < 10:
            task_bash = BashOperator(
                task_id='bash_task_' + str(i),
                bash_command=f"echo {i}",
            )
            task_bash.doc_md = dedent(
                """\
            #### Bash Task
            `task_bash` *executes* a `bash_command=f"echo {i}"` which prints **task number** for `BashOperator`
            """
            )
        else:
            task_python = PythonOperator(
                task_id='python_task_' + str(i),
                python_callable=task,
                op_kwargs={'task_number': i},
            )
        task_python.doc_md = dedent(
            """\
        #### Python Task
        `task_python` *call* task function `def task(task_number):` 
         which prints `task number is: {task_number}` for `PythonOperator`
        """
        )

