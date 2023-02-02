from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

from datetime import datetime, timedelta

with DAG(
        'dv_dag_m2_l11_s2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='m2 l11 s2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False
) as dag:
    def print_context(task_number):
        print(f'task number is: {task_number}')
        return 'task number was printed'


    for i in range(10):
        task = BashOperator(
            task_id='dynamic_bash_task_' + str(i),
            bash_command=f'echo "task #{i}"'
        )

    for i in range(20):
        task = PythonOperator(
            task_id='dynamic_python_task_' + str(i),
            python_callable=print_context,
            op_kwargs={'task_number': i},
        )

        task.doc_md = dedent(
            """\
            #### Task Documentation
            `print('stupid doc')`
            **полужирный текст**
            *тект курсивом*
            # Абзац""")
