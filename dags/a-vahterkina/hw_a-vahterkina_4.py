from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_task_num(task_number):
    print(f'task_num is {task_number}')
    return 'Whatever you return gets printed in the logs'


with DAG(
        'hw_3_a-vahterkina',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_a-vahterkina_4',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 10, 17),
        catchup=False,
        tags=['hw_4_a-vahterkina']
) as dag:

    for i in range(1,31):
        if i <= 10:
            t = BashOperator(
                task_id=f'hw_4_a-vahterkina_{i}',
                bash_command=f'echo {i}'
            )
        else:
            t1 = PythonOperator(
                task_id = f'hw_4_a-vahterkina_{i}',
                python_callable = print_task_num,
                op_kwargs = {'task_number' : i}
            )
    t.doc_md = dedent(
        """\
        # Task documentations:`code,`*italic text,***bold text**
        """
    )

t >> t1


