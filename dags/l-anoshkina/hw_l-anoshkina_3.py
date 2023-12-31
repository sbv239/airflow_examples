

from airflow import DAG
from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from textwrap import dedent

with DAG(
        'hw_l-anoshkina_7',

        default_args={
        'depends_on_past': False,
                           'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description = 'HomeWork task7',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2023, 5, 29),

        catchup = False,

        ) as dag:

        for i in range(10):
                task1 = BashOperator(
                        task_id = 'print' + str(i),
                        bash_command = "echo $NUMBER",
                        env={"NUMBER": i},
                )

        def print_task_number(ts, run_id, **kwargs):
                print(f"task number is: {kwargs['task_number']}")
                print(ts)
                print(run_id)

        for i in range(10, 30):
            task = PythonOperator(
                    task_id = 'task_number'+str(i),
                    python_callable = print_task_number,
                    op_kwargs = {'task_number': int(i)},
            )

        task.doc_md = dedent(
                """\
                #### Test documentation
                **bold text**
                _italic text_
                `some code`
                # abzac
                """
        )
