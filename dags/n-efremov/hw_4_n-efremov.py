from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        "hw_n-efremov_4",
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description="hw_n-efremov_4 DAG",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 22),
        catchup=False,
        tags=['second_task'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id=f'echo_task{i}',
            bash_command=f'echo {i}',
        )


    def print_context(task_number, **kwargs):
        print(kwargs)
        return f'task number is: task_number'


    for i in range(20):
        t2 = PythonOperator(
            task_id=f'print_{i}',
            python_callable=print_context,
            op_kwargs={'task_number': i},
        )

    t1.doc_md = dedent(
        """\
            #### Task Documentation
            You can document your task using the attributes `doc_md` (markdown),
            `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
            rendered in the UI's Task Instance Details page.
            ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
            **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
        """
    )

    t2.doc_md = dedent(
        """\
            #### Task Documentation
            You can document your task using the attributes `doc_md` (markdown),
            `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
            rendered in the UI's Task Instance Details page.
            ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
            **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
        """
    )

    t1 >> t2
