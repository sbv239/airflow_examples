"""
# doc
This task **boldly** __suggests__ a _daily_ *activity*.
`some code`
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from textwrap import dedent
from datetime import datetime, timedelta


def print_context(task_number):
    print(f"task number is: ${task_number}")


with DAG(
        "hw_3_n-chuviurova",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        description="dynamic tasks",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 6, 15),
        catchup=False,
        tags=["task_03"],
) as dag:
    dag.doc_md = __doc__

    for i in range(10):
        t1 = BashOperator(
            task_id="hw_n-chuviurova_" + str(i),
            bash_command=f"echo {i}"
        )

    for j in range(10, 30):
        t2 = PythonOperator(
            task_id="hw_n-chuviurova_" + str(j),
            python_callable=print_context,
            op_kwargs={'task_number': j},
        )

    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can __document__ your task using the attributes `doc_md` (_markdown_),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    """
    )
    t2.doc_md = dedent(
        """\
    #### Task Documentation
    You can __document__ your task using the attributes `doc_md` (_markdown_),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    """
    )

    t1 >> t2
