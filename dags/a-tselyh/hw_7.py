"""
# Foo
Hello, these are DAG docs.
"""
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime

with DAG(
    "what_the_hw7",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='fck_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(year=2022, month=3, day=22),
    catchup=False,
    tags=['hw_7'],
) as dag:
    templated_command = dedent(
        """
    {% for i in range({NUMBER}) %}
        echo "{{ ts }}"
    {% endfor %}
    echo "{{ run_id }}"
    """)
    for i in range(1,11):
        t1 = BashOperator(
        task_id='BO7' + str(i)'
        bash_command= templated_command
    )

    def print_context(task_n: int, ts, run_id, **kwargs):
        print("task number is: {kwargs['task_number']}")
        print(ts)
        print(run_id)

    for task_number in range(1, 21):
        t2 = PythonOperator(
            task_id=f'print_task_{task_number}',
            python_callable=print_context,
            op_kwargs={'task_number': task_n}
          )
        t2.doc_md = dedent(
            """\
        #### Task Documentation
        You can document your task using the attributes `doc_md` (markdown),
        `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
        rendered in the UI's Task Instance Details page.
        ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

        """
        )


    t1 >> t2