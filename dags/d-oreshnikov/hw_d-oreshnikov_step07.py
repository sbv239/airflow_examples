from datetime import timedelta, datetime


from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator



def print_range_number(task_number, ts, run_id):
    print(ts)
    print(run_id)
    print(f"task number is:{task_number}")



with DAG(
    'hw_d-oreshnikov_02',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    start_date=datetime(2023, 1, 1)
) as dag:
    
    for i in range(10):
    
        t1 = BashOperator(
            task_id = f'print_echo_{i}',
            bash_command = f'echo {i}'
        )
        t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    *ghbdtn*
    **ghbdghdbghb**
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    """
    )
    for i in range(20):

        t2 = PythonOperator(
            task_id = f'print_range_number_{i}',
            python_callable= print_range_number,
            op_kwargs={'task_number' : i}
        )

        t2.doc_md = dedent(
        """\
    #### Task Documentation
    *ghbdtn*
    **ghbdghdbghb**
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    """
    )
t1 >> t2