from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_context(task_number, **kwargs):
    return 'task number is: {task_number}'

with DAG(
    'hw_4_i-loskutov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
},

    description='task04',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 27),
    catchup=False

) as dag:
    for i in range(10):
        t1 = BashOperator(
        task_id = f'task03_BashOperator_{i}',
        bash_command = f"echo {i}",
        )
    for i in range(20):
        t2 = PythonOperator(
        task_id = f'task03_PythonOperator_{i}',
        python_callable = print_context,
        op_kwargs={'task_number': i}
        )

    t1.doc_md = dedent(
                """\
            # Task Documentation
            You can *document* your **task** using the attributes `doc_md` (markdown),
            `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
            rendered in the UI's Task Instance Details page.
            ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

            """
        )  

    t2.doc_md = dedent(
                """\
            # Task Documentation
            You can *document* your **task** using the attributes `doc_md` (markdown),
            `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
            rendered in the UI's Task Instance Details page.
            ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

            """
        ) 


    t1 >> t2




