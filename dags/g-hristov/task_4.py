from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_g-hristov_4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='task4',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 28),
    catchup=False,
) as dag:
    for i in range(10):
        taskbo = BashOperator(
            task_id='hw_g-hristov_4_BO'+str(i),
            bash_command=f"echo {i}",
        )

    def print_context(task_number):
        print(f"task number is: {task_number}")


    for i in range(20):
        taskpo = PythonOperator(
            task_id='hw_g-hristov_4_PO'+str(i),
            python_callable=print_context,
            op_kwargs={"task_number": i}
        )

    taskbo.doc_md = dedent(
        """
    # DOC
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` 
    _Kursiv_**chir**
    """

    )
    taskpo.doc_md = dedent(
        """
    # DOC
    #### Task Documentation
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml`
    _Kursiv_**chir**
    """

    )

    dag.doc_md = __doc__  # Можно забрать докстрингу из начала файла вот так
    dag.doc_md = """
    This is a documentation placed anywhere
    """


    taskbo >> taskpo