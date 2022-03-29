from datetime import timedelta, datetime
from textwrap import dedent

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'hw_6_t-volkov-5',
    default_args=default_args,
    description='God bless my creature',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 20),
    catchup=False
) as dag:

    def print_task_number(task_number, ts, run_id, **kwargs):
        print(f'task number is: {task_number}')
        print(ts)
        print(run_id)
   
    for i in range(20):
        t2 = PythonOperator(
            task_id='looping_python_operator_and_ts_and_runid' + str(i),
            python_callable=print_task_number,  
            op_kwargs={'task_number' : i}
        )
        t2.doc_md = dedent(
            """\
        #### Task 2 Documentation
        **current** _python_ command loops `task_number is: {i}` and prints `run_id` and `ts`

        """
        )  
        t2
