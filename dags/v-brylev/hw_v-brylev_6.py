from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def print_context(ds, **kwargs):
    task_number = kwargs['task_number']
    print(f"task number is: {task_number}")
    return 'Whatever you return gets printed in the logs'


with DAG(
        'hw_v-brylev_6',
        start_date=datetime(2023, 7, 26),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=5),
        default_args=default_args,
        catchup=False
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='print_info_' + str(i),
            bash_command="echo $NUMBER",
            env={'NUMBER': str(i)}
        )
