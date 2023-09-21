from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

def print_num(ds, **kwargs):
    print(f'task number is: {kwargs["number"]}')

with DAG(
    'hw_al-pivovarov_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime.now()
) as dag:
    for i in range(10):
        t1 = BashOperator(task_id=f'echo_{i}', bash_command=f'echo $NUMBER', env={'NUMBER': i})