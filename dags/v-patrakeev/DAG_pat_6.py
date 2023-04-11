from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.operators.bash import BashOperator

def print_task_num(ts, run_id, **kwargs):
    print(f'task number is: {kwargs}, {{ts}}, {{run_id}}')


with DAG(
    'HW_7_v-patrakeev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime.now(),
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='print_command_' + str(i),
            bash_command=f'echo {i}',
        )

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_task_num_' + str(i),
            python_callable=print_task_num,
            op_kwargs={'task_number': i},
        )


    t1 >> t2