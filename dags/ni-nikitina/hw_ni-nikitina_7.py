from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_context(ts, run_id, task_number, **kwargs):
    return print(task_number, ts, run_id)

with DAG(
    'hw_ni-nikitina_7', 
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Seventh Task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 29),
    catchup=False
) as dag:
    for i in range(20):
	    t2 = PythonOperator(
            task_id=f'hw_7_nn_po_{i}',
            python_callable=print_context,
            op_kwargs={'task_number': i}
        )