from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from textwrap import dedent


default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
with DAG(
        'andre-karasev_hw_3',
        default_args=default_args,
        description='hw_3_',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 9, 9),
        catchup=False,
        tags=['andre-karasev_hw_3']) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='echo' + str(i),
            bash_command=f'echo {i}',
        )


    def print_number(task_number, ts, run_id):
        print(ts)
        print(run_id)
        return f"task number is: {task_number}"

    for i in range(20):
        t2 = PythonOperator(
            task_id="task_number" + str(i),
            python_callable=print_number,
            op_kwargs={'task_number': i},
        )

    t1 >> t2
