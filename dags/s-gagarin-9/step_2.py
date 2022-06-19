from airflow import DAG
from airflow.operators.bash import BashOperator, PythonOperator
from datetime import timedelta

with DAG(
        'first_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        }
) as dag:
    t_bash = BashOperator(
        task_id='print current dir',
        bash_command='pwd',
    )


    def print_ds(ds):
        print(ds)


    t_python = PythonOperator(
        task_id='print ds',
        python_collable=print_ds,
    )

t_bash >> t_python
