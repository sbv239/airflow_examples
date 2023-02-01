from airflow import DAG
from airflow.operators.bash import BashOperator, PythonOperator
with DAG(
    'task_1'
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    })
as dag:
    t1 = BashOperator(
        task_id = 'pwd'
        bash_command = 'pwd'
    )
    def print_ds(ds):
        print(ds)
        return 'ok'
    t2 = PythonOperator(
        task_id = 'python'
        python_callable = print_ds
    )
    t1 >> t2
