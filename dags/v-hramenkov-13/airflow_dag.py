from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import timedelta




with DAG(
    'hw_2_v-hramenkov-13',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),  # timedelta из пакета datetime
}) as dag:

    t1 = BashOperator(
        task_id = 'id_1',
        bash_command = 'pwd'
    )

    def print_context(ds, **kwargs):
        print(ds)
        print(kwargs)

        return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
        task_id = 'id_2',
        python_callable = print_context
    )

    t1 >> t2