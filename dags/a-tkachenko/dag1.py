from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG

with DAG(
    'tutorial',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
) as dag:

    date = "{{ ds }}"

    t_bash = BashOperator(
    task_id="test_pwd",
    bash_command="pwd"
    )

    def print_context(ds):
        # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t_py = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context
    )