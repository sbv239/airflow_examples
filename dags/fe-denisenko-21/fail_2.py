"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(
    'hw_fe-denisenko-21_1',
        default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG',
    start_date=datetime(2023, 6, 24),
    catchup=False,
    tags=['example'],
    def print_context(ds):
    """Пример PythonOperator"""
    print(ds)
    return 'Whatever you return gets printed in the logs'
run_this = PythonOperator(
    task_id='print_the_context',  # нужен task_id, как и всем операторам
    python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
)
) as dag:
    t1 = BashOperator(
        task_id='print_derictory',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )
t1 >> run_this
