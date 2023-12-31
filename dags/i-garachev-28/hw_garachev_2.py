from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent


with DAG(
    'tutorial',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_garachev_2_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_2'],
) as dag:

    
    t1 = BashOperator(
        task_id="hw_garachev_1_bash",
        bash_command="pwd"
    )

    def print_context(ds):
        """Пример PythonOperator"""
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
        task_id='hw_garachev_1_python',
        python_callable=print_context
    )

    t1 >> t2