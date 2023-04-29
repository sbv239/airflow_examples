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
    description='hw_garachev_1_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_1'],
) as dag:

    def print_context(ds, i):
        """Пример PythonOperator"""
        print(f"task numer is: {i}")
        return 'Whatever you return gets printed in the logs'
    
    for i in range(9):
        t1 = BashOperator(
            task_id="hw_garachev_1_bash",
            bash_command=f"echo {i}"
        )

        t1
    
    for i in range(19):
        t2 = PythonOperator(
            task_id='hw_garachev_1_python',
            python_callable=print_context ,
            op_kwargs = {"i": i}
        )

        t1 >> t2