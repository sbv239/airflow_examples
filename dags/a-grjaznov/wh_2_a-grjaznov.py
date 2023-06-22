from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
        # 'tutorial_gryaznov'
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)}
        , description='test Gryaznov'
        , schedule_interval=timedelta(days=1)
        , start_date=datetime(2023, 6, 20)
        , catchup=False
        , tags=['example']
        , dag_id='dag_2_agryaznov')as dag_agryaznov:
    bash_gryaz_2 = BashOperator(
        task_id='gryaz_bash'
        , bash_command='pwd'
    )


    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'


    py_gryaz_2 = PythonOperator(task_id='gryaz_py'
                                , python_callable=print_context
                                )
    bash_gryaz_2 >> py_gryaz_2