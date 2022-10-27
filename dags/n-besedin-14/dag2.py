from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_2_n-besedin-14',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)}
) as dag:
    t1 = BashOperator(
        task_id='pwd',
        bash_command='pwd')


    def print_ds(ds):
        print(ds)
        return 'Ok'


    t2 = PythonOperator(
        task_id='print ds',
        python_callable=print_ds)
    