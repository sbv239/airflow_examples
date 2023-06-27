from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'hw_e.mironenko-13_2',
) as dag:
    t1 = BashOperator(
        task_id='task_bash_pwd',
        bash_command='pwd',
    )

    def print_ds(ds):
        print(ds)
        print(f'time: {ds}')

    t2 = PythonOperator(
        task_id='task_python_ds',
        python_callable=print_ds,
    )

t1 >> t2