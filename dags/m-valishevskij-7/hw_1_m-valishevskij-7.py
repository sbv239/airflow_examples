from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_1_m-valishevskij-7'
) as dag:
    t1 = BashOperator(
        task_id='hw_1_m-valishevskij-7_1',
        bash_command='pwd'
    )


    def print_ds(ds):
        print(ds)
        return 'my return'


    t2 = PythonOperator(
        task_id='hw_1_m-valishevskij-7_2',
        python_callable=print_ds
    )

    t1 >> t2
