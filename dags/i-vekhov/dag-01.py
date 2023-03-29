from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

with DAG(
    'simple'
) as dag:
    t1 = BashOperator(
        task_id="print_folder_path",
        bash_command="pwd "
    )

    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
        task_id="print_ds_arg",
        python_callable=print_context(),
    )

    t1 >> t2
