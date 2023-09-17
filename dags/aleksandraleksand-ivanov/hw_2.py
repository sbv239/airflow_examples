from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime
#aleksandraleksand-ivanov
default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        start_date= datetime(2023, 9, 17),
        dag_id="hw_aleksandraleksand-ivanov_2",
        default_args=default_args,
        schedule_interval=timedelta(days=1)
) as dag:

    task1 = BashOperator(
        task_id="task1",
        bash_command="pwd"
    )


    def print_info(ds, **kwargs):
        print(ds)


    task2 = PythonOperator(
        task_id="task2",
        python_callable=print_info
    )

    task1 >> task2
