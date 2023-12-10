from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime,timedelta

with DAG(
    "hw_7_n_vojtova",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Python DAG with more variables',
    schedule_interval= timedelta(days=1),
    start_date= datetime(2023,12,9),
    catchup=False,
    tags = ['hw_7','n_vojtova'],
) as dag:
    t1 = DummyOperator(task_id='start_dag')
    t2 = DummyOperator(task_id='end_dag')

    def task_nos(ts,run_id,task_number,**kwargs):
        print(ts)
        print(run_id)
        print(task_number)
        return "task number"

    for i in range(20):
        py_task = PythonOperator(
            task_id=f'print_number_{i}',
            python_callable= task_nos,
            op_kwargs={'task_number': i}
        )
        t1 >> py_task >> t2