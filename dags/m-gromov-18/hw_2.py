from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_2_m-gromov-18',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for unit 2',
        tegs=['DAG-2'],
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 22),

) as dag:
    t1 = BashOperator(
        task_id='BO_hw2',
        bash_command=pwd
    )

    def print_ds(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
        task_id='PO_hw2_print_date',
        python_callable=print_ds
    )
