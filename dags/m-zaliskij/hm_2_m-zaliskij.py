from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hm_2_m-zaliskij',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)# timedelta из пакета datetime
        },
        start_date=datetime(2022, 1, 1),
        catchup=False

) as dag:
    t1 = BashOperator(
        task_id="test",
        bash_command="pwd"
    )


    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'


    t2 = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context, )

    t1 >> t2
