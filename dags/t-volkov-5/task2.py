from datetime import timedelta, datetime
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


from airflow import DAG
from airflow.operator.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'task11.2',
    default_args=default_args,
    description='God bless my creature',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 20),
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id="pwd_execute",
        bash_command="pwd"
    )
    def print_context(ds, **kwargs):
        print(ds)
        return 'Yip'

    t2 = PythonOperator(
        task_id='print_the_ds',
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )
    t1>>t2