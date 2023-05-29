from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator, PythonOperator
from airflow import DAG


default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }

with DAG('andre-karasev_hw_2',
         default_args=default_args,
         description='hw_2_',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2022, 9, 9),
         catchup=False,
         tags=['andre-karasev_hw_2']) as dag:
    def print_date(ds):
        print(ds)
        return ds

    bash = BashOperator(
        task_id='python_pwd',
        bash_command='pwd',
    )

    operator = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )

    bash >> operator
