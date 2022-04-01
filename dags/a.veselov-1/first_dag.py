from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import datetime

              
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),  # timedelta из пакета datetime
    'catchup': False
}
with DAG(
        dag_id = 'a_veselov_task_1_DAG',
        default_args = default_args,
        description = '',
        schedule_interval = datetime.timedelta(days=1),
        start_date=datetime.datetime(2022, 1, 1),
        tags=['a.veselov-1'],
) as dag:


def print_ds(ds):
    print(f'Внутренняя переменная airflow ds: {ds}')
    print(f'А также посмотрим и остальные переменные: {kwargs}')

python_task = PythonOperator(
    task_id = 'print_ds',
    python_callable = print_ds,
    dag = dag
)

bash_task = BashOperator(
    task_id = 'print_our_directory',
    bash_command = 'pwd',
    dag = dag
)

python_task >> bash_task
