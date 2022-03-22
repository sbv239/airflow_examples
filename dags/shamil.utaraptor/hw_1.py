from airflow import DAG
from airflow.operator.bash import BashOperator
from airflow.operator.python import PythonOperator

from datetime import timedelta


def print_context(ds):
    print(ds)


with DAG(
    "hw_1_shamil.utaraptor",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG for the first homework',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 22),
    catchup=False,
    tags=['hw_1_shamil.utaraptor'],
) as dag:
    t1 = BashOperator(
        task_id='print_current_dir',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )

    t2 = PythonOperator(
        task_id="print_context",
        python_callable=print_context,
        op_kwargs={'ds': "{{ ds }}"}
    )

    t1 >> t2