from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.operators.bash import BashOperator

def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


with DAG(
    'HW_2_v-patrakeev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime.now(),
) as dag:

    # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)
    t1 = BashOperator(
        task_id='print_pwd',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )
    t2 =PythonOperator(
        task_id = 'print_ds',
        python_callable=print_context,
    )
    t1 >> t2