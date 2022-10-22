from airflow.operators.bash import BashOperator
from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
def get_ds(ds):
    print(ds)
with DAG("firsttask",default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),},
    description='creating first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 21),
    catchup=False,
    tags=['task_two'],) as dag:
    # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)
    t1 = BashOperator(
        task_id='print_pwd1',
        bash_command='pwd')

    t2 = PythonOperator(
        task_id='print_ds1',
        python_callable=get_ds)

