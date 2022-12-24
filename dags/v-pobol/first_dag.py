from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta

with DAG(
        default_args = {
            'depends_on_past':False,
            'email':['pobol.email@yandex.ru']
            'email_on_failure':True,
            'email_on_retries':False,
            'retries':1,
            'retry_daley': timedelta(minutes=5)
            },
        descriptions = "My first dag",
        schedule_interval = timedelta(days=1),
        start_date = datetime.datetime(2022,12,22),
        cetchup=False,
        tags=['first_dag']
        ) as dag:
    
    ### FUNC
    def print_dt(dt):
        print(dt)

    python_task = PythonOperator(
            task_id = 'print dt',
            python_callable = print_dt
            )

    bash_task = BashOperator(
            task_id = 'print current_directory',
            bash_command = 'pwd'
            )

    bash_task >> python_task




