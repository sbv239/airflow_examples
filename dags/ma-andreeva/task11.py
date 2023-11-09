
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'task_11_andreeva',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A DAG for task 11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 30),
    catchup=False,
    tags=['task11','task_11','andreeva'],
) as dag:

    def get_variable():
        from airflow.models import Variable  #Лучше такие вещи импортировать там, где они нужны, внутри функции
        print(Variable.get("is_startml"))


    t1 = PythonOperator(
        task_id='get_VAR',
        python_callable=get_variable
        #python_callable=lambda: print(Variable.get("is_startml"))
        #Можно и без написания отдельной функции вернуть зн-е Variable, но надо не забыть сделать импорт  from airflow.models import Variable
    )





