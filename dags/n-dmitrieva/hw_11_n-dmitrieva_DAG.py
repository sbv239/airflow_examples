from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

'''Напишите DAG, состоящий из одного PythonOperator. Этот оператор должен печатать 
значение Variable с названием is_startml

Сдайте задание через MR и залейте файл с дагом ниже.


'''
from airflow.models import Variable
def get_variable():
    is_prod = Variable.get("is_startml")  # необходимо передать имя, заданное при создании Variable
# теперь в is_prod лежит значение Variable
    print(is_prod)


with DAG(
    'hw_10_n-dmitrieva_postgress',
    default_args = { # Default settings applied to all tasks
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    },
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id = 'hw_11_n-dmitrieva_veriblr', #task ID
        python_callable = get_variable,
        )
    
    task1
