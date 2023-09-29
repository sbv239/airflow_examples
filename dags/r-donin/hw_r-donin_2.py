"""
Напишите DAG, который будет содержать BashOperator и PythonOperator. 
В функции PythonOperator примите аргумент ds и распечатайте его. 
Можете распечатать дополнительно любое другое сообщение.
В BashOperator выполните команду pwd, которая выведет директорию, где выполняется ваш код Airflow.
Результат может оказаться неожиданным, не пугайтесь - 
Airflow может запускать ваши задачи на разных машинах или контейнерах с разными настройками и путями по умолчанию.
Сделайте так, чтобы сначала выполнялся BashOperator, потом PythonOperator. 
NB! Давайте своим DAGs уникальные названия. Лучше именовать их в формате hw_{логин}_1, hw_{логин}_2
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

with DAG(
    'hw_r-donin_2',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'Exercise dag from step 2',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2023, 9, 29),
    catchup = False,
    tags = ['startml', 'airflow', 'r-donin']
) as dag:
    def print_ds(ds, **kwargs):
        some_text = str
        some_int = int
        some_date = datetime
        print(ds)
        print(kwargs)
        
    t1 = PythonOperator(
        task_id = 'print_ds_s2',
        python_callable = print_ds,
        op_kwargs = {'some_text': 'Текст', 
                     'some_int': 2023,
                     'some_date': datetime(1992, 6, 23)}
    )
    
    t2 = BashOperator(
        task_id = 'print_pwd',
        bash_command = 'pwd'
    )
        
    dag.doc_md = __doc__
    
    t2 >> t1