from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator


# Функция, которая возвращает строку
def get_varialbe():
        return Variable.get("is_startml")

# Сравниваем результат, который возвращается
# и делаем соответствующий оператор
def brancing(get_varialbe):
        if get_varialbe() == "is_startml":
                return t2
        else:
                return t3

# Два метода, которые возвращают строки
def get_variable_true():
        return "StartML is a starter course for ambitious people"

def get_variable_false():
        return "Not a startML course, sorry"


# Даг
with DAG(
#описание DAG'a
    'hw_13_s-birjukov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description = 'DAG_xcom',
        schedule_interval = timedelta(days = 1),
        start_date = datetime(2023,3,25),
        catchup=False,
        tags = ['hw_13_s-birjukov_Variables_and_Branching']
) as dag:

        t0 = DummyOperator(task_id = 'Start')

        t1 = PythonOperator(
        task_id = 'get_variable',
        python_callable = get_varialbe
        )
# в зависимости от результата t1, берем или t2 или t3.
# Здесь это 2 питон оператора, который вызывают соответствующие функции
        t2 = PythonOperator(
        task_id = 'startml_desc',
        python_callable= get_variable_true
        )

        t3 = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable= get_variable_false
        )
        t4 = DummyOperator(task_id= 'Finish')

#Последовательно выполнения DAG'ovЫ
t0 >> t1 >> [t2, t3] >> t4