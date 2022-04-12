from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.models import Variable

with DAG(
    'shuteev_my_dag_11',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='My Hyper DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['shuteev'],
) as dag:
    def my_set(ti):
        rez = Variable.get("is_startml")
        print(rez)
        return rez

    def my_get(ti):
        text_val = ti.xcom_pull(
            task_ids='my_p_set'
        )
        print(f'My XCOM val is: {text_val} ')
        # print('The value is: {}'.format(
        #     ti.xcom_pull(task_ids='hello_world')
        # ))

    pygetter = PythonOperator(
        task_id='my_p_get',  # в id можно делать все, что разрешают строки в python
        python_callable=my_get,)

    pysetter = PythonOperator(
        task_id='my_p_set',  # в id можно делать все, что разрешают строки в python
        python_callable=my_set,)

    pysetter >> pygetter
    # А вот так в Airflow указывается последовательность задач
    # t2 >> taskp
    # t1 >> [t2, t3] >> taskp
    # будет выглядеть вот так
    #      -> t2
    #  t1 | 
    #      -> t3