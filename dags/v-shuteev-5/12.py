from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
classairflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.models import Variable

with DAG(
    'shuteev_my_dag_12',
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
    def choose_val():
        rez = Variable.get("is_startml")
        if (rez):
            return 'Task_Yes'
        return 'Task_No'

    # def my_get(ti):
    #     text_val = ti.xcom_pull(
    #         task_ids='my_p_set'
    #     )
    #     print(f'My XCOM val is: {text_val} ')
    #     # print('The value is: {}'.format(
    #     #     ti.xcom_pull(task_ids='hello_world')
    #     # ))


    dummy1=DummyOperator(
        task_id='before_branching'
    )
    dummy2=DummyOperator(
        task_id='after_branching'
    )

    dummy4ye=DummyOperator(
        task_id='Task_Yes'
    )
    dummy4ye2=DummyOperator(
        task_id='Task_No'
    )
    # pygetter = PythonOperator(
    #     task_id='my_p_get',  # в id можно делать все, что разрешают строки в python
    #     python_callable=my_get,)

    branchf = BranchPythonOperator(
        task_id='determine_course',
        python_callable=choose_val
    )
    # pysetter = PythonOperator(
    #     task_id='my_p_set',  # в id можно делать все, что разрешают строки в python
    #     python_callable=my_set,)

    dummy1 >> branchf >> dummy2
    # А вот так в Airflow указывается последовательность задач
    # t2 >> taskp
    # t1 >> [t2, t3] >> taskp
    # будет выглядеть вот так
    #      -> t2
    #  t1 | 
    #      -> t3