from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
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
            return 'startml_desc'
        return 'not_startml_desc'

    dummy1=DummyOperator(
        task_id='before_branching'
    )
    dummy2=DummyOperator(
        task_id='after_branching'
    )

    is_true = BashOperator(
        task_id='startml_desc',  # id, будет отображаться в интерфейсе
        bash_command='echo "StartML is a starter course for ambitious people"',  # какую bash команду выполнить в этом таске
    )
    is_false = BashOperator(
        task_id='not_startml_desc',  # id, будет отображаться в интерфейсе
        bash_command='echo "Not a startML course, sorry"',  # какую bash команду выполнить в этом таске
    )

    branchf = BranchPythonOperator(
        task_id='determine_course',
        python_callable=choose_val
    )

    dummy1 >> branchf >> [is_true,is_false] >> dummy2
    # А вот так в Airflow указывается последовательность задач
    # t2 >> taskp
    # t1 >> [t2, t3] >> taskp
    # будет выглядеть вот так
    #      -> t2
    #  t1 | 
    #      -> t3