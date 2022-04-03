from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator


def get_var():
        from airflow.models import Variable
        result = Variable.get("is_startml")
        if result:
                return "startml_desc"
        else:
                return "not_startml_desc"



with DAG(
    'HW_12_a-betin-5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_retry': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='A 12th DAG',
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 2),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # теги, способ помечать даги
    tags=['task_12'],
) as dag:
        def startml_desc():
                print("StartML is a starter course for ambitious people")


        def not_startml_desc():
                print("Not a startML course, sorry")

        t1 = BranchPythonOperator(
                task_id='check_course',
                python_callable=get_var,
                trigger_rule='one_success'
        )

        t2 = PythonOperator(
                task_id='startml_desc',
                python_callable=startml_desc,
        )

        t3 = PythonOperator(
                task_id='not_startml_desc',
                python_callable=not_startml_desc,
        )

        start = DummyOperator(
                task_id='before_branching'
        )

        finish = DummyOperator(
                task_id='after_branching'
        )

        start >> t1 >> [t2, t3] >> finish

