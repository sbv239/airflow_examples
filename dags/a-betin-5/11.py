from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_var():
        from airflow.models import Variable
        result = Variable.get("is_startml")
        print(result)



with DAG(
    'HW_11_a-betin-5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_retry': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='A 11th DAG',
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 2),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # теги, способ помечать даги
    tags=['task_11'],
) as dag:
        t1 = PythonOperator(
                task_id = "print_is_startml",
                python_callable = get_var,
        )

        t1
