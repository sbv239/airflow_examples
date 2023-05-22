from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


"""
В лекции говорилось, что любой вывод return уходит неявно в XCom. Давайте это проверим.

Создайте новый DAG, содержащий два PythonOperator. Первый оператор должен вызвать функцию, возвращающую строку 
"Airflow tracks everything".

Второй оператор должен получить эту строку через XCom. Вспомните по лекции, какой должен быть ключ. 
Настройте правильно последовательность операторов.

NB! Давайте своим DAGs уникальные названия. Лучше именовать их в формате hw_{логин}_1, hw_{логин}_2
"""

with DAG(
'hw_d-ivashkin-23_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework 9-th step DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 21),
    catchup=False,
    tags=['homework', 'di']
) as dag:

    def airflow_print():
        return "Airflow tracks everything"


    def value_pull(ti):
        value = ti.xcom_pull(
            key='return_value',
            task_ids='airflow_return'
        )
        print(value)

    airflow_return = PythonOperator(
        task_id='airflow_return',
        python_callable=airflow_print
    )

    pull_value = PythonOperator(
        task_id='xcom_pull',
        python_callable=value_pull
    )

airflow_return >> pull_value