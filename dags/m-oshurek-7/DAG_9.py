"""

В лекции говорилось, что любой вывод return уходит неявно
в XCom. Давайте это проверим.

Создайте новый DAG, содержащий два PythonOperator.
Первый оператор должен вызвать функцию, возвращающую
строку "Airflow tracks everything".

Второй оператор должен получить эту строку через XCom.
Вспомните по лекции, какой должен быть ключ. Настройте
правильно последовательность операторов.

"""

from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator


def pusher():
    return 'Airflow tracks everything'


def puller(ti):
    xcom_print = ti.xcom_pull(
        key='return_value',
        task_ids='pusher_return'
    )
    print(xcom_print)


with DAG(
        'DAG_9_oshurek',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='DAG9 Rakhimova',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 25),
        catchup=False,
        tags=['example_9_oshurek'],
) as dag:

    t1 = PythonOperator(
        task_id='pusher_return',
        python_callable=pusher,
    )

    t2 = PythonOperator(
        task_id='puller_return',
        python_callable=puller,
    )

    t1 >> t2
