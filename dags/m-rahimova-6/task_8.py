"""

Сделайте новый DAG, содержащий два Python оператора.
Первый PythonOperator должен класть в XCom значение
"xcom test" по ключу "sample_xcom_key".

Второй PythonOperator должен доставать это значение и
печатать его. Настройте правильно последовательность
операторов.

"""

from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator


def pusher(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )


def puller(ti):
    xcom_value = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='pusher'
    )
    print(xcom_value)


with DAG(
    'rakhimova_task8',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

    },
    description='DAG8 Rakhimova',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 25),
    catchup=False,
    tags=['hehe'],
) as dag:

    t1 = PythonOperator(
        task_id='pusher',
        python_callable=pusher,
    )

    t2 = PythonOperator(
        task_id='puller',
        python_callable=puller,
    )

    t1 >> t2
