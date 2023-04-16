from datetime import datetime, timedelta

from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.python import PythonOperator

def get_connection():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
      with conn.cursor() as cursor:
        cursor.execute(
            f"""Select user_id,
            count(action) as count
            from feed_action
            where action = 'like'
            group by user_id
            order by count desc""")
        result = cursor.fetchone()
        return result

with DAG(
    'xcom_d_semenov',
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
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['d-semenov'],
) as dag:

    task1 = PythonOperator(
        task_id='get_user',
        python_callable=get_connection,
    )
