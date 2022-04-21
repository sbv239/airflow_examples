from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
'''
Напишите DAG, состоящий из одного PythonOperator.
Этот оператор должен, используя подключение с
conn_id="startml_feed", найти пользователя, который
поставил больше всего лайков, и вернуть пару
(user_id, количество_лайков). Эта пара, кстати,
сохранится в XCom.
'''


with DAG(
    # имя дага, которое отразиться на сервере airflow
    'DAG_10_oshurek',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание самого дага
    description='Задание1. Напишите DAG, который будет содержать BashOperator и PythonOperator.'
                ' В функции PythonOperator примите аргумент ds и распечатайте его.',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example_10_oshurek'],
) as dag:
    def get_user_id():
        from airflow.providers.postgres.operators.postgres import PostgresHook
        from psycopg2.extras import RealDictCursor

        postgres = PostgresHook(postgres_conn_id='startml_feed')
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT user_id, count(action) as count
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP by user_id
                    ORDER BY count(action) DESC
                    LIMIT 1
                    """
                )
                return cursor.fetchone()
    
    t1 = PythonOperator(
        task_id='top_user',
        python_callable=get_user_id,
    )
    
    t1
