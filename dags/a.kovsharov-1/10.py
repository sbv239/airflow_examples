from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor


default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        }
data = {"sample_xcom_key": "xcom test"}

with DAG(
        'hw_10_a.kosharov',
        # Параметры по умолчанию для тасок
        default_args=default_args,
    # Описание DAG (не тасок, а самого DAG)
    description='A cycle generated DAG of homework of Lesson 11',
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
    tags=['HW_10', 'a.kovsharov']
) as dag:
    
    def get_user_max_likes():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                res = cursor.execute(
                    """
                    SELECT user_id, COUNT(action) AS count
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY count DESC
                    LIMIT 1
                    """)
                return res
            
    
    t1 = PythonOperator(
        task_id = "user_max_likes",
        python_callable = get_user_max_likes)
    