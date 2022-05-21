from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

with DAG(
        'step9nazarov',
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
        description='step9',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['nazarov9'],
) as dag:

    def connect():
        from airflow.providers.postgres.operators.postgres import PostgresHook

        postgres = PostgresHook(postgres_conn_id='startml_feed')
        with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT 
                        user_id, 
                        count(action) as count_likes
                    FROM "feed_action" 
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY 2 DESC
                    LIMIT 1
                    """
                )
                f = cursor.fetchone()
                return f


    t1 = PythonOperator(
        task_id='connect',
        python_callable=connect,
    )
