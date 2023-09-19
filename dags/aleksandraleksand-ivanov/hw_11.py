from airflow.providers.postgres.hooks.postgres import PostgresHook, RealDictCursor
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def get_most_liked_user():
    # используем PostgresHook для подключения к базе данных
    postgres = PostgresHook(postgres_conn_id="startml_feed")

    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # SQL-запрос для извлечения пользователя с наибольшим количеством лайков
            query = """
                SELECT user_id, COUNT(*)
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY count DESC
                LIMIT 1;
            """
            cursor.execute(query)
            result = cursor.fetchone()
            # если результат есть, сохраняем его в XCom
            return result


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        dag_id="hw_aleksandraleksand-ivanov_11",
        default_args=default_args,
        start_date=datetime(2023, 9, 18),
        schedule_interval=timedelta(days=1)
) as dag:

    pull = PythonOperator(
        task_id="pull",
        python_callable=get_most_liked_user
    )

    pull