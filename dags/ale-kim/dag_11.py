from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from textwrap import dedent
from psycopg2.extras import RealDictCursor

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def sql_query():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn(cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """                   
            SELECT "user_id", count(user_id) AS "count"
            FROM feed_action
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY "count" desc
            LIMIT 1
            """
            )
            return cursor.fetchall()


default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
    description="A simple tutorial DAG",
    dag_id="ale-kim_dag_11",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["ale-kim"],
) as dag:
    first_oper = PythonOperator(
        task_id="sql_query",
        python_callable=sql_query,
    )

    first_oper.doc_md = dedent(
        """\
    # Task Documentation
    Return result of sql query
    """
    )
