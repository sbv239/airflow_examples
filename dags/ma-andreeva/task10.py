from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'task_10_andreeva',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A DAG for task 10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 10, 30),
        catchup=False,
        tags=['task10', 'task_10', 'andreeva'],
) as dag:
    def get_active_user():
        from airflow.providers.postgres.operators.postgres import PostgresHook
        from psycopg2.extras import RealDictCursor
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        # .get_conn() работает схоже с psycopg2
        with postgres.get_conn() as conn:
            # как и в psycopg2, необходимо создавать курсор
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT f.user_id, COUNT(f.user_id)
                    FROM feed_action f
                    WHERE f.action = 'like'
                    GROUP BY f.user_id
                    ORDER BY COUNT(f.user_id) DESC
                    LIMIT 1
                    """
                )
                results = cursor.fetchone()
        return results


    t1 = PythonOperator(
        task_id="postgres_query",
        python_callable=get_active_user
    )

    # В принципе, он не обязателен - Это просто вывод (печать значений, которые ушли в XCom)
    t2 = PythonOperator(
        task_id="print_operator",
        doc_md="Распечатать значение",
        python_callable=lambda ti: print(ti.xcom_pull(task_ids="postgres_query", key="return_value"))
    )
    t1 >> t2


    # еще один вариант, таски t1 и t3 делают одно и то же
    def get_active_user_base_hook():
        from airflow.hooks.base import BaseHook
        import psycopg2
        from psycopg2.extras import RealDictCursor

        creds = BaseHook.get_connection("startml_feed")
        with psycopg2.connect(
                f"postgresql://{creds.login}:{creds.password}"
                f"@{creds.host}:{creds.port}/{creds.schema}"
        ) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT f.user_id, COUNT(f.user_id)
                    FROM feed_action f
                    WHERE f.action = 'like'
                    GROUP BY f.user_id
                    ORDER BY COUNT(f.user_id) DESC
                    LIMIT 1
                    """
                )
                results = cursor.fetchone()
        return results


    t3 = PythonOperator(
        task_id="another_sql_query",
        python_callable=get_active_user_base_hook
    )




