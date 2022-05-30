"""
##Assignment 10 DAG documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator
from psycopg2.extras import RealDictCursor

with DAG(
        'gul_assignment_10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Connection-to-databases practice',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 28),
        catchup=False,
        tags=['gul_dags']
) as dag:
    def get_most_likes():
        from airflow.providers.postgres.operators.postgres import PostgresHook

        postgres = PostgresHook(postgres_conn_id='startml_feed')
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT user_id, COUNT(post_id)
                    FROM "feed_action"
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY COUNT(post_id) DESC
                    LIMIT(1)
                    """
                )
                result = cursor.fetchall()


    p1 = PythonOperator(
        task_id='sql_query_task',
        python_callable=get_most_likes
    )

    dag.doc_md = __doc__
