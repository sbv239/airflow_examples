from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import psycopg2
from psycopg2.extras import RealDictCursor



def connection_to():
    creds = BaseHook.get_connection(conn_id="startml_feed")
    with psycopg2.connect(
            f"postgresql://{creds.login}:{creds.password}@{creds.host}:{creds.port}/{creds.schema}"
    ) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(action) as total_like
                FROM feed_action
                WHERE action::text = 'like'
                GROUP BY user_id
                ORDER BY total_like DESC
                LIMIT 1;
                """
            )
            result = cursor.fetchone()
            user_id = result['user_id']
            count = result['total_like']
            result_dict = {"user_id": user_id, "count": count}
    return result_dict


with DAG(
        'les_11_task_11_i-osiashvili-19',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 4, 20),
        schedule_interval=timedelta(days=1),

) as dag:
    find_user = PythonOperator(
        task_id="user_and_count",
        python_callable=connection_to,
    )


# from airflow import DAG
# from datetime import timedelta, datetime
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.hooks.base import BaseHook
# import psycopg2
# from psycopg2.extras import RealDictCursor
# from twisted.conch.insults.window import cursor
#
#
# def connection_to():
#     creds = BaseHook.get_connection(conn_id="startml_feed")
#     with psycopg2.connect(
#             f"postgresql://{creds.login}:{creds.password}"
#             f"@{creds.host}:{creds.port}/{creds.schema}"
#     ) as conn:
#         with cursor = conn.cursor(cursor_factory=RealDictCursor)
#             cursor.execute(
#             """
#             SELECT user_id, COUNT(action) as total_like
#             FROM feed_action
#             WHERE action::text = 'like'
#             GROUP BY user_id
#             ORDER BY total_like DESC
#             LIMIT 1;
#             """)
#             result = cursor.fetchone()
#             user_id = result[0]
#             count = result[1]
#             result_dict = {"user_id": user_id, "count": count}
#     return result_dict
#
#
# with DAG(
#         'les_11_task_11_i-osiashvili-19',
#         default_args={
#             'depends_on_past': False,
#             'email': ['airflow@example.com'],
#             'email_on_failure': False,
#             'email_on_retry': False,
#             'retries': 1,
#             'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
#         },
#         start_date=datetime(2023, 4, 20),
#         schedule_interval=timedelta(days=1),
#
# ) as dag:
#     find_user = PythonOperator(
#         task_id="user_and_count",
#         python_callable=connection_to,
#     )




# from airflow import DAG
# from datetime import timedelta, datetime
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
#
# def connection_to():
#     postgres = PostgresHook(postgres_conn_id="startml_feed")
#     with postgres.get_conn() as conn:
#         with conn.cursor() as cursor:
#             cursor.execute("""
#             SELECT user_id, COUNT(action) as total_like
#             FROM feed_action
#             WHERE action::text = 'like'
#             GROUP BY user_id
#             ORDER BY total_like DESC
#             LIMIT 1;
#             """)
#             result = cursor.fetchone()
#             user_id = result[0]
#             count = result[1]
#             result_dict = {"user_id": user_id, "count": count}
#     return result_dict
#
#
# with DAG(
#         'les_11_task_11_i-osiashvili-19',
#         default_args={
#             'depends_on_past': False,
#             'email': ['airflow@example.com'],
#             'email_on_failure': False,
#             'email_on_retry': False,
#             'retries': 1,
#             'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
#         },
#         start_date=datetime(2023, 4, 20),
#         schedule_interval=timedelta(days=1),
#
# ) as dag:
#     find_user = PythonOperator(
#         task_id="user_and_count",
#         python_callable=connection_to,
#     )