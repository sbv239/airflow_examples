from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
        "hw_11_s-kim",
        description="Homework 11",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 8, 1),
        catchup=True,
        tags=["s-kim"],
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        }
) as dag:

    def most_likes_user():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                # your code
                cursor.execute("""
                select
                    f.user_id
                    , count(*) as count
                from public.feed_action f
                where
                    f.action = 'like'
                group by
                    f.user_id
                order by
                    count desc
                limit 1;""")

                results = cursor.fetchone()

        final_dict = {}
        final_dict["user_id"] = results[0]
        final_dict["count"] = result[1]

        return final_dict

    t1 = PythonOperator(task_id="pring_sql_result",
                        python_callable=most_likes_user)

    t1
