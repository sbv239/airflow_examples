from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
#from airflow import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook

default_args={
    'start_date': datetime(2022, 11, 18),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(is_startml)
    return is_startml

def connect_get():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")

    with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute("""SELECT user_id, COUNT(action) as count FROM "feed_action"
                       WHERE "action"='like'
                       GROUP BY user_id
                       ORDER BY count DESC
                       LIMIT 1
                       """)
            result = cursor.fetchone()
            return result

def push_var(ti):
    var = ti.xcom_push(key="sample_xcom_key", value="Airflow tracks everything")
    return var

def pull_var(ti):
    var = ti.xcom_pull(task_ids="push", key="return_value")
    print(var)

with DAG(
    "hw_11_v-jasnitskij_dag",
    default_args=default_args,
    schedule_interval=None
    ) as dag:
        python_push = PythonOperator(
            task_id="is_startml",
            python_callable=get_variable,
            dag=dag
        )

   # python_push >> python_pull