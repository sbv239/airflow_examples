from airflow.providers.postgres.operators.postgres import PostgresHook


def get_connection():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
      with conn.cursor() as cursor:
        cursor.execute(
            f"""Select user_id,
            count(action) as count
            from feed_action
            where action = 'like'
            order by count desc""")
        result = cursor.fetchone()
        return result

