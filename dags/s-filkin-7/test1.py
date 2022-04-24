import psycopg2
from psycopg2.extras import RealDictCursor

def func1():
    conn = psycopg2.connect("postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml")

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
                """
        SELECT
            f.user_id,
            COUNT(f.user_id) as count
        FROM feed_action f
        WHERE f.action = 'like'
        GROUP BY f.user_id
        ORDER BY COUNT(f.user_id) DESC
        LIMIT 1
        """
                )
        return cursor.fetchone()

print(func1())