from psycopg2.extras import RealDictCursor
import psycopg2

# database="startml",
# user="robot-startml-ro",
# password="pheiph0hahj1Vaif",
# host="postgres.lab.karpov.courses",
# port=6432



conn = psycopg2.connect(
    "postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml",
    cursor_factory=RealDictCursor
    )
cursor = conn.cursor()
cursor.execute(
    """
    SELECT interim_table.user_id, interim_table.like_count
    FROM(
        SELECT user_id AS user_id, COUNT(action) AS count
        FROM feed_action
        WHERE action = 'like'
        GROUP BY user_id
    ) AS interim_table
    ORDER BY interim_table.like_count DESC
    LIMIT 1
    """
)
result = dict(cursor.fetchone())
print(result)
cursor.close()
conn.close()