"""\
    #### Krasivya documentation
    **Krasotolya** (markdown),
    `doc` (plain text), _doc 55 55 55 rst_, `from datetime import datetime, timedelta`, ***doc_json***, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    #абзац сукка

    #ужас

    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    Mark *italic text* with one asterisk, **bold text** with two.
    For ``monospaced text``, use two "backquotes" instead
    ```text
    _БУУУя_
    # Тут могла быть функция

    """
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'blabla_6',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='harder DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    def print_context(ts, run_id, **kwargs):
        lg = kwargs["task_number"]
        print(ts)
        print(run_id)
        return print(lg)

    for i in range(10):
        t1 = BashOperator(
            task_id=f'buya_huya_{i}',
            bash_command="echo $NUMBER",
            env={"NUMBER": i},
        )

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_buya' + str(i),
            python_callable=print_context,
            op_kwargs={"lg": i},
            )

    t1 >> t2
