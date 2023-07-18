"""
DAG docs:
# Heading level 1
## Heading level 2
...
###### Heading level 6
**bold text**
*italic text*
***bold and italic***
> blockquotes

`some code`

---

> #### The quarterly results look great!
>
> - Revenue was off the chart.
> - Profits were higher than ever.
>
>  *Everything* is going according to **plan**.

1. item1
2. item2
3. item3
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator


default_args={
    'depends_on_past': False,
    'owner': 'and-f',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='hw_5_and-f',
         default_args=default_args,
         description='--DAG description here--',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 7, 17),
         catchup=False,
         tags=['--DAG tag here--']) as dag:

    dag.doc_md = __doc__

    templated_command = dedent("""
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}" 
    {% endfor %}
    """)

    t1 = BashOperator(task_id=f'temp_task', bash_command=templated_command)
    t1.doc_md = dedent("---\n#### Task Title  \n**bold text**  \n*italic text*  \n`some code`\n---")
