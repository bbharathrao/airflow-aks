from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'subject area 1 work flow 2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Subject Area 1 DAG Work Flow 2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = BashOperator(
        task_id='check_for_source_files',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='source schema check',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    t3 = BashOperator(
        task_id='get note book details',
        depends_on_past=False,
        bash_command='sleep 5',
    )

    t4 = BashOperator(
        task_id='trigger note book (Transform 1)',
        bash_command='date',
    )

    t5 = BashOperator(
        task_id='trigger note book (Transform 2)',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    t6 = BashOperator(
        task_id='Email Status Notification',
        depends_on_past=False,
        bash_command=sleep 5,
    )


    t1 >> t2 >> t3 >> t4 >> t5 >> t6
