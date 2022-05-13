from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'subject-area-1-work-flow-1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Subject Area 1 DAG Work Flow 1',
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
        task_id='source_schema_check',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    t3 = BashOperator(
        task_id='get_note_book_details',
        depends_on_past=False,
        bash_command='sleep 5',
    )

    t4 = BashOperator(
        task_id='trigger_note_book_Transform_1',
        bash_command='date',
    )

    t5 = BashOperator(
        task_id='trigger_note_book_Transform_2',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    t6 = BashOperator(
        task_id='check_target_schema',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    t7 = BashOperator(
        task_id='Email_Status_Notification',
        depends_on_past=False,
        bash_command='sleep 5',
    )


    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7