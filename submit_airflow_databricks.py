from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 


notebook_task = {
    'existing_cluster_id': '0516-223556-z0uc5wle',
    'notebook_path': '/Users/211531@corpaa.aa.com/test_sunny',
}


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('databricks_dag',
    start_date=datetime(2022, 6, 15),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
#         databricks_conn_id='aa-databricks-test',
        existing_cluster_id='0516-223556-z0uc5wle',
        notebook_task=notebook_task
    )

    opr_submit_run
