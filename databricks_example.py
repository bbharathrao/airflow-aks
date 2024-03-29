#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This is an example DAG which uses the DatabricksSubmitRunOperator.
In this example, we create two tasks which execute sequentially.
The first task is to run a notebook at the workspace path "/test"
and the second task is to run a JAR uploaded to DBFS. Both,
tasks use new clusters.

Because we have set a downstream dependency on the notebook task,
the spark jar task will NOT run until the notebook task completes
successfully.

The definition of a successful run is if the run has a result_state of "SUCCESS".
For more information about the state of a run refer to
https://docs.databricks.com/api/latest/jobs.html#runstate
"""

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'depends_on_past': False,
}

with DAG(
    dag_id='example_databricks_operator',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(2),
    tags=['example'],
) as dag:
  
#     notebook_task_params = {
#         'existing_cluster_id':'0516-223556-z0uc5wle',
#         'notebook_task': {
#             'notebook_path': '/Users/211531@corpaa.aa.com/test_sunny',
#         },
#     }
    # [START howto_operator_databricks_json]
    # Example of using the JSON parameter to initialize the operator.
#     notebook_task = DatabricksSubmitRunOperator(task_id='notebook_task', json=notebook_task_params)
    # [END howto_operator_databricks_json]

    # [START howto_operator_databricks_named]
    # Example of using the named parameters of DatabricksSubmitRunOperator
    # to initialize the operator.
#     spark_jar_task = DatabricksSubmitRunOperator(
#         task_id='spark_jar_task',
#         existing_cluster_id='0516-223556-z0uc5wle',
# #         spark_jar_task={'main_class_name': 'com.example.ProcessData'},
#         libraries=[{'jar': 'dbfs:/FileStore/jars/48af2dc8_a438_42b8_964e_ac132b4bfb96-terajdbc4.jar'}],
#     )
#     # [END howto_operator_databricks_named]
# #     notebook_task >> spark_jar_task
#     spark_jar_task
    job_id=925227695686083

    notebook_params = {
        "dry-run": "true",
        "oldest-time-to-consider": "1457570074236"
    }

    python_params = ["douglas adams", "42"]

    jar_params = ["douglas adams", "42"]

    spark_submit_params = ["--class", "org.apache.spark.examples.SparkPi"]

    notebook_run = DatabricksRunNowOperator(
        task_id='notebook_run_task',
        job_id=job_id,
        notebook_params=notebook_params,
        python_params=python_params,
        jar_params=jar_params,
        spark_submit_params=spark_submit_params
    )

    notebook_run_2 = DatabricksRunNowOperator(
        task_id='notebook_run_task_2',
        job_id=638465510945648,
        notebook_params=notebook_params,
        python_params=python_params,
        jar_params=jar_params,
        spark_submit_params=spark_submit_params
    )
    
    notebook_run >> notebook_run_2
