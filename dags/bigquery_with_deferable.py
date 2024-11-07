from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 11, 7),
}

with DAG('bigquery_deferrable_example',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False) as dag:

    create_table = BigQueryInsertJobOperator(
        task_id='create_table',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `ford-743794c14d5ab9bafaac1a08.dag_test_dataset.public_data_test` AS
                    SELECT * FROM `bigquery-public-data.github_repos.contents`
                """,
                "useLegacySql": False,
            }
        },
        location='US',
        deferrable=True
    )
