from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 11, 7),
}

with DAG('test_loop',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False) as dag:

    create_table = BigQueryInsertJobOperator(
        task_id='create_table',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `ford-743794c14d5ab9bafaac1a08.dag_test_dataset.sample_repos` AS
                    SELECT * FROM `bigquery-public-data.github_repos.sample_repos`
                """,
                "useLegacySql": False,
            }
        },
        location='US'
    )

    for task in range(5):
        tn = BigQueryInsertJobOperator(
              task_id='update_table_{task}',
              configuration={
                  "query": {
                      "query": """
                          INSERT INTO `ford-743794c14d5ab9bafaac1a08.dag_test_dataset.sample_repos`
                          SELECT * FROM `bigquery-public-data.github_repos.sample_repos` LIMIT 1000
                      """,
                      "useLegacySql": False,
                  }
              },
              location='US'
          )
