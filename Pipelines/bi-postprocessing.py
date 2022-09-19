import datetime
import os
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv

load_dotenv()
env = os.environ["ENV"]
branch_name = os.environ["BRANCH"]

with DAG(

    dag_id= f'bi-postprocessing-{env}',
    start_date=days_ago(1),
    schedule_interval=None,
    template_searchpath=f"/opt/airflow/dags/{env}/scripts/",
    catchup=False,
    tags=[f"env:{env}", f"branch-name:{branch_name}"]

) as dag:

    update_feature_collection_columns = PostgresOperator(
        task_id='update_feature_collection_columns',
        postgres_conn_id=f"SurfRiderDb_{env}_manager_user",
        sql='update_feature_collection_columns.sql',
        dag=dag
    )

