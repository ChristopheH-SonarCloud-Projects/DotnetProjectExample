import datetime
import os
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv

load_dotenv()
env = os.environ["ENV"]
branch_name = os.environ["BRANCH"]

with DAG(

    dag_id= f'bi-reset-campaign-{env}',
    start_date=days_ago(1),
    schedule_interval=None,
    template_searchpath=f"/opt/airflow/dags/scripts/",
    catchup=False,
    tags=[f"env:{env}", f"branch-name:{branch_name}"]

) as dag:

    reset_campaign = PostgresOperator(
        task_id='reset_campaign',
        postgres_conn_id=f"SurfRiderDb_{env}_manager_user",
        sql='reset_campaign.sql',
        dag=dag
    )

    run_bi_pipeline = TriggerDagRunOperator(
        task_id='run_bi_pipeline',
        trigger_dag_id=f'bi-pipeline-{env}',
        wait_for_completion=True,
        dag=dag
    )
    reset_campaign >> run_bi_pipeline
