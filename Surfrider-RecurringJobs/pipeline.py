import datetime
import os
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

from dag_data.settings import *
from dag_data.utils import (
    format_sql_query,
)

campaign_id = "test"
PWD = os.environ.get("PWD")
AIRFLOW_DAG_PATH = f"{PWD}"

# parameters = {
#             "campaign_id": "b843c94c-256c-4a4c-877e-2e928f448d67",
#             "pipeline_id": "b843c94c-256c-4a4c-877e-2e928f448d67"
#             }
with DAG(

    dag_id="main-pipeline",
    start_date=datetime.datetime(2021, 1, 1),
    template_searchpath=f"./SqlScripts/",
    catchup=False,

) as dag:

    # Insert new ids in bi_temp
    get_campaign_ids = PostgresOperator(
        task_id="task1",
        postgres_conn_id="SurfRiderDb",
        sql="init_get_new_campaigns_id.sql",
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH),
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    )

    # Get new ids to process from bi_temp
    insert_campaign_ids = PostgresOperator(
        task_id="task2",
        postgres_conn_id="SurfRiderDb",
        sql="init_insert_new_campaigns_id.sql",
        # sql=format_sql_query(INSERT_CAMPAIGN_ID_QUERY_PATH),
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    )



    # Donc là au lieu de trigger on enchaîne :
    # TODO rename
    # TODO format_sql (pass params & co)
    # Copy data from campaign to bi_temp
    insert_campaign_data = PostgresOperator(
        task_id="task3",
        postgres_conn_id="SurfRiderDb",
        sql=format_sql_query("0_insert_main_tables.sql"),
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    )

    # Compute indicators for each trajectory_point
    compute_trajectory_point = PostgresOperator(
        task_id="task4",
        postgres_conn_id="SurfRiderDb",
        sql=format_sql_query("1_update_bi_temp_trajectory_point.sql"),
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    )
    
    # Compute indicators at campaign level
    compute_campaign = PostgresOperator(
        task_id="task5",
        postgres_conn_id="SurfRiderDb",
        sql=format_sql_query("2_update_bi_temp_campaign.sql"),
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    )    

    # Test that camp is inside country => DEPRECATED
    # test_campaign = PostgresOperator(
    #     task_id="task6",
    #     postgres_conn_id="SurfRiderDb",
    #     sql=format_sql_query("2_update_bi_temp_campaign.sql"),
    #     # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    # )    

    # Associate each trajectory_point to a river
    associate_trajectory_point = PostgresOperator(
        task_id="task7",
        postgres_conn_id="SurfRiderDb",
        sql=format_sql_query("4_insert_bi_temp_trajectory_point_river.sql"),
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    )    



    # Associate each campaign to a river
    associate_campaign = PostgresOperator(
        task_id="task8",
        postgres_conn_id="SurfRiderDb",
        sql=format_sql_query("5_insert_bi_temp_campaign_river.sql"),
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    )    

    # //SABLE
    # Compute trash indicators
    compute_trash = PostgresOperator(
        task_id="task9",
        postgres_conn_id="SurfRiderDb",
        sql=format_sql_query("6_update_bi_temp_trash.sql"),
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    )    

    # //SABLE
    # Insert trash indicators
    insert_trash = PostgresOperator(
        task_id="task10",
        postgres_conn_id="SurfRiderDb",
        sql=format_sql_query("7_insert_bi_temp_trash_river.sql"),
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    )    

    # Get old campaign id (to update with new data)
    # Q => comment update sur les "campagnes" ?
    # = trash ?
    get_old_campaign_ids = PostgresOperator(
        task_id="task11",
        postgres_conn_id="SurfRiderDb",
        sql=format_sql_query("8_get_old_campaign_id.sql"),
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    )    

    # Get old river id (to update with new data)
    get_old_river_ids = PostgresOperator(
        task_id="task12",
        postgres_conn_id="SurfRiderDb",
        sql=format_sql_query("9_get_river_name.sql"),
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    )    

    # Migrate data from bi to bi_temp (using ids gotten from tasks above)
    # NB là ya des params à passer (pas d'insert)
    migrate_old_data_to_bi_temp = PostgresOperator(
        task_id="task13",
        postgres_conn_id="SurfRiderDb",
        sql=format_sql_query("10_import_bi_table_to_bi_temp.sql"),
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    )        



    # Compute updated indicators at river level (with old and new campaigns)
    compute_updated_river_indicators = PostgresOperator(
        task_id="task14",
        postgres_conn_id="SurfRiderDb",
        sql=format_sql_query("11_update_bi_river.sql"),
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    )    

    # Migrate data from bi_temp to bi (migrate updated data)
    migrate_new_data_to_bi = PostgresOperator(
        task_id="task15",
        postgres_conn_id="SurfRiderDb",
        sql=format_sql_query("12_import_bi_temp_table_to_bi.sql"),
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    )   

    # Delete data in bi_temp once completed
    delete_bi_temp_data = PostgresOperator(
        task_id="task16",
        postgres_conn_id="SurfRiderDb",
        sql=format_sql_query("13_delete_from_bi_temp_table.sql"),
        # sql=format_sql_query(GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    )           

    # WRITE LOGS once finished 
    log_status_pipeline = PostgresOperator(
        task_id="task17",
        postgres_conn_id="SurfRiderDb",
        sql="logs_status_pipeline.sql",
    )


    # Tasks orchestration
    get_campaign_ids >> insert_campaign_ids >> insert_campaign_data >> compute_trajectory_point >> compute_campaign >> associate_trajectory_point >> associate_campaign
    associate_campaign >> compute_trash >> insert_trash
    associate_campaign >> get_old_campaign_ids >> get_old_river_ids >> migrate_old_data_to_bi_temp >> compute_updated_river_indicators 
    compute_updated_river_indicators >> migrate_new_data_to_bi >> delete_bi_temp_data >> log_status_pipeline
    insert_trash >> migrate_new_data_to_bi >> delete_bi_temp_data >> log_status_pipeline