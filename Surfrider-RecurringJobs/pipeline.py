import datetime
import os
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

campaign_id = "test"
PWD = os.environ.get("PWD")
AIRFLOW_DAG_PATH = f"{PWD}"

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
    )

    # Get new ids to process from bi_temp
    insert_campaign_ids = PostgresOperator(
        task_id="task2",
        postgres_conn_id="SurfRiderDb",
        sql="init_insert_new_campaigns_id.sql",
    )

    # Copy data from campaign to bi_temp
    insert_campaign_data = PostgresOperator(
        task_id="task3",
        postgres_conn_id="SurfRiderDb",
        sql="0_insert_main_tables.sql",
    )

    # Compute indicators for each trajectory_point
    compute_trajectory_point = PostgresOperator(
        task_id="task4",
        postgres_conn_id="SurfRiderDb",
        sql="1_update_bi_temp_trajectory_point.sql",
    )
    
    # Compute indicators at campaign level
    compute_campaign = PostgresOperator(
        task_id="task5",
        postgres_conn_id="SurfRiderDb",
        sql="2_update_bi_temp_campaign.sql",
    )

    # Test that camp is inside country => DEPRECATED
    # test_campaign = PostgresOperator(
    #     task_id="task6",
    #     postgres_conn_id="SurfRiderDb",
    #     sql="2_update_bi_temp_campaign.sql",
    #     # sql=GET_CAMPAIGN_ID_QUERY_PATH, "other params"),
    # )   

    # Associate each trajectory_point to a river
    associate_trajectory_point = PostgresOperator(
        task_id="task7",
        postgres_conn_id="SurfRiderDb",
        sql="4_insert_bi_temp_trajectory_point_river.sql",
    )

    # Associate each campaign to a river
    associate_campaign = PostgresOperator(
        task_id="task8",
        postgres_conn_id="SurfRiderDb",
        sql="5_insert_bi_temp_campaign_river.sql",
    )

    # Compute trash indicators
    compute_trash = PostgresOperator(
        task_id="task9",
        postgres_conn_id="SurfRiderDb",
        sql="6_update_bi_temp_trash.sql",
    )

    # Insert trash indicators
    insert_trash = PostgresOperator(
        task_id="task10",
        postgres_conn_id="SurfRiderDb",
        sql="7_insert_bi_temp_trash_river.sql",
    )

    # Get old campaign id (to update with new data)
    get_old_campaign_ids = PostgresOperator(
        task_id="task11",
        postgres_conn_id="SurfRiderDb",
        sql="8_get_old_campaign_id.sql",
    )

    # Get old river id (to update with new data)
    get_old_river_ids = PostgresOperator(
        task_id="task12",
        postgres_conn_id="SurfRiderDb",
        sql="9_get_river_name.sql",
    )

    # Migrate data from bi to bi_temp (using ids gotten from tasks above)
    migrate_old_data_to_bi_temp = PostgresOperator(
        task_id="task13",
        postgres_conn_id="SurfRiderDb",
        sql="10_import_bi_table_to_bi_temp.sql",
    )

    # Compute updated indicators at river level (with old and new campaigns)
    compute_updated_river_indicators = PostgresOperator(
        task_id="task14",
        postgres_conn_id="SurfRiderDb",
        sql="11_update_bi_river.sql",
    )

    # Migrate data from bi_temp to bi (migrate updated data)
    migrate_new_data_to_bi = PostgresOperator(
        task_id="task15",
        postgres_conn_id="SurfRiderDb",
        sql="12_import_bi_temp_table_to_bi.sql",
    )

    # Delete data in bi_temp once completed
    delete_bi_temp_data = PostgresOperator(
        task_id="task16",
        postgres_conn_id="SurfRiderDb",
        sql="13_delete_from_bi_temp_table.sql",
    )

    # Log pipeline once finished, if successful
    log_pipeline_if_successful = PostgresOperator(
        task_id="task17",
        postgres_conn_id="SurfRiderDb",
        sql="logs_successful_pipeline.sql",
        # Task will be executed if all of the upstream tasks were successful
        trigger_rule="all_success",
    )

    # Log pipeline once finished, if failed
    log_pipeline_if_failed = PostgresOperator(
        task_id="task18",
        postgres_conn_id="SurfRiderDb",
        sql="logs_failed_pipeline.sql",
        # Task will be executed if one of the upstream tasks has failed
        trigger_rule="one_failed",
    )


    # Tasks orchestration
    get_campaign_ids >> insert_campaign_ids >> insert_campaign_data >> compute_trajectory_point >> compute_campaign >> associate_trajectory_point >> associate_campaign
    associate_campaign >> compute_trash >> insert_trash
    associate_campaign >> get_old_campaign_ids >> get_old_river_ids >> migrate_old_data_to_bi_temp >> compute_updated_river_indicators
    compute_updated_river_indicators >> migrate_new_data_to_bi
    insert_trash >> migrate_new_data_to_bi
    migrate_new_data_to_bi >> delete_bi_temp_data >> log_status_pipeline
