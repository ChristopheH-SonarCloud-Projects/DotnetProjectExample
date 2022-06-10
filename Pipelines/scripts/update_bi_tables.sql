DROP TABLE IF EXISTS river_name;
CREATE TEMP TABLE river_name AS
SELECT bi_temp.campaign_river.river_name
FROM bi_temp.campaign_river
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);

DELETE FROM
    bi.campaign
WHERE id IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);
INSERT INTO bi.campaign (
    id,
    locomotion,
    isaidriven,
    remark,
    id_ref_user_fk,
    riverside,
    start_date,
    end_date,
    start_point,
    end_point,
    total_distance,
    distance_on_river,
    avg_speed,
    duration,
    trash_count,
    trash_per_km,
    trash_per_km_on_river,
    id_ref_model_fk,
    createdon
)
SELECT
    id,
    locomotion,
    isaidriven,
    remark,
    id_ref_user_fk,
    riverside,
    start_date,
    end_date,
    start_point,
    end_point,
    total_distance,
    distance_on_river,
    avg_speed,
    duration,
    trash_count,
    trash_per_km,
    trash_per_km_on_river,
    id_ref_model_fk,
    createdon
FROM bi_temp.campaign
WHERE id IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);

DELETE FROM
    bi.campaign_river
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);
INSERT INTO bi.campaign_river (
    id,
    id_ref_campaign_fk,
    river_name,
    id_ref_river_fk,
    distance,
    the_geom,
    the_geom_raw,
    createdon
)
SELECT
    id,
    id_ref_campaign_fk,
    river_name,
    id_ref_river_fk,
    distance,
    the_geom,
    the_geom_raw,
    createdon
FROM bi_temp.campaign_river
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute)
ORDER BY id_ref_campaign_fk ASC, river_name, distance DESC;

DELETE FROM
    bi.trajectory_point
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);
INSERT INTO bi.trajectory_point (
    id,
    the_geom,
    id_ref_campaign_fk,
    elevation,
    distance,
    time_diff,
    time,
    speed,
    lat,
    lon,
    createdon
)
SELECT
    id,
    the_geom,
    id_ref_campaign_fk,
    elevation,
    distance,
    time_diff,
    time,
    speed,
    lat,
    lon,
    createdon
FROM bi_temp.trajectory_point
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);

-- QUERY 4: migration for table trajectory_point_river
DELETE FROM
    bi.trajectory_point_river
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);
INSERT INTO bi.trajectory_point_river (
    id,
    id_ref_trajectory_point_fk,
    id_ref_campaign_fk,
    id_ref_river_fk,
    trajectory_point_the_geom,
    river_the_geom,
    closest_point_the_geom,
    importance,
    river_name,
    createdon
)
SELECT
    id,
    id_ref_trajectory_point_fk,
    id_ref_campaign_fk,
    id_ref_river_fk,
    trajectory_point_the_geom,
    river_the_geom,
    closest_point_the_geom,
    importance,
    river_name,
    createdon
FROM bi_temp.trajectory_point_river
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);

DELETE FROM
    bi.trash
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);
INSERT INTO bi.trash (
    id,
    id_ref_campaign_fk,
    the_geom,
    elevation,
    id_ref_trash_type_fk,
    precision,
    id_ref_model_fk,
    time,
    lat,
    lon,
    municipality_code,
    municipality_name,
    department_code,
    department_name,
    state_code,
    state_name,
    country_code,
    country_name,
    createdon
)
SELECT
    id,

    id_ref_campaign_fk,
    the_geom,
    elevation,
    id_ref_trash_type_fk,
    precision,
    id_ref_model_fk,
    time,
    lat,
    lon,
    municipality_code,
    municipality_name,
    department_code,
    department_name,
    state_code,
    state_name,
    country_code,
    country_name,
    createdon
FROM bi_temp.trash
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);

-- QUERY 6: migration for table trash_river
DELETE FROM
    bi.trash_river
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);
INSERT INTO bi.trash_river (
    id,
    id_ref_trash_fk,
    id_ref_campaign_fk,
    id_ref_river_fk,
    trash_the_geom,
    river_the_geom,
    closest_point_the_geom,
    distance_river_trash,
    importance,
    river_name,
    createdon
)
SELECT
    id,

    id_ref_trash_fk,
    id_ref_campaign_fk,
    id_ref_river_fk,
    trash_the_geom,
    river_the_geom,
    closest_point_the_geom,
    distance_river_trash,
    importance,
    river_name,
    createdon
FROM bi_temp.trash_river
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);

-- QUERY 7 : Migration for table river
DELETE FROM bi.river
WHERE name IN  (SELECT * from river_name);

INSERT INTO bi.river (
    name,
    the_geom,
    length,
    count_trash,
    distance_monitored,
    the_geom_monitored,
    trash_per_km
)
SELECT
    name,
    the_geom,
    length,
    count_trash,
    distance_monitored,
    the_geom_monitored,
    trash_per_km
FROM bi_temp.river
WHERE name IN  (SELECT * from river_name);
