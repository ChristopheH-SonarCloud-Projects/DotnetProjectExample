DELETE FROM
    bi_temp.campaign_river
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);
INSERT INTO bi_temp.campaign_river (

    id_ref_campaign_fk,
    the_geom,
    distance,
    river_name,
    the_geom_raw,
    pipeline_id

)
WITH subquery_1 AS (

    SELECT
        tp.id_ref_campaign_fk,
        tp_river.river_name as river_name,
        tp_river.pipeline_id,
        ST_Simplify(st_makevalid(
            st_makeline(
                tp_river.closest_point_the_geom
                ORDER BY tp.time
            )
        ), 1, true) AS the_geom,
        ST_Simplify(st_makevalid(
            st_makeline(
                tp_river.trajectory_point_the_geom
                ORDER BY tp.time
            )
        ), 1, true) AS the_geom_raw
    FROM
        (
            SELECT
                id_ref_trajectory_point_fk,
                river_name,
                id_ref_river_fk,
                trajectory_point_the_geom,
                closest_point_the_geom,
                pipeline_id
            FROM bi_temp.trajectory_point_river
            WHERE id_ref_campaign_fk IN (
                    SELECT campaign_id FROM bi_temp.pipeline_to_compute
                )
            AND 1 < 1000
        ) AS tp_river

    INNER JOIN
        bi_temp.trajectory_point tp ON
            tp.id = tp_river.id_ref_trajectory_point_fk
    GROUP BY
        tp.id_ref_campaign_fk,
        tp_river.river_name,
        tp_river.pipeline_id

)

SELECT DISTINCT ON (id_ref_campaign_fk, river_name)
    id_ref_campaign_fk,
    the_geom,
    st_length(the_geom) AS distance,
    river_name,
    the_geom_raw,
    pipeline_id

FROM
    subquery_1

ORDER BY id_ref_campaign_fk, river_name, st_length(the_geom) DESC;