DELETE FROM bi_temp.trajectory_point_river;
INSERT INTO bi_temp.trajectory_point_river (

    id_ref_trajectory_point_fk,
    id_ref_campaign_fk,
    id_ref_river_fk,

    trajectory_point_the_geom,
    river_the_geom,
    projection_trajectory_point_river_the_geom,
    closest_point_the_geom,

    distance_river_trajectory_point,

    river_name,
    createdon


)

WITH subquery_1 AS (

    SELECT
        bi_temp.trajectory_point.id AS id_ref_trajectory_point_fk,
        bi_temp.trajectory_point.id_ref_campaign_fk AS id_ref_campaign_fk,

        closest_r.id AS id_ref_river_fk,
        bi_temp.trajectory_point.the_geom AS trajectory_point_the_geom,
        closest_r.the_geom AS river_the_geom,
        closest_r.name AS river_name,
        bi_temp.trajectory_point.pipeline_id,
        st_closestpoint(
            closest_r.the_geom, bi_temp.trajectory_point.the_geom
        ) AS closest_point_the_geom
    FROM
        bi_temp.trajectory_point

    INNER JOIN lateral(

            SELECT
                *
            FROM
                referential.river

            WHERE name IS NOT NULL
            ORDER BY referential.river.the_geom <-> bi_temp.trajectory_point.the_geom

            LIMIT 1

        ) AS closest_r ON TRUE


    WHERE
        bi_temp.trajectory_point.id_ref_campaign_fk IN (
            SELECT campaign_id FROM bi_temp.pipeline_to_compute
        )

)

SELECT
    id_ref_trajectory_point_fk,
    id_ref_campaign_fk,
    id_ref_river_fk,
    trajectory_point_the_geom,
    river_the_geom,
    closest_point_the_geom,
    st_makeline(
        trajectory_point_the_geom, closest_point_the_geom
    ) AS projection_trajectory_point_river_the_geom,
    st_distance(
        closest_point_the_geom, trajectory_point_the_geom
    ) AS distance_river_trajectory_point,
        river_name,

    current_timestamp


FROM
    subquery_1

WHERE st_distance(closest_point_the_geom, trajectory_point_the_geom) < 100;

DROP INDEX IF EXISTS bi_temp.trajectory_point_river_id_ref_trajectory_point_fk;

CREATE INDEX trajectory_point_river_id_ref_trajectory_point_fk
ON bi_temp.trajectory_point_river (id_ref_trajectory_point_fk);

DROP INDEX IF EXISTS bi_temp.trajectory_point_river_closest_point_the_geom;

CREATE INDEX trajectory_point_river_closest_point_the_geom
ON bi_temp.trajectory_point_river USING gist(closest_point_the_geom);
