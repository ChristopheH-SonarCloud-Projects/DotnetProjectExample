DELETE FROM
    bi_temp.campaign_river
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);
INSERT INTO bi_temp.campaign_river (

    id_ref_campaign_fk,
    the_geom,
    distance,
    river_name,
    pipeline_id

)
WITH subquery_1 AS (

    SELECT

        bi_temp.trajectory_point.id_ref_campaign_fk,
        river_name,
        tr.pipeline_id,
        st_makevalid(
            st_makeline(
                projection_trajectory_point_river_the_geom ORDER BY

                    bi_temp.trajectory_point.time
            )
        ) AS the_geom,
        st_union(river_the_geom) AS river_the_geom

    FROM
        (
            SELECT
            row_number() over(partition by id_ref_campaign_fk order by random()) as random_number,
            *

            FROM bi_temp.trajectory_point_river
            WHERE
                id_ref_campaign_fk IN (
                    SELECT campaign_id FROM bi_temp.pipeline_to_compute
                )
            AND 1 < 1000
        ) AS tr

    INNER JOIN
        bi_temp.trajectory_point ON
            bi_temp.trajectory_point.id = tr.id_ref_trajectory_point_fk
    GROUP BY
        bi_temp.trajectory_point.id_ref_campaign_fk,
        river_name,
        tr.pipeline_id

)

SELECT DISTINCT ON (id_ref_campaign_fk)
    id_ref_campaign_fk,
    the_geom,
    st_length(the_geom) AS distance,
    river_name,
    pipeline_id

FROM
    subquery_1

ORDER BY id_ref_campaign_fk, st_length(the_geom) DESC;
