DROP TABLE IF EXISTS river_name;
CREATE TEMP TABLE river_name AS
SELECT bi_temp.campaign_river.river_name
FROM bi_temp.campaign_river
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);

DROP TABLE IF EXISTS old_campaign_ids;

CREATE TEMP TABLE old_campaign_ids AS
SELECT bi.campaign_river.id_ref_campaign_fk
FROM bi.campaign_river
INNER JOIN
    bi_temp.campaign_river ON

        bi_temp.campaign_river.river_name = bi.campaign_river.river_name
INNER JOIN
    bi_temp.pipelines ON

        bi_temp.pipelines.campaign_id =
        bi_temp.campaign_river.id_ref_campaign_fk AND
        bi_temp.pipelines.campaign_has_been_computed = TRUE
WHERE

    bi_temp.campaign_river.id_ref_campaign_fk IN (
        SELECT campaign_id FROM bi_temp.pipeline_to_compute
    );

-- QUERY 1: updates distance monitored and the geom monitored
UPDATE bi_temp.river
SET
    distance_monitored = st_length(
        st_intersection(r2.the_geom_monitored, river.the_geom)
    ),
    the_geom_monitored = r2.the_geom_monitored
FROM
    (
        SELECT
            bi_temp.campaign_river.river_name,
            st_union(
                st_buffer(bi_temp.campaign_river.the_geom, 0.01)
            ) AS the_geom_monitored
        FROM
            bi_temp.campaign_river

        INNER JOIN bi.river ON bi.river.name = bi_temp.campaign_river.river_name
        WHERE bi.river.name IN (SELECT river_name FROM river_name)
        GROUP BY bi_temp.campaign_river.river_name

    ) AS r2
WHERE r2.river_name = river.name;

UPDATE bi_temp.river
SET count_trash = t.count_trash,
    trash_per_km = t.count_trash / (nullif(distance_monitored, 0) / 1000)
FROM
    (

        SELECT
            bi.trash_river.river_name,
            count(distinct(bi.trash_river.id_ref_trash_fk)) AS count_trash
        FROM
            bi.trash_river

        INNER JOIN bi.river ON bi.river.name = bi.trash_river.river_name
        WHERE bi.river.name IN (SELECT river_name FROM river_name)
        GROUP BY bi.trash_river.river_name

    ) AS t
WHERE t.river_name = river.name;
