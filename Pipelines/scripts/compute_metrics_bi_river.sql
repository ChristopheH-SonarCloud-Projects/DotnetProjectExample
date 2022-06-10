DROP TABLE IF EXISTS river_name;
CREATE TEMP TABLE river_name AS
SELECT bi_temp.campaign_river.river_name
FROM bi_temp.campaign_river
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);

DROP TABLE IF EXISTS old_campaign_ids;

CREATE TEMP TABLE old_campaign_ids AS
SELECT cr.id_ref_campaign_fk
FROM bi.campaign_river cr
    INNER JOIN bi_temp.campaign_river cr_temp ON cr_temp.river_name = cr.river_name
    INNER JOIN bi_temp.pipelines p
        ON p.campaign_id = cr_temp.id_ref_campaign_fk
        AND p.campaign_has_been_computed = TRUE
WHERE
    cr_temp.id_ref_campaign_fk IN (
        SELECT campaign_id FROM bi_temp.pipeline_to_compute
    );

DELETE FROM bi_temp.river;
INSERT INTO bi_temp.river(name, the_geom, length)
SELECT
    name,
    st_union(the_geom),
    st_length(st_union(the_geom))
FROM referential.river
WHERE name in (SELECT * from river_name)
GROUP BY name;

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
            cr.river_name,
            st_union(
                st_buffer(cr.the_geom, 200)
            ) AS the_geom_monitored
        FROM bi_temp.campaign_river cr
            INNER JOIN bi_temp.river r ON r.name = cr.river_name
        WHERE r.name IN (SELECT river_name FROM river_name)
        GROUP BY cr.river_name

    ) AS r2
WHERE r2.river_name = river.name;

UPDATE bi_temp.river
SET count_trash = t.count_trash,
    trash_per_km = t.count_trash / (nullif(distance_monitored, 0) / 1000)
FROM
    (
        SELECT
            tr.river_name,
            count(distinct(tr.id_ref_trash_fk)) AS count_trash
        FROM bi_temp.trash_river tr
            INNER JOIN bi_temp.river r ON r.name = tr.river_name
        WHERE r.name IN (SELECT river_name FROM river_name)
        GROUP BY tr.river_name

    ) AS t
WHERE t.river_name = river.name;
