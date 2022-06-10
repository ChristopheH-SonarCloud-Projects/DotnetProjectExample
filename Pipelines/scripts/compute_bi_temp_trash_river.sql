DROP INDEX IF EXISTS  bi_temp.bi_temp_trash_id_ref_campaign_fk;
CREATE INDEX bi_temp_trash_id_ref_campaign_fk on bi_temp.trash (id_ref_campaign_fk);

DELETE FROM bi_temp.trash_river;
INSERT INTO bi_temp.trash_river (
    id_ref_trash_fk,
    id_ref_campaign_fk,
    trash_the_geom,
    importance,
    id_ref_river_fk,
    river_name,
    createdon,
    pipeline_id,
    river_the_geom,
    closest_point_the_geom,
    distance_river_trash
)
WITH subquery AS (
    SELECT
            t.id AS id_ref_trash_fk,
            t.id_ref_campaign_fk AS id_ref_campaign_fk,
            closest_r.id AS id_ref_river_fk,
            t.the_geom AS trash_the_geom,
            closest_r.the_geom AS river_the_geom,
            closest_r.importance,
            closest_r.name AS river_name,
            t.pipeline_id,
            st_closestpoint(closest_r.the_geom, t.the_geom) AS closest_point_the_geom
    FROM bi_temp.trash t
    INNER JOIN lateral(
            SELECT r.id, r.name, r.importance, r.the_geom
            FROM referential.river r
            WHERE r.name IS NOT NULL
            ORDER BY r.the_geom <-> t.the_geom
            LIMIT 1
        ) AS closest_r ON TRUE
    WHERE t.id_ref_campaign_fk IN (
            SELECT campaign_id FROM bi_temp.pipeline_to_compute
          )
   )
SELECT
    id_ref_trash_fk,
    id_ref_campaign_fk,
    trash_the_geom,
    importance,
    id_ref_river_fk,
    river_name,
    current_timestamp,
    pipeline_id,
    river_the_geom,
    closest_point_the_geom,
    st_distance(
        closest_point_the_geom, trash_the_geom
    ) AS distance_river_trash
FROM subquery
WHERE st_distance(closest_point_the_geom, trash_the_geom) < 500;

DROP INDEX IF EXISTS bi_temp.trash_river_id_ref_trash_fk;
CREATE INDEX trash_river_id_ref_trash_fk
ON bi_temp.trash_river (id_ref_trash_fk);
DROP INDEX IF EXISTS bi_temp.trash_river_closest_point_the_geom;
CREATE INDEX trash_river_closest_point_the_geom
ON bi_temp.trash_river USING gist(closest_point_the_geom);
