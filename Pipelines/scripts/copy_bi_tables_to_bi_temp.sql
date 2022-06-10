DROP TABLE IF EXISTS river_name;
CREATE TEMP TABLE river_name AS
SELECT bi_temp.campaign_river.river_name
FROM bi_temp.campaign_river;
DROP TABLE IF EXISTS old_campaign_ids;

CREATE TEMP TABLE old_campaign_ids AS
SELECT cr.id_ref_campaign_fk
FROM bi.campaign_river cr
WHERE id_ref_campaign_fk NOT in (SELECT campaign_id FROM bi_temp.pipeline_to_compute)
;

DELETE FROM
    bi_temp.campaign_river
WHERE id_ref_campaign_fk IN (SELECT id_ref_campaign_fk FROM old_campaign_ids);

INSERT INTO bi_temp.campaign_river (
    id,
    id_ref_campaign_fk,
    river_name,
    distance,
    the_geom,
    createdon,
    pipeline_id
)
SELECT

    bi.campaign_river.id,
    bi.campaign_river.id_ref_campaign_fk,
    bi.campaign_river.river_name,
    bi.campaign_river.distance,
    bi.campaign_river.the_geom,
    bi.campaign_river.createdon,
    NULL::uuid
FROM bi.campaign_river
WHERE
    bi.campaign_river.id_ref_campaign_fk IN (
        SELECT id_ref_campaign_fk
    ) AND id_ref_campaign_fk <> bi.campaign_river.id_ref_campaign_fk;

DELETE FROM
    bi_temp.trash_river
WHERE id_ref_campaign_fk IN (SELECT id_ref_campaign_fk FROM old_campaign_ids);
INSERT INTO bi_temp.trash_river (
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
    createdon,
    pipeline_id
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
    createdon,
    NULL::uuid
FROM bi.trash_river
WHERE
    bi.trash_river.id_ref_campaign_fk IN (
        SELECT id_ref_campaign_fk FROM old_campaign_ids
    );

DELETE FROM bi_temp.river
WHERE name IN (SELECT DISTINCT river_name FROM bi_temp.campaign_river);

INSERT INTO bi_temp.river
SELECT
    *
FROM bi.river
WHERE name IN (SELECT DISTINCT river_name FROM bi_temp.campaign_river);

DROP INDEX IF EXISTS bi_temp.bi_temp_river_name;
CREATE INDEX bi_temp_river_name ON bi_temp.river (name);
