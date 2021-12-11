DROP INDEX IF EXISTS  bi_temp.bi_temp_trash_id_ref_campaign_fk;
CREATE INDEX bi_temp_trash_id_ref_campaign_fk on bi_temp.trash (id_ref_campaign_fk);

DELETE FROM bi_temp.trash_river;
INSERT INTO bi_temp.trash_river (

    id_ref_trash_fk,
    id_ref_campaign_fk,
    id_ref_river_fk,
    trash_the_geom,
    importance,
    river_name,
    createdon,
    pipeline_id,
    river_the_geom,
    closest_point_the_geom,
    distance_river_trash,
    projection_trash_river_the_geom

)
SELECT
        bi_temp.trash.id AS id_ref_trash_fk,
        bi_temp.trash.id_ref_campaign_fk AS id_ref_campaign_fk,
        referential.river.id AS id_ref_river_fk,
        bi_temp.trash.the_geom AS trash_the_geom,
        referential.river.importance,
        referential.river.name AS river_name,
        current_timestamp,
        bi_temp.trash.pipeline_id,
        ST_POINT(1,1) as river_the_geom,
        ST_POINT(1,1) as closest_point_the_geom,
        0 as distance_river_trash,
        ST_POINT(1,1) as projection_trash_river_the_geom

FROM
	bi_temp.trash
LEFT JOIN bi_temp.campaign_river  on bi_temp.campaign_river.id_ref_campaign_fk  =  bi_temp.trash.id_ref_campaign_fk
INNER JOIN referential.river  on referential.river.id = bi_temp.campaign_river.id_ref_river_fk

WHERE
        bi_temp.trash.id_ref_campaign_fk IN (
            SELECT campaign_id FROM bi_temp.pipeline_to_compute
        )
;

DROP INDEX IF EXISTS bi_temp.trash_river_id_ref_trash_fk;
CREATE INDEX trash_river_id_ref_trash_fk
ON bi_temp.trash_river (id_ref_trash_fk);
DROP INDEX IF EXISTS bi_temp.trash_river_closest_point_the_geom;
CREATE INDEX trash_river_closest_point_the_geom
ON bi_temp.trash_river USING gist(closest_point_the_geom);
