DELETE FROM bi_temp.trash;
INSERT INTO bi_temp.trash (
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
    createdon,
    pipeline_id
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
    st_y(st_transform(the_geom, 4326)),
    st_x(st_transform(the_geom, 4326)),
    createdon,
    NULL

FROM campaign.trash
WHERE id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);
DROP INDEX IF EXISTS bi_temp.trash_the_geom;
CREATE INDEX trash_the_geom on bi_temp.trash using gist(the_geom);