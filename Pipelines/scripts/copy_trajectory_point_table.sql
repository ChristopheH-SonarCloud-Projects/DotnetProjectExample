DELETE FROM bi_temp.trajectory_point
WHERE id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);
INSERT INTO bi_temp.trajectory_point (
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
    createdon,
    pipeline_id

)
SELECT
    id,
    the_geom,
    id_ref_campaign_fk,
    elevation,
    NULL::int,
    NULL::interval,
    time,
    speed,
    st_y(st_transform(the_geom, 4326)),
    st_x(st_transform(the_geom, 4326)),
    createdon,
    NULL::uuid

FROM campaign.trajectory_point
WHERE
    id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute) AND the_geom IS NOT NULL;

DROP INDEX IF EXISTS bi_temp.trajectory_point_the_geom;
CREATE INDEX trajectory_point_the_geom on bi_temp.trajectory_point using gist(the_geom);
