DROP INDEX IF EXISTS bi_temp.bi_temp_campaign_id;
CREATE INDEX bi_temp_campaign_id
ON bi_temp.campaign (id);

UPDATE bi_temp.campaign c
SET start_date = point.min_time,
    end_date = point.max_time,
    duration = age(point.max_time, point.min_time)

FROM (
        SELECT

            id_ref_campaign_fk,
            min(time) AS min_time,
            max(time) AS max_time

        FROM campaign.trajectory_point
        GROUP BY id_ref_campaign_fk

    ) AS point

WHERE
    point.id_ref_campaign_fk = c.id AND c.id IN (
        SELECT campaign_id FROM bi_temp.pipeline_to_compute
    );

UPDATE bi_temp.campaign c
SET start_point = start_geom.the_geom
FROM bi_temp.trajectory_point AS start_geom
WHERE
    start_geom.id_ref_campaign_fk = c.id AND start_geom.time = c.start_date AND c.id IN (
        SELECT campaign_id FROM bi_temp.pipeline_to_compute
    );

UPDATE bi_temp.campaign c
SET end_point = end_geom.the_geom
FROM bi_temp.trajectory_point AS end_geom
WHERE
    end_geom.id_ref_campaign_fk = c.id AND end_geom.time = c.end_date AND c.id IN (
        SELECT campaign_id FROM bi_temp.pipeline_to_compute
    );

UPDATE bi_temp.campaign c
SET distance_start_end = st_distance(start_point, end_point)
WHERE c.id IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);

UPDATE bi_temp.campaign c
SET total_distance = agg.total_distance,
    avg_speed = agg.avg_speed
FROM (
    SELECT
        id_ref_campaign_fk,
        sum(distance) AS total_distance,
        avg(speed) AS avg_speed
    FROM bi_temp.trajectory_point
    WHERE distance > 0
    GROUP BY id_ref_campaign_fk
    ) AS agg
WHERE
    agg.id_ref_campaign_fk = c.id AND c.id IN (
        SELECT campaign_id FROM bi_temp.pipeline_to_compute
    );


UPDATE bi_temp.campaign c
SET trash_count = trash_n.trash_count,
    createdon = current_timestamp
FROM (
        SELECT
            id_ref_campaign_fk,
            count(*) AS trash_count
        FROM bi_temp.trash
        GROUP BY id_ref_campaign_fk

    ) AS trash_n

WHERE
    trash_n.id_ref_campaign_fk = c.id AND c.id IN (
        SELECT campaign_id FROM bi_temp.pipeline_to_compute
    );

DROP INDEX IF EXISTS bi_temp.campaign_start_point;
CREATE INDEX campaign_start_point ON bi_temp.campaign USING gist(start_point);

DROP INDEX IF EXISTS bi_temp.campaign_end_point;
CREATE INDEX campaign_end_point ON bi_temp.campaign USING gist(end_point);
