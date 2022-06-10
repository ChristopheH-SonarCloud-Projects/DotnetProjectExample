

update bi.campaign_river
set feature_collection  = json_build_object('type', 'Feature', 'geometry', st_asgeojson(st_transform(ST_SetSRID(the_geom, 2154), 4326)) :: json, 'properties', (select json_strip_nulls (row_to_json(t))  FROM ( select distance,createdon, river_name ) t))
where id_ref_campaign_fk in (SELECT campaign_id FROM bi_temp.pipelines)
;

DROP TABLE IF EXISTS river_name;
CREATE TEMP TABLE river_name AS
SELECT bi_temp.campaign_river.river_name
FROM bi_temp.campaign_river
WHERE id_ref_campaign_fk IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute)
;

update referential.river
set feature_collection  = json_build_object('type', 'Feature', 'geometry', st_asgeojson(st_transform(ST_SetSRID(referential.river.the_geom, 2154), 4326)) :: json, 'properties', (select json_strip_nulls (row_to_json(t))  FROM (  SELECT bir.name,bir.trash_per_km, bir.distance_monitored ) t))
from bi.river bir
where referential.river.name = bir.name and referential.river.name in (select river_name from river_name)
;
/*
update referential.basin
set feature_collection  = json_build_object('type', 'Feature', 'geometry', st_asgeojson(st_transform(ST_SetSRID(the_geom, 2154), 4326)) :: json, 'properties', (SELECT json_strip_nulls (row_to_json(t))  FROM (SELECT basin_name, country_code, area_square_km) t))
;
*/
