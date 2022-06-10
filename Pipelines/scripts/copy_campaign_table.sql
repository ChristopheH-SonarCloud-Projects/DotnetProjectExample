DELETE FROM bi_temp.campaign;
INSERT INTO bi_temp.campaign (
    id,
    locomotion,
    isaidriven,
    remark,
    id_ref_user_fk,
    riverside,
    createdon,
    pipeline_id
)
SELECT
    id,
    locomotion,
    isaidriven,
    remark,
    id_ref_user_fk,
    riverside,
    createdon,
    NULL
FROM campaign.campaign
WHERE id IN (SELECT campaign_id FROM bi_temp.pipeline_to_compute);