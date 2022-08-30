TRUNCATE bi_temp.pipelines;
UPDATE campaign.campaign
SET has_been_computed = False;


UPDATE campaign.campaign
SET has_been_computed = null