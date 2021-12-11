UPDATE bi_temp.pipelines
	SET campaign_has_been_computed = TRUE,
	    river_has_been_computed = TRUE
WHERE
	campaign_id = @campaignID;


UPDATE campaign.campaign
    SET has_been_computed = TRUE
WHERE id IN (@campaignID);