UPDATE bi_temp.pipelines
	SET campaign_has_been_computed = FALSE,
	    river_has_been_computed = FALSE
WHERE
	campaign_id = @campaignID;


UPDATE campaign.campaign
    SET has_been_computed = FALSE
WHERE id IN (@campaignID);