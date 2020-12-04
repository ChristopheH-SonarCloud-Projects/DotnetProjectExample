using Azure.Storage.Blobs;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Surfrider;
using Surfrider.Jobs;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Surfrider.Jobs_RecurringJobs.Tests
{
    [TestClass]
    public class ExecuteScriptsTests
    {
        
        [TestMethod]
        public async Task ExecuteScriptsSteps_SUCCESS()
        {
            Console.WriteLine("USING " + GetTestsConnectionString());
            
            IDatabase Database = new PostgreDatabase(GetTestsConnectionString());
            Guid fakeCampaignId = Guid.NewGuid();
            IDictionary<string, string> Params = new Dictionary<string, string>();
            Params.Add("campaignId", fakeCampaignId.ToString());

            Assert.IsTrue(await Database.ExecuteScriptsAsync(GetSuccessfulTestsStepsToExecute(), Params));
            var res = await Database.ExecuteStringQueryAsync("SELECT * FROM bi_temp.pipelines WHERE campaign_id = '@campaignId'", Params);
            Assert.AreEqual(res, "");
        }
        [TestMethod]
        public async Task MarkCampaignPipelineAsFailedAsync_SUCCESS()
        {
            Console.WriteLine("USING " + GetTestsConnectionString());
            
            IDatabase Database = new PostgreDatabase(GetTestsConnectionString());
            Guid fakeCampaignId = Guid.NewGuid();
            IDictionary<string, string> Params = new Dictionary<string, string>();
            Params.Add("campaignId", fakeCampaignId.ToString());

            // adds a new pipeline entry
            await Database.ExecuteScriptAsync(@"./TestsScripts/1_add_fake_campaign.sql", Params);
            // check the entry is null so far
            var shouldBeEmpty = await Database.ExecuteStringQueryAsync("SELECT campaign_has_been_computed FROM bi_temp.pipelines WHERE campaign_id = '@campaignId'", Params);
            Assert.AreEqual(shouldBeEmpty, "");
            
            // run the method we test
            ICampaignPipeline pipeline = new CampaignPipeline(GetTestsConnectionString());
            await pipeline.MarkCampaignPipelineAsFailedAsync(fakeCampaignId);
            var shouldBeFalse = await Database.ExecuteStringQueryAsync("SELECT campaign_has_been_computed FROM bi_temp.pipelines WHERE campaign_id = '@campaignId'", Params);
            Assert.AreEqual(shouldBeFalse, "False");

            // remove the added campaign
            await Database.ExecuteScriptAsync(@"./TestsScripts/2_remove_fake_campaign.sql", Params);
        }
        [TestMethod]
        public async Task MarkCampaignPipelineAsSucccessedAsync_SUCCESS()
        {
            Console.WriteLine("USING " + GetTestsConnectionString());
            
            IDatabase Database = new PostgreDatabase(GetTestsConnectionString());
            Guid fakeCampaignId = Guid.NewGuid();
            IDictionary<string, string> Params = new Dictionary<string, string>();
            Params.Add("campaignId", fakeCampaignId.ToString());

            // adds a new pipeline entry
            await Database.ExecuteScriptAsync(@"./TestsScripts/1_add_fake_campaign.sql", Params);
            // check the entry is null so far
            var shouldBeEmpty = await Database.ExecuteStringQueryAsync("SELECT campaign_has_been_computed FROM bi_temp.pipelines WHERE campaign_id = '@campaignId'", Params);
            Assert.AreEqual(shouldBeEmpty, "");
            
            // run the method we test
            ICampaignPipeline pipeline = new CampaignPipeline(GetTestsConnectionString());
            await pipeline.MarkCampaignPipelineAsSuccessedAsync(fakeCampaignId);
            var shouldBeFalse = await Database.ExecuteStringQueryAsync("SELECT campaign_has_been_computed FROM bi_temp.pipelines WHERE campaign_id = '@campaignId'", Params);
            Assert.AreEqual(shouldBeFalse, "True");

            // remove the added campaign
            await Database.ExecuteScriptAsync(@"./TestsScripts/2_remove_fake_campaign.sql", Params);
        }


        private SortedList<int, string> GetSuccessfulTestsStepsToExecute()
        {
            SortedList<int, string> SqlSteps = new SortedList<int, string>();
            SqlSteps.Add(1, @"./TestsScripts/1_add_fake_campaign.sql");
            SqlSteps.Add(2, @"./TestsScripts/2_remove_fake_campaign.sql");
            return SqlSteps;
        }
        public static string GetTestsConnectionString()
        {
                return "Server=test-pgdb.postgres.database.azure.com;Database=surfriderdb;Port=5432;User Id=testpgdbrootuser@test-pgdb;Password=LePlastiqueCaPiqueBeaucoup!;Ssl Mode=Require;";
        }
    }
}
