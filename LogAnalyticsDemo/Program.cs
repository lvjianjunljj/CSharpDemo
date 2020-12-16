namespace LogAnalyticsDemo
{
    using AzureLib.KeyVault;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Start kusto demo process:");
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string workspaceId = secretProvider.GetSecretAsync("datacop-prod", "BuildLogMonitorWorkspaceId").Result;
            string clientId = secretProvider.GetSecretAsync("datacop-prod", "BuildLogMonitorClientId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacop-prod", "BuildLogMonitorClientKey").Result;
            BuildLogProvider buildLogProvider = new BuildLogProvider(workspaceId, clientId, clientKey);
            DateTime now = DateTime.UtcNow;

            //Console.WriteLine("GetRecentTriggerEntries:");
            //var entities = provider.GetRecentTriggerEntries(now.AddHours(-12), now, false);
            //foreach (var entity in entities)
            //{
            //    Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(entity)));
            //    Console.WriteLine($"LastFailedActivityForRun '{entity.PipelineRunId}':");
            //    var lastFailedActivityForRun = provider.GetLastFailedActivityForRun(entity.PipelineRunId, now.AddDays(-3), now.AddDays(-2));
            //    Console.WriteLine(lastFailedActivityForRun);
            //}


            //int count = 0;
            //Console.WriteLine("GetADFActivityRun:");
            //var list = buildLogProvider.GetADFActivityRun(now.AddDays(-48), now);
            //foreach (var dict in list)
            //{
            //    Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(dict)));
            //    if (count++ > 20)
            //    {
            //        break;
            //    }
            //}

            //HashSet<string> set = new HashSet<string>();
            //foreach (var dict in list)
            //{
            //    set.Add(dict["Status"]);
            //}
            //foreach (var status in set)
            //{
            //    Console.WriteLine(status);
            //}

            Console.WriteLine("```````````````````````````````````````````````````````");
            Console.WriteLine("GetADFPipelineRun:");
            //list = buildLogProvider.GetADFPipelineRun(now.AddDays(-48), now);
            //foreach (var dict in list)
            //{
            //    Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(dict)));
            //    if (count++ > 40)
            //    {
            //        break;
            //    }
            //}

            //set = new HashSet<string>();
            //foreach (var dict in list)
            //{
            //    set.Add(dict["Status"]);
            //}
            //foreach (var status in set)
            //{
            //    Console.WriteLine(status);
            //}



            Console.WriteLine("``````````````````````````````````````````````````````````````````````````````");
            DateTime date = new DateTime(2020, 4, 25);
            for (int i = 0; i < 10; i++)
            {
                var testDate = date.AddDays(i);
                var endDate = testDate.AddDays(1);
                Console.WriteLine(testDate);
                var lastPipelineRun = buildLogProvider.GetLastPipelineRun(@"PLSCoreDataCommercial/pipelines/PaidSubsTenantHistoryPipeline", testDate, endDate, out string pipelineRunStatus);
                Console.WriteLine($"pipelineRunStatus: {pipelineRunStatus}");
                if (lastPipelineRun == null)
                {
                    Console.WriteLine("lastPipelineRun is null...");
                    continue;
                }

                Console.WriteLine($"lastPipelineRun.BuildEntityId: {lastPipelineRun.BuildEntityId}");
                Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(lastPipelineRun)));
                if (pipelineRunStatus.Equals("Succeeded"))
                {
                    //await this.CreateTestRunEntry(dataset, lastPipelineRun);
                }
                else
                {
                    var lastActivityRun = buildLogProvider.GetLastActivityRun(lastPipelineRun.PipelineRunId, testDate, endDate, out string activityRunStatus);
                    Console.WriteLine($"activityRunStatus: {activityRunStatus}");
                    Console.WriteLine($"lastActivityRun.ActivityRunId: {lastActivityRun.ActivityRunId}");
                    Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(lastActivityRun)));
                    //await this.CreateTestRunEntry(dataset, lastPipelineRun, lastActivityRun);
                }
            }


            if (Debugger.IsAttached)
            {
                Console.WriteLine("press enter key to exit...");
                Console.ReadLine();
            }
        }
    }
}
