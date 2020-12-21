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
        private static BuildLogProvider buildLogProvider;
        static void Main(string[] args)
        {
            Console.WriteLine("Start Log Analytics demo process: ");
            Initialize(@"datacop-prod");

            JobSubmissionQueryResulDemo();
            //ADFLogQueryDemo();
            //JobSubmissionQueryDemo();



            if (Debugger.IsAttached)
            {
                Console.WriteLine("press enter key to exit...");
                Console.ReadLine();
            }
        }

        private static void JobSubmissionQueryDemo()
        {
            DateTime date = new DateTime(2020, 10, 5);

            for (int i = 0; i < 100; i++)
            {
                string buildEntityId = @"";
                var testDate = date.AddDays(i);
                Console.WriteLine(testDate);
                // "PLSDigitalAnalytics/pipelines/DeleteAgentPipeline"

                var lastPipelineRun = buildLogProvider.GetLastPipelineRun(@"", testDate, out string pipelineRunStatus);

                if (lastPipelineRun == null)
                {
                    Console.WriteLine("lastPipelineRun is null...");
                }
                else
                {
                    Console.WriteLine($"lastPipelineRun.BuildEntityId: {lastPipelineRun.BuildEntityId}");
                    Console.WriteLine($"lastPipelineRun.PipelineRunId: {lastPipelineRun.PipelineRunId}");
                    Console.WriteLine($"pipelineRunStatus: {pipelineRunStatus}");
                    buildEntityId = lastPipelineRun.BuildEntityId;

                    if (pipelineRunStatus.Equals("Succeeded"))
                    {
                        //await this.CreateTestRunEntry(dataset, lastPipelineRun);
                    }
                    else
                    {
                        var lastActivityRun = buildLogProvider.GetLastActivityRun(lastPipelineRun.PipelineRunId, "", null, out string activityRunStatus);
                        Console.WriteLine($"lastActivityRun.ActivityRunId: {lastActivityRun.ActivityRunId}");
                        Console.WriteLine($"activityRunStatus: {activityRunStatus}");
                        Console.WriteLine($"activityName: {lastActivityRun.ActivityName}");
                    }

                }

                // "PLSDigitalAnalytics/pipelines/DeleteAgentPipeline"
                // "DeleteCommandDQ_IFT"
                var jobSubmissionResult = buildLogProvider.GetJobSubmissionResult(buildEntityId, "", testDate, out string status);

                if (jobSubmissionResult == null)
                {
                    Console.WriteLine("No jobSubmissionResult...");
                }
                else
                {
                    //Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(failedPipeline)));
                    Console.WriteLine($"buildEntityId: {buildEntityId}");
                    Console.WriteLine($"jobSubmissionResult.PipelineRunId: {jobSubmissionResult.PipelineRunId}");
                    Console.WriteLine($"jobSubmissionResult.ActivityRunId: {jobSubmissionResult.ActivityRunId}");
                    Console.WriteLine($"jobSubmissionResult.ActivityName: {jobSubmissionResult.ActivityName}");
                    Console.WriteLine($"status: {status}");

                    var jobSubmissionQueryResult = buildLogProvider.GetJobSubmissionQueryResult(jobSubmissionResult.ActivityName, testDate, out string activityStatus);

                    if (jobSubmissionQueryResult == null)
                    {
                        Console.WriteLine("No jobSubmissionQueryResult...");
                    }
                    else
                    {
                        //Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(failedPipeline)));
                        Console.WriteLine($"jobSubmissionQueryResult.ActivityRunId: {jobSubmissionResult.ActivityRunId}");
                        Console.WriteLine($"jobSubmissionQueryResult.ActivityName: {jobSubmissionResult.ActivityName}");
                        Console.WriteLine($"activityStatus: {activityStatus}");
                    }
                }

            }
        }

        private static void ADFLogQueryDemo()
        {
            DateTime now = DateTime.UtcNow;
            Console.WriteLine("GetRecentTriggerEntries:");
            var entities = buildLogProvider.GetRecentTriggerEntries(now.AddHours(-12), now, false);
            foreach (var entity in entities)
            {
                Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(entity)));
                Console.WriteLine($"LastFailedActivityForRun '{entity.PipelineRunId}':");
                var lastFailedActivityForRun = buildLogProvider.GetLastFailedActivityForRun(entity.PipelineRunId, now.AddDays(-3), now.AddDays(-2));
                Console.WriteLine(lastFailedActivityForRun);
            }


            int count = 0;
            Console.WriteLine("GetADFActivityRun:");
            var list = buildLogProvider.GetADFActivityRun(now.AddDays(-48), now);
            foreach (var dict in list)
            {
                Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(dict)));
                if (count++ > 20)
                {
                    break;
                }
            }

            HashSet<string> set = new HashSet<string>();
            foreach (var dict in list)
            {
                set.Add(dict["Status"]);
            }
            foreach (var status in set)
            {
                Console.WriteLine(status);
            }

            Console.WriteLine("```````````````````````````````````````````````````````");
            Console.WriteLine("GetADFPipelineRun:");
            list = buildLogProvider.GetADFPipelineRun(now.AddDays(-48), now);
            foreach (var dict in list)
            {
                Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(dict)));
                if (count++ > 40)
                {
                    break;
                }
            }

            set = new HashSet<string>();
            foreach (var dict in list)
            {
                set.Add(dict["Status"]);
            }
            foreach (var status in set)
            {
                Console.WriteLine(status);
            }
        }

        private static void JobSubmissionQueryResulDemo()
        {
            DateTime date = new DateTime(2020, 10, 5);
            for (int i = 0; i < 90; i++)
            {
                var testDate = date.AddDays(i);
                Console.WriteLine(testDate);

                var result = buildLogProvider.GetJobSubmissionQueryResult(@"Win10EduByTPId_Rolling_V3_activity_WFD", testDate, out string resultStatus);
                if (result == null)
                {
                    Console.WriteLine(@"JobSubmissionQueryResult is null...");
                    continue;
                }
                Console.WriteLine($"jobSubmissionQueryResult.ActivityRunId: {result.ActivityRunId}");
                Console.WriteLine($"jobSubmissionQueryResult.ActivityName: {result.ActivityName}");
                Console.WriteLine(resultStatus);
            }
        }

        private static void Initialize(string keyVaultName)
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string workspaceId = secretProvider.GetSecretAsync(keyVaultName, "BuildLogMonitorWorkspaceId").Result;
            string clientId = secretProvider.GetSecretAsync(keyVaultName, "BuildLogMonitorClientId").Result;
            string clientKey = secretProvider.GetSecretAsync(keyVaultName, "BuildLogMonitorClientKey").Result;
            buildLogProvider = new BuildLogProvider(workspaceId, clientId, clientKey);
        }
    }
}
