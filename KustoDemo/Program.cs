namespace KustoDemo
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
            BuildLogProvider provider = new BuildLogProvider(workspaceId, clientId, clientKey);
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


            int count = 0;
            Console.WriteLine("GetADFActivityRun:");
            var list = provider.GetADFActivityRun(now.AddDays(-48), now);
            //foreach (var dict in list)
            //{
            //    Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(dict)));
            //    if (count++ > 20)
            //    {
            //        break;
            //    }
            //}

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
            list = provider.GetADFPipelineRun(now.AddDays(-48), now);
            //foreach (var dict in list)
            //{
            //    Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(dict)));
            //    if (count++ > 40)
            //    {
            //        break;
            //    }
            //}

            set = new HashSet<string>();
            foreach (var dict in list)
            {
                set.Add(dict["Status"]);
            }
            foreach (var status in set)
            {
                Console.WriteLine(status);
            }


            if (Debugger.IsAttached)
            {
                Console.WriteLine("press enter key to exit...");
                Console.ReadLine();
            }
        }
    }
}
