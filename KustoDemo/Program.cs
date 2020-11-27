namespace KustoDemo
{
    using AzureLib.KeyVault;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
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
            var entities = provider.GetRecentTriggerEntries(now.AddDays(-3), now.AddDays(-2), false);
            foreach (var entity in entities)
            {
                Console.WriteLine(JObject.Parse(JsonConvert.SerializeObject(entity)));
                Console.WriteLine($"LastFailedActivityForRun '{entity.PipelineRunId}':");
                provider.GetLastFailedActivityForRun(entity.PipelineRunId, now.AddDays(-3), now.AddDays(-2));
            }

            if (Debugger.IsAttached)
            {
                Console.WriteLine("press enter key to exit...");
                Console.ReadLine();
            }
        }
    }
}
