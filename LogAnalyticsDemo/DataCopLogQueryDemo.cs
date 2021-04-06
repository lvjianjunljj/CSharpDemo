namespace LogAnalyticsDemo
{
    using AzureLib.KeyVault;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;

    class DataCopLogQueryDemo
    {
        private static DataCopLogProvider datacopLogProvider;

        public static void MainMethod()
        {
            Console.WriteLine("Start Log Analytics demo process: ");
            Initialize(@"datacop-prod");

            CosmosWorkerFallbackLogQuery();
        }

        private static void CosmosWorkerFallbackLogQuery()
        {
            Console.WriteLine(@"Start querying CosmosWorkerFallbackLog...");
            string queryString = "CosmosWorkerTrace_CL ";
            queryString += @"| where TagId_g == 'a83c16b8-c7c1-4676-b6b0-2dd8b4ca2a14' ";
            queryString += @"| where Message endswith 'Success\'.' ";
            queryString += @"| project TestRunId_g";
            var queryResult = datacopLogProvider.GetQueryresult(queryString);
            var testRunIds = JArray.Parse(JsonConvert.SerializeObject(queryResult));
            Console.WriteLine("Output testRun ids: ");
            foreach (var testRunId in testRunIds)
            {
                Console.WriteLine(testRunId["TestRunId_g"]);
            }
        }

        private static void Initialize(string keyVaultName)
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string workspaceId = secretProvider.GetSecretAsync(keyVaultName, "DataCopLogMonitorWorkspaceId").Result;
            string clientId = secretProvider.GetSecretAsync(keyVaultName, "BuildLogMonitorClientId").Result;
            string clientKey = secretProvider.GetSecretAsync(keyVaultName, "BuildLogMonitorClientKey").Result;
            datacopLogProvider = new DataCopLogProvider(workspaceId, clientId, clientKey);
        }
    }
}
