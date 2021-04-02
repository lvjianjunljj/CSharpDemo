
namespace LogAnalyticsDemo
{
    using AzureLib.KeyVault;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    class DataCopLogQueryDemo
    {
        private static DataCopLogProvider datacopLogProvider;

        public static void MainMethod()
        {
            Console.WriteLine("Start Log Analytics demo process: ");
            Initialize(@"datacop-prod");
            CosmosWorkerLogDemo();

            if (Debugger.IsAttached)
            {
                Console.WriteLine("press enter key to exit...");
                Console.ReadLine();
            }
        }

        private static void CosmosWorkerLogDemo()
        {
            string queryString = "CosmosWorkerTrace_CL ";
            queryString += @"| where TagId_g == 'a83c16b8-c7c1-4676-b6b0-2dd8b4ca2a14' ";
            queryString += @"| where Message endswith 'Success\'.' ";
            queryString += @"| project TestRunId_g";
            var queryResult = datacopLogProvider.GetQueryresult(queryString);
            Console.WriteLine(JArray.Parse(JsonConvert.SerializeObject(queryResult)));
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
