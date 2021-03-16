namespace LogAnalyticsDemo
{
    using AzureLib.KeyVault;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Diagnostics;

    class CloudScopeLogQueryDemo
    {
        private static CloudScopeLogProvider cloudScopeLogProvider;

        public static void MainMethod()
        {
            Console.WriteLine("Start Log Analytics demo process: ");
            Initialize(@"datacop-prod");
            CloudScopeLogQuery();


            if (Debugger.IsAttached)
            {
                Console.WriteLine("press enter key to exit...");
                Console.ReadLine();
            }
        }

        private static void CloudScopeLogQuery()
        {
            var dict = cloudScopeLogProvider.GetTraces();
            Console.WriteLine(JArray.Parse(JsonConvert.SerializeObject(dict)));
        }

        private static void Initialize(string keyVaultName)
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string applicationId = secretProvider.GetSecretAsync(keyVaultName, "CloudScopeLogMonitorApplicationId").Result;
            string clientId = secretProvider.GetSecretAsync(keyVaultName, "BuildLogMonitorClientId").Result;
            string clientKey = secretProvider.GetSecretAsync(keyVaultName, "BuildLogMonitorClientKey").Result;

            cloudScopeLogProvider = new CloudScopeLogProvider(applicationId, clientId, clientKey);
        }
    }
}
