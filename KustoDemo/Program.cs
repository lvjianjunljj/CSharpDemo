namespace KustoDemo
{
    using AzureLib.KeyVault;
    using System;
    using System.Diagnostics;

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(1234);
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            //tenantId = @"72f988bf-86f1-41af-91ab-2d7cd011db47";// For tenant Microsoft
            string tenantId = @"cdc5aeea-15c5-4db6-b079-fcadd2505dc2";// For tenant Torus
            string clientId = secretProvider.GetSecretAsync("datacop-prod", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacop-prod", "AdlsAadAuthAppSecret").Result;
            BuildLogProvider provider = new BuildLogProvider();
            if (Debugger.IsAttached)
            {
                Console.WriteLine("press enter key to exit...");
                Console.ReadLine();
            }
        }
    }
}
