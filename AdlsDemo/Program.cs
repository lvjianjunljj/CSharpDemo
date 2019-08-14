using AzureLib.KeyVault;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdlsDemo
{
    class Program
    {
        // We need the package from nuget Microsoft.Rest.ClientRuntime.Azure.Authentication and Microsoft.Azure.DataLake.Store

        static void Main(string[] args)
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string clientId = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppSecret").Result;


            var dataLakeClient = new DataLakeClient(clientId, clientKey);
            //Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net",
            //    "local/datacop/TenantsHistory.ss"));
            //Console.WriteLine(dataLakeClient.GetFileSize("ideas-prod-c14.azuredatalakestore.net",
            //    "local/datacop/TenantsHistory.ss"));


            //Console.WriteLine(dataLakeClient.CheckExists("cfr-ppe-c14.azuredatalakestore.net",
            //   "local/Cooked/StateUserDirectory/StateUserDirectory_2019_07_24.ss"));
            //Console.WriteLine(dataLakeClient.GetFileSize("cfr-ppe-c14.azuredatalakestore.net",
            //   "local/Cooked/StateUserDirectory/StateUserDirectory_2019_07_24.ss"));


            Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net",
                "local/Scheduled/Datasets/Public/Profiles/OlsLicenses/v4/2019/08/01/LicensesCommercialHistory_2019-08-01.ss"));
            Console.WriteLine(dataLakeClient.GetFileSize("ideas-prod-c14.azuredatalakestore.net",
                "local/Scheduled/Datasets/Public/Profiles/OlsLicenses/v4/2019/08/01/LicensesCommercialHistory_2019-08-01.ss"));

            Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net",
                "local/Scheduled/Datasets/Public/Profiles/OlsLicenses/2019/08/01/LicensesCommercialHistory_2019-08-01.ss"));
            Console.WriteLine(dataLakeClient.GetFileSize("ideas-prod-c14.azuredatalakestore.net",
                "local/Scheduled/Datasets/Public/Profiles/OlsLicenses/2019/08/01/LicensesCommercialHistory_2019-08-01.ss"));

            Console.ReadKey();
        }
    }
}
