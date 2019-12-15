using AzureLib.KeyVault;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
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
            //CheckAdlsFileExistsDemo();
            //GetAdlsFileSizeDemo();
            GetEnumerateAdlsMetadataEntityDemo();


            Console.ReadKey();
        }

        public static void CheckAdlsFileExistsDemo()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string clientId = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppSecret").Result;

            var dataLakeClient = new DataLakeClient(clientId, clientKey);

            Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net",
                "local/Scheduled/Datasets/Public/Profiles/OlsLicenses/v4/2019/08/20/LicensesCommercialHistory_2019-08-20.ss"));
        }

        public static void GetAdlsFileSizeDemo()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string clientId = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppSecret").Result;

            var dataLakeClient = new DataLakeClient(clientId, clientKey);

            Console.WriteLine(dataLakeClient.GetFileSize("ideas-prod-c14.azuredatalakestore.net",
                "local/Scheduled/Datasets/Public/Profiles/OlsLicenses/v4/2019/08/20/LicensesCommercialHistory_2019-08-20.ss"));
        }

        public static void GetEnumerateAdlsMetadataEntityDemo()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string clientId = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppSecret").Result;

            var dataLakeClient = new DataLakeClient(clientId, clientKey);

            IEnumerable<JObject> entities = dataLakeClient.EnumerateAdlsMetadataEntity("ideas-prod-c14.azuredatalakestore.net",
                "local/Klondike/Raw/");

            foreach (var entity in entities)
            {
                Console.WriteLine(entity);
            }

            entities = dataLakeClient.EnumerateAdlsMetadataEntity("ideas-prod-c14.azuredatalakestore.net",
                "");

            foreach (var entity in entities)
            {
                Console.WriteLine(entity);
            }
        }
    }
}
