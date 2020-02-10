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
            InsertAdlsFileDemo();
            //DeleteAdlsFileDemo();
            //GetEnumerateAdlsMetadataEntityDemo();

            Console.ReadKey();
        }

        public static void CheckAdlsFileExistsDemo()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string clientId = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppSecret").Result;

            var dataLakeClient = new DataLakeClient(clientId, clientKey);

            Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net",
                "/local/Scheduled/Datasets/Private/Users/DataQuality/ConsumerUserProfile/v1/2019/11/ConsumerUserFABBSProfile_CorrectnessStats_2019_11_25.ss"));
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

        public static void InsertAdlsFileDemo()
        {
            Console.WriteLine("Insert Adls File...");

            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string clientId = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppSecret").Result;

            var dataLakeClient = new DataLakeClient(clientId, clientKey);

            string folderPath = @"/users/jianjlv/";
            for (int i = 1; i < 11; i++)
            {
                string fileName = "datacop_service_monitor_test_2019_12_" + (i > 9 ? $"{i}" : $"0{i}");
                //dataLakeClient.CreateFile("ideas-prod-c14.azuredatalakestore.net", folderPath + fileName + ".ss", fileName);
                Console.WriteLine(dataLakeClient.GetFileSize("ideas-prod-c14.azuredatalakestore.net", folderPath + fileName + ".ss"));
            }
        }

        public static void DeleteAdlsFileDemo()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string clientId = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppSecret").Result;

            var dataLakeClient = new DataLakeClient(clientId, clientKey);

            string folderPath = @"local/users/jianjlv/";
            for (int i = 1; i < 11; i++)
            {
                string fileName = "datacop_service_monitor_adls_test_2019_12_" + (i > 9 ? $"{i}" : $"0{i}");
                //dataLakeClient.DeleteFile("ideas-prod-c14.azuredatalakestore.net", folderPath + fileName + ".ss");
                Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net", folderPath + fileName + ".ss"));
            }
        }

        public static void GetEnumerateAdlsMetadataEntityDemo()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string clientId = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppSecret").Result;

            var dataLakeClient = new DataLakeClient(clientId, clientKey);

            IEnumerable<JObject> entities = dataLakeClient.EnumerateAdlsMetadataEntity("cfr-prod-c14.azuredatalakestore.net",
                "local/users/jianjlv/");

            foreach (var entity in entities)
            {
                Console.WriteLine(entity);
            }
            Console.WriteLine();

            entities = dataLakeClient.EnumerateAdlsMetadataEntity("ideas-prod-c14.azuredatalakestore.net",
                "");

            foreach (var entity in entities)
            {
                Console.WriteLine(entity);
            }
        }
    }
}
