namespace AdlsDemo
{
    using AzureLib.KeyVault;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    class Program
    {
        // We need the package from nuget Microsoft.Rest.ClientRuntime.Azure.Authentication and Microsoft.Azure.DataLake.Store
        static void Main(string[] args)
        {
            Console.WriteLine("Start...");
            //CheckAdlsFileExistsDemo();
            //GetIDEAsProdAdlsFileSizeDemo();
            //GetObdTestFileSizeDemo();
            InsertAdlsFileDemo();
            //DeleteAdlsFileDemo();
            //GetEnumerateAdlsMetadataEntityDemo();
            Console.WriteLine("End...");

            if (Debugger.IsAttached)
            {
                Console.WriteLine("press enter key to exit...");
                Console.ReadLine();
            }
        }

        public static void CheckAdlsFileExistsDemo()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string clientId = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppSecret").Result;

            var dataLakeClient = new DataLakeClient(clientId, clientKey);

            Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net",
                "/local/Scheduled/Datasets/Private/Users/DataQuality/ConsumerUserProfile/v1/2019/11/ConsumerUserFABBSProfile_CorrectnessStats_2019_11_25.ss"));
            // We don't have access to store "ideas-ppe-c14.azuredatalakestore.net". It will throw exception: forbidden
            //Console.WriteLine(dataLakeClient.CheckExists("ideas-ppe-c14.azuredatalakestore.net", "/local/build/user/dekashet/TeamsMeetingProdAfterTimeZoneFixApril18th/directViewCodeWithAdjustEndDate.csv"));
        }

        public static void GetIDEAsProdAdlsFileSizeDemo()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string clientId = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppSecret").Result;

            var dataLakeClient = new DataLakeClient(clientId, clientKey);

            Console.WriteLine(dataLakeClient.GetFileSize("ideas-prod-c14.azuredatalakestore.net",
                "local/Scheduled/Datasets/Public/Profiles/OlsLicenses/v4/2020/06/20/LicensesCommercialHistory_2020-06-20.ss"));
        }

        public static void GetObdTestFileSizeDemo()
        {

        }

        public static void InsertAdlsFileDemo()
        {
            Console.WriteLine("Insert Adls File...");

            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string clientId = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppSecret").Result;

            var dataLakeClient = new DataLakeClient(clientId, clientKey);

            string folderPath = @"local/users/jianjlv/datacop_service_monitor/";
            for (int i = 1; i < 11; i++)
            {
                string fileName = "datacop_service_monitor_test_2019_12_" + (i > 9 ? $"{i}" : $"0{i}");
                //dataLakeClient.CreateFile("ideas-prod-c14.azuredatalakestore.net", folderPath + fileName + ".ss", fileName);
                //Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net", folderPath + fileName + ".ss"));
                Console.WriteLine(dataLakeClient.GetFileSize("ideas-prod-c14.azuredatalakestore.net", folderPath + fileName + ".ss"));
            }
        }

        public static void DeleteAdlsFileDemo()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string clientId = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppSecret").Result;

            var dataLakeClient = new DataLakeClient(clientId, clientKey);

            string folderPath = @"/users/jianjlv/";
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

            IEnumerable<JObject> entities = dataLakeClient.EnumerateAdlsMetadataEntity("ideas-prod-c14.azuredatalakestore.net",
                "local/users/jianjlv/");

            foreach (var entity in entities)
            {
                Console.WriteLine(entity);
            }
            Console.WriteLine();

            //entities = dataLakeClient.EnumerateAdlsMetadataEntity("ideas-prod-c14.azuredatalakestore.net",
            //    "");

            //foreach (var entity in entities)
            //{
            //    Console.WriteLine(entity);
            //}
        }
    }
}
