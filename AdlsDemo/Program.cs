namespace AdlsDemo
{
    using AzureLib.KeyVault;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    class Program
    {
        static string tenantId;
        // We need the package from nuget Microsoft.Rest.ClientRuntime.Azure.Authentication and Microsoft.Azure.DataLake.Store
        static void Main(string[] args)
        {
            Console.WriteLine("Start...");

            // For Microsoft tenant
            //tenantId = @"72f988bf-86f1-41af-91ab-2d7cd011db47";
            //CheckAdlsFileExistsDemo();
            //GetIDEAsProdAdlsFileSizeDemo();
            //GetObdTestFileSizeDemo();
            //InsertAdlsFileDemo();
            //DeleteAdlsFileDemo();
            //GetEnumerateAdlsMetadataEntityDemo();


            // For Torus tenant
            tenantId = @"cdc5aeea-15c5-4db6-b079-fcadd2505dc2";
            // Check the file with the certificate in Torus tenant.
            TorusAccessCFRFileDemo();

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

            var dataLakeClient = new DataLakeClient(tenantId, clientId, clientKey);

            Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net",
                "/shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/Internal/IDEAsTenantProfile/PostValidation/Streams/v3/2020/09/TenantProfileStats_2020_09_16.ss"));
            // We don't have access to store "ideas-ppe-c14.azuredatalakestore.net". It will throw exception: forbidden
            //Console.WriteLine(dataLakeClient.CheckExists("ideas-ppe-c14.azuredatalakestore.net", "/local/build/user/dekashet/TeamsMeetingProdAfterTimeZoneFixApril18th/directViewCodeWithAdjustEndDate.csv"));
        }

        public static void TorusAccessCFRFileDemo()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string clientId = secretProvider.GetSecretAsync("datacop-prod", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacop-prod", "AdlsAadAuthAppSecret").Result;
            var dataLakeClient = new DataLakeClient(tenantId, clientId, clientKey);

            // Status code: Forbidden
            Console.WriteLine(dataLakeClient.CheckExists("cfr-ppe-c14.azuredatalakestore.net",
                "local/Cooked/ActivityUserYammer/ActivityUserYammer_2020_09_14.ss"));
            Console.WriteLine(dataLakeClient.CheckExists("ideas-ppe-c14.azuredatalakestore.net",
                "/local/Publish/Profiles/Tenant/Commercial/Internal/IDEAsTenantProfile/PostValidation/Streams/v3/2020/09/TenantProfileStats_2020_09_16.ss"));
            Console.WriteLine(dataLakeClient.CheckExists("ideas-ppe-c14.azuredatalakestore.net",
               $"/local/UXlog/Report/Weekly/2020/06/gal/ReportWeekly_2020_06_21.ss"));
            Console.WriteLine(dataLakeClient.CheckExists("cfr-prod-c14.azuredatalakestore.net",
                "/local/Cooked/StateUserYammer/StateUserYammer_2020_09_19.ss"));
            Console.WriteLine(dataLakeClient.CheckExists("cfr-prod-c14.azuredatalakestore.net",
                "/local/Cubes/ProPlusUsage/ProPlusUsage_2020_09_06.ss"));

            // Access successfully
            Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net",
                "/shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/Internal/IDEAsTenantProfile/PostValidation/Streams/v3/2020/09/TenantProfileStats_2020_09_16.ss"));
            Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net",
                "/local/Partner/PreRelease/dev/tpidactiveusagecounts/useros/daily/aadauth/2019/07/TpidAadAuthActiveUserCount_2019_07_31.ss"));
            Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-data-c14.azuredatalakestore.net",
                "/local/Publish/Profiles/Tenant/Commercial/Internal/IDEAsTenantProfile/PostValidation/Streams/v3/2020/09/TenantProfileStats_2020_09_16.ss"));

            // We don't have access to store "ideas-ppe-c14.azuredatalakestore.net". It will throw exception: forbidden
            //Console.WriteLine(dataLakeClient.CheckExists("ideas-ppe-c14.azuredatalakestore.net", "/local/build/user/dekashet/TeamsMeetingProdAfterTimeZoneFixApril18th/directViewCodeWithAdjustEndDate.csv"));
        }

        public static void GetIDEAsProdAdlsFileSizeDemo()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string clientId = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppSecret").Result;

            var dataLakeClient = new DataLakeClient(tenantId, clientId, clientKey);

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

            var dataLakeClient = new DataLakeClient(tenantId, clientId, clientKey);

            string folderPath = @"local/users/jianjlv/datacop_service_monitor/";
            for (int i = 1; i < 11; i++)
            {
                string fileName = "datacop_service_monitor_test_2019_12_" + (i > 9 ? $"{i}" : $"0{i}");
                dataLakeClient.CreateFile("ideas-prod-c14.azuredatalakestore.net", folderPath + fileName + ".ss", fileName);
                Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net", folderPath + fileName + ".ss"));
                Console.WriteLine(dataLakeClient.GetFileSize("ideas-prod-c14.azuredatalakestore.net", folderPath + fileName + ".ss"));
            }
        }

        public static void DeleteAdlsFileDemo()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string clientId = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacopdev", "AdlsAadAuthAppSecret").Result;

            var dataLakeClient = new DataLakeClient(tenantId, clientId, clientKey);

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

            var dataLakeClient = new DataLakeClient(tenantId, clientId, clientKey);

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
