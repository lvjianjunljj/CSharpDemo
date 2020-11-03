namespace AdlsDemo
{
    using AzureLib.KeyVault;
    using Microsoft.Azure.DataLake.Store;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Text;

    public class AdlsDemoClass
    {
        static DataLakeClient dataLakeClient;

        /// <summary>
        /// The Dictionary for cosmos vc and adls account mapping.
        /// </summary>
        static Dictionary<string, string> vcAdlaAdlsAccountDict = new Dictionary<string, string>
        {
            ["https://cosmos14.osdinfra.net/cosmos/exchange.storage.prod"] = "exchange-storage-prod-c14.azuredatalakestore.net",
            ["https://cosmos14.osdinfra.net/cosmos/ideas.private.data"] = "ideas-private-data-c14.azuredatalakestore.net",
            ["https://cosmos14.osdinfra.net/cosmos/ideas.prod"] = "ideas-prod-c14.azuredatalakestore.net",
            ["https://cosmos14.osdinfra.net/cosmos/ideas.prod.build"] = "ideas-prod-build-c14.azuredatalakestore.net",
            ["https://cosmos14.osdinfra.net/cosmos/ideas.prod.data"] = "ideas-prod-data-c14.azuredatalakestore.net",
        };

        // We need the package from nuget Microsoft.Rest.ClientRuntime.Azure.Authentication and Microsoft.Azure.DataLake.Store
        public static void MainMethod()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            //tenantId = @"72f988bf-86f1-41af-91ab-2d7cd011db47";// For tenant Microsoft
            string tenantId = @"cdc5aeea-15c5-4db6-b079-fcadd2505dc2";// For tenant Torus
            string clientId = secretProvider.GetSecretAsync("datacop-prod", "AdlsAadAuthAppId").Result;
            string clientKey = secretProvider.GetSecretAsync("datacop-prod", "AdlsAadAuthAppSecret").Result;

            dataLakeClient = new DataLakeClient(tenantId, clientId, clientKey);

            //CheckAdlsFileExistsDemo();
            //GetIDEAsProdAdlsFileSizeDemo();
            //GetObdTestFileSizeDemo();
            //InsertAdlsFileDemo();
            //DeleteAdlsFileDemo();
            //GetEnumerateAdlsMetadataEntityDemo();


            // For Torus tenant
            // Check the file with the certificate in Torus tenant.
            //TorusAccessCFRFileDemo();
            CheckAdlsPermissionByJson();
        }

        public static void CheckAdlsPermissionByJson()
        {
            JArray result = new JArray();
            Console.WriteLine("Path prefixs without permission: ");
            string jsonPath = @"D:\data\company_work\M365\IDEAs\path.json";
            var JsonStr = File.ReadAllText(jsonPath, Encoding.UTF8);
            JArray jArray = JArray.Parse(JsonStr);
            foreach (var json in jArray)
            {
                bool hasVaule = false;
                JObject jObject = new JObject();
                string dataLakeStore;
                var dataFabric = json["dataFabric"].ToString();
                if (dataFabric.Equals("ADLS"))
                {
                    dataLakeStore = json["dataLakeStore"].ToString();
                }
                else if (dataFabric.Equals("CosmosStream"))
                {
                    dataLakeStore = vcAdlaAdlsAccountDict[json["cosmosVC"].ToString()];
                }
                else
                {
                    Console.WriteLine($"Not support this datafabric {dataFabric}");
                    continue;
                }

                foreach (var pathPrefix in json["pathPrefixs"].ToArray())
                {
                    if (!dataLakeClient.CheckPermission(dataLakeStore, pathPrefix + "/Test/Test.ss"))
                    {
                        if (!hasVaule)
                        {
                            jObject["dataFabric"] = dataFabric;
                            jObject["dataLakeStore"] = dataLakeStore;
                            jObject["pathPrefixs"] = new JArray();
                            hasVaule = true;
                        }

                        ((JArray)jObject["pathPrefixs"]).Add(pathPrefix);
                    }
                }

                if (hasVaule)
                {
                    result.Add(jObject);
                }
            }

            Console.WriteLine(result);
        }

        public static void CheckAdlsFileExistsDemo()
        {
            Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net",
                "/shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/Internal/IDEAsTenantProfile/PostValidation/Streams/v3/2020/09/TenantProfileStats_2020_09_16.ss"));
            // We don't have access to store "ideas-ppe-c14.azuredatalakestore.net". It will throw exception: forbidden
            //Console.WriteLine(dataLakeClient.CheckExists("ideas-ppe-c14.azuredatalakestore.net", "/local/build/user/dekashet/TeamsMeetingProdAfterTimeZoneFixApril18th/directViewCodeWithAdjustEndDate.csv"));
        }

        public static void TorusAccessCFRFileDemo()
        {
            Console.WriteLine(dataLakeClient.CheckExists("cfr-ppe-c14.azuredatalakestore.net",
                "local/Cooked/ActivityUserYammer/ActivityUserYammer_2020_09_14.ss"));
            Console.WriteLine(dataLakeClient.CheckExists("cfr-ppe-c14.azuredatalakestore.net",
                "local/CFRUsageDashboard/Report/UXAndAPIWeekly/2020/10/gal/ReportUXAndAPIWeekly_2020_10_09.ss"));
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

            Console.WriteLine("EnumerateAdlsMetadataEntity:");
            foreach (var item in dataLakeClient.EnumerateAdlsMetadataEntity("cfr-ppe-c14.azuredatalakestore.net", "local/"))
            {

            }
        }

        public static void GetIDEAsProdAdlsFileSizeDemo()
        {
            Console.WriteLine(dataLakeClient.GetFileSize("ideas-prod-c14.azuredatalakestore.net",
                "local/Scheduled/Datasets/Public/Profiles/OlsLicenses/v4/2020/06/20/LicensesCommercialHistory_2020-06-20.ss"));
        }

        public static void GetObdTestFileSizeDemo()
        {

        }

        public static void InsertAdlsFileDemo()
        {
            Console.WriteLine("Insert Adls File...");

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
