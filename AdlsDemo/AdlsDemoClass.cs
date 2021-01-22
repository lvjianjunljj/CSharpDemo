namespace AdlsDemo
{
    using AdlsDemo.FileOperation;
    using AzureLib.KeyVault;
    using CommonLib.IDEAs;
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
            ["https://cosmos14.osdinfra.net/cosmos/ideas.ppe"] = "ideas-ppe-c14.azuredatalakestore.net",
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
            Console.WriteLine($"For AAD application 'AdlsAadAuthApp'");

            Console.WriteLine($"clientId: {clientId}");
            Console.WriteLine($"clientKey: {clientKey}");
            dataLakeClient = new DataLakeClient(tenantId, clientId, clientKey);
            RunDemo();




            clientId = secretProvider.GetSecretAsync("datacop-prod", "IDEAsBuildVNextAppId").Result;
            clientKey = secretProvider.GetSecretAsync("datacop-prod", "IDEAsBuildVNextAppSecret").Result;
            Console.WriteLine($"For AAD application 'IDEAsBuildVNextApp'");

            Console.WriteLine($"clientId: {clientId}");
            Console.WriteLine($"clientKey: {clientKey}");
            dataLakeClient = new DataLakeClient(tenantId, clientId, clientKey);
            RunDemo();
        }

        private static void RunDemo()
        {
            //CheckAdlsFileExistsDemo();
            CheckAdlsDirectoryDemo();
            //GetAclStatusDemo();
            //GetIDEAsProdAdlsFileSizeDemo();
            //GetObdTestFileSizeDemo();
            //InsertAdlsFileDemo();
            //DeleteAdlsFileDemo();
            //GetEnumerateAdlsMetadataEntityDemo();


            // For Torus tenant
            // Check the file with the certificate in Torus tenant.
            //TorusAccessCFRFileDemo();
            //CheckPermissionByJson();
            //CheckShareSettingByJson();
            //CheckPermission();
            //CheckShareSetting();
        }

        private static void CheckPermission()
        {
            Console.WriteLine("CheckPermission:");
            DateTime startDate = DateTime.Now.Date.AddDays(-10);
            DateTime endDate = DateTime.Now.Date.AddDays(-3);
            var dataLakeStores = new List<string>
            {
                //@"ideas-ppe-c14.azuredatalakestore.net",
                @"ideas-prod-c14.azuredatalakestore.net",
                @"ideas-prod-data-c14.azuredatalakestore.net",
                @"ideas-prod-build-c14.azuredatalakestore.net",
            };
            var pathPrefixs = new List<string>()
            {
                @"Publish/Usage/Tenant/Commercial/ProductivityScore/Prod/ContentCollab/TenantScores_028D/Streams/v1/%Y/%m/TenantScores_028D_%Y_%m_%d.ss",
                @"shares/IDEAs.Prod.Data/Publish/Usage/Tenant/Commercial/ProductivityScore/Prod/ContentCollab/TenantScores_028D/Streams/v1/%Y/%m/TenantScores_028D_%Y_%m_%d.ss"
            };

            var checkingPaths = new List<string>();
            foreach (var pathPrefix in pathPrefixs)
            {
                string path;
                if (pathPrefix.ToString().EndsWith(".ss") || pathPrefix.ToString().EndsWith(".view"))
                {
                    path = pathPrefix;
                }
                else
                {
                    path = pathPrefix + "Test.ss";
                }

                checkingPaths.AddRange(StreamSetUtils.GenerateStreamsetPaths(path, startDate, endDate, null, null, Grain.Daily));
            }
            foreach (var dataLakeStore in dataLakeStores)
            {
                Console.WriteLine($"DataLakeStore: {dataLakeStore}");
                foreach (var checkingPath in checkingPaths)
                {
                    if (!dataLakeClient.CheckPermission(dataLakeStore, checkingPath))
                    {
                        Console.WriteLine($"Have no permission for pathPrefix: '{checkingPath}'");
                    }
                }
            }
        }

        private static void CheckShareSetting()
        {
            Console.WriteLine("CheckShareSetting:");
            var dataLakeStores = new List<string>
            {
                @"ideas-ppe-c14.azuredatalakestore.net",
                @"ideas-prod-c14.azuredatalakestore.net",
                @"ideas-prod-data-c14.azuredatalakestore.net",
                @"ideas-prod-build-c14.azuredatalakestore.net",
            };
            var streamPaths = new List<string>()
            {
                "/shares/User360/UserProfile/resources/LCID_Locale.ss",
                "shares/IDEAs.Prod.Data/Publish.Usage.User.Neutral.Reporting.Dashboards.AppDashboard.AverageDAU.Excel/",
                "/shares/CFR.ppe/Internal/YammerGroupTenantSegmentationMap/YammerGroupTenantSegmentationMap_2020_11_20.tsv",
                "/shares/CFR.prod/Internal/YammerGroupTenantSegmentationMap/YammerGroupTenantSegmentationMap_2020_11_20.tsv",
                "/shares/User360_Shared/user360_shared.local/Upload/Skype/Production/",
            };
            foreach (var dataLakeStore in dataLakeStores)
            {
                Console.WriteLine($"DataLakeStore: {dataLakeStore}");
                foreach (var streamPath in streamPaths)
                {
                    string rootPath = GetPathPrefix(streamPath.ToString());
                    try
                    {
                        if (!dataLakeClient.CheckDirectoryExists(dataLakeStore, rootPath))
                        {
                            Console.WriteLine($"No share setting for rootPath: '{rootPath}'");
                        }
                    }
                    catch (AdlsException e)
                    {
                        if (e.HttpStatus == HttpStatusCode.Forbidden)
                        {
                            Console.WriteLine($"No permission for dataLakeStore '{dataLakeStore}' and root path '{rootPath}'");
                        }
                        else
                        {
                            throw;
                        }
                    }
                }
            }
        }

        private static void CheckPermissionByJson()
        {
            Console.WriteLine("Check Adls Permission By Json: ");
            JArray result = new JArray();
            Console.WriteLine("Path prefixs without permission: ");
            string jsonPath = @"D:\data\company_work\M365\IDEAs\pathsWithoutDate.json";
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
                else if (dataFabric.Equals("CosmosStream") || dataFabric.Equals("CosmosView"))
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
                    string path;
                    if (pathPrefix.ToString().EndsWith(".ss") || pathPrefix.ToString().EndsWith(".view"))
                    {
                        path = pathPrefix.ToString();
                    }
                    else
                    {
                        path = pathPrefix.ToString() + "/Test/Test.ss";
                    }
                    if (!dataLakeClient.CheckPermission(dataLakeStore, path))
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

        private static void CheckShareSettingByJson()
        {
            Console.WriteLine("Check Adls Share Setting By Json: ");
            JArray result = new JArray();
            Console.WriteLine("Path prefixs without permission: ");
            string jsonPath = @"D:\data\company_work\M365\IDEAs\pathsWithoutDate.json";
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
                else if (dataFabric.Equals("CosmosStream") || dataFabric.Equals("CosmosView"))
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

                    string rootPath = GetPathPrefix(pathPrefix.ToString());
                    try
                    {

                        if (!dataLakeClient.CheckDirectoryExists(dataLakeStore, rootPath))
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
                    catch (AdlsException e)
                    {
                        if (e.HttpStatus == HttpStatusCode.Forbidden)
                        {
                            Console.WriteLine($"No permission for dataLakeStore '{dataLakeStore}' and path '{rootPath}'");
                            continue;
                        }

                        throw;
                    }
                }

                if (hasVaule)
                {
                    result.Add(jObject);
                }
            }

            Console.WriteLine(result);
        }


        private static void CheckAdlsFileExistsDemo()
        {
            var endDate = DateTime.UtcNow.Date.AddDays(-2);
            var startDate = endDate.AddDays(-10);
            string pathFormat = @"shares/IDEAs.Prod.Data/Publish/Usage/User/Commercial/Actions/EXO/Streams/v1/%Y/%m/WorldwidePIIDetectSDFV2Stat_%Y_%m_%d.ss";
            var streamPaths = StreamSetUtils.GenerateStreamsetPaths(pathFormat, startDate, endDate, null, null, Grain.Daily);

            Console.WriteLine("CheckAdlsFileExistsDemo: ");
            Console.WriteLine(@"For datalake: 'skypedata-adhoc-c11.azuredatalakestore.net'...");


            Console.WriteLine(@"For datalake: 'ideas-sensitive-build-c14.azuredatalakestore.net'...");
            Console.WriteLine(dataLakeClient.CheckExists("ideas-sensitive-build-c14.azuredatalakestore.net",
                @"/shares/IDEAs.Prod.Data/Private/Features/TPID/Commercial/FeatureStore/pbi/Views/v1/pbi.view"));

            Console.WriteLine(@"For datalake: 'ideas-prod-build-c14.azuredatalakestore.net'...");
            Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-build-c14.azuredatalakestore.net",
                @"/shares/IDEAs.Prod.Data/Publish/Usage/User/Commercial/CustomerFacing/Ppe/Cubes/TeamsActivityUserRange/Streams/v1/2021/01/TeamsActivityUser_180D_2021_01_11.ss"));
            Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-build-c14.azuredatalakestore.net",
                @"/shares/CFR.ppe/Internal/Pfizer/MailboxState_2021_01_11.tsv"));

            //foreach (var streamPath in streamPaths)
            //{
            //    Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-build-c14.azuredatalakestore.net", streamPath));
            //}

            //Console.WriteLine(@"For datalake: 'ideas-prod-c14.azuredatalakestore.net'...");
            //foreach (var streamPath in streamPaths)
            //{
            //    Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net", streamPath));
            //}

            // We don't have access to store "ideas-ppe-c14.azuredatalakestore.net". It will throw exception: forbidden
            //Console.WriteLine(dataLakeClient.CheckExists("ideas-ppe-c14.azuredatalakestore.net", "/local/build/user/dekashet/TeamsMeetingProdAfterTimeZoneFixApril18th/directViewCodeWithAdjustEndDate.csv"));
        }

        private static void CheckAdlsDirectoryDemo()
        {
            Console.WriteLine("CheckAdlsDirectoryDemo: ");

            Console.WriteLine(@"For datalake: 'skypedata-adhoc-c11.azuredatalakestore.net'...");


            Console.WriteLine(@"For datalake: 'ideas-ppe-c14.azuredatalakestore.net'...");
            Console.WriteLine(dataLakeClient.CheckDirectory("ideas-prod-build-c14.azuredatalakestore.net",
                @"/shares/IDEAs.Ppe/Publish.Usage.User.Commercial.Samples.Raw/"));


            Console.WriteLine(@"For datalake: 'ideas-prod-c14.azuredatalakestore.net'...");
            Console.WriteLine(dataLakeClient.CheckDirectory("ideas-prod-c14.azuredatalakestore.net",
                "local/Partner/PreRelease/dev/countedactions/powerpoint/2021/01/PowerPointDailyUsage_2021_01_10.ss"));
            var allLevelPaths = GetAllLevelPaths("/shares/IDEAs.Prod.Data/Publish/Usage/Tenant/Commercial/ProductivityScore/Prod/Meeting/TenantScores_028D/Streams/v1/2021/01/TenantScores_028D_2021_01_10.ss");
            foreach (var levelPath in allLevelPaths)
            {
                Console.WriteLine(levelPath);
                Console.WriteLine(dataLakeClient.CheckDirectory("ideas-prod-c14.azuredatalakestore.net", levelPath));
            }
            allLevelPaths = GetAllLevelPaths("/shares/IDEAs.Prod.Data/Publish/Usage/User/Commercial/Actions/EXO/Streams/v1/2021/01/WorldwidePIIDetectSDFV2Stat_2021_01_10.ss");
            foreach (var levelPath in allLevelPaths)
            {
                Console.WriteLine(levelPath);
                Console.WriteLine(dataLakeClient.CheckDirectory("ideas-prod-c14.azuredatalakestore.net", levelPath));
            }

            Console.WriteLine(@"For datalake: 'ideas-prod-build-c14.azuredatalakestore.net'...");
            Console.WriteLine(dataLakeClient.CheckDirectory("ideas-prod-build-c14.azuredatalakestore.net",
                @"/shares/IDEAs.Prod.Data/Private/"));
            Console.WriteLine(dataLakeClient.CheckDirectory("ideas-prod-build-c14.azuredatalakestore.net",
                @"/shares/IDEAs.Prod.Data/Collect/"));
            Console.WriteLine(dataLakeClient.CheckDirectory("ideas-prod-build-c14.azuredatalakestore.net",
                @"/shares/IDEAs.Prod.Data/"));
            Console.WriteLine(dataLakeClient.CheckDirectory("ideas-prod-build-c14.azuredatalakestore.net",
                @"/shares/"));
            // So wired this output is "Forbidden"
            Console.WriteLine(dataLakeClient.CheckDirectory("ideas-prod-build-c14.azuredatalakestore.net",
                @"/"));


            Console.WriteLine(@"For datalake: 'ideas-sensitive-build-c14.azuredatalakestore.net'...");
            Console.WriteLine(dataLakeClient.CheckDirectory("ideas-sensitive-build-c14.azuredatalakestore.net",
                @"/shares/IDEAs.Prod.Data/Private/"));
            Console.WriteLine(dataLakeClient.CheckDirectory("ideas-sensitive-build-c14.azuredatalakestore.net",
                @"/shares/IDEAs.Prod.Data/"));
            Console.WriteLine(dataLakeClient.CheckDirectory("ideas-sensitive-build-c14.azuredatalakestore.net",
                @"/shares/"));
            Console.WriteLine(dataLakeClient.CheckDirectory("ideas-sensitive-build-c14.azuredatalakestore.net",
                @"/"));
        }

        private static void GetAclStatusDemo()
        {
            Console.WriteLine("GetAclStatusDemo: ");

            Console.WriteLine(@"For datalake: 'skypedata-adhoc-c11.azuredatalakestore.net'...");


            Console.WriteLine(@"For datalake: 'ideas-sensitive-build-c14.azuredatalakestore.net'...");

            // Remvoe the fucntion GetAclStatus
            //Console.WriteLine(@"For datalake: 'ideas-prod-build-c14.azuredatalakestore.net'...");
            //dataLakeClient.GetAclStatus("ideas-prod-build-c14.azuredatalakestore.net",
            //    @"/shares/IDEAs.Prod.Data/Private/");
            //dataLakeClient.GetAclStatus("ideas-prod-build-c14.azuredatalakestore.net",
            //    @"/shares/CFR.ppe/Internal/Pfizer/MailboxState_2021_01_11.tsv");
            //dataLakeClient.GetAclStatus("ideas-prod-build-c14.azuredatalakestore.net",
            //    @"/shares/IDEAs.Prod.Data/Publish/Usage/User/Commercial/CustomerFacing/Ppe/Cubes/TeamsActivityUserRange/Streams/v1/2021/01/TeamsActivityUser_180D_2021_01_11.ss");
        }

        private static void TorusAccessCFRFileDemo()
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

        private static void GetIDEAsProdAdlsFileSizeDemo()
        {
            Console.WriteLine(dataLakeClient.GetFileSize("ideas-prod-c14.azuredatalakestore.net",
                "local/Scheduled/Datasets/Public/Profiles/OlsLicenses/v4/2020/06/20/LicensesCommercialHistory_2020-06-20.ss"));

            Console.WriteLine(dataLakeClient.GetFileSize("ideas-prod-c14.azuredatalakestore.net",
                "shares/IDEAs.Prod/Public/Resources/WDATPSkuMapping.ss"));
        }

        private static void GetObdTestFileSizeDemo()
        {

        }

        private static void InsertAdlsFileDemo()
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

        private static void DeleteAdlsFileDemo()
        {
            string folderPath = @"/users/jianjlv/";
            for (int i = 1; i < 11; i++)
            {
                string fileName = "datacop_service_monitor_adls_test_2019_12_" + (i > 9 ? $"{i}" : $"0{i}");
                //dataLakeClient.DeleteFile("ideas-prod-c14.azuredatalakestore.net", folderPath + fileName + ".ss");
                Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net", folderPath + fileName + ".ss"));
            }
        }

        private static void GetEnumerateAdlsMetadataEntityDemo()
        {
            //var directoryExists = dataLakeClient.CheckDirectoryExists("ideas-prod-data-c14.azuredatalakestore.net",
            //                "shares/IDEAs.Prod.Data");
            //Console.WriteLine(directoryExists);
            //var entities = dataLakeClient.EnumerateAdlsMetadataEntity("ideas-prod-data-c14.azuredatalakestore.net",
            //                "/shares/IDEAs.Prod.Data/Publish.Usage.User.Neutral.Reporting.Dashboards.AppDashboard.AverageDAU.Excel/");
            var entities = dataLakeClient.EnumerateAdlsMetadataEntity("ideas-prod-build-c14.azuredatalakestore.net",
                            "/shares/CFR.Gal/");

            foreach (var entity in entities)
            {
                Console.WriteLine(entity);
            }

            entities = dataLakeClient.EnumerateAdlsMetadataEntity("ideas-prod-build-c14.azuredatalakestore.net",
                            "/shares/CFR.Ppe/");

            foreach (var entity in entities)
            {
                Console.WriteLine(entity);
            }

            entities = dataLakeClient.EnumerateAdlsMetadataEntity("ideas-prod-build-c14.azuredatalakestore.net",
                            "/shares/CFR.Prod/");

            foreach (var entity in entities)
            {
                Console.WriteLine(entity);
            }

            Console.WriteLine("`````````````");
            var paths = dataLakeClient.EnumerateAdlsNexLevelPath("ideas-prod-build-c14.azuredatalakestore.net",
                            "/shares/");

            foreach (var path in paths)
            {
                Console.WriteLine(path);
            }

            paths = dataLakeClient.EnumerateAdlsNexLevelPath("ideas-prod-build-c14.azuredatalakestore.net",
                           "/shares/User360_Shared/");

            foreach (var path in paths)
            {
                Console.WriteLine(path);
            }

            paths = dataLakeClient.EnumerateAdlsNexLevelPath("ideas-prod-build-c14.azuredatalakestore.net",
                           "/shares/User360_SharedTest/");

            foreach (var path in paths)
            {
                Console.WriteLine(path);
            }
        }

        private static string GetPathPrefix(string streamPath)
        {
            streamPath = streamPath.Trim('/');
            if (string.IsNullOrEmpty(streamPath))
            {
                return string.Empty;
            }

            string pathPrefix = string.Empty;
            var splits = streamPath.Split(new char[] { '/' });

            if (streamPath.StartsWith("share"))
            {
                for (int i = 0; i < 3 && i < splits.Length; i++)
                {
                    pathPrefix += "/" + splits[i];

                }
            }
            else
            {
                for (int i = 0; i < 2 && i < splits.Length; i++)
                {
                    pathPrefix += "/" + splits[i];
                }
            }

            return pathPrefix;
        }

        private static IList<string> GetAllLevelPaths(string streamPath)
        {
            IList<string> result = new List<string>();
            streamPath = streamPath.Trim('/');
            if (string.IsNullOrEmpty(streamPath))
            {
                return result;
            }

            string pathPrefix = string.Empty;
            var splits = streamPath.Split(new char[] { '/' });

            for (int i = 0; i < splits.Length; i++)
            {
                pathPrefix += "/" + splits[i];
                if (i > 0)
                {
                    result.Add(pathPrefix);
                }
            }

            return result;
        }
    }
}
