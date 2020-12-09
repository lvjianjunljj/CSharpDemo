namespace AdlsDemo
{
    using AdlsDemo.FileOperation;
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
            //clientId = secretProvider.GetSecretAsync("datacop-prod", "IDEAsBuildVNextAppId").Result;
            //clientKey = secretProvider.GetSecretAsync("datacop-prod", "IDEAsBuildVNextAppSecret").Result;

            Console.WriteLine($"clientId: {clientId}");
            Console.WriteLine($"clientKey: {clientKey}");
            dataLakeClient = new DataLakeClient(tenantId, clientId, clientKey);

            CheckAdlsFileExistsDemo();
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

            //CheckBuildStreamsAvailability();
        }

        private static void CheckBuildStreamsAvailability()
        {
            Console.WriteLine("Check Build streams availability By tsv: ");
            string tsvFilePath = @"D:\data\company_work\M365\IDEAs\build\sharepaths.tsv";
            string noShareSettingTsvFilePath = @"D:\data\company_work\M365\IDEAs\build\nosharesettingpaths.tsv";
            string noPermissionTsvFilePath = @"D:\data\company_work\M365\IDEAs\build\nopermissionpaths.tsv";
            var dataLakeStore = @"ideas-prod-build-c14.azuredatalakestore.net";
            HashSet<string> noShareSetting = new HashSet<string>();
            HashSet<string> noPermission = new HashSet<string>();

            var streamPaths = File.ReadAllLines(tsvFilePath, Encoding.UTF8);
            foreach (var streamPath in streamPaths)
            {
                string rootPath = GetPathPrefix(streamPath.ToString());
                try
                {
                    if (!dataLakeClient.CheckDirectoryExists(dataLakeStore, rootPath))
                    {
                        noShareSetting.Add(rootPath);
                    }
                }
                catch (AdlsException e)
                {
                    if (e.HttpStatus == HttpStatusCode.Forbidden)
                    {
                        Console.WriteLine($"No permission for dataLakeStore '{dataLakeStore}' and path '{rootPath}'");
                    }
                    else
                    {
                        throw;
                    }
                }

                if (!dataLakeClient.CheckPermission(dataLakeStore, streamPath))
                {
                    noPermission.Add(streamPath);
                }
            }

            Console.WriteLine("noShareSetting: ");
            var noShareSettingList = noShareSetting.ToList();
            var noPermissionList = noPermission.ToList();
            noShareSettingList.Sort();
            noPermissionList.Sort();
            foreach (var streamPath in noShareSettingList)
            {
                Console.WriteLine(streamPath);
            }
            WriteFile.Save(noShareSettingTsvFilePath, noShareSettingList);

            Console.WriteLine("noPermission: ");
            foreach (var streamPath in noPermissionList)
            {
                Console.WriteLine(streamPath);
            }
            WriteFile.Save(noPermissionTsvFilePath, noPermissionList);
        }

        private static void CheckPermission()
        {
            Console.WriteLine("CheckPermission:");
            var dataLakeStores = new List<string>
            {
                //@"ideas-ppe-c14.azuredatalakestore.net",
                //@"ideas-prod-c14.azuredatalakestore.net",
                //@"ideas-prod-data-c14.azuredatalakestore.net",
                @"ideas-prod-build-c14.azuredatalakestore.net",
            };
            var pathPrefixs = new List<string>()
            {
                //"shares/IDEAs.Prod.Data/Publish/Usage/Device/Neutral/Metrics/Field/ModernLife/WindowsEdu/Streams/v1/2020/10/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Device/Neutral/Metrics/Field/ModernLife/Win10EduActivationsByTPId/Streams/v1/2020/10/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Device/Neutral/Metrics/Field/ModernLife/Win10EduDailyActivations/Streams/v1/2020/10/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Device/Neutral/Metrics/Field/ModernLife/WindowsOEMPro/Streams/v1/2020/10/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Device/Neutral/Metrics/Field/ModernLife/Win10OEMProActivationsByTPId/Streams/v1/2020/10/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Device/Neutral/Metrics/Field/ModernLife/Win10ConsumerOEMProDaily/Streams/v1/2020/10/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Device/Neutral/Metrics/Field/ModernLife/WindowsModern/Streams/v1/2020/10/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Device/Neutral/Metrics/Field/ModernLife/Win10ModernActivationsByGeo/Streams/v1/2020/10/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Device/Neutral/Metrics/Field/ModernLife/Win0ModernDailyActivations/Streams/v1/2020/10/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Device/Neutral/Metrics/Field/ModernLife/WindowsConsumer/Streams/v1/2020/10/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Device/Neutral/Metrics/Field/ModernLife/Win10ConsumerActivationsByGeo/Streams/v1/2020/10/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Tenant/Commercial/Metrics/Field/ModernWorkplace/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Tenant/Commercial/Metrics/Field/SecurityAndCompliance/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Tenant/Commercial/Metrics/FieldLG/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Tenant/Commercial/Metrics/Partner/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Tenant/Commercial/Metrics/PartnerLG/",
                //"shares/IDEAs.Prod.Data/Publish/Usage/Device/Neutral/Metrics/Field/ModernLife/",
                //"local/Collect/General/DBDigitalAnalytics/KAS/v1/",
                //"local/Publish/Acquisitions/User/Neutral/LinkedInAudience/Streams/v1/",
                //"local/Variant/Profiles/Subscription/Consumer/Metrics/Dimensions/DimAcquisitionChannel/Streams/v1/"
                //"shares/ideas.prod.data/private/Usage/Device/Neutral/Metrics/Field/ModernLife/",
                //"shares/exchange.storage.prod/local/Aggregated/Datasets/Private/FocusedInbox/FISuccessfulUser/FI_SuccessfulUser_Commercial",
                //"shares/AD_DataAnalytics/AD_DataAnalytics/",
                //"shares/AD_DataAnalytics/TenantSaasAppUsage/",
                //"shares/adPlatform.AudienceIntelligence.Scoring.proxy/PublicShare/UserSegment/Snapshots/IBT/",
                //"shares/adPlatform.AudienceIntelligence.Scoring.proxy/PublicShare",
                //"shares/AD_DataAnalytics/AD_DataAnalytics/Resources/Views/IDEAs/IDEAsPolicyMapping.view",
                //"shares/onedrive.data/public/Resources/Views/Public/ODC_State_Users.view",
                //"shares/bus.prod/local/office/Odin/Action/OfficeDataAction.view",
                //"shares/IDEAs.Prod.Data/Private/Profiles/Subscription/Commercial/IDEAsPrivateSubscriptionProfile/Streams/v1"
                //"shares/IDEAs.Prod.Data/Publish/Profiles/User/Commercial/Internal/IDEAsUserSKUProfile/Streams/v1/2020/11/UserSKUsStats_2020_11_08.ss"
                //"/shares/User360/UserProfile/resources/LCID_Locale.ss",
                //"shares/IDEAs.Prod.Data/Publish.Usage.User.Neutral.Reporting.Dashboards.AppDashboard.AverageDAU.Excel/",
                //"/shares/User360/user360.local/projects/UserSegmentation/UserProfile/",
                //"/shares/User360_Shared/user360_shared.local/",
                //"/shares/User360_Shared/user360_shared.local/Upload/Skype/Production/",

                //"/shares/CFR.ppe/MWS/Features/UserAttachment_001D",
                //"/shares/CFR.ppe/MWS/Features",
                //"/shares/CFR.ppe/MWS",
                //"/shares/CFR.ppe",
                //"/shares/CFR.prod/MWS/Features/UserAttachment_001D",
                //"/shares/CFR.prod/MWS/Features",
                //"/shares/CFR.prod/MWS",
                //"/shares/CFR.prod",

                "shares/IDEAs.Prod.Data/Publish/Profiles/Subscription/Commercial/IDEAsCommercialSubscriptionProfile/Views/v3/IDEAsCommercialSubscriptionProfile.view"

            };
            foreach (var dataLakeStore in dataLakeStores)
            {
                Console.WriteLine($"DataLakeStore: {dataLakeStore}");
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
                    if (!dataLakeClient.CheckPermission(dataLakeStore, path))
                    {
                        Console.WriteLine($"Have no permission for pathPrefix: '{path}'");
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
            //Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net",
            //    "/shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/Internal/IDEAsTenantProfile/PostValidation/Streams/v3/2020/09/TenantProfileStats_2020_09_16.ss"));
            //Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-build-c14.azuredatalakestore.net",
            //    "shares/IDEAs.Prod.Data/Publish/Profiles/User/Commercial/Internal/IDEAsUserSKUProfile/Streams/v1/2020/11/UserSKUsStats_2020_11_08.ss"));
            //Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-c14.azuredatalakestore.net",
            //    "shares/ffo.prod/local/Aggregated/Datasets/Private/MIGMetrics/MIGFeaturesMapping.ss"));

            //Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-build-c14.azuredatalakestore.net", "/shares/OCV/OfficeCustomerVoice-Prod/OcvItems.ss"));
            //Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-build-c14.azuredatalakestore.net", "/shares/ocv/OfficeCustomerVoice-Prod/OcvItems.ss"));

            Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-build-c14.azuredatalakestore.net", "/shares/ffo.antispam/partner/IDEAs/ATPUsage/ATPMapping.ss"));
            Console.WriteLine(dataLakeClient.CheckExists(
                "ideas-prod-build-c14.azuredatalakestore.net",
                "shares/IDEAs.Prod.Data/Publish/Profiles/Subscription/Commercial/IDEAsCommercialSubscriptionProfile/Views/v3/IDEAsCommercialSubscriptionProfile.view"));
            Console.WriteLine(dataLakeClient.CheckExists(
                "ideas-prod-build-c14.azuredatalakestore.net",
                "/shares/IDEAs.Prod.Data/Collect/General/DBDigitalAnalytics/Marin/v1/2020/06/DimDimensions_2020_06_30.tsv"));
            Console.WriteLine(dataLakeClient.CheckExists(
                "ideas-prod-build-c14.azuredatalakestore.net",
                "/shares/IDEAs.Prod.Data/Collect/General/DBDigitalAnalytics/Marin/v1/2020/06/DimKeyword_2020_06_30.tsv"));
            Console.WriteLine(dataLakeClient.CheckDirectoryExists(
                "ideas-prod-build-c14.azuredatalakestore.net",
                "/shares/IDEAs.Prod.Data/Collect/"));
            Console.WriteLine(dataLakeClient.CheckDirectoryExists(
                "ideas-prod-build-c14.azuredatalakestore.net",
                "/shares/IDEAs.Prod.Data/Collect"));
            Console.WriteLine(dataLakeClient.CheckDirectory(
                "ideas-prod-build-c14.azuredatalakestore.net",
                "/shares/IDEAs.Prod.Data/Collect"));
            DateTime now = DateTime.UtcNow.Date;
            for (int i = 0; i < 60; i++)
            {
                DateTime date = now.AddDays(-i);
                Console.WriteLine($"For date: {date:yyyy}-{date:MM}-{date:dd}");
                Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-data-c14.azuredatalakestore.net",
                    $"local/Publish/Usage/User/Neutral/Reporting/Dashboards/AppDashboard/External/NPS/Streams/v1/{date:yyyy}/{date:MM}/NPSJson_{date:yyyy}_{date:MM}_{date:dd}.ndjson"));
                Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-data-c14.azuredatalakestore.net",
                    $"local/Publish/Usage/User/Neutral/Reporting/Dashboards/AppDashboard/External/NPS/Streams/v1/{date:yyyy}/{date:MM}/NPSJson_{date:yyyy}_{date:MM}_{date:dd}.json"));
                Console.WriteLine(dataLakeClient.CheckExists("ideas-prod-data-c14.azuredatalakestore.net",
                    $"local/Publish/Usage/User/Neutral/Reporting/Dashboards/AppDashboard/External/NPS/Streams/v1/{date:yyyy}/{date:MM}/NPS_{date:yyyy}_{date:MM}_{date:dd}.ss"));
            }
            // We don't have access to store "ideas-ppe-c14.azuredatalakestore.net". It will throw exception: forbidden
            //Console.WriteLine(dataLakeClient.CheckExists("ideas-ppe-c14.azuredatalakestore.net", "/local/build/user/dekashet/TeamsMeetingProdAfterTimeZoneFixApril18th/directViewCodeWithAdjustEndDate.csv"));
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
    }
}
