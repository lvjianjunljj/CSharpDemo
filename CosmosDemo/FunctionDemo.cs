namespace CosmosDemo
{
    using CosmosDemo.CosmosView;
    using Newtonsoft.Json.Linq;
    using ScopeClient;
    using System;
    using System.Collections.Generic;
    using System.IO;

    public class FunctionDemo
    {
        public static void MainMethod()
        {
            //CheckStreamExists();
            //CheckRowCount();
            //CheckDirectoryExists();

            //GetRowCountIteratively("2019-07-10T00:00:00.0000000Z");

            //GetStreamInfosDemo();
            //GetFileStream();

            //CompareStreamInfoRowCount();

            //UploadFileDemo();
            //DownloadFileDemo();
            DownloadViewScripts();
            // 

            /*
             * Not work with the error message:
             * E_CSC_SYSTEM_INTERNAL: Internal error! Could not load file or assembly 'ScopeEngineManaged, Version=10.2.0.0, Culture=neutral, 
             * PublicKeyToken=a06e40edc83d4f79' or one of its dependencies. An attempt was made to load a program with an incorrect format.
             *
             * Not yet fixed it.
             */
            //CheckCosmosViewAvailability();
        }

        public static void CheckStreamExists()
        {
            string stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants/2019/07/12/TenantsHistory_2019_07_12.ss";
            //stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants/TenantsHistory.ss";
            //stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants";
            //stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Ppe/local/ParquetConverter.py";

            // This link is not able to be accessed
            //stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Ppe/shares/CFR.ppe/local/Cooked/StateUserDirectory/StateUserDirectory_2019_07_24.ss";InnerException

            //stream = "https://cosmos14.osdinfra.net/cosmos/Ideas.prod//users/jianjlv/datacop_service_monitor_test_2019_12_07.ss";
            stream = @"https://cosmos14.osdinfra.net/cosmos/ideas.ppe/shares/bus.prod/local/office/Odin/Action/OfficeDataAction.view";
            stream = @"https://cosmos14.osdinfra.net/cosmos/Ideas.prod.build/shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/IDEAsTenantProfile/Views/v1/IDEAsTenantProfile.view";
            Console.WriteLine(CosmosClient.CheckStreamExists(stream));
        }

        public static void CheckRowCount()
        {
            string stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/users/jianjlv/datacop_service_monitor_test_2019_12_01.ss";
            //string stream = @"https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/ExoAdTenantConfigUnitGallatin/TenantConfigUnitRaw/Streams/v1/2020/04/TenantConfigUnitRaw_2020_04_10.ss";
            //stream = @"https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod//shares/IDEAs.Prod.Data/Publish/Profiles/User/Commercial/ExoAdUserWorldwide/TenantUserSnapshot/Streams/v1/2020/04/TenantUserSnapshot_2020_04_14.ss";
            stream = @"https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/shares/IDEAs.Prod.Data/local/Publish/Acquisitions/User/Neutral/Clickstream/Raw/Streams/v1/2020/01/RawClickstream_2020_01_25.ss";
            long? rowCount = CosmosClient.GetRowCount(stream);
            Console.WriteLine($"rowCount: {rowCount}");
        }

        public static void CheckDirectoryExists()
        {
            string directoryPath = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/";
            directoryPath = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod.Data/";
            directoryPath = @"https://cosmos14.osdinfra.net/cosmos/Ideas.prod//shares/IDEAs.Prod.Data/Publish/Usage/User/Commercial/CountedActions/TeamsApps/Streams/v1/";
            directoryPath = @"https://cosmos14.osdinfra.net/cosmos/Ideas.prod.build/local/";
            directoryPath = @"https://cosmos14.osdinfra.net/cosmos/IDEAs.ppe/shares/IDEAs.Prod.Data/Publish/Usage/Device/Neutral/Metrics/Field/ModernLife/WindowsModern/Streams/v1/2020/10/";
            directoryPath = @"https://cosmos14.osdinfra.net/cosmos/Ideas.prod.build/shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/";


            Console.WriteLine(CosmosClient.CheckDirectoryExists(directoryPath));
        }

        public static void GetRowCountIteratively(string startDateString)
        {
            DateTime date = DateTime.Parse(startDateString);
            while (date < DateTime.Now)
            {
                Console.WriteLine(date);

                var year = date.Year.ToString();
                var month = date.Month.ToString("00");
                var day = date.Day.ToString("00");

                // For function GetRowCount, we just can get the row count value for file whose name ends with .ss, or it will throw an exception. Such as the below steam path, it will throw exception. But for function VC.StreamExists is OK.
                //string stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Ppe/local/ParquetConverter.py";

                string stream = $"https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants/{year}/{month}/{day}/TenantsHistory_{year}_{month}_{day}.ss";

                long? rowCount = CosmosClient.GetRowCount(stream);
                Console.WriteLine(rowCount);
                date = date.AddDays(1);
            }
        }

        public static void GetStreamInfosDemo()
        {
            string directoryPath = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod//local/Partner/PreRelease/dev/activeusage/sharepointcommercial/2019/08/";

            List<JToken> streamInfos = CosmosClient.GetStreamInfos(directoryPath);
            foreach (var streamInfo in streamInfos)
            {
                Console.WriteLine(streamInfo);
            }

            directoryPath = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod//local/Partner/PreRelease/dev/activeusage/sharepointcommercial/2019/";
            streamInfos = CosmosClient.GetStreamInfos(directoryPath);
            foreach (var streamInfo in streamInfos)
            {
                Console.WriteLine(streamInfo);
            }
        }

        public static void UploadFileDemo()
        {
            string directoryPath = @"https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/users/jianjlv/";
            string folderPath = @"D:\data\company_work\M365\IDEAs\DataCopServiceMonitor\datacop_service_monitor_test_file\";
            for (int i = 1; i < 11; i++)
            {
                string fileName = "datacop_service_monitor_test_2019_12_" + (i > 9 ? $"{i}" : $"0{i}");
                Console.WriteLine(CosmosClient.CheckExists(directoryPath + fileName + ".ss", out long rowCount));
                Console.WriteLine(rowCount);
                //CosmosClient.UploadFile(folderPath + fileName + ".ss", directoryPath + fileName + ".ss", 365);
            }
        }

        public static void DownloadFileDemo()
        {
            string streamPath = "https://cosmos14.osdinfra.net/cosmos/Ideas.prod.build/" +
                "shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/IDEAsTenantProfile/Views/v1/IDEAsTenantProfile.view";

            var stream = CosmosClient.ReadStream(streamPath);
            StreamReader reader = new StreamReader(stream);
            File.WriteAllText(@"D:\data\company_work\M365\IDEAs\datacop\cosmosworker\marked_aborted\IDEAsTenantProfile.view", reader.ReadToEnd());
        }

        private static void DownloadViewScripts()
        {
            var testRunsJArrayPath = @"D:\data\company_work\M365\IDEAs\datacop\cosmosworker\builddeployment\testRuns.json";
            JArray testRuns = JArray.Parse(File.ReadAllText(testRunsJArrayPath));
            foreach (var testRun in testRuns)
            {
                var datasetId = testRun["datasetId"].ToString();
                var cosmosVC = testRun["cosmosVC"].ToString();
                var cosmosScriptContent = testRun["cosmosScriptContent"].ToString();
                Console.WriteLine(datasetId);

                if (!cosmosVC.EndsWith("/"))
                {
                    Console.WriteLine(datasetId);
                    Console.WriteLine(cosmosVC);
                    cosmosVC += "/";
                }

                var startContent = "ViewSamples = VIEW \"";
                var endContent = "\"";
                cosmosScriptContent = cosmosScriptContent.Substring(startContent.Length);
                var scriptRelativePath = cosmosScriptContent.Substring(0, cosmosScriptContent.IndexOf(endContent));
                var scriptPath = cosmosVC + scriptRelativePath;
                var stream = CosmosClient.ReadStream(scriptPath);
                StreamReader reader = new StreamReader(stream);

                File.WriteAllText($@"D:\data\company_work\M365\IDEAs\datacop\cosmosworker\builddeployment\views\{datasetId}.view", reader.ReadToEnd());
            }
        }

        public static void GetFileStream()
        {
            string streamPath = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants/TenantsHistory.ss";

            var stream = CosmosClient.ReadStream(streamPath);
            StreamReader reader = new StreamReader(stream);
            Console.WriteLine(reader.ReadToEnd());
        }

        public static void CheckCosmosViewAvailability()
        {
            string viewPath = @"shares/IDEAs.Prod.Data/Publish/Profiles/Subscription/Consumer/IDEAsConsumerPerpetualProfile/Views/v2/IDEAsConsumerPerpetualProfile.view";
            viewPath = @"https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/shares/IDEAs.Prod.Data/Publish/Usage/User/Commercial/ActiveUsage/A310/Views/v1/A310TenantActiveUsage.view";
            viewPath = @"https://cosmos14.osdinfra.net/cosmos/ideas.ppe/shares/bus.prod/local/office/Odin/Action/OfficeDataAction.view";


            //List<CosmosViewParameter> viewParameters = new List<CosmosViewParameter>
            //{
            //    new CosmosViewParameter
            //    {
            //        Name = "StartDate",
            //        Type = typeof(DateTime),
            //        Value = "@@TestDate@@",
            //    },
            //    new CosmosViewParameter
            //    {
            //        Name = "EndDate",
            //        Type = typeof(DateTime),
            //        Value = "@@TestDate@@",
            //    }
            //};

            List<CosmosViewParameter> viewParameters = new List<CosmosViewParameter>
                                                        {
                                                            new CosmosViewParameter
                                                            {
                                                                Name = "PurchaseDate",
                                                                Type = typeof(DateTime),
                                                                Value = "@@TestDate@@",
                                                            }
                                                        };

            DateTime testDate = new DateTime(2020, 9, 29);

            string cosmosScriptContent = CosmosViewClient.BuildScriptForViewAvailabilityTest(viewPath, viewParameters);
            cosmosScriptContent = "ViewSamples = VIEW \"shares/IDEAs.Prod.Data/Publish/Profiles/Tenant/Commercial/IDEAsTenantServicePlanProfile/Views/v1/IDEAsTenantServicePlanProfile.view\"PARAMS(HistoryDate = DateTime.Parse(@@TestDate@@));OUTPUT ViewSamples TO \"/my/output.tsv\" USING DefaultTextOutputter();";
            var scriptToCompile = cosmosScriptContent.Replace("@@TestDate@@", "\"" + testDate.ToString() + "\"");

            CosmosViewClient.CheckViewAvailability(scriptToCompile, out string output);
            Console.WriteLine(output);
        }
    }
}
