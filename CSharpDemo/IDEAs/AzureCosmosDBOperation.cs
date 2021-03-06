﻿namespace CSharpDemo.IDEAs
{
    using System;
    using System.Collections.Generic;
    using Newtonsoft.Json.Linq;
    using System.Threading;
    using Newtonsoft.Json;
    using AzureLib.KeyVault;
    using System.Text.RegularExpressions;
    using CSharpDemo.FileOperation;
    using System.Linq;
    using System.IO;
    using System.Text;
    using System.Diagnostics;
    using CommonLib.IDEAs;
    using AzureLib.CosmosDB;

    public class AzureCosmosDBOperation
    {
        public static void MainMethod()
        {
            // for datacop: "datacop-ppe" and "datacop-prod"
            Initialize("datacop-prod");

            //UpdateAllAlertSettingsDemo();
            //UpdateAllDatasetTestCreatedBy();
            //UpdateAllDatasetForMerging();
            //UpdateAllDatasetTestForMerging();
            //UpdateAllCosmosTestResultExpirePeriod();
            //UpdateAllCosmosTestCreateTime();
            //UpdateAlertSettingToGitFolder();
            //UpsertServiceMonitorDemo();
            //UpdateVcToBuild();
            //UpdateScheduleMonitorReportSampleDemo();
            //UpdateCosmosViewScriptContent();
            //ResetIncidentIdForMonitorReportDemo();

            //DisableAllCFRMonitor();
            //InsertCFRMonitorConfig();
            //UpsertDatasetDemo();
            //UpsertDatasetTestDemo();
            //UpdateBuildDeploymentViewBooleanParameters();
            //UpdateWrongDataFabricInDatasets();


            //DisableDatasets();
            //EnableDatasets();
            //DisableAllCosmosDatasetTest();
            //EnableAllCosmosDatasetTestSuccessively();
            //EnableAllCosmosDatasetTestWhenNoActiveMessage();

            GetTestRunMessagesForEveryDataset();
            //QueryTestRunStatusForDatasets();
            //GetDataFactoriesInCosmosViewMonitors();
            //GetDatasetsCountForEveryDataFactory();
            //GetDisabledDatasetsWithoutRightModifiedTime();

            //QueryAlertSettingDemo();
            //QueryScheduleMonitorReportDemo();
            //QueryAlertSettingInDatasetTestsDemo();
            //QueryDataSetDemo();
            //QueryTestRunTestContentDemo();
            //QueryMonitroReportDemo();
            //QueryServiceMonitorDemo();
            //QueryTestRunCount();
            //QueryAlertCount();
            //QueryKenshoDataset();
            //QueryMonthlyTestRunCount();
            //QueryTestRuns();
            //QueryTestRunsByDatasets();
            //QueryForbiddenTestRuns();
            //QueryDataCopScores();
            //QueryDatasets();
            //QueryDatasetTests();
            //QueryServiceMonitorReports();
            //QueryDatasetCount();
            //QueryTestRunCountByDatasetId();
            //GetAlertSettingsForEveryDataFactory();


            //DeleteTestRunDemo();
            //DeleteTestRuns();
            //DeleteWaitingOrchestrateTestRuns();
            //DeleteCosmosTestRunByResultExpirePeriod();
            // We can use this function to delete instance without any limitation.
            //DeleteAlertsWithoutIncidentId();
            //DeleteWrongAlertsFromDataCopTest();

            //string prodEndpoint = secretProvider.GetSecretAsync("datacop-prod", "CosmosDBEndPoint").Result;
            //string prodKey = secretProvider.GetSecretAsync("datacop-prod", "CosmosDBAuthKey").Result;
            //string ppeEndpoint = secretProvider.GetSecretAsync("datacop-ppe", "CosmosDBEndPoint").Result;
            //string ppeKey = secretProvider.GetSecretAsync("datacop-ppe", "CosmosDBAuthKey").Result;
            //IList<string> filters = new List<string>
            //{
            //    @"c.level = 'DataCop'",
            //    @"c.reportStartTimeStamp > '2020-02-22T15:00:00'"
            //};
            //MigrateData("DataCop", "ServiceMonitorReport", filters, prodEndpoint, prodKey, ppeEndpoint, ppeKey);
            //MigrateData("DataCop", "Dataset", @"SELECT * FROM c where c.dataFabric = 'CosmosStream'", prodEndpoint, prodKey, ppeEndpoint, ppeKey);
            //MigrateData("DataCop", "DatasetTest", @"SELECT * FROM c where (c.testContentType = 'CosmosAvailability' or c.testContentType = 'CosmosCompleteness') and c.status = 'Enabled'", prodEndpoint, prodKey, ppeEndpoint, ppeKey);

            //AddCompletenessMonitors4ADLS();

            //CheckDatasetTestIntegrity();
            //CheckPPEAlertsettingOwningTeamAndRouting();
            //CheckAdlsConnectionInfoMappingCorrectness();
            //CheckCosmosConnectionInfoMappingCorrectness();
            //CheckDuplicatedEnabledDatasetTest();

            //SetOutdatedForDuplicatedDatasetTest();

            //DisableAbortedTest();

            //UpdateSqlDatasetKeyVaultName();
            //UpdateTestRunStatus();
            //CreateContainers();
            //ShowADLSStreamPathPrefix();
            //ShowADLSStreamPathWithoutDate();
            //ShowStreamPathsWithoutDate();
            //GetNonAuthPath();
            //GetTimeCostDemo();


            // For cloudscope
            // For cosmos db data torus migration, we cannot get secret from different tenant with one account
            // So I just define the endpoint and key but not get them from key vault
            // This SDK doesn't work for CosmosDB Table, we need to use SDK Microsoft.Azure.Cosmos.Table
            //string microsoftEndPoint = "https://cloudscopeppe.table.cosmos.azure.com:443/";
            //string microsoftKey = "";
            //string torusEndpoint = "https://cloudscope-ppe.table.cosmos.azure.com:443/";
            //string torusKey = "";
            //MigrateData("TablesDB", "TestPerf", null, microsoftEndPoint, microsoftKey, torusEndpoint, torusKey);
        }

        private static void GetNonAuthPath()
        {
            //string testRunsQueryStr = @"SELECT top 1000 * FROM c WHERE c.status = 'Aborted' and (c.dataFabric = 'ADLS' or contains(c.dataFabric, 'Cosmos')) order by c.createTime desc";
            string nonAuthTestRunsQueryStr = @"SELECT distinct c.datasetId FROM c WHERE c.status = 'Aborted' and (c.dataFabric = 'ADLS' or contains(c.dataFabric, 'Cosmos')) and contains(c.message, 'HttpStatus:Forbidden') and c.createTime > '2020-10-07' order by c.createTime desc";
            string authTestRunsQueryStr = @"SELECT distinct c.datasetId FROM c WHERE (c.status = 'Success' or c.status = 'Failed') and (c.dataFabric = 'ADLS' or contains(c.dataFabric, 'Cosmos')) and c.createTime > '2020-10-07'  order by c.createTime desc";
            string datasetInfosQueryStr = @"SELECT * FROM c WHERE c.isEnabled = true and (c.dataFabric = 'ADLS' or contains(c.dataFabric, 'Cosmos'))";

            var azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            IList<JObject> nonAuthTestRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((nonAuthTestRunsQueryStr)).Result;
            Dictionary<string, JObject> datasets = GetIdJTokenDict(datasetInfosQueryStr, "Dataset");

            Console.WriteLine("nonAuthTestRuns");
            Dictionary<string, HashSet<string>> nonAuthDict = new Dictionary<string, HashSet<string>>();
            foreach (JObject nonAuthTestRun in nonAuthTestRuns)
            {
                string datasetId = nonAuthTestRun["datasetId"].ToString();
                if (!datasets.ContainsKey(datasetId))
                {
                    continue;
                }
                var dataset = datasets[datasetId];

                string dataFabric = dataset["dataFabric"].ToString();
                string dataLakeStore = dataset["connectionInfo"]["dataLakeStore"]?.ToString().Trim(new char[] { '/' }).ToLower();
                string cosmosVC = dataset["connectionInfo"]["cosmosVC"]?.ToString().Trim(new char[] { '/' }).ToLower();
                string streamPath = dataset["connectionInfo"]["streamPath"]?.ToString().Trim(new char[] { '/' }).ToLower();
                string pathPrefix = GetPathPrefix(streamPath);

                string key = dataFabric;
                if (dataFabric.Equals("ADLS"))
                {
                    key += "\t" + dataLakeStore + "\t" + pathPrefix;
                }
                else if (dataFabric.Equals("CosmosStream") || dataFabric.Equals("CosmosView"))
                {
                    key += "\t" + cosmosVC + "\t" + pathPrefix;
                }
                else
                {
                    throw new NotImplementedException($"Not support this dataFabric '{dataFabric}'");
                }

                if (!nonAuthDict.ContainsKey(key))
                {
                    nonAuthDict.Add(key, new HashSet<string>());
                }
                nonAuthDict[key].Add(datasetId);
            }

            var outPut = new List<string>();
            foreach (var key in nonAuthDict.Keys)
            {
                outPut.Add(key);
            }
            outPut.Sort();
            foreach (var key in outPut)
            {
                Console.WriteLine(key);
                foreach (var datasetId in nonAuthDict[key])
                {
                    Console.WriteLine(datasetId);
                }
            }
            Console.WriteLine();

            Console.WriteLine("authTestRuns");
            IList<JObject> authTestRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((authTestRunsQueryStr)).Result;
            Dictionary<string, HashSet<string>> authDict = new Dictionary<string, HashSet<string>>();
            foreach (JObject authTestRun in authTestRuns)
            {
                string datasetId = authTestRun["datasetId"].ToString();
                if (!datasets.ContainsKey(datasetId))
                {
                    continue;
                }
                var dataset = datasets[datasetId];

                string dataFabric = dataset["dataFabric"].ToString();
                string dataLakeStore = dataset["connectionInfo"]["dataLakeStore"]?.ToString().Trim(new char[] { '/' }).ToLower();
                string cosmosVC = dataset["connectionInfo"]["cosmosVC"]?.ToString().Trim(new char[] { '/' }).ToLower();
                string streamPath = dataset["connectionInfo"]["streamPath"]?.ToString().Trim(new char[] { '/' }).ToLower(); ;
                string pathPrefix = GetPathPrefix(streamPath);

                string key = dataFabric;
                if (dataFabric.Equals("ADLS"))
                {
                    key += "\t" + dataLakeStore + "\t" + pathPrefix;
                }
                else if (dataFabric.Equals("CosmosStream") || dataFabric.Equals("CosmosView"))
                {
                    key += "\t" + cosmosVC + "\t" + pathPrefix;
                }
                else
                {
                    throw new NotImplementedException($"Not support this dataFabric '{dataFabric}'");
                }

                if (!authDict.ContainsKey(key))
                {
                    authDict.Add(key, new HashSet<string>());
                }
                authDict[key].Add(datasetId);
            }

            outPut = new List<string>();
            foreach (var key in authDict.Keys)
            {
                outPut.Add(key);
            }

            outPut.Sort();
            foreach (var key in outPut)
            {
                Console.WriteLine(key);
                foreach (var datasetId in authDict[key])
                {
                    Console.WriteLine(datasetId);
                }
            }
        }

        private static void ShowADLSStreamPathPrefix()
        {
            var result = GetADLSStreamPaths(GetPathPrefix);
            WriteFile.FirstMethod(@"D:\data\company_work\M365\IDEAs\pathPrefixs.json", result);
        }
        private static void ShowADLSStreamPathWithoutDate()
        {
            var result = GetADLSStreamPaths(GetPathWithoutDateInfo);
            WriteFile.FirstMethod(@"D:\data\company_work\M365\IDEAs\pathsWithoutDate.json", result);
        }

        private static void ShowStreamPathsWithoutDate()
        {
            IList<string> properties = new List<string>
            {
                "dataFabric",
                "connectionInfo"
            };
            IList<string> filters = new List<string>
            {
                @"(c.dataFabric = 'ADLS' or contains(c.dataFabric, 'Cosmos'))",
                @"c.isEnabled = true",
            };
            IList<JObject> datasets = GetQueryResult("DataCop", "Dataset", properties, filters);
            StringBuilder sb = new StringBuilder();
            foreach (var dataset in datasets)
            {
                string streamPath = GetPathWithoutDateInfo(dataset["connectionInfo"]["streamPath"].ToString().Trim(new char[] { '/' }));
                var line = dataset["dataFabric"] + "\t" +
                    dataset["connectionInfo"]["dataLakeStore"] + "\t" +
                    dataset["connectionInfo"]["cosmosVC"] + "\t" +
                    streamPath + "\n";
                Console.WriteLine(line);
                sb.Append(line);
            }
            File.WriteAllText(@"D:\data\company_work\M365\IDEAs\pathsWithoutDate2.json", sb.ToString());
            Console.WriteLine(datasets.Count);
        }

        private static void UpdateCosmosViewScriptContent()
        {
            AzureCosmosDBClient datasetTestCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            // Collation: asc and desc is ascending and descending
            IList<JObject> azureDatasetTests = datasetTestCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                "SELECT * FROM c where c.testContentType = 'CosmosViewAvailability' and c.createdBy = 'BuildDeployment'")).Result;
            Console.WriteLine($"azureDataset count: '{azureDatasetTests.Count}'");

            int count = 0;
            foreach (JObject azureDatasetTest in azureDatasetTests)
            {
                Console.WriteLine(azureDatasetTest["id"]);
                count++;
                azureDatasetTest["testContent"]["cosmosScriptContent"] = azureDatasetTest["testContent"]["cosmosScriptContent"].ToString().Replace(
                    "OUTPUT ViewSamples TO \"/my/output.tsv\" USING DefaultTextOutputter();",
                    "Count = SELECT COUNT() AS NumSessions FROM ViewSamples; OUTPUT Count TO SSTREAM \"/my/output.ss\";");
                Console.WriteLine(azureDatasetTest["testContent"]["cosmosScriptContent"]);
                datasetTestCosmosDBClient.UpsertDocumentAsync(azureDatasetTest).Wait();
            }
            Console.WriteLine($"Wrong datasetTests count: {count}");
        }

        private static void GetTestRunMessagesForEveryDataset()
        {
            if (!Directory.Exists(CosmosViewErrorMessageOperation.RootFolderPath))
            {
                Directory.CreateDirectory(CosmosViewErrorMessageOperation.RootFolderPath);
            }

            DateTime nowDate = new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day);
            // Just collect the testRuns created by the last thirty days.
            string startDateStrFilter = nowDate.AddDays(-30).ToString("s");
            AzureCosmosDBClient datasetCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            AzureCosmosDBClient datasetTestCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            AzureCosmosDBClient testRunCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            // Collation: asc and desc is ascending and descending
            IList<JObject> azureDatasets = datasetCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                @"SELECT * FROM c where c.dataFabric = 'CosmosView' and c.createdBy = 'BuildDeployment' and c.isEnabled = true and not contains(c.buildEntityId, 'PLSCoreData')")).Result;
            Console.WriteLine($"azureDataset count: '{azureDatasets.Count}'");

            JArray allTestRunMessages = new JArray();
            foreach (JObject azureDataset in azureDatasets)
            {
                string buildEntityId = azureDataset["buildEntityId"].ToString();
                var dataFactoryName = buildEntityId.Substring(0, buildEntityId.IndexOf("/"));
                string datasetId = azureDataset["id"].ToString();
                string datasetName = azureDataset["name"].ToString();
                Console.WriteLine(datasetId);


                IList<JObject> successTestRuns = testRunCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                    $@"SELECT top 10 * FROM c where c.datasetId = '{datasetId}' and " +
                    $@"c.status = 'Success' and c.createTime > '{startDateStrFilter}' order by c.createTime desc")).Result;

                IList<JObject> datasetTests = datasetTestCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                    $@"SELECT * FROM c where c.datasetId = '{datasetId}'")).Result;

                if (successTestRuns.Count > 0)
                {
                    Console.WriteLine($@"There is success testRuns for dataset '{datasetId}'");
                    continue;
                }

                if (datasetTests.Count == 0)
                {
                    Console.WriteLine($@"There is no mapping datasetTest for dataset '{datasetId}'");
                    continue;
                }

                // Disable the cosmos view datasets without successful testRuns.
                //Console.ForegroundColor = ConsoleColor.Red;
                //Console.WriteLine($"Disable the cosmos view dataset '{datasetId}' without successful testRuns");
                //Console.ForegroundColor = ConsoleColor.Gray;
                //azureDataset["isEnabled"] = false;
                //datasetCosmosDBClient.UpsertDocumentAsync(azureDataset).Wait();

                IList<JObject> testRuns = testRunCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                $@"SELECT top 20 * FROM c where c.datasetId = '{datasetId}' and c.createTime > '{startDateStrFilter}' order by c.createTime desc")).Result;
                if (testRuns.Count == 0)
                {
                    Console.WriteLine($"No usefully testRun of dataset : '{datasetId}'");
                    continue;
                }

                JObject testRunMessage = new JObject
                {
                    ["datasetId"] = datasetId,
                    ["dataFactoryName"] = dataFactoryName,
                    ["datasetName"] = datasetName
                };

                var messages = new JArray();
                var testRunIds = new JArray();
                var testDates = new JArray();
                var cosmosVC = string.Empty;
                var cosmosScriptContent = string.Empty;

                foreach (var testRun in testRuns)
                {
                    // Just set the cosmosVC/cosmosScriptContent as the latest one
                    if (!string.IsNullOrEmpty(cosmosVC))
                    {
                        cosmosVC = testRun["testContent"]["cosmosVC"].ToString();
                    }

                    // Just define the cosmos script content one time based on the latest testRun.
                    if (string.IsNullOrEmpty(cosmosScriptContent))
                    {
                        cosmosScriptContent = testRun["testContent"]["cosmosScriptContent"].ToString();
                    }

                    testRunIds.Add(testRun["id"]);
                    messages.Add(testRun["message"]);
                    testDates.Add(testRun["testDate"]);
                }

                testRunMessage["cosmosVC"] = cosmosVC;
                testRunMessage["cosmosScriptContent"] = cosmosScriptContent;
                testRunMessage["testRunIds"] = testRunIds;
                testRunMessage["testDates"] = testDates;
                testRunMessage["messages"] = messages;

                allTestRunMessages.Add(testRunMessage);
            }
            File.WriteAllText(Path.Combine(CosmosViewErrorMessageOperation.RootFolderPath, @"allTestRuns.json"), allTestRunMessages.ToString());
        }

        private static string GetADLSStreamPaths(Func<string, string> getPathFunc)
        {
            IList<string> properties = new List<string>
            {
                "dataFabric",
                "connectionInfo"
            };
            IList<string> filters = new List<string>
            {
                @"(c.dataFabric = 'ADLS' or contains(c.dataFabric, 'Cosmos'))",
                @"c.isEnabled = true"
            };
            IList<JObject> datasets = GetQueryResult("DataCop", "Dataset", null, filters);
            Dictionary<string, HashSet<string>> dict = new Dictionary<string, HashSet<string>>();

            foreach (JObject dataset in datasets)
            {
                string dataFabric = dataset["dataFabric"].ToString();
                string key;
                try
                {

                    if (dataFabric.Equals("ADLS"))
                    {
                        string dataLakeStore = dataset["connectionInfo"]["dataLakeStore"]?.ToString().Trim(new char[] { '/' }).ToLower();
                        key = dataFabric + " " + dataLakeStore;
                    }
                    else
                    {
                        string cosmosVC = dataset["connectionInfo"]["cosmosVC"]?.ToString().Trim(new char[] { '/' }).ToLower();
                        key = dataFabric + " " + cosmosVC;
                    }

                    // Take care of letter case in stream path
                    // The access result will be different if letter case of  stream path changes
                    string streamPath = dataset["connectionInfo"]["streamPath"]?.ToString().Trim(new char[] { '/' });
                    string pathPrefix = getPathFunc(streamPath);
                    if (!dict.ContainsKey(key))
                    {
                        dict.Add(key, new HashSet<string>());
                    }

                    dict[key].Add(pathPrefix);
                }
                catch (InvalidOperationException e)
                {
                    Console.WriteLine("Error dataset: ");
                    Console.WriteLine(dataset);
                }
            }
            JArray result = new JArray();

            foreach (var map in dict)
            {
                JToken json = new JObject();
                json["dataFabric"] = map.Key.Split(' ')[0];

                if (json["dataFabric"].ToString().Equals("ADLS"))
                {
                    json["dataLakeStore"] = map.Key.Split(' ')[1];
                }
                else
                {
                    json["cosmosVC"] = map.Key.Split(' ')[1];
                }

                List<string> pathList = new List<string>(map.Value);
                pathList.Sort();
                JArray paths = new JArray(pathList);
                json["pathPrefixs"] = paths;
                result.Add(json);
            }

            return result.ToString();
        }

        private static Dictionary<string, JObject> GetIdJTokenDict(string queryStr, string collectionId)
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", collectionId);
            IList<JObject> jObjects = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((queryStr)).Result;
            Dictionary<string, JObject> dict = new Dictionary<string, JObject>();

            foreach (JObject jObject in jObjects)
            {
                string id = jObject["id"].ToString();
                dict.Add(id, jObject);
            }

            return dict;
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


        private static string GetPathWithoutDateInfo(string streamPath)
        {
            if (string.IsNullOrEmpty(streamPath))
            {
                return string.Empty;
            }
            var segments = streamPath.Split('/');
            var segmentsToEnumerate = new List<string>();
            foreach (var segment in segments)
            {
                if (segment.Contains("%"))
                {
                    // Find the "%" mark, so the path to enumerate should be the parent of this segment
                    break;
                }

                segmentsToEnumerate.Add(segment);
            }

            // Append slash to mark the end of the path
            return string.Join("/", segmentsToEnumerate.ToArray());
        }

        private static void CreateContainers()
        {
            // Create some defualt containers
            string[] collectionIds = new string[] {"ActiveAlertTrend",
                                        "AdlsMetadataV2",
                                        "Alert",
                                        "AlertRule",
                                        "AlertSettings",
                                        "DataCopScore",
                                        "Dataset",
                                        "DatasetTest",
                                        "RecentDataCopScoreDetails",
                                        "Scenario",
                                        "ScenarioReport",
                                        "ScenarioRun",
                                        "ServiceMonitorReport",
                                        "Stats",
                                        "TestContentOverride",
                                        "TestRunResultForScore",
                                        };
            foreach (var collectionId in collectionIds)
            {
                AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", collectionId);
                IList<JObject> items = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(
                (@"SELECT * FROM c")).Result;
                Console.WriteLine($"{collectionId}:{items.Count}");
            }
        }

        // Disable all the CFR monitor dataset and datasetTest
        private static void DisableAllCFRMonitor()
        {
            AzureCosmosDBClient datasetCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> azureDatasets = datasetCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c WHERE contains(c.id, 'CFR')")).Result;
            foreach (JObject azureDataset in azureDatasets)
            {
                string id = azureDataset["id"].ToString();
                Console.WriteLine(id);
                azureDataset["isEnabled"] = false;
                datasetCosmosDBClient.UpsertDocumentAsync(azureDataset).Wait();
            }
            Console.WriteLine(azureDatasets.Count);

            var datasetIdsStr = string.Join(",", azureDatasets.Select(d => $"'{d["id"]}'"));
            Console.WriteLine(datasetIdsStr);

            AzureCosmosDBClient datasetTestCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            IList<JObject> azureDatasetTests = datasetTestCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(($@"SELECT * FROM c WHERE c.datasetId in ({datasetIdsStr}) and c.status = 'Enabled'")).Result;
            foreach (JObject azureDatasetTest in azureDatasetTests)
            {
                string id = azureDatasetTest["id"].ToString();
                Console.WriteLine(id);
                azureDatasetTest["status"] = "Disabled";
                datasetTestCosmosDBClient.UpsertDocumentAsync(azureDatasetTest).Wait();
            }
            Console.WriteLine(azureDatasetTests.Count);
        }

        // Insert all the CFR monitor dataset and datasetTest in repo
        private static void InsertCFRMonitorConfig()
        {
            AzureCosmosDBClient datasetCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            AzureCosmosDBClient datasetTestCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            IList<JObject> azureDatasets = datasetCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c WHERE contains(c.id, 'CFR')")).Result;
            var datasetIdsStr = string.Join(",", azureDatasets.Select(d => $"'{d["id"]}'"));
            IList<JObject> azureDatasetTests = datasetTestCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(($@"SELECT * FROM c WHERE c.datasetId in ({datasetIdsStr}) and c.status = 'Enabled'")).Result;
            var azureDatasetDict = new Dictionary<string, JObject>();
            var azureDatasetTestDict = new Dictionary<string, JObject>();

            foreach (var azureDataset in azureDatasets)
            {
                azureDatasetDict.Add(azureDataset["id"].ToString(), azureDataset);
            }
            foreach (var azureDatasetTest in azureDatasetTests)
            {
                azureDatasetTestDict.Add(azureDatasetTest["id"].ToString(), azureDatasetTest);
            }

            string folderPath = @"D:\IDEAs\Ibiza\Source\DataCopMonitors\PROD\CFR";
            var filePaths = ReadFile.GetAllFilePath(folderPath);
            var updatedDatasets = new List<JObject>();
            var updatedDatasetTests = new List<JObject>();
            foreach (var filePath in filePaths)
            {
                var newFilePath = filePath.Substring(folderPath.Length);
                var jObject = JObject.Parse(ReadFile.ThirdMethod(filePath));
                if (newFilePath.Contains("Datasets"))
                {
                    if ((bool)jObject["isEnabled"])
                    {
                        string id = jObject["id"].ToString();
                        if (azureDatasetDict.ContainsKey(id))
                        {
                            jObject = azureDatasetDict[id];
                            jObject["isEnabled"] = true;
                        }
                        updatedDatasets.Add(jObject);
                    }
                }
                else if (newFilePath.Contains("Monitors"))
                {
                    if (jObject["status"].ToString().Equals("Enabled"))
                    {
                        string id = jObject["id"].ToString();
                        if (azureDatasetDict.ContainsKey(id))
                        {
                            jObject = azureDatasetDict[id];
                            jObject["status"] = "Enabled";
                        }
                        updatedDatasetTests.Add(jObject);
                    }
                }
            }

            foreach (JObject updatedDataset in updatedDatasets)
            {
                string id = updatedDataset["id"].ToString();
                Console.WriteLine(id);
                //Console.WriteLine(updatedDataset);
                datasetCosmosDBClient.UpsertDocumentAsync(updatedDataset).Wait();
            }
            Console.WriteLine(updatedDatasets.Count);

            foreach (JObject updatedDatasetTest in updatedDatasetTests)
            {
                string id = updatedDatasetTest["id"].ToString();
                Console.WriteLine(id);
                //Console.WriteLine(updatedDatasetTest);
                datasetTestCosmosDBClient.UpsertDocumentAsync(updatedDatasetTest).Wait();
            }
            Console.WriteLine(updatedDatasetTests.Count);
        }

        private static void SetOutdatedForDuplicatedDatasetTest()
        {
            AzureCosmosDBClient azureDatasetTestCosmosDB = new AzureCosmosDBClient("DataCop", "DatasetTest");
            AzureCosmosDBClient azureDatasetCosmosDB = new AzureCosmosDBClient("DataCop", "Dataset");

            // Collation: asc and desc is ascending and descending
            IList<JObject> datasetList = azureDatasetCosmosDB.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.dataFabric = 'ADLS' and c.isEnabled = true")).Result;
            IList<JObject> completenessDatasetTestList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.testContentType = 'AdlsCompleteness' and c.status = 'Enabled' and c.createdBy = 'DefaultTestGenerator' and c.lastModifiedBy = 'DefaultTestGenerator' order by c.lastModifiedTime")).Result;

            HashSet<string> datasetIdSet = new HashSet<string>();
            Dictionary<string, List<JObject>> completenessDatasetIdTestDict = new Dictionary<string, List<JObject>>();

            foreach (JObject jObject in datasetList)
            {
                string id = jObject["id"].ToString();
                datasetIdSet.Add(id);
            }

            foreach (JObject datasetTest in completenessDatasetTestList)
            {
                string datasetId = datasetTest["datasetId"].ToString();

                if (datasetIdSet.Contains(datasetId))
                {
                    if (!completenessDatasetIdTestDict.ContainsKey(datasetId))
                    {
                        completenessDatasetIdTestDict.Add(datasetId, new List<JObject>());
                    }
                    completenessDatasetIdTestDict[datasetId].Add(datasetTest);
                }

            }

            foreach (var completenessDatasetIdTest in completenessDatasetIdTestDict)
            {
                if (completenessDatasetIdTest.Value.Count > 1)
                {
                    //if (completenessDatasetIdTest.Key == "700663cc-cb12-4fb7-877b-262ab0160690")
                    {
                        Console.WriteLine($"datasetId: {completenessDatasetIdTest.Key}   completenessDatasetIdCount: {completenessDatasetIdTest.Value.Count}");
                        for (int i = 0; i < completenessDatasetIdTest.Value.Count - 1; i++)
                        {
                            completenessDatasetIdTest.Value[i]["status"] = "Outdated";
                            azureDatasetTestCosmosDB.UpsertDocumentAsync(completenessDatasetIdTest.Value[i]).Wait();
                        }
                    }
                }
            }
        }

        private static void CheckDuplicatedEnabledDatasetTest()
        {
            AzureCosmosDBClient azureDatasetTestCosmosDB = new AzureCosmosDBClient("DataCop", "DatasetTest");
            AzureCosmosDBClient azureDatasetCosmosDB = new AzureCosmosDBClient("DataCop", "Dataset");

            // Collation: asc and desc is ascending and descending
            IList<JObject> datasetList = azureDatasetCosmosDB.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.dataFabric = 'ADLS' and c.isEnabled = true")).Result;
            IList<JObject> completenessDatasetTestList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.testContentType = 'AdlsCompleteness' and c.status = 'Enabled' order by c.lastModifiedTime")).Result;

            HashSet<string> datasetIdSet = new HashSet<string>();
            Dictionary<string, List<string>> completenessDatasetIdCountDict = new Dictionary<string, List<string>>();
            Dictionary<string, string> completenessDatasetIdNameDict = new Dictionary<string, string>();


            foreach (JObject jObject in datasetList)
            {
                string id = jObject["id"].ToString();
                datasetIdSet.Add(id);
            }

            foreach (JObject jObject in completenessDatasetTestList)
            {
                string datasetId = jObject["datasetId"].ToString();
                string datasetTestId = jObject["id"].ToString();
                string datasetTestName = jObject["lastModifiedBy"].ToString();

                if (datasetIdSet.Contains(datasetId))
                {
                    if (!completenessDatasetIdNameDict.ContainsKey(datasetId))
                    {
                        completenessDatasetIdNameDict.Add(datasetId, "");
                    }
                    completenessDatasetIdNameDict[datasetId] += datasetTestName + "\n";

                    if (!completenessDatasetIdCountDict.ContainsKey(datasetId))
                    {
                        completenessDatasetIdCountDict.Add(datasetId, new List<string>());
                    }
                    completenessDatasetIdCountDict[datasetId].Add(datasetTestId);
                }

            }

            foreach (var completenessDatasetIdCount in completenessDatasetIdCountDict)
            {
                if (completenessDatasetIdCount.Value.Count > 1)
                {
                    Console.WriteLine($"datasetId: {completenessDatasetIdCount.Key}   completenessDatasetIdCount: {completenessDatasetIdCount.Value.Count}");
                    Console.WriteLine(completenessDatasetIdNameDict[completenessDatasetIdCount.Key]);
                }
                //Console.WriteLine($"datasetTestName: {completenessDatasetIdNameDict[completenessDatasetIdCount.Key]}");
            }
        }

        private static void CheckAdlsConnectionInfoMappingCorrectness()
        {
            AzureCosmosDBClient azureDatasetCosmosDB = new AzureCosmosDBClient("DataCop", "Dataset");
            AzureCosmosDBClient azureDatasetTestCosmosDB = new AzureCosmosDBClient("DataCop", "DatasetTest");
            IList<JObject> adlsDatasetTestJObjectList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where (c.testContentType = 'AdlsCompleteness' or c.testContentType = 'AdlsAvailability') and c.status = 'Enabled'")).Result;
            IList<JObject> datasetJObjectList = azureDatasetCosmosDB.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.dataFabric = 'ADLS' and c.isEnabled = true")).Result;

            foreach (var datasetJObject in datasetJObjectList)
            {
                JObject connectionInfo = JObject.Parse(datasetJObject["connectionInfo"].ToString());
                string dataLakeStoreInDataset = connectionInfo["dataLakeStore"].ToString();
                string dataLakePathInDataset = connectionInfo["dataLakePath"].ToString();
                foreach (var datasetTestJObject in adlsDatasetTestJObjectList)
                {
                    if (!datasetTestJObject["datasetId"].ToString().Equals(datasetJObject["id"].ToString()))
                        continue;
                    string dataLakeStoreInDatasetTest = "", dataLakePathInDatasetTest = "";
                    try
                    {
                        dataLakeStoreInDatasetTest = datasetTestJObject["testContent"]["dataLakeStore"].ToString();
                        dataLakePathInDatasetTest = datasetTestJObject["testContent"]["streamPath"].ToString();
                        if (dataLakeStoreInDataset.Equals(dataLakeStoreInDatasetTest) && dataLakePathInDataset.Equals(dataLakePathInDatasetTest))
                            continue;
                    }
                    catch (Exception e)
                    {

                    }
                    Console.WriteLine(datasetTestJObject["id"]);
                    Console.WriteLine(dataLakeStoreInDataset);
                    Console.WriteLine(dataLakeStoreInDatasetTest);
                    Console.WriteLine(dataLakePathInDataset);
                    Console.WriteLine(dataLakePathInDatasetTest);
                }
            }
            Console.WriteLine("END!!!");
        }

        private static void CheckCosmosConnectionInfoMappingCorrectness()
        {
            AzureCosmosDBClient azureDatasetCosmosDB = new AzureCosmosDBClient("DataCop", "Dataset");
            AzureCosmosDBClient azureDatasetTestCosmosDB = new AzureCosmosDBClient("DataCop", "DatasetTest");
            IList<JObject> cosmosDatasetTestJObjectList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where (c.testContentType = 'CosmosCompleteness' or c.testContentType = 'CosmosAvailability') and c.status = 'Enabled'")).Result;
            IList<JObject> datasetJObjectList = azureDatasetCosmosDB.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.dataFabric = 'Cosmos' and c.isEnabled = true")).Result;

            foreach (var datasetJObject in datasetJObjectList)
            {
                JObject connectionInfo = JObject.Parse(datasetJObject["connectionInfo"].ToString());
                string cosmosVC = connectionInfo["cosmosVC"].ToString();
                string cosmosPath = connectionInfo["cosmosPath"].ToString();
                string streamPathInDataset = $"https://{cosmosVC}/cosmos/{cosmosPath}";
                foreach (var datasetTestJObject in cosmosDatasetTestJObjectList)
                {
                    if (!datasetTestJObject["datasetId"].ToString().Equals(datasetJObject["id"].ToString()))
                        continue;
                    string streamPathInDatasetTest = "";
                    try
                    {
                        streamPathInDatasetTest = datasetTestJObject["testContent"]["streamPath"].ToString();
                        if (streamPathInDatasetTest.Equals(streamPathInDataset)) continue;
                    }
                    catch (Exception e)
                    {

                    }
                    Console.WriteLine(datasetTestJObject["id"]);
                    Console.WriteLine(streamPathInDataset);
                    Console.WriteLine(streamPathInDatasetTest);
                }
            }
            Console.WriteLine("END!!!");
        }

        private static void CheckPPEAlertsettingOwningTeamAndRouting()
        {
            string cfrId = "da71491f-c49a-475e-9d54-d2fde4a6403f";
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "AlertSettings");
            IList<JObject> alertSettings = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c")).Result;
            foreach (var alertSetting in alertSettings)
            {
                if (alertSetting["id"].ToString().Equals(cfrId)) continue;
                if ((alertSetting["routingId"].ToString().Equals("IDEAS://IDEAsDataCopTest") || alertSetting["routingId"].ToString().Equals("IDEAs://IDEAsDataCopTest")) && alertSetting["owningTeamId"].ToString().Equals("IDEAS\\IDEAsDataCopTest")) continue;
                //Console.WriteLine(alertSetting["routingId"].ToString() + "\t" + alertSetting["owningTeamId"].ToString());
                Console.WriteLine(alertSetting);

                alertSetting["routingId"] = "IDEAS://IDEAsDataCopTest";
                alertSetting["owningTeamId"] = "IDEAS\\IDEAsDataCopTest";
                azureCosmosDBClient.UpsertDocumentAsync(alertSetting).Wait();
            }
        }

        private static void CheckDatasetTestIntegrity()
        {
            AzureCosmosDBClient azureDatasetTestCosmosDB = new AzureCosmosDBClient("DataCop", "DatasetTest");
            AzureCosmosDBClient azureDatasetCosmosDB = new AzureCosmosDBClient("DataCop", "Dataset");
            IList<JObject> availabilityDatasetIdJObjectList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>((@"SELECT c.datasetId FROM c where c.testCategory = 'Availability' and c.status = 'Enabled'")).Result;
            IList<JObject> completenessDatasetIdJObjectList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>((@"SELECT c.datasetId FROM c where c.testCategory = 'Completeness' and c.status = 'Enabled'")).Result;
            IList<JObject> datasetIdJObjectList = azureDatasetCosmosDB.GetAllDocumentsInQueryAsync<JObject>((@"SELECT c.id FROM c where c.dataFabric != 'SQL' and c.isEnabled = true")).Result;


            HashSet<string> availabilityDatasetIdStringSet = new HashSet<string>();
            HashSet<string> completenessDatasetIdStringSet = new HashSet<string>();
            HashSet<string> datasetIdStringSet = new HashSet<string>();
            foreach (var availabilityDatasetIdJObject in availabilityDatasetIdJObjectList)
            {
                availabilityDatasetIdStringSet.Add(availabilityDatasetIdJObject["datasetId"].ToString());
            }
            foreach (var completenessDatasetIdJObject in completenessDatasetIdJObjectList)
            {
                completenessDatasetIdStringSet.Add(completenessDatasetIdJObject["datasetId"].ToString());
            }
            foreach (var datasetIdJObject in datasetIdJObjectList)
            {
                string datasetId = datasetIdJObject["id"].ToString();
                bool existAvailability = availabilityDatasetIdStringSet.Contains(datasetId);
                bool existCompleteness = completenessDatasetIdStringSet.Contains(datasetId);
                if (existAvailability && existCompleteness)
                {
                    continue;
                }
                else
                {
                    Console.WriteLine(datasetId);
                    if (!existAvailability) Console.WriteLine("lackAvailability");
                    if (!existCompleteness) Console.WriteLine("lackCompleteness");
                }
            }

        }

        private static void AddCompletenessMonitors4ADLS()
        {
            string dirPath = @"D:\data\company_work\M365\IDEAs\work_item_file\1161175\write\";
            string duplicateDirPath = @"D:\data\company_work\M365\IDEAs\work_item_file\1161175\duplicate\";


            AzureCosmosDBClient azureDatasetTestCosmosDB = new AzureCosmosDBClient("DataCop", "DatasetTest");
            AzureCosmosDBClient azureDatasetCosmosDB = new AzureCosmosDBClient("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> availabilityList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.testContentType = 'AdlsAvailability' and c.status = 'Enabled'")).Result;

            IList<JObject> completenessList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.testContentType = 'AdlsCompleteness' and c.status = 'Enabled'")).Result;

            Dictionary<string, int> completenessDatasetIdDict = new Dictionary<string, int>();
            foreach (JObject jObject in completenessList)
            {
                if (!completenessDatasetIdDict.ContainsKey(jObject["datasetId"].ToString()))
                {
                    completenessDatasetIdDict.Add(jObject["datasetId"].ToString(), 1);
                }
                else
                {
                    completenessDatasetIdDict[jObject["datasetId"].ToString()]++;
                }
            }
            foreach (JObject jObject in availabilityList)
            {
                string datasetId = jObject["datasetId"].ToString();

                if (completenessDatasetIdDict.ContainsKey(datasetId))
                {
                    completenessDatasetIdDict[datasetId]--;
                    continue;
                }
                if (!CheckDatasetEnabled(azureDatasetCosmosDB, jObject["datasetId"].ToString()))
                {
                    Console.WriteLine($"dataset '{jObject["datasetId"]}' is not enabled");
                    continue;
                }

                jObject["id"] = Guid.NewGuid();
                jObject["name"] = jObject["name"].ToString().Replace("Availability", "Completeness");

                jObject["description"] = jObject["description"].ToString().Replace("Availability", "Completeness");
                jObject["testCategory"] = jObject["testCategory"].ToString().Replace("Availability", "Completeness");
                jObject["testContentType"] = jObject["testContentType"].ToString().Replace("Availability", "Completeness");
                jObject["createdBy"] = "jianjlv";
                jObject["lastModifiedBy"] = "jianjlv";
                jObject["testContent"]["fileSizeMaxLimit"] = long.MaxValue;
                jObject["testContent"]["fileSizeMinLimit"] = 0;
                jObject.Remove("_rid");
                jObject.Remove("_self");
                jObject.Remove("_etag");
                jObject.Remove("_attachments");
                jObject.Remove("_ts");

                string path = dirPath + jObject["name"].ToString().Replace(" ", "_") + ".json";
                if (FileOperation.ReadFile.CheckFileExist(path))
                {
                    path = duplicateDirPath + jObject["name"].ToString().Replace(" ", "_") + ".json";
                }
                while (FileOperation.ReadFile.CheckFileExist(path))
                {
                    path = path.Replace(".json", "_.json");
                }
                //FileOperation.SaveFile.FirstMethod(path, jObject.ToString());

                azureDatasetTestCosmosDB.UpsertDocumentAsync(jObject).Wait();
            }

            Console.WriteLine(completenessList.Count);
            Console.WriteLine(availabilityList.Count);
            foreach (var completenessDatasetId in completenessDatasetIdDict)
            {
                if (completenessDatasetId.Value > 0)
                {
                    Console.WriteLine($"datasetId: {completenessDatasetId.Key}   completenessBiggerCount: {completenessDatasetId.Value}");
                }
            }
        }

        private static bool CheckDatasetEnabled(AzureCosmosDBClient azureDatasetCosmosDB, string datasetId)
        {
            IList<JObject> availabilityList = azureDatasetCosmosDB.GetAllDocumentsInQueryAsync<JObject>(($@"SELECT * FROM c where c.id = '{datasetId}'")).Result;
            if (availabilityList.Count != 1)
            {
                Console.WriteLine($"Cannot get the dataset with id '{datasetId}'");
                return false;
            }
            else
            {
                return bool.Parse(availabilityList[0]["isEnabled"].ToString());
            }
        }

        private static void MigrateData(string databaseId, string collectionId, IList<string> filters, string fromEndPoint, string fromKey, string toEndPoint, string toKey)
        {
            StringBuilder sqlQuerySb = new StringBuilder(@"SELECT * FROM c");
            if (filters?.Count > 0 == true)
            {
                sqlQuerySb.Append($" where {filters[0]}");
                for (int i = 1; i < filters.Count; i++)
                {
                    sqlQuerySb.Append($" and {filters[i]}");
                }
            }

            MigrateData(databaseId, collectionId, sqlQuerySb.ToString(), fromEndPoint, fromKey, toEndPoint, toKey);
        }

        private static void MigrateData(string databaseId, string collectionId, string queryStr, string fromEndPoint, string fromKey, string toEndPoint, string toKey)
        {
            AzureCosmosDBClient.Endpoint = fromEndPoint;
            AzureCosmosDBClient.Key = fromKey;
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient(databaseId, collectionId);

            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((queryStr)).Result;

            // This is a funny thing. azureCosmosDBClient has not been changed.
            // Root cause is that we use the single module
            AzureCosmosDBClient.Endpoint = toEndPoint;
            AzureCosmosDBClient.Key = toKey;
            azureCosmosDBClient = new AzureCosmosDBClient(databaseId, collectionId, CosmosDBDocumentClientMode.NoSingle);
            int count = 0;
            foreach (JObject json in list)
            {
                count++;
                try
                {
                    Console.WriteLine(json["id"]);
                    azureCosmosDBClient.UpsertDocumentAsync(json).Wait();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"ErrorMessage: {e.Message}");
                }
            }
            Console.WriteLine(count);
        }

        private static void UpdateNoneAlertTypeDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Alert");
            // Collation: asc and desc is ascending and descending
            IList<JObject> alerts = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.testCategory = 'None' ORDER BY c.issuedOnDate ASC")).Result;
            foreach (JObject alert in alerts)
            {
                try
                {
                    JObject testRun = GetTestRunBy(alert["id"].ToString());
                    if (testRun == null)
                    {
                        Console.WriteLine($"testRun with id '{alert["id"]}' is null");
                        continue;
                    }
                    alert["testCategory"] = testRun["testCategory"];
                    var statusCode = azureCosmosDBClient.UpsertDocumentAsync(alert).Result;
                    Console.WriteLine($"Status Code: {statusCode}");
                }
                catch (Exception e)
                {
                    Console.WriteLine($"ErrorMessage: {e.Message}");
                    Console.WriteLine(alert);
                }
            }
        }


        private static JObject GetTestRunBy(string testRunId)
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "TestRun");
            // Collation: asc and desc is ascending and descending
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(($@"SELECT * FROM c where c.id = '{testRunId}'")).Result;
            return list.Count > 0 ? list[0] : null;
        }

        private static void UpdateAllAlertSettingsDemo()
        {
            var serviceCustomFieldNames = new string[] {"DatasetId",
                                                            "AlertType",
                                                            "DisplayInSurface",
                                                            "BusinessOwner",
                                                            "TitleOverride"};
            /* 
             * Use string, the json we upload will like this:
             * Because the type of property serviceCustomFieldNames in this json is string
             * 
               {
                    ......
                    "serviceCustomFieldNames": "[\"DatasetId\",\"AlertType\",\"DisplayInSurface\",\"BusinessOwner\",\"TitleOverride\"]",
                    ......
                }
             * 
             * But use JArray, the json we upload will like this:
             * 
               {
                    ......
                    "serviceCustomFieldNames": [
                        "DatasetId",
                        "AlertType",
                        "DisplayInSurface",
                        "BusinessOwner",
                        "TitleOverride"
                    ],
                    ......
                }
             * 
             * 
             */

            //string serviceCustomFieldNamesString = JsonConvert.SerializeObject(serviceCustomFieldNames);
            var serviceCustomFieldNamesJArray = JArray.FromObject(serviceCustomFieldNames);

            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "AlertSettings");
            // Collation: asc and desc is ascending and descending
            IList<JObject> alertSettings = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c")).Result;
            foreach (JObject alertSetting in alertSettings)
            {
                Console.WriteLine(alertSetting["id"].ToString());
                alertSetting["serviceCustomFieldNames"] = serviceCustomFieldNamesJArray;
                azureCosmosDBClient.UpsertDocumentAsync(alertSetting).Wait();
            }
        }

        private static void UpdateAllDatasetForMerging()
        {
            int count = 0;

            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasets = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.dataFabric = 'ADLS' and c.isEnabled = false and not is_defined(c.connectionInfo.streamPath)")).Result;
            foreach (var dataset in datasets)
            {
                count++;
                string id = dataset["id"].ToString();

                //if (id != "3b73bfab-b99f-4e38-8a92-a49534a10856") continue;

                var connectionInfo = dataset["connectionInfo"];
                if (connectionInfo == null)
                {
                    Console.WriteLine("connectionInfo is null");
                    Console.WriteLine(id);
                    Console.WriteLine(connectionInfo);
                    continue;
                }


                if (connectionInfo.GetType() == typeof(JObject) || connectionInfo.GetType() == typeof(JValue))
                {
                    //if (connectionInfo.GetType() == typeof(JValue))
                    //{
                    connectionInfo = JObject.Parse(connectionInfo.ToString());
                    //}
                    if (connectionInfo["dataLakeStore"].ToString() == "ideas-prod-c14.azuredatalakestore.net")
                    {
                        connectionInfo["cosmosVC"] = "https://cosmos14.osdinfra.net/cosmos/Ideas.prod/";
                        //connectionInfo["streamPath"] = connectionInfo["streamPath"] ?? connectionInfo["dataLakePath"] ?? connectionInfo["path"];
                        connectionInfo["streamPath"] = connectionInfo["streamPath"] ?? connectionInfo["dataLakePath"];
                        ((JObject)connectionInfo).Remove("dataLakePath");
                        ((JObject)connectionInfo).Remove("auth");
                        //((JObject)connectionInfo).Remove("path");
                        dataset["connectionInfo"] = connectionInfo;
                        dataset["lastModifiedTime"] = DateTime.UtcNow.ToString("o");
                        //Console.WriteLine(dataset);
                        Console.WriteLine(id);
                        azureCosmosDBClient.UpsertDocumentAsync(dataset).Wait();
                    }
                    //else if (connectionInfo["dataLakeStore"].ToString() == "cfr-prod-c14.azuredatalakestore.net")
                    else
                    {
                        connectionInfo["streamPath"] = connectionInfo["streamPath"] ?? connectionInfo["dataLakePath"];
                        connectionInfo["streamPath"] = string.IsNullOrEmpty(connectionInfo["streamPath"].ToString()) ? connectionInfo["dataLakePath"] : connectionInfo["streamPath"];
                        ((JObject)connectionInfo).Remove("dataLakePath");
                        ((JObject)connectionInfo).Remove("cosmosPath");
                        ((JObject)connectionInfo).Remove("cosmosVC");
                        ((JObject)connectionInfo).Remove("auth");
                        dataset["connectionInfo"] = connectionInfo;
                        dataset["lastModifiedTime"] = DateTime.UtcNow.ToString("o");
                        Console.WriteLine(id);
                        azureCosmosDBClient.UpsertDocumentAsync(dataset).Wait();
                    }
                    //else
                    //{
                    //    Console.WriteLine("dataLakeStore is wrong");
                    //    Console.WriteLine(id);
                    //    Console.WriteLine(connectionInfo);
                    //}
                }
                else
                {
                    Console.WriteLine("connectionInfo type wrong");
                    Console.WriteLine(id);
                    Console.WriteLine(connectionInfo);
                }
            }
            Console.WriteLine($"count: {count}!!!");
        }

        private static void UpdateAllDatasetTestForMerging()
        {
            int count = 0;

            AzureCosmosDBClient datasetAzureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            AzureCosmosDBClient datasetTestAzureCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            // Collation: asc and desc is ascending and descending
            IList<JToken> datasets = datasetAzureCosmosDBClient.GetAllDocumentsInQueryAsync<JToken>((@"SELECT * FROM c where c.dataFabric = 'ADLS' and c.isEnabled = true")).Result;
            IList<JObject> adlsCompletenessTests = datasetTestAzureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.testContentType = 'AdlsCompleteness' and c.status = 'Enabled'")).Result;
            IList<JObject> cosmosCompletenessTests = datasetTestAzureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.testContentType = 'CosmosCompleteness' and c.status = 'Enabled'")).Result;

            foreach (JObject dataset in datasets)
            {
                count++;
                string id = dataset["id"].ToString();

                //if (id != "3b73bfab-b99f-4e38-8a92-a49534a10856") continue;

                if (dataset["connectionInfo"]["dataLakeStore"].ToString() != "ideas-prod-c14.azuredatalakestore.net")
                {
                    Console.WriteLine("dataLakeStore is wrong");
                    Console.WriteLine(dataset["connectionInfo"]);
                    continue;
                }

                bool existCosmosTest = false;
                foreach (JObject cosmosCompletenessTestTemp in cosmosCompletenessTests)
                {
                    string datasetId = cosmosCompletenessTestTemp["datasetId"].ToString();
                    if (datasetId == id)
                    {
                        if (existCosmosTest)
                        {
                            Console.WriteLine($"error!!! datasetId({datasetId}) has duplicated cosmos completeness test");
                        }
                        existCosmosTest = true;
                    }
                }
                if (existCosmosTest)
                {
                    Console.WriteLine($"datasetId({id}) has cosmos completeness test");
                    continue;
                }


                bool hasValue = false, continueLoop = false;

                JObject cosmosCompletenessTest = null;
                foreach (JObject adlsCompletenessTest in adlsCompletenessTests)
                {
                    string datasetId = adlsCompletenessTest["datasetId"].ToString();
                    if (datasetId == id)
                    {
                        if (hasValue)
                        {
                            Console.WriteLine($"dataset({id}) has multi completeness test");
                            continueLoop = true;
                            break;
                        }
                        hasValue = true;
                        cosmosCompletenessTest = adlsCompletenessTest;
                    }
                }

                if (continueLoop)
                {
                    continue;
                }

                if (cosmosCompletenessTest != null)
                {
                    cosmosCompletenessTest["id"] = Guid.NewGuid();
                    cosmosCompletenessTest["name"] = cosmosCompletenessTest["name"].ToString().Replace("ADLS", "Cosmos");

                    cosmosCompletenessTest["description"] = cosmosCompletenessTest["description"].ToString().Replace("ADLS", "Cosmos");
                    cosmosCompletenessTest["testContentType"] = "CosmosCompleteness";
                    cosmosCompletenessTest["createTime"] = DateTime.UtcNow.ToString("o");
                    cosmosCompletenessTest["lastModifiedTime"] = DateTime.UtcNow.ToString("o");
                    cosmosCompletenessTest["createdBy"] = "DefaultTestGenerator";
                    cosmosCompletenessTest["lastModifiedBy"] = "DefaultTestGenerator";
                    cosmosCompletenessTest["dataFabric"] = "Cosmos";
                    cosmosCompletenessTest["testContent"]["fileRowCountMaxLimit"] = long.MaxValue - 1000;
                    cosmosCompletenessTest["testContent"]["fileRowCountMinLimit"] = 0;
                    cosmosCompletenessTest["testContent"]["streamPath"] =
                        "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/" +
                        cosmosCompletenessTest["testContent"]["streamPath"];
                    ((JObject)cosmosCompletenessTest["testContent"]).Remove("dataLakeStore");
                    ((JObject)cosmosCompletenessTest["testContent"]).Remove("fileSizeMaxLimit");
                    ((JObject)cosmosCompletenessTest["testContent"]).Remove("fileSizeMinLimit");
                    cosmosCompletenessTest.Remove("_rid");
                    cosmosCompletenessTest.Remove("_self");
                    cosmosCompletenessTest.Remove("_etag");
                    cosmosCompletenessTest.Remove("_attachments");
                    cosmosCompletenessTest.Remove("_ts");

                    Console.WriteLine($"success: {id}");
                    //Console.WriteLine(cosmosCompletenessTest);
                    datasetTestAzureCosmosDBClient.UpsertDocumentAsync(cosmosCompletenessTest).Wait();
                }
                else
                {
                    Console.WriteLine($"dataset({id}) has no adlsCompletenessTest");
                }
            }
            Console.WriteLine($"count: {count}!!!");
        }

        private static void UpdateAllDatasetTestCreatedBy()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasetTests = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.createdBy = 'jianjlv'")).Result;
            foreach (JObject datasetTest in datasetTests)
            {
                Console.WriteLine(datasetTest["id"].ToString());
                datasetTest["createdBy"] = "DefaultTestGenerator";
                datasetTest["lastModifiedBy"] = "DefaultTestGenerator";

                azureCosmosDBClient.UpsertDocumentAsync(datasetTest).Wait();
            }
        }

        private static void UpdateAllCosmosTestResultExpirePeriod()
        {
            string oldResultExpirePeriod = "48.00:00:00";
            string newResultExpirePeriod = "2.00:00:00";
            int count = 0;

            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            // Collation: asc and desc is ascending and descending
            IList<JObject> testRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(($@"SELECT * FROM c WHERE c.dataFabric= 'Cosmos' and c.resultExpirePeriod = '{oldResultExpirePeriod}'")).Result;

            Console.WriteLine($"testRuns.Count: {testRuns.Count}");
            foreach (JObject testRun in testRuns)
            {
                //if (testRun["id"].ToString() != "6a31c782-3151-48f6-babd-5a0f428a493f") continue;
                Console.WriteLine(testRun["id"].ToString());
                //Console.WriteLine(testRun);
                string partitionKey = (Regex.Split(testRun.ToString(), "\"partitionKey\": \""))[1].Split('\"')[0];
                Console.WriteLine(partitionKey);
                testRun["resultExpirePeriod"] = newResultExpirePeriod;
                testRun["partitionKey"] = partitionKey;
                azureCosmosDBClient.UpsertDocumentAsync(testRun).Wait();

                count++;
            }
            Console.WriteLine($"count: {count}");
        }

        private static void UpdateAllCosmosTestCreateTime()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            for (int min = 0; min < 16; min++)
            {
                for (int sec = 0; sec < 60; sec++)
                {
                    for (int millisec = 0; millisec < 10; millisec++)
                    {
                        int count = 0;
                        // Collation: asc and desc is ascending and descending
                        IList<JObject> testRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(($@"SELECT * FROM c WHERE c.dataFabric= 'Cosmos' and c.createTime > '2019-09-09T05:{min}:{sec}.{millisec}' and c.createTime < '2019-09-09T05:{min}:{sec}.{millisec + 1}' order by c.startTime desc")).Result;

                        Console.WriteLine($"testRuns.Count: {testRuns.Count}");
                        foreach (JObject testRun in testRuns)
                        {
                            if (testRun["id"].ToString() != "84c5a7cb-2c04-4142-bb57-fdb7b20fff2b") continue;
                            Console.WriteLine(testRun["id"].ToString());
                            //Console.WriteLine(testRun);
                            string partitionKey = (Regex.Split(testRun.ToString(), "\"partitionKey\": \""))[1].Split('\"')[0];
                            Console.WriteLine(partitionKey);
                            testRun["createTime"] = testRun["startTime"];
                            testRun["partitionKey"] = partitionKey;
                            azureCosmosDBClient.UpsertDocumentAsync(testRun).Wait();

                            count++;
                        }
                        Console.WriteLine($"count: {count}");
                    }

                }
            }
        }

        private static void UpsertServiceMonitorDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "ServiceMonitor");
            var jObject = JObject.Parse(@"{'datasetId': 'a4353e4b-1611-4965-8334-4c81fd824dad', 'expectedTestRunStatus': 'Success','isEnabled': true}");
            azureCosmosDBClient.UpsertDocumentAsync(jObject).Wait();
        }

        private static void UpdateVcToBuild()
        {
            HashSet<string> set = new HashSet<string>();
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasets = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.dataFabric = 'CosmosStream' and c.isEnabled = true")).Result;
            var count = 0;
            foreach (JObject dataset in datasets)
            {
                var cosmosVC = dataset["connectionInfo"]["cosmosVC"].ToString().ToLower().Trim('/');
                var streamPath = dataset["connectionInfo"]["streamPath"].ToString().ToLower().Trim('/');
                var kenshoData = dataset["kenshoData"]?.ToString();
                set.Add(cosmosVC);
                if (!cosmosVC.Equals("https://cosmos14.osdinfra.net/cosmos/ideas.prod.build") &&
                    streamPath.StartsWith("share") &&
                    string.IsNullOrEmpty(kenshoData))
                {
                    Console.WriteLine(dataset);
                    if (dataset["connectionInfo"]["dataLakeStore"]?.ToString().Length > 0)
                    {
                        dataset["connectionInfo"]["dataLakeStore"] = "ideas-prod-build-c14.azuredatalakestore.net";
                    }
                    dataset["connectionInfo"]["cosmosVC"] = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod.Build/";
                    azureCosmosDBClient.UpsertDocumentAsync(dataset).Wait();
                    count++;
                }
            }

            foreach (var cosmosVc in set)
            {
                Console.WriteLine(cosmosVc);
            }
            Console.WriteLine(datasets.Count);
            Console.WriteLine(count);
        }

        private static void DisableDatasets()
        {
            string[] ids = new string[]
            {
                "00D3EA46-3658-43CD-8288-41C0F908FB50",
                "E7575A0C-A7D4-45B7-9FA8-DCFB5ECBA47E",
                "04BC25E4-6D3F-418D-8B90-182E957D5EE2",
                "BCEE3FD4-1A54-43B3-82D1-3C2FED5C8419"
            };
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            IList<JObject> datasets = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                @$"SELECT * FROM c where c.id in ({string.Join(",", ids.Select(id => $"'{id}'"))}) and c.isEnabled = true")).Result;
            foreach (JObject dataset in datasets)
            {
                Console.WriteLine(dataset["id"]);
                dataset["isEnabled"] = false;
                azureCosmosDBClient.UpsertDocumentAsync(dataset).Wait();
            }
        }

        private static void EnableDatasets()
        {
            string[] ids = new string[]
            {
                "d178cf94-07a5-4f1f-9cb0-7e84580a26f4",
                "4941fd17-20da-4f32-b7be-310499bbc53b",
                "116cafb5-283a-4445-a95d-74a82af32997",
                "1fb3a294-ec77-4213-8e81-6f0c704c97f9",
                "38b41243-1605-4678-8fee-ae1a1f75ec47",
                "5e9b6e16-add9-4f40-92eb-ea97184cb237",
                "7d4cc036-95e3-4781-ba3c-3a1b3491e2e8",
                "cb72b154-bbb8-440d-98e2-e4d88e755a54",
                "be2e4174-abbb-4f39-a205-c3ca61787903",
                "462742fd-0522-4c25-9370-5a75878e475e",
                "9bd4a6c1-ee2f-42b6-b42a-8782aebef3ab",
                "070678f7-83c2-4280-aa9d-e807a743bd52",
                "3b702d6b-15ee-4fd3-a037-cedca4869120",
                "1a8079f8-1885-41ce-8ab5-1717fd885632",
                "916e9311-8cdb-4cf4-99b5-0b65e562757b"
            };

            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            IList<JObject> datasets = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                @$"SELECT * FROM c where c.id in ({string.Join(",", ids.Select(id => $"'{id}'"))}) and c.isEnabled = false")).Result;
            foreach (JObject dataset in datasets)
            {
                Console.WriteLine(dataset["id"]);
                dataset["isEnabled"] = true;
                azureCosmosDBClient.UpsertDocumentAsync(dataset).Wait();
            }
            Console.WriteLine(datasets.Count);
        }

        private static void QueryTestRunStatusForDatasets()
        {
            JArray jsons = JArray.Parse(File.ReadAllText(Path.Combine(CosmosViewErrorMessageOperation.RootFolderPath, @"allTestRuns.json")));
            var datasetIds = jsons.Select(j => j["datasetId"].ToString()).ToArray();

            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            foreach (var datasetId in datasetIds)
            {
                Console.WriteLine($"datasetId: {datasetId}");
                IList<JObject> testRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(
                    ($"SELECT top 5 c.status FROM c where c.datasetId = '{datasetId}' order by c.createTime desc")).Result;
                foreach (var testRun in testRuns)
                {
                    Console.WriteLine(testRun["status"]);
                }
            }
        }

        private static void GetDataFactoriesInCosmosViewMonitors()
        {
            HashSet<string> dataFactoriesSet = new HashSet<string>();
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            IList<JObject> datasets = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                @"SELECT distinct c.buildEntityId FROM c WHERE c.dataFabric = 'CosmosView' and c.createdBy = 'BuildDeployment'")).Result;
            foreach (JObject dataset in datasets)
            {
                string buildEntityId = dataset["buildEntityId"].ToString();
                var dataFactoryName = buildEntityId.Substring(0, buildEntityId.IndexOf("/"));
                dataFactoriesSet.Add(dataFactoryName);
            }
            Console.WriteLine($"datasets count: {datasets.Count}");
            Console.WriteLine($"dataFactories count: {dataFactoriesSet.Count}");

            List<string> dataFactories = new List<string>(dataFactoriesSet);
            dataFactories.Sort();
            foreach (var dataFactory in dataFactories)
            {
                Console.WriteLine(dataFactory);
            }
        }

        private static void GetAlertSettingsForEveryDataFactory()
        {
            string dataFactoriesFolder = @"D:\IDEAs\repos\DBInterfaces\SharedInterfaces\linkedServices\Prod\DataFactory";
            var dataFactoryFilePaths = Directory.EnumerateFiles(dataFactoriesFolder);
            IList<string> dataFactoryNames = new List<string>();
            foreach (var dataFactoryFilePath in dataFactoryFilePaths)
            {
                var dataFactoryJson = JObject.Parse(File.ReadAllText(dataFactoryFilePath));
                var enabledFeatures = new HashSet<string>(dataFactoryJson["enabledFeatures"].Select(c => c.ToString()).ToList<string>());
                //if (enabledFeatures.Contains("EnableDataCopSlaTestsForCosmosViews"))
                dataFactoryNames.Add(dataFactoryJson["dataFactoryName"].ToString());
            }

            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "AlertSettings");
            IList<JObject> alertSettings = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * from c")).Result;

            HashSet<string> dataFactories = new HashSet<string>();
            foreach (JObject alertSetting in alertSettings)
            {
                if (!alertSetting.ContainsKey("dataFactories"))
                {
                    continue;
                }

                foreach (var dataFactory in alertSetting["dataFactories"].Select(c => c.ToString()).ToList<string>())
                {
                    dataFactories.Add(dataFactory);
                }
            }

            foreach (var dataFactoryName in dataFactoryNames)
            {
                if (!dataFactories.Contains(dataFactoryName.ToUpper()))
                {
                    Console.WriteLine(dataFactoryName);
                }
            }


        }


        private static void GetDatasetsCountForEveryDataFactory()
        {
            Dictionary<string, int> enabledDatasetCounts = new Dictionary<string, int>();
            Dictionary<string, int> disabledDatasetCounts = new Dictionary<string, int>();
            HashSet<string> dataFactoryNames = new HashSet<string>();

            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            IList<JObject> datasets = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                @"SELECT * FROM c WHERE c.createdBy = 'BuildDeployment'")).Result;

            foreach (JObject dataset in datasets)
            {
                string buildEntityId = dataset["buildEntityId"].ToString();
                var dataFactoryName = buildEntityId.Substring(0, buildEntityId.IndexOf("/"));
                var isEnabled = bool.Parse(dataset["isEnabled"].ToString());
                if (!dataFactoryNames.Contains(dataFactoryName))
                {
                    dataFactoryNames.Add(dataFactoryName);
                    enabledDatasetCounts.Add(dataFactoryName, 0);
                    disabledDatasetCounts.Add(dataFactoryName, 0);
                }

                if (isEnabled)
                {
                    enabledDatasetCounts[dataFactoryName]++;
                }
                else
                {
                    disabledDatasetCounts[dataFactoryName]++;
                }
            }

            Console.WriteLine($"datasets count: {datasets.Count}");
            Console.WriteLine($"dataFactories count: {dataFactoryNames.Count}");

            List<string> dataFactoryNameList = new List<string>(dataFactoryNames);
            dataFactoryNameList.Sort();
            foreach (var dataFactoryName in dataFactoryNameList)
            {
                Console.WriteLine(dataFactoryName);
                Console.WriteLine(enabledDatasetCounts[dataFactoryName]);
                Console.WriteLine(disabledDatasetCounts[dataFactoryName]);
            }

            // Outout the data factories just contains disabled datasets but no enabled dataset
            foreach (var dataFactoryName in dataFactoryNameList)
            {
                if (enabledDatasetCounts[dataFactoryName] == 0)
                {
                    Console.WriteLine(dataFactoryName);
                }
            }
        }

        private static void GetDisabledDatasetsWithoutRightModifiedTime()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            IList<JObject> datasets = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                @"SELECT * FROM c WHERE c.createdBy = 'BuildDeployment'")).Result;
            Dictionary<string, int> dataFactoryNames = new Dictionary<string, int>();

            DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            foreach (JObject dataset in datasets)
            {
                if (bool.Parse(dataset["isEnabled"].ToString()))
                {
                    continue;
                }
                var ts = long.Parse(dataset["_ts"].ToString());
                DateTime tsTime = dtDateTime.AddSeconds(ts);
                var lastModifiedTime = DateTime.Parse(dataset["lastModifiedTime"].ToString());
                var tsHourlyTime = tsTime.Date.AddHours(tsTime.Hour);
                var lastModifiedHourlyTime = lastModifiedTime.Date.AddHours(lastModifiedTime.Hour).ToUniversalTime();
                Console.WriteLine(tsHourlyTime.ToString("o"));
                Console.WriteLine(lastModifiedHourlyTime.ToString("o"));
                if (!tsHourlyTime.Equals(lastModifiedHourlyTime))
                {
                    string buildEntityId = dataset["buildEntityId"].ToString();
                    var dataFactoryName = buildEntityId.Substring(0, buildEntityId.IndexOf("/"));
                    if (!dataFactoryNames.ContainsKey(dataFactoryName))
                    {
                        dataFactoryNames.Add(dataFactoryName, 0);
                    }

                    dataFactoryNames[dataFactoryName]++;
                }
            }

            foreach (var dataFactoryName in dataFactoryNames.Keys)
            {
                Console.WriteLine(dataFactoryName);
                Console.WriteLine(dataFactoryNames[dataFactoryName]);
            }
        }

        private static void UpsertDatasetDemo()
        {
            Console.WriteLine("Upsert demo Dataset.");
            string demoDatasetFilePath = @"D:\data\company_work\M365\IDEAs\datacop\LogAnalytics\dataset.json";
            var demoDatasetJson = JObject.Parse(File.ReadAllText(demoDatasetFilePath));
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            azureCosmosDBClient.UpsertDocumentAsync(demoDatasetJson).Wait();
            Console.WriteLine("End.");
        }

        private static void UpsertDatasetTestDemo()
        {
            Console.WriteLine("Upsert demo DatasetTest.");
            string demoDatasetTestFilePath = @"D:\data\company_work\M365\IDEAs\datacop\LogAnalytics\datasetTest.json";
            var demoDatasetTestJson = JObject.Parse(File.ReadAllText(demoDatasetTestFilePath));
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            azureCosmosDBClient.UpsertDocumentAsync(demoDatasetTestJson).Wait();
            Console.WriteLine("End.");
        }

        private static void UpdateWrongDataFabricInDatasets()
        {
            Console.WriteLine("UpdateWrongDataFabricInDatasets.");
            AzureCosmosDBClient datasetCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            AzureCosmosDBClient datasetTestCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasets = datasetCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(@"SELECT * FROM c where c.dataFabric = 1").Result;
            foreach (var dataset in datasets)
            {
                var datasetId = dataset["id"].ToString();
                Console.WriteLine($"Dataset id: '{datasetId}'");
                IList<JObject> datasetTests = datasetTestCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(
                    ($"SELECT * FROM c WHERE c.datasetId = '{datasetId}'")).Result;
                if (datasetTests.Count == 0)
                {
                    continue;
                }

                var testContentType = datasetTests[0]["testContentType"].ToString();
                if (testContentType.Equals(@"AdlsAvailability"))
                {
                    dataset["dataFabric"] = @"ADLS";
                    datasetCosmosDBClient.UpsertDocumentAsync(dataset).Wait();
                }

                datasetCosmosDBClient.UpsertDocumentAsync(dataset).Wait();
            }


            Console.WriteLine("End.");
        }

        private static void UpdateBuildDeploymentViewBooleanParameters()
        {
            AzureCosmosDBClient datasetCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            AzureCosmosDBClient datasetTestCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasets = datasetCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(
                (@"SELECT * FROM c WHERE c.createdBy = 'BuildDeployment' and c.dataFabric = 'CosmosView'")).Result;
            IList<JObject> datasetTests = datasetTestCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(
                (@"SELECT * FROM c WHERE c.createdBy = 'BuildDeployment' and c.testContentType = 'CosmosViewAvailability'")).Result;

            Console.WriteLine(@"Update datasets: ");
            var count = 0;
            foreach (JObject dataset in datasets)
            {
                var change = false;
                foreach (var parameter in dataset["cosmosViewParameters"].ToArray())
                {
                    if (parameter["value"].ToString().Equals("True"))
                    {
                        change = true;
                        parameter["value"] = "true";
                    }

                    if (parameter["value"].ToString().Equals("False"))
                    {
                        change = true;
                        parameter["value"] = "false";
                    }
                }

                if (change)
                {
                    Console.WriteLine(dataset["id"].ToString());
                    //datasetCosmosDBClient.UpsertDocumentAsync(dataset).Wait();
                    count++;
                }
            }

            Console.WriteLine($"Changed datasets count: {count}");

            Console.WriteLine(@"Update datasetTests: ");
            count = 0;
            foreach (JObject datasetTest in datasetTests)
            {
                var change = false;
                foreach (var parameter in datasetTest["testContent"]["otherParameters"].ToArray())
                {
                    if (parameter["value"].ToString().Equals("True"))
                    {
                        change = true;
                        parameter["value"] = "true";
                    }

                    if (parameter["value"].ToString().Equals("False"))
                    {
                        change = true;
                        parameter["value"] = "false";
                    }
                }

                var cosmosScriptContent = datasetTest["testContent"]["cosmosScriptContent"].ToString();
                if (cosmosScriptContent.Contains(" = True") || cosmosScriptContent.Contains(" = False"))
                {
                    change = true;
                    datasetTest["testContent"]["cosmosScriptContent"] = cosmosScriptContent
                                                                                          .Replace(" = True", " = true")
                                                                                          .Replace(" = False", " = false");
                }

                if (change)
                {
                    Console.WriteLine(datasetTest["id"].ToString());
                    //datasetTestCosmosDBClient.UpsertDocumentAsync(datasetTest).Wait();
                    count++;
                }
            }

            Console.WriteLine($"Changed datasetTests count: {count}");
        }

        private static void DisableAllBuildDeploymentDataset()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasets = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(
                (@"SELECT * FROM c WHERE c.createdBy = 'BuildDeployment'")).Result;
            foreach (JObject dataset in datasets)
            {
                Console.WriteLine(dataset["id"].ToString());
                dataset["isEnabled"] = false;
                azureCosmosDBClient.UpsertDocumentAsync(dataset).Wait();
                //Console.WriteLine(dataset);

                //string id = dataset["id"].ToString();
                //Console.WriteLine(id);
                //string documentLink = GetDocumentLink("DataCop", "Dataset", id);
                //var statusCode = azureCosmosDBClient.DeleteDocumentAsync(documentLink).Result;
            }
            Console.WriteLine(datasets.Count);
        }

        private static void UpdateSqlDatasetKeyVaultName()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasets = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(
                (@"SELECT * FROM c WHERE c.dataFabric = 'SQL'")).Result;
            foreach (JObject dataset in datasets)
            {
                Console.WriteLine(dataset["id"].ToString());
                Console.WriteLine(dataset["connectionInfo"]["auth"]["keyVaultName"]);
                dataset["connectionInfo"]["auth"]["keyVaultName"] = "datacop-prod";
                azureCosmosDBClient.UpsertDocumentAsync(dataset).Wait();
            }
            Console.WriteLine(datasets.Count);
        }

        private static void UpdateTestRunStatus()
        {
            Console.WriteLine("UpdateTestRunStatus");
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            // Collation: asc and desc is ascending and descending
            IList<JObject> testRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(
                (@"SELECT * FROM c WHERE contains(c.message, 'E_VCCLIENT_USER_FAILURE') and not  contains(c.message, '0x83090b74') and c.status != 'Failed' and c.createTime > '2021-01-28T0' order by c.createTime desc")).Result;
            foreach (JObject testRun in testRuns)
            {
                Console.WriteLine(testRun["id"]);
                Console.WriteLine(testRun["status"]);
                testRun["status"] = "Failed";
                azureCosmosDBClient.UpsertDocumentAsync(testRun).Wait();
            }
            Console.WriteLine(testRuns.Count);
        }

        private static void DisableAllCosmosDatasetTest()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasetTests = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.dataFabric = 'Cosmos' and c.status = 'Enabled'")).Result;
            foreach (JObject datasetTest in datasetTests)
            {
                string curTime = DateTime.UtcNow.ToString("o");
                Console.WriteLine(curTime);
                Console.WriteLine(datasetTest["id"]);
                datasetTest["status"] = "Disabled";
                datasetTest["lastModifiedTime"] = curTime;
                //datasetTest["resultExpirePeriod"] = "2.00:00:00";

                azureCosmosDBClient.UpsertDocumentAsync(datasetTest).Wait();
            }
        }

        private static void EnableAllCosmosDatasetTestSuccessively()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasetTests = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.dataFabric = 'Cosmos' and c.status = 'Disabled'")).Result;

            // Enable 1 cosmos datasetTest every 7 minute
            int enableCountOnce = 1, periodOnce = 7 * 60 * 1000;
            int count = 0;
            foreach (JObject datasetTest in datasetTests)
            {
                string curTime = DateTime.UtcNow.ToString("o");
                Console.WriteLine(curTime);
                Console.WriteLine(datasetTest["id"]);
                datasetTest["status"] = "Enabled";
                datasetTest["lastModifiedTime"] = curTime;
                azureCosmosDBClient.UpsertDocumentAsync(datasetTest).Wait();
                count++;
                if (count == enableCountOnce)
                {
                    count = 0;
                    Thread.Sleep(periodOnce);
                }

            }
        }

        private static void EnableAllCosmosDatasetTestWhenNoActiveMessage()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasetTests = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.dataFabric = 'Cosmos' and c.status = 'Disabled'")).Result;

            // Enable 1 cosmos datasetTest every 15 minute
            int periodOnce = 15 * 60 * 1000;
            foreach (JObject datasetTest in datasetTests)
            {
                string curTime = DateTime.UtcNow.ToString("o");
                Console.WriteLine(curTime);
                Console.WriteLine(datasetTest["id"]);
                datasetTest["status"] = "Enabled";
                datasetTest["lastModifiedTime"] = curTime;
                azureCosmosDBClient.UpsertDocumentAsync(datasetTest).Wait();
                bool existCosmosMessage = true;
                while (existCosmosMessage)
                {
                    Thread.Sleep(periodOnce);
                    existCosmosMessage = ExistCosmosMessage();
                }

            }
        }

        private static bool ExistCosmosMessage()
        {
            string queueName = "cosmostest";
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string serviceBusConnectionString = secretProvider.GetSecretAsync("datacop-prod", "ServiceBusConnectionString").Result;
            Dictionary<string, long> messageCountDetails = MicrosoftServiceBusLib.MicrosoftServiceBusClient.GetMessageCountDetails(serviceBusConnectionString, queueName);
            //foreach (var messageCountDetail in messageCountDetails)
            //{
            //    Console.WriteLine($"{messageCountDetail.Key}\t{messageCountDetail.Value}");
            //}
            long existCosmosMessage = messageCountDetails["activeCount"] + messageCountDetails["scheduledMessageCount"];
            Console.WriteLine($"ExistCosmosMessage: {existCosmosMessage}");
            return existCosmosMessage > 0;
        }

        private static void QueryAlertSettingDemo()
        {
            Console.WriteLine("QueryAlertSettingDemo:");
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "AlertSettings");
            // Collation: asc and desc is ascending and descending
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                @"SELECT distinct c.owningTeamId,c.targetIcMConnector,c.containerPublicId, c.routingId FROM c")).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine($"{jObject["owningTeamId"]}\t{jObject["targetIcMConnector"]}\t{jObject["containerPublicId"]}\t{jObject["routingId"]}");
            }
            Console.WriteLine(list.Count);

            list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
               @"SELECT * FROM c where c.routingId = 'IDEAS://SMB'")).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject);
            }
            Console.WriteLine(list.Count);

            list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
               @"SELECT distinct c.targetIcMConnector,c.containerPublicId FROM c")).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine($"{jObject["targetIcMConnector"]}\t{jObject["containerPublicId"]}");
            }
            Console.WriteLine(list.Count);
        }

        private static void QueryScheduleMonitorReportDemo()
        {
            Console.WriteLine("QueryScheduleMonitorReportDemo:");
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "ScheduleMonitorReport");
            string queryStr = @"SELECT c.reportStartDate,c.reportEndDate,c.datasetName,c.datasetTestName,c.allTestRunCount," +
                "c.successTestRunCount,c.abortedTestRunCount,c.testRunErrorMessage,c.incidentId FROM c where c.datasetTestName = 'DimCampaign Default CosmosAvailability Test'";
            queryStr = @"SELECT * FROM c where c.datasetTestName = 'DimCampaign Default CosmosAvailability Test'";

            // Collation: asc and desc is ascending and descending
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((queryStr)).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject);
            }
            Console.WriteLine(list.Count);
        }

        private static void ResetIncidentIdForMonitorReportDemo()
        {
            Console.WriteLine("ResetIncidentIdForMonitorReportDemo:");
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "ScheduleMonitorReport");

            // Remove useless
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                @"SELECT top 1000 * FROM c where is_defined(c.incidentId)")).Result;
            while (list.Any())
            {
                Console.WriteLine(list.Count);
                foreach (var report in list)
                {
                    string id = report["id"].ToString();
                    report["id"] = id + "/test";
                    //report.Remove("incidentId");
                    report["resultExpirePeriod"] = "12:00:00";
                    Console.WriteLine(id);
                    azureCosmosDBClient.UpsertDocumentAsync(report).Wait();
                }
                list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                @"SELECT top 1000 * FROM c where is_defined(c.incidentId)")).Result;
            }
        }

        private static void UpdateScheduleMonitorReportSampleDemo()
        {

            DateTime now = DateTime.UtcNow.Date;
            Random r = new Random();
            Console.WriteLine("UpdateScheduleMonitorReportSampleDemo:");
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "ScheduleMonitorReport");

            // Remove useless
            IList<JObject> removedList = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                @"SELECT * FROM c where c.reportStartDate != '2020-11-20T00:00:00Z'")).Result;
            Console.WriteLine("Remove useless instances:");
            foreach (var remove in removedList)
            {
                string id = remove["id"].ToString();
                Console.WriteLine(id);
                string documentLink = GetDocumentLink("DataCop", "ScheduleMonitorReport", id);
                var resource = azureCosmosDBClient.DeleteDocumentAsync(documentLink).Result;
            }

            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                @"SELECT * FROM c where c.reportStartDate = '2020-11-20T00:00:00Z'")).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject["id"]);
                for (int i = 1; i < 10; i++)
                {
                    jObject["id"] = Guid.NewGuid();
                    jObject["reportStartDate"] = now.AddDays(-i - 1).Date;
                    jObject["reportEndDate"] = now.AddDays(-i).Date;
                    azureCosmosDBClient.UpsertDocumentAsync(jObject).Wait();
                    int all = r.Next(100);
                    int success = r.Next(all);
                    int aborted = r.Next(all - success);

                    jObject["allTestRunCount"] = all;
                    jObject["successTestRunCount"] = success;
                    jObject["abortedTestRunCount"] = aborted;
                    var allIds = new JArray();
                    var successIds = new JArray();
                    var abortedIds = new JArray();
                    for (int j = 0; j < all; j++)
                    {
                        allIds.Add(Guid.NewGuid());
                    }
                    for (int j = 0; j < success; j++)
                    {
                        successIds.Add(Guid.NewGuid());
                    }
                    for (int j = 0; j < aborted; j++)
                    {
                        abortedIds.Add(Guid.NewGuid());
                    }
                    jObject["allTestIds"] = allIds;
                    jObject["successTestRunIds"] = successIds;
                    jObject["allTestIds"] = abortedIds;

                    int ran = r.Next(3);
                    jObject["testRunErrorMessage"] = ran == 0 ? "" : (ran == 1 ? "InternalException" : "Forbidden");
                    azureCosmosDBClient.UpsertDocumentAsync(jObject).Wait();
                }
            }
        }

        private static void QueryAlertSettingInDatasetTestsDemo()
        {
            Console.WriteLine("QueryAlertSettingInDatasetTestsDemo:");
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT distinct c.alertSettingId FROM c where c.status = 'Enabled'")).Result;

            AzureCosmosDBClient alertSettingsAzureCosmosDBClient = new AzureCosmosDBClient("DataCop", "AlertSettings");
            // Collation: asc and desc is ascending and descending
            IList<JObject> alertSettings = alertSettingsAzureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                @"SELECT * FROM c")).Result;
            Console.WriteLine(alertSettings.Count);
            Console.WriteLine(list.Count);

            HashSet<string> set = new HashSet<string>();
            foreach (JObject jObject in list)
            {
                if (jObject["alertSettingId"] != null)
                {
                    var alertSettingId = jObject["alertSettingId"].ToString();
                    foreach (var alertSetting in alertSettings)
                    {
                        if (alertSetting["id"].ToString().Equals(alertSettingId))
                        {
                            set.Add($"{alertSetting["owningTeamId"]}\t{alertSetting["targetIcMConnector"]}\t{alertSetting["containerPublicId"]}\t{alertSetting["routingId"]}");
                        }
                    }
                }
            }

            foreach (var alertSettingContent in set)
            {
                Console.WriteLine(alertSettingContent);
            }
            Console.WriteLine(set.Count);
        }

        private static void QueryDataSetDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.dataFabric = 'CosmosView'")).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject);
            }
        }

        private static void QueryTestRunTestContentDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            // Collation: asc and desc is ascending and descending
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.id = 'ba51c2ee-de0b-4b36-9793-3eca1a893af1'")).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject["testContent"].ToString());
                var compReq = JsonConvert.DeserializeObject<CosmosCompletenessTestContent>(jObject["testContent"].ToString());
                Console.WriteLine(compReq.FileRowCountMaxLimit);
            }
        }

        private static void QueryMonitroReportDemo()
        {
            // TODO: I don't think it works as my expection. Will follow up this function. 
            // ToString("s") of DateTime doesn't contains 'Z' in the string end if the datetime is the hour(minute/second and millisecond are zero) 
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "MonitorReport");

            DateTime timeStamp = DateTime.Parse("2019-11-26T06:00:00");
            string datasetTestId = "0faf52e7-b4bc-4674-8f1e-ff2c65e12f02";
            Grain grain = Grain.Hourly;
            string sqlQueryString = @"SELECT * FROM c" +
                                                         $" WHERE c.datasetTestId='{datasetTestId}' and c.timeStamp='{timeStamp.ToString("s")}' and c.grain='{grain.ToString()}'";
            Console.WriteLine(sqlQueryString);
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((sqlQueryString)).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject);
            }
        }

        private static void QueryServiceMonitorDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "ServiceMonitor");

            string sqlQueryString = @"SELECT * FROM c where c.isEnabled = true";
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((sqlQueryString)).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject);
            }
        }

        private static void QueryTestRunCount()
        {
            int minute = 5;
            var startTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            var startDate = DateTime.UtcNow.AddDays(-1);

            for (int i = 0; i < 100; i++)
            {
                var endDate = startDate.AddMinutes(minute);
                var startTs = (startDate - startTime).TotalSeconds;
                var endTs = (endDate - startTime).TotalSeconds;
                startDate = endDate;

                var count = GetQueryCount(
                    "DataCop",
                    "PartitionedTestRun",
                    new List<string> { $"c._ts > {startTs}", $"c._ts < {endTs}", "c.lastModifiedBy = 'AlertService'" });
                Console.WriteLine($"{startDate:o}: {count}");
            }
        }

        private static void QueryAlertCount()
        {
            var startTime = new DateTime(2021, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Alert");

            for (int i = 0; i < 30; i++)
            {
                var startDate = startTime.AddDays(i);
                var endDate = startDate.AddDays(1);
                string sqlQueryString = $"SELECT count(0) as count_num FROM c where (c.dataFabric= 'ADLS' or c.dataFabric= 'CosmosStream') and c.timestamp > '{startDate:o}' and c.timestamp < '{endDate:o}'";
                IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((sqlQueryString)).Result;

                Console.WriteLine($"{startDate:o} - {endDate:o}: {list[0]["count_num"]}");
            }
        }

        private static void QueryMonthlyTestRunCount()
        {
            string databaseId = "DataCop";
            string collectionId = "PartitionedTestRun";
            for (int year = 19; year < 21; year++)
            {
                for (int month = 1; month < 13; month++)
                {
                    var count = GetQueryCount(
                        databaseId,
                        collectionId,
                        new List<string> { $"c.createTime >= '20{year:00}-{month:00}-01'", $"c.createTime < '20{year:00}-{month + 1:00}-01'" });
                    Console.WriteLine($"Date: 20{year:00}-{month:00}\tcount: {count}");
                }
            }
            var sum = GetQueryCount(databaseId, collectionId);
            Console.WriteLine($"Sum: {sum}");
        }

        private static void QueryTestRuns()
        {
            IList<string> filters = new List<string>
            {
                //"c.id = '8286d39e-7efb-44e4-a6d9-d3568ea6200b'",
                "c.datasetId = '8a0ad95b-20f6-4da5-9719-b82f59b00691'",
                //"c.datasetTestId = 'd2c2991c-4d9b-43df-ac98-acd10a4caf0d'",
                //"c.partitionKey = ''",
                //"c.dataFabric = 'Spark'",
                //"c.status = 'Success'",
                //"c.createTime > '2020-11-28'",
            };

            IList<JObject> list = GetQueryResult("DataCop", "PartitionedTestRun", null, filters);
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject);
            }
            Console.WriteLine(list.Count);
        }

        private static void QueryTestRunsByDatasets()
        {
            AzureCosmosDBClient datasetCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            IList<JObject> azureDatasets = datasetCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((
                @"SELECT c.id FROM c WHERE c.createdBy = 'BuildDeployment' and (c.dataFabric = 'CosmosView' or c.dataFabric = 'CosmosStream')")).Result;
            Console.WriteLine($"BuildDeployment cosmos view dataset cout: {azureDatasets.Count}");

            var datasetIdsStr = string.Join(",", azureDatasets.Select(d => $"'{d["id"]}'"));

            AzureCosmosDBClient datasetTestCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            IList<JObject> testRuns = datasetTestCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(($@"SELECT * FROM c WHERE c.datasetId in ({datasetIdsStr})")).Result;
            foreach (JObject testRun in testRuns)
            {
                Console.WriteLine(testRun);
            }
            Console.WriteLine($"BuildDeployment cosmos view testRun count: {testRuns.Count}");
        }

        private static void QueryForbiddenTestRuns()
        {
            IList<string> properties = new List<string>
            {
                "datasetId",
                "message"
            };
            IList<string> filters = new List<string>
            {
                "(c.dataFabric = 'CosmosStream' or c.dataFabric = 'Adls' or c.dataFabric = 'ADLS')",
                "c.status = 'Aborted'",
                "contains(c.message, 'Forbidden')",
                "c.createTime > '2020-11-12'",
            };

            IList<JObject> list = GetQueryResult("DataCop", "PartitionedTestRun", properties, filters);
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject);
            }
            Console.WriteLine($"ForbiddenTestRunsCount: {list.Count}");
            WriteFile.FirstMethod(@"D:\data\company_work\M365\IDEAs\ForbiddenTestRuns.json", JsonConvert.SerializeObject(list));
        }

        private static void QueryDataCopScores()
        {
            var azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "TestRunResultForScore");
            var counts = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT count(1) FROM c")).Result;
            foreach (var count in counts)
            {
                Console.WriteLine($"count: {count}");
            }
            var dataCopScores = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT top 1000 * FROM c")).Result;
            Console.WriteLine("DataCopScores: ");
            foreach (var dataCopScore in dataCopScores)
            {
                Console.WriteLine(dataCopScore.ToString().Length);
            }
        }

        private static void QueryTestRunCountByDatasetId()
        {
            string filePath = @"D:\data\company_work\M365\IDEAs\BuildDeploymentDatasets.json";
            var content = File.ReadAllText(filePath, Encoding.UTF8);
            var datasets = JArray.Parse(content);
            var jArray = new JArray();
            foreach (var dataset in datasets)
            {
                string datasetId = dataset["id"].ToString();
                var count = GetQueryCount("DataCop", "PartitionedTestRun", new List<string> { $"c.datasetId = '{datasetId}'" });
                var abortedCount = GetQueryCount("DataCop", "PartitionedTestRun", new List<string> { "c.status = 'Aborted'", $"c.datasetId = '{datasetId}'" });
                var successCount = GetQueryCount("DataCop", "PartitionedTestRun", new List<string> { "c.status = 'Success'", $"c.datasetId = '{datasetId}'" });
                Console.WriteLine($"DatasetId: {datasetId}\t Count: {count}\t AbortedCount: {abortedCount}\t SuccessCount: {successCount}");
                var json = new JObject();
                json["datasetId"] = datasetId;
                json["count"] = count;
                json["abortedCount"] = abortedCount;
                json["successCount"] = successCount;
                jArray.Add(json);
            }
            WriteFile.FirstMethod(@"D:\data\company_work\M365\IDEAs\BuildDeploymentTestRunSummary.json", jArray.ToString());
        }

        private static void QueryDatasets()
        {
            Console.WriteLine("Query Datasets: ");

            var filters = new List<string>
            {
                //"c. id = '700663cc-cb12-4fb7-877b-262ab0160690'",
                "(c.createdBy = 'BuildDeployment' or c.createdBy = 'BuildTestExecution')",
                @"(c.dataFabric = 'CosmosView')",
                //@"(c.dataFabric = 'Adls' or c.dataFabric = 'ADLS')",
                //@"c.isEnabled = true"

            };
            IList<JObject> list = GetQueryResult("DataCop", "Dataset", null, filters);
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject);
            }
            Console.WriteLine(list.Count);
            //WriteFile.FirstMethod(@"D:\data\company_work\M365\IDEAs\BuildDeploymentDatasets.json", JsonConvert.SerializeObject(list));
        }

        private static void QueryDatasetTests()
        {
            Console.WriteLine("Query DatasetTests: ");

            var filters = new List<string>
            {
                //"c.id = 'e32035f7-2fa3-4b54-9114-5605138a3c89'",
                "(c.createdBy = 'BuildDeployment' or c.createdBy = 'BuildTestExecution')",
                //"c.datasetId = '700663cc-cb12-4fb7-877b-262ab0160690'",
                //@"(c.dataFabric = 'Adls' or c.dataFabric = 'ADLS')",
                @"(c.dataFabric = 'Cosmos' or c.dataFabric = 'CosmosView')",
                //@"c.status = 'Enabled'"
            };
            IList<JObject> list = GetQueryResult("DataCop", "DatasetTest", null, filters);
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject);
            }
            Console.WriteLine(list.Count);
            //WriteFile.FirstMethod(@"D:\data\company_work\M365\IDEAs\BuildDeploymentDatasets.json", JsonConvert.SerializeObject(list));
        }

        private static void QueryServiceMonitorReports()
        {
            var filters = new List<string>
            {
                //"c. id = 'eaf6509f-e2ed-4dae-bead-f1a40d45ee6d'"
                @"(c.dataFabric = 'Adls' or c.dataFabric = 'ADLS')",
                @"c.reportStartTimeStamp > '2020-11-11'",
            };
            IList<JObject> list = GetQueryResult("DataCop", "ServiceMonitorReport", null, filters);
            //IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((sqlQueryString.ToString())).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject);
            }
            Console.WriteLine(list.Count);
            //WriteFile.FirstMethod(@"D:\data\company_work\M365\IDEAs\BuildDeploymentDatasets.json", JsonConvert.SerializeObject(list));
        }

        private static void QueryDatasetCount()
        {
            var filters = new List<string>
            {
                "(c.createdBy = 'BuildDeployment' or c.createdBy = 'BuildTestExecution')",
                @"(c.dataFabric = 'Adls' or c.dataFabric = 'ADLS')",

            };
            var buildAdlsCount = GetQueryCount("DataCop", "Dataset", filters);
            Console.WriteLine($"buildAdlsCount：{buildAdlsCount}");

            filters = new List<string>
            {
                "(c.createdBy = 'BuildDeployment' or c.createdBy = 'BuildTestExecution')",
                @"c.dataFabric = 'CosmosStream'",

            };
            var buildCosmosStreamCount = GetQueryCount("DataCop", "Dataset", filters);
            Console.WriteLine($"buildCosmosStreamCount：{buildCosmosStreamCount}");

            filters = new List<string>
            {
                "(c.createdBy = 'BuildDeployment' or c.createdBy = 'BuildTestExecution')",
                @"c.dataFabric = 'CosmosView'",

            };
            var buildCosmosViewCount = GetQueryCount("DataCop", "Dataset", filters);
            Console.WriteLine($"buildCosmosViewCount：{buildCosmosViewCount}");

        }

        private static long GetQueryCount(string databaseId, string collectionId, IList<string> filters = null)
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient(databaseId, collectionId);
            // Cross partition query only supports 'VALUE <AggreateFunc>' for aggregates.
            StringBuilder queryStr = new StringBuilder(@"SELECT value count(1) FROM c");
            if (filters != null && filters.Count > 0)
            {
                queryStr.Append($" where {string.Join(" and ", filters)}");
            }

            // The reuslt schema is not json, so we cannot use JObject, or it will throw a serialization error.
            IList<JToken> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JToken>((queryStr.ToString())).Result;
            return long.Parse(list[0].ToString());
        }

        private static IList<JObject> GetQueryResult(string databaseId,
                                                     string collectionId,
                                                     IList<string> properties = null,
                                                     IList<string> filters = null,
                                                     string suffix = null)
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient(databaseId, collectionId);
            // Cross partition query only supports 'VALUE <AggreateFunc>' for aggregates.
            StringBuilder queryStr;
            if (properties == null || properties.Count < 1)
            {
                queryStr = new StringBuilder(@"SELECT * FROM c");
            }
            else
            {
                queryStr = new StringBuilder($"SELECT {string.Join(",", properties.Select(p => $"c.{p}"))} FROM c");
            }
            if (filters != null && filters.Count > 0)
            {
                queryStr.Append($" where {string.Join(" and ", filters)}");
            }

            if (string.IsNullOrEmpty(suffix))
            {
                queryStr.Append(" " + suffix);
            }

            // The reuslt schema is not json, so we cannot use JObject, or it will throw a serialization error.
            return azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((queryStr.ToString())).Result;
        }


        private static long GetQueryKeyWord(string databaseId, string collectionId, string keyWord, string propertyName, IList<string> filters = null)
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient(databaseId, collectionId);
            // Cross partition query only supports 'VALUE <AggreateFunc>' for aggregates.
            StringBuilder queryStr = new StringBuilder($"SELECT value {keyWord}(c.{propertyName}) FROM c");
            if (filters != null && filters.Count > 0)
            {
                queryStr.Append($" where {string.Join(" and ", filters)}");
            }

            // The reuslt schema is not json, so we cannot use JObject, or it will throw a serialization error.
            IList<JToken> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JToken>((queryStr.ToString())).Result;
            return long.Parse(list[0].ToString());
        }


        /// <summary>
        /// Test to get the time cost of using try-catch/select-value-count(0) for the function "GetQueryResult"
        /// Make sure which way is better
        /// Time cost of try-catch is more than 10 times that of select-value-count(0), 
        /// select-value-count(0) is much better.
        /// </summary>
        /// <returns></returns>
        private static void GetTimeCostDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");

            int len = 10;
            string tryQuey = @"SELECT * FROM c";
            string countQuery = @"SELECT value count(0) FROM c";
            string errorMessage = @"Too many documents found in the query specified. Please break your query into smaller chunks.";

            // Same as: 
            // var watch = Stopwatch.StartNew();
            Stopwatch watch = new Stopwatch();
            watch.Start();
            for (int i = 0; i < len; i++)
            {
                try
                {
                    IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((tryQuey)).Result;
                }
                catch (AggregateException ae)
                {
                    if (ae.InnerException is InvalidOperationException && ae.InnerException.Message.ToString().Equals(errorMessage))
                    {
                        continue;
                    }

                    throw;
                }
            }

            Console.WriteLine($"try-catch time cost: {watch.ElapsedMilliseconds} ms");

            watch = new Stopwatch();
            watch.Start();
            for (int i = 0; i < len; i++)
            {
                IList<JToken> counts = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JToken>((countQuery)).Result;
            }

            Console.WriteLine($"select count time cost: {watch.ElapsedMilliseconds} ms");
        }

        private static void QueryKenshoDataset()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c where c.kenshoData != null and c.isEnabled = true")).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject);
            }
        }

        private class CosmosCompletenessTestContent
        {
            /// <summary>
            /// Gets or sets the row count min limit.
            /// </summary>
            /// <value>The row count min limit.</value>
            [JsonProperty("fileRowCountMinLimit", Required = Required.Always)]
            public long FileRowCountMinLimit { get; set; }

            /// <summary>
            /// Gets or sets the row count max limit.
            /// Using double type to avoid the issue of digital out of bounds.
            /// </summary>
            /// <value>The row count max limit.</value>
            [JsonProperty("fileRowCountMaxLimit", Required = Required.Always)]
            public double FileRowCountMaxLimit { get; set; }
        }

        private static void DeleteTestRunDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            string id = "900c3f79-79e1-45e1-82ff-17f308a62179";
            string partitionKey = "2020-04-03T00:00:00Z";
            string documentLink = GetDocumentLink("DataCop", "PartitionedTestRun", id).ToString();
            var statusCode = azureCosmosDBClient.DeleteDocumentAsync("DataCop", "PartitionedTestRun", id, partitionKey).Result;
            Console.WriteLine(statusCode);
        }

        private static void DeleteTestRuns()
        {
            string datasetId = @"";
            string partitionKey = @"";
            string dataFabric = @"";
            string status = @"";
            string createTimeMin = @"";

            int count = 1000;
            datasetId = @"8a0ad95b-20f6-4da5-9719-b82f59b00691";
            //partitionKey = @"2020-10-10T00:00:00";
            //dataFabric = "CosmosStream";
            //status = @"Waiting";
            //createTimeMin = @"2020-11-01";

            var start = true;
            StringBuilder sqlQueryString = new StringBuilder($"SELECT top {count} * FROM c");

            if (!string.IsNullOrEmpty(datasetId))
            {
                if (start)
                    sqlQueryString.Append(" WHERE");
                else
                    sqlQueryString.Append(" and");
                sqlQueryString.Append($" c.datasetId = '{datasetId}'");
                start = false;
            }

            if (!string.IsNullOrEmpty(partitionKey))
            {
                if (start)
                    sqlQueryString.Append(" WHERE");
                else
                    sqlQueryString.Append(" and");
                sqlQueryString.Append($" c.partitionKey = '{partitionKey}'");
                start = false;
            }

            if (!string.IsNullOrEmpty(dataFabric))
            {
                if (start)
                    sqlQueryString.Append(" WHERE");
                else
                    sqlQueryString.Append(" and");
                sqlQueryString.Append($" c.dataFabric = '{dataFabric}'");
                start = false;
            }

            if (!string.IsNullOrEmpty(status))
            {
                if (start)
                    sqlQueryString.Append(" WHERE");
                else
                    sqlQueryString.Append(" and");
                sqlQueryString.Append($" c.status = '{status}'");
                start = false;
            }

            if (!string.IsNullOrEmpty(createTimeMin))
            {
                if (start)
                    sqlQueryString.Append(" WHERE");
                else
                    sqlQueryString.Append(" and");
                sqlQueryString.Append($" c.createTime >= '{createTimeMin}'");
            }

            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            // Collation: asc and desc is ascending and descending
            IList<JObject> testRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((sqlQueryString.ToString())).Result;
            while (testRuns.Count > 0)
            {
                foreach (JObject testRun in testRuns)
                {
                    string id = testRun["id"].ToString();
                    string partitionKeyInJson = testRun["partitionKey"].ToString();
                    Console.WriteLine(id);
                    try
                    {
                        string documentLink = GetDocumentLink("DataCop", "PartitionedTestRun", id).ToString();
                        var statusCode = azureCosmosDBClient.DeleteDocumentAsync(documentLink, partitionKeyInJson).Result;
                        Console.WriteLine(statusCode);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Removing testRun '{id}' has error:");
                        Console.WriteLine(JsonConvert.SerializeObject(e));
                    }
                }
                Console.WriteLine($"Remove TestRun count: {testRuns.Count}");
                testRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((sqlQueryString.ToString())).Result;
            }
        }

        private static void DeleteWaitingOrchestrateTestRuns()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            // Collation: asc and desc is ascending and descending
            IList<JObject> testRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT top 1000 * FROM c WHERE c.status = 'Waiting' and contains(c.partitionKey, 'T00:00:00') order by c.createTime desc")).Result;
            while (testRuns.Count > 0)
            {
                foreach (JObject testRun in testRuns)
                {
                    string id = testRun["id"].ToString();
                    Console.WriteLine(id);
                    try
                    {
                        string partitionKey = ((DateTime)testRun["partitionKey"]).ToString("s");
                        string documentLink = GetDocumentLink("DataCop", "PartitionedTestRun", id).ToString();
                        var statusCode = azureCosmosDBClient.DeleteDocumentAsync(documentLink, partitionKey).Result;
                        Console.WriteLine(statusCode);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                    try
                    {
                        string partitionKey = ((DateTime)testRun["partitionKey"]).ToString("s") + "Z";
                        string documentLink = GetDocumentLink("DataCop", "PartitionedTestRun", id).ToString();
                        var statusCode = azureCosmosDBClient.DeleteDocumentAsync(documentLink, partitionKey).Result;
                        Console.WriteLine(statusCode);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                }
                testRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT top 1000 * FROM c WHERE c.status = 'Waiting' and contains(c.partitionKey, 'T00:00:00') order by c.createTime desc")).Result;
            }
        }

        // Delete CosmosDB instance without partitionKey
        private static void DeleteAlertsWithoutIncidentId()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Alert");
            // Collation: asc and desc is ascending and descending
            IList<JObject> alerts = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT top 1000 * FROM c WHERE not is_defined(c.incidentId)")).Result;
            while (alerts.Count > 0)
            {
                foreach (JObject alert in alerts)
                {
                    string id = alert["id"].ToString();
                    Console.WriteLine(id);
                    string documentLink = GetDocumentLink("DataCop", "Alert", id);
                    var statusCode = azureCosmosDBClient.DeleteDocumentAsync(documentLink).Result;
                    //Console.WriteLine(statusCode);
                }
                alerts = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT top 1000 * FROM c WHERE not is_defined(c.incidentId)")).Result;
            }
        }

        private static void DeleteWrongAlertsFromDataCopTest()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Alert");
            // Collation: asc and desc is ascending and descending
            IList<JObject> alerts = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT top 200 * FROM c WHERE c.owningTeamId = 'IDEAS\\IDEAsDataCopTest' and not is_defined(c.impactedTestRunIds) ORDER BY c._ts DESC")).Result;
            foreach (JObject alert in alerts)
            {
                string id = alert["id"].ToString();
                Console.WriteLine(id);
                string documentLink = GetDocumentLink("DataCop", "Alert", id);
                var statusCode = azureCosmosDBClient.DeleteDocumentAsync(documentLink).Result;
                Console.WriteLine(statusCode);
            }
        }

        private static void UpdateAlertSettingToGitFolder()
        {
            AzureCosmosDBClient alertSettingsCosmosDBClient = new AzureCosmosDBClient("DataCop", "AlertSettings");

            // Filter out duplicates alertSettings
            IList<JObject> alertSettingJObjects = alertSettingsCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT * FROM c")).Result;
            //var alertSettingDict = ClusterAlertSettings(alertSettingJObjects);
            //Console.WriteLine(alertSettingJObjects.Count);
            //Console.WriteLine(alertSettingDict.Count);
            //foreach (var item in alertSettingDict.Keys)
            //{
            //    Console.WriteLine(item);
            //}

            //// Filter out alertSetting identifier used by datasetTest
            //AzureCosmosDBClient datasetTestCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            //IList<JObject> datasetTestJObjects = datasetTestCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>((@"SELECT distinct c.alertSettingId FROM c where c.status = 'Enabled' and is_defined(c.alertSettingId)")).Result;
            //HashSet<string> alertSettingIdsSet = new HashSet<string>();
            //foreach (JObject datasetTestJObject in datasetTestJObjects)
            //{
            //    string alertSettingId = datasetTestJObject["alertSettingId"].ToString();
            //    alertSettingIdsSet.Add(alertSettingId);
            //}
            //foreach (var alertSettingId in alertSettingIdsSet)
            //{
            //    Console.WriteLine(alertSettingId);
            //}
            //Console.WriteLine(alertSettingIdsSet.Count);

            //Console.WriteLine("...............");


            var filePaths = ReadFile.GetAllFilePath(@"D:\IDEAs\repos\Ibiza\Source\DataCopMonitors\PROD");

            Dictionary<string, Tuple<string, JObject>> gitAlertSettingDict = new Dictionary<string, Tuple<string, JObject>>();
            foreach (var filePath in filePaths)
            {
                if (filePath.ToLower().Contains("alertsetting"))
                {
                    Console.WriteLine(filePath);
                    string fileContent = ReadFile.ThirdMethod(filePath);
                    JObject gitAlertSettingJObject = JObject.Parse(fileContent);
                    gitAlertSettingDict.Add(gitAlertSettingJObject["id"].ToString(), new Tuple<string, JObject>(filePath, gitAlertSettingJObject));
                }
            }

            foreach (JObject alertSettingJObject in alertSettingJObjects)
            {
                string id = alertSettingJObject["id"].ToString();
                string owningTeamId = alertSettingJObject["owningTeamId"].ToString();
                var environment = alertSettingJObject["environment"];
                var onCallPlaybookLink = alertSettingJObject["onCallPlaybookLink"];
                var dataCopPortalLink = alertSettingJObject["dataCopPortalLink"];
                alertSettingJObject.Remove("environment");
                alertSettingJObject.Remove("onCallPlaybookLink");
                alertSettingJObject.Remove("dataCopPortalLink");
                alertSettingJObject["environment"] = environment == null ? "Prod" : environment;
                alertSettingJObject["onCallPlaybookLink"] = onCallPlaybookLink;
                alertSettingJObject["dataCopPortalLink"] = dataCopPortalLink;
                if (gitAlertSettingDict.ContainsKey(id))
                {
                    Console.WriteLine(id);
                    if (!string.IsNullOrEmpty(alertSettingJObject["serviceCustomFieldNames"].ToString()) && owningTeamId.StartsWith("IDEAS"))
                    {
                        var serviceCustomFieldNames = new JArray() {
                            "DatasetId",
                            "AlertType",
                            "DisplayInSurface",
                            "BusinessOwner",
                            "ScenarioIds"
                        };
                        alertSettingJObject["serviceCustomFieldNames"] = serviceCustomFieldNames;
                    }

                    alertSettingJObject.Remove("ttl");
                    alertSettingJObject.Remove("_rid");
                    alertSettingJObject.Remove("_self");
                    alertSettingJObject.Remove("_etag");
                    alertSettingJObject.Remove("_attachments");
                    alertSettingJObject.Remove("_ts");
                    WriteFile.FirstMethod(gitAlertSettingDict[id].Item1, alertSettingJObject.ToString());
                }
                else if (!ContainsNumber(id) && owningTeamId.StartsWith("IDEAS"))
                {
                    string gitFolderPath = @"C:\Users\jianjlv\source\repos\Ibiza\Source\DataCopMonitors\PROD\AlertSettings\";
                    string gitFilePath = gitFolderPath + id + ".json";
                    if (!string.IsNullOrEmpty(alertSettingJObject["serviceCustomFieldNames"].ToString()) && owningTeamId.StartsWith("IDEAS"))
                    {
                        var serviceCustomFieldNames = new JArray() {
                            "DatasetId",
                            "AlertType",
                            "DisplayInSurface",
                            "BusinessOwner",
                            "ScenarioIds"
                        };
                        alertSettingJObject["serviceCustomFieldNames"] = serviceCustomFieldNames;
                    }
                    alertSettingJObject.Remove("ttl");
                    alertSettingJObject.Remove("_rid");
                    alertSettingJObject.Remove("_self");
                    alertSettingJObject.Remove("_etag");
                    alertSettingJObject.Remove("_attachments");
                    alertSettingJObject.Remove("_ts");
                    WriteFile.FirstMethod(gitFilePath, alertSettingJObject.ToString());
                }
            }
        }

        private static Dictionary<string, IList<JObject>> ClusterAlertSettings(IList<JObject> alertSettingJObjects)
        {
            Dictionary<string, IList<JObject>> alertSettingDict = new Dictionary<string, IList<JObject>>();


            foreach (JObject alertSettingJObject in alertSettingJObjects)
            {
                string id = alertSettingJObject["id"].ToString();
                var serviceCustomFieldNames = JArray.Parse(alertSettingJObject["serviceCustomFieldNames"].ToString());
                serviceCustomFieldNames.Add("ScenarioIds");
                alertSettingJObject["serviceCustomFieldNames"] = serviceCustomFieldNames;
                alertSettingJObject.Remove("_rid");
                alertSettingJObject.Remove("_self");
                alertSettingJObject.Remove("_etag");
                alertSettingJObject.Remove("_attachments");
                alertSettingJObject.Remove("_ts");
                alertSettingJObject.Remove("id");
                string alertSettingStr = alertSettingJObject.ToString();
                if (alertSettingDict.ContainsKey(alertSettingStr))
                {
                    alertSettingDict[alertSettingStr].Add(alertSettingJObject);
                }
                else
                {
                    alertSettingDict[alertSettingStr] = new List<JObject>() { alertSettingJObject };
                    Console.WriteLine(id);
                }
                alertSettingJObject["id"] = id;
            }

            return alertSettingDict;
        }

        private static bool ContainsLetter(string word)
        {
            for (var letter = 'a'; letter <= 'z'; letter++)
            {
                if (word.Contains(letter.ToString())) return true;
            }
            for (var letter = 'A'; letter <= 'Z'; letter++)
            {
                if (word.Contains(letter.ToString())) return true;
            }
            return false;
        }

        private static bool ContainsNumber(string word)
        {
            for (var letter = '0'; letter <= '9'; letter++)
            {
                if (word.Contains(letter.ToString())) return true;
            }
            return false;
        }
        private static string GetDocumentLink(
            string databaseName,
            string collectionName,
            string documentId) => GetCollectionLink(databaseName, collectionName) + $"docs/{documentId}";

        private static string GetCollectionLink(
            string databaseName,
            string collectionName) => $"/dbs/{databaseName}/colls/{collectionName}/";
        private static void DeleteCosmosTestRunByResultExpirePeriod()
        {
            string resultExpirePeriod = "2.00:00:00";
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            // Collation: asc and desc is ascending and descending
            IList<JObject> testRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(($@"SELECT * FROM c WHERE c.dataFabric= 'Cosmos' and c.resultExpirePeriod = '{resultExpirePeriod}'")).Result;
            foreach (JObject testRun in testRuns)
            {
                string id = testRun["id"].ToString();

                if (id != "6df33f69-706e-43bd-8736-b3cb04515ad9") continue;

                Console.WriteLine(testRun);
                Console.WriteLine(testRun["partitionKey"]);
                string partitionKey = testRun["partitionKey"].ToString();
                partitionKey = JsonConvert.SerializeObject(DateTime.Parse(testRun["partitionKey"].ToString())).Trim('"');
                string documentLink = GetDocumentLink("DataCop", "Dataset", id).ToString();
                Console.WriteLine(id);
                Console.WriteLine(partitionKey);
                //var reqOptions = new RequestOptions { PartitionKey = new PartitionKey(partitionKey) };
                //var statusCode = azureCosmosDBClient.DeleteDocumentAsync(documentLink, reqOptions).Result;
                //Console.WriteLine(statusCode);
            }
        }
        private static void DisableAbortedTest()
        {
            // Based on the testRuns in the past half a month
            DateTime startDate = DateTime.UtcNow.AddDays(-15);
            HashSet<string> datasetIds = new HashSet<string>();
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");

            IList<JObject> testRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(
                ($@"SELECT distinct c.datasetId FROM c WHERE c.dataFabric = 'ADLS' and c.status = 'Aborted' and contains(c.message, 'Forbidden') and c.createTime > '{startDate:o}' order by c.createTime desc")).Result;
            foreach (var testRun in testRuns)
            {
                datasetIds.Add(testRun["datasetId"].ToString());
                Console.WriteLine(testRun["datasetId"]);
            }

            string root = @"D:\IDEAs\repo\Ibiza\Source\DataCopMonitors";
            var datasetTestFolders = Directory.GetDirectories(root, "Dataset*", SearchOption.AllDirectories);
            var files = GetFiles(datasetTestFolders);
            Console.WriteLine(files.Count);
            foreach (var filePath in files)
            {
                var fileContent = ReadFile.ThirdMethod(filePath);
                JToken dataset = JToken.Parse(fileContent);
                if (datasetIds.Contains(dataset["id"].ToString()))
                {
                    dataset["isEnabled"] = false;
                    dataset["createTime"] = DateTime.Parse(dataset["createTime"].ToString()).ToString("o") + "Z";
                    dataset["lastModifiedTime"] = DateTime.Parse(dataset["lastModifiedTime"].ToString()).ToString("o") + "Z";
                    WriteFile.FirstMethod(filePath, dataset.ToString());
                }

            }


        }
        private static List<string> GetFiles(string[] folders)
        {
            var allFiles = new List<string>();
            foreach (var folder in folders)
            {
                var files = Directory.EnumerateFiles(folder, "*.json", SearchOption.AllDirectories);
                allFiles.AddRange(files);
            }

            return allFiles;
        }

        private static void Initialize(string keyVaultName)
        {
            var secretProvider = KeyVaultSecretProvider.Instance;
            string endpoint = secretProvider.GetSecretAsync(keyVaultName, "CosmosDBEndPoint").Result;
            string key = secretProvider.GetSecretAsync(keyVaultName, "CosmosDBAuthKey").Result;
            AzureCosmosDBClient.Endpoint = endpoint;
            AzureCosmosDBClient.Key = key;
        }
    }
}
