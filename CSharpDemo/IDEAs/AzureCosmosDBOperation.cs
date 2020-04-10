namespace CSharpDemo.IDEAs
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json.Linq;
    using CSharpDemo.Azure.CosmosDB;
    using System.Threading;
    using Newtonsoft.Json;
    using AzureLib.KeyVault;
    using System.Text.RegularExpressions;
    using CSharpDemo.FileOperation;
    using System.Linq;

    public class AzureCosmosDBClientOperation
    {
        // "datacopdev","ideasdatacopppe" or "datacopprod"
        static readonly string KeyVaultName = "ideasdatacopppe";

        public static void MainMethod()
        {
            AzureCosmosDBClient.KeyVaultName = KeyVaultName;

            //UpdateAllAlertSettingsDemo();
            //UpdateAllDatasetTestCreatedBy();
            //UpdateAllDatasetForMerging();
            //UpdateAllDatasetTestForMerging();
            //UpdateAllCosmosTestResultExpirePeriod();
            //UpdateAllCosmosTestCreateTime();
            //UpdateAlertSettingToGitFolder();
            //UpsertServiceMonitorDemo();

            //DisableAllCFRMonitor();
            //InsertCFRMonitorConfig();

            //DisableAllDataset();
            //EnableDataset();
            //DisableAllCosmosDatasetTest();
            //EnableAllCosmosDatasetTestSuccessively();
            //EnableAllCosmosDatasetTestWhenNoActiveMessage();

            //QueryAlertSettingDemo();
            //QueryDataSetDemo();
            //QueryTestRunTestContentDemo();
            //QueryMonitroReportDemo();
            //QueryServiceMonitorDemo();
            //QueryTestRunCount();

            //DeleteTestRunById();
            DeleteWaitingTestRun();
            //DeleteCosmosTestRunByResultExpirePeriod();
            // We can use this function to delete instance without any limitation.
            //DeleteAlertsWithoutIncidentId();

            //MigrateData("ServiceMonitorReport", "datacopprod", "ideasdatacopppe");

            //AddCompletenessMonitors4ADLS();

            //CheckDatasetTestIntegrity();
            //CheckPPEAlertsettingOwningTeamAndRouting();
            //CheckAdlsConnectionInfoMappingCorrectness();
            //CheckCosmosConnectionInfoMappingCorrectness();
            //CheckDuplicatedEnabledDatasetTest();

            //SetOutdatedForDuplicatedDatasetTest();
        }

        // Disable all the CFR monitor dataset and datasetTest
        public static void DisableAllCFRMonitor()
        {
            AzureCosmosDBClient datasetCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> azureDatasets = datasetCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c WHERE contains(c.id, 'CFR')")).Result;
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
            IList<JObject> azureDatasetTests = datasetTestCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec($@"SELECT * FROM c WHERE c.datasetId in ({datasetIdsStr}) and c.status = 'Enabled'")).Result;
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
        public static void InsertCFRMonitorConfig()
        {
            AzureCosmosDBClient datasetCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            AzureCosmosDBClient datasetTestCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            IList<JObject> azureDatasets = datasetCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c WHERE contains(c.id, 'CFR')")).Result;
            var datasetIdsStr = string.Join(",", azureDatasets.Select(d => $"'{d["id"]}'"));
            IList<JObject> azureDatasetTests = datasetTestCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec($@"SELECT * FROM c WHERE c.datasetId in ({datasetIdsStr}) and c.status = 'Enabled'")).Result;
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
            var filePaths = ReadFile.GetAllFile(folderPath);
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

        public static void SetOutdatedForDuplicatedDatasetTest()
        {
            AzureCosmosDBClient azureDatasetTestCosmosDB = new AzureCosmosDBClient("DataCop", "DatasetTest");
            AzureCosmosDBClient azureDatasetCosmosDB = new AzureCosmosDBClient("DataCop", "Dataset");

            // Collation: asc and desc is ascending and descending
            IList<JObject> datasetList = azureDatasetCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.dataFabric = 'ADLS' and c.isEnabled = true")).Result;
            IList<JObject> completenessDatasetTestList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.testContentType = 'AdlsCompleteness' and c.status = 'Enabled' and c.createdBy = 'DefaultTestGenerator' and c.lastModifiedBy = 'DefaultTestGenerator' order by c.lastModifiedTime")).Result;

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

        public static void CheckDuplicatedEnabledDatasetTest()
        {
            AzureCosmosDBClient azureDatasetTestCosmosDB = new AzureCosmosDBClient("DataCop", "DatasetTest");
            AzureCosmosDBClient azureDatasetCosmosDB = new AzureCosmosDBClient("DataCop", "Dataset");

            // Collation: asc and desc is ascending and descending
            IList<JObject> datasetList = azureDatasetCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.dataFabric = 'ADLS' and c.isEnabled = true")).Result;
            IList<JObject> completenessDatasetTestList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.testContentType = 'AdlsCompleteness' and c.status = 'Enabled' order by c.lastModifiedTime")).Result;

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

        public static void CheckAdlsConnectionInfoMappingCorrectness()
        {
            AzureCosmosDBClient azureDatasetCosmosDB = new AzureCosmosDBClient("DataCop", "Dataset");
            AzureCosmosDBClient azureDatasetTestCosmosDB = new AzureCosmosDBClient("DataCop", "DatasetTest");
            IList<JObject> adlsDatasetTestJObjectList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where (c.testContentType = 'AdlsCompleteness' or c.testContentType = 'AdlsAvailability') and c.status = 'Enabled'")).Result;
            IList<JObject> datasetJObjectList = azureDatasetCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.dataFabric = 'ADLS' and c.isEnabled = true")).Result;

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

        public static void CheckCosmosConnectionInfoMappingCorrectness()
        {
            AzureCosmosDBClient azureDatasetCosmosDB = new AzureCosmosDBClient("DataCop", "Dataset");
            AzureCosmosDBClient azureDatasetTestCosmosDB = new AzureCosmosDBClient("DataCop", "DatasetTest");
            IList<JObject> cosmosDatasetTestJObjectList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where (c.testContentType = 'CosmosCompleteness' or c.testContentType = 'CosmosAvailability') and c.status = 'Enabled'")).Result;
            IList<JObject> datasetJObjectList = azureDatasetCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.dataFabric = 'Cosmos' and c.isEnabled = true")).Result;

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

        public static void CheckPPEAlertsettingOwningTeamAndRouting()
        {
            string cfrId = "da71491f-c49a-475e-9d54-d2fde4a6403f";
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "AlertSettings");
            IList<JObject> alertSettings = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c")).Result;
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

        public static void CheckDatasetTestIntegrity()
        {
            AzureCosmosDBClient azureDatasetTestCosmosDB = new AzureCosmosDBClient("DataCop", "DatasetTest");
            AzureCosmosDBClient azureDatasetCosmosDB = new AzureCosmosDBClient("DataCop", "Dataset");
            IList<JObject> availabilityDatasetIdJObjectList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT c.datasetId FROM c where c.testCategory = 'Availability' and c.status = 'Enabled'")).Result;
            IList<JObject> completenessDatasetIdJObjectList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT c.datasetId FROM c where c.testCategory = 'Completeness' and c.status = 'Enabled'")).Result;
            IList<JObject> datasetIdJObjectList = azureDatasetCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT c.id FROM c where c.dataFabric != 'SQL' and c.isEnabled = true")).Result;


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

        public static void AddCompletenessMonitors4ADLS()
        {
            string dirPath = @"D:\data\company_work\M365\IDEAs\work_item_file\1161175\write\";
            string duplicateDirPath = @"D:\data\company_work\M365\IDEAs\work_item_file\1161175\duplicate\";


            AzureCosmosDBClient azureDatasetTestCosmosDB = new AzureCosmosDBClient("DataCop", "DatasetTest");
            AzureCosmosDBClient azureDatasetCosmosDB = new AzureCosmosDBClient("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> availabilityList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.testContentType = 'AdlsAvailability' and c.status = 'Enabled'")).Result;

            IList<JObject> completenessList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.testContentType = 'AdlsCompleteness' and c.status = 'Enabled'")).Result;

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
            IList<JObject> availabilityList = azureDatasetCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec($@"SELECT * FROM c where c.id = '{datasetId}'")).Result;
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

        public static void MigrateData(string collectionId, string fromKeyVaultName, string toKeyVaultName)
        {
            AzureCosmosDBClient.KeyVaultName = fromKeyVaultName;
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", collectionId);
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.level = 'DataCop' and c.reportStartTimeStamp > '2020-02-22T15:00:00'")).Result;

            // This is a funny thing. azureCosmosDBClient has not been changed.
            AzureCosmosDBClient.KeyVaultName = toKeyVaultName;
            azureCosmosDBClient = new AzureCosmosDBClient("DataCop", collectionId, CosmosDBDocumentClientMode.NoSingle);
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

        public static void UpdateNoneAlertTypeDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Alert");
            // Collation: asc and desc is ascending and descending
            IList<JObject> alerts = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.testCategory = 'None' ORDER BY c.issuedOnDate ASC")).Result;
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
                    ResourceResponse<Document> resource = azureCosmosDBClient.UpsertDocumentAsync(alert).Result;
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
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec($@"SELECT * FROM c where c.id = '{testRunId}'")).Result;
            return list.Count > 0 ? list[0] : null;
        }

        public static void UpdateAllAlertSettingsDemo()
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
            IList<JObject> alertSettings = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c")).Result;
            foreach (JObject alertSetting in alertSettings)
            {
                Console.WriteLine(alertSetting["id"].ToString());
                alertSetting["serviceCustomFieldNames"] = serviceCustomFieldNamesJArray;
                azureCosmosDBClient.UpsertDocumentAsync(alertSetting).Wait();
            }
        }

        public static void UpdateAllDatasetForMerging()
        {
            int count = 0;

            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasets = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.dataFabric = 'ADLS' and c.isEnabled = false and not is_defined(c.connectionInfo.streamPath)")).Result;
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

        public static void UpdateAllDatasetTestForMerging()
        {
            int count = 0;

            AzureCosmosDBClient datasetAzureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            AzureCosmosDBClient datasetTestAzureCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            // Collation: asc and desc is ascending and descending
            IList<JToken> datasets = datasetAzureCosmosDBClient.GetAllDocumentsInQueryAsync<JToken>(new SqlQuerySpec(@"SELECT * FROM c where c.dataFabric = 'ADLS' and c.isEnabled = true")).Result;
            IList<JObject> adlsCompletenessTests = datasetTestAzureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.testContentType = 'AdlsCompleteness' and c.status = 'Enabled'")).Result;
            IList<JObject> cosmosCompletenessTests = datasetTestAzureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.testContentType = 'CosmosCompleteness' and c.status = 'Enabled'")).Result;

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

        public static void UpdateAllDatasetTestCreatedBy()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasetTests = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.createdBy = 'jianjlv'")).Result;
            foreach (JObject datasetTest in datasetTests)
            {
                Console.WriteLine(datasetTest["id"].ToString());
                datasetTest["createdBy"] = "DefaultTestGenerator";
                datasetTest["lastModifiedBy"] = "DefaultTestGenerator";

                azureCosmosDBClient.UpsertDocumentAsync(datasetTest).Wait();
            }
        }

        public static void UpdateAllCosmosTestResultExpirePeriod()
        {
            string oldResultExpirePeriod = "48.00:00:00";
            string newResultExpirePeriod = "2.00:00:00";
            int count = 0;

            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            // Collation: asc and desc is ascending and descending
            IList<JObject> testRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec($@"SELECT * FROM c WHERE c.dataFabric= 'Cosmos' and c.resultExpirePeriod = '{oldResultExpirePeriod}'")).Result;

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

        public static void UpdateAllCosmosTestCreateTime()
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
                        IList<JObject> testRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec($@"SELECT * FROM c WHERE c.dataFabric= 'Cosmos' and c.createTime > '2019-09-09T05:{min}:{sec}.{millisec}' and c.createTime < '2019-09-09T05:{min}:{sec}.{millisec + 1}' order by c.startTime desc")).Result;

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

        public static void UpsertServiceMonitorDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "ServiceMonitor");
            var jObject = JObject.Parse(@"{'datasetId': 'a4353e4b-1611-4965-8334-4c81fd824dad', 'expectedTestRunStatus': 'Success','isEnabled': true}");
            azureCosmosDBClient.UpsertDocumentAsync(jObject).Wait();
        }

        public static void DisableAllDataset()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasets = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c")).Result;
            foreach (JObject dataset in datasets)
            {
                Console.WriteLine(dataset["id"].ToString());
                dataset["isEnabled"] = false;
                azureCosmosDBClient.UpsertDocumentAsync(dataset).Wait();
            }
        }

        public static void EnableDataset()
        {
            var datasetIds = new string[]
            {
                "7299ec58-75d2-4e99-9165-7231268f92c8",
                "918124d3-b87e-409f-ad1e-804475c04653",
                "84142b9b-f6d2-4d86-8303-b42cd3145bc3"
            };
            HashSet<string> datasetIdSet = new HashSet<string>(datasetIds);

            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasets = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c")).Result;
            foreach (JObject dataset in datasets)
            {
                string id = dataset["id"].ToString();
                Console.WriteLine(id);
                if (datasetIdSet.Contains(id))
                {
                    dataset["isEnabled"] = true;
                    azureCosmosDBClient.UpsertDocumentAsync(dataset).Wait();
                }
            }
        }

        public static void DisableAllCosmosDatasetTest()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasetTests = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.dataFabric = 'Cosmos' and c.status = 'Enabled'")).Result;
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

        public static void EnableAllCosmosDatasetTestSuccessively()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasetTests = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.dataFabric = 'Cosmos' and c.status = 'Disabled'")).Result;

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

        public static void EnableAllCosmosDatasetTestWhenNoActiveMessage()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasetTests = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.dataFabric = 'Cosmos' and c.status = 'Disabled'")).Result;

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
            string serviceBusConnectionString = secretProvider.GetSecretAsync(KeyVaultName, "ServiceBusConnectionString").Result;
            Dictionary<string, long> messageCountDetails = MicrosoftServiceBusLib.MicrosoftServiceBusClient.GetMessageCountDetails(serviceBusConnectionString, queueName);
            //foreach (var messageCountDetail in messageCountDetails)
            //{
            //    Console.WriteLine($"{messageCountDetail.Key}\t{messageCountDetail.Value}");
            //}
            long existCosmosMessage = messageCountDetails["activeCount"] + messageCountDetails["scheduledMessageCount"];
            Console.WriteLine($"ExistCosmosMessage: {existCosmosMessage}");
            return existCosmosMessage > 0;
        }

        public static void QueryAlertSettingDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "AlertSettings");
            // Collation: asc and desc is ascending and descending
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT distinct c.owningTeamId FROM c")).Result;
            foreach (JObject jObject in list)
            {
                if (jObject["owningTeamId"] != null)
                    Console.WriteLine(jObject["owningTeamId"]);
            }
        }

        public static void QueryDataSetDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.id = 'b3969342-a5ef-44a3-bb14-afbdefbf5aba'")).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject);
            }
        }

        public static void QueryTestRunTestContentDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            // Collation: asc and desc is ascending and descending
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.id = 'ba51c2ee-de0b-4b36-9793-3eca1a893af1'")).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject["testContent"].ToString());
                var compReq = JsonConvert.DeserializeObject<CosmosCompletenessTestContent>(jObject["testContent"].ToString());
                Console.WriteLine(compReq.FileRowCountMaxLimit);
            }
        }

        public static void QueryMonitroReportDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "MonitorReport");

            DateTime timeStamp = DateTime.Parse("2019-11-26T06:00:00");
            string datasetTestId = "0faf52e7-b4bc-4674-8f1e-ff2c65e12f02";
            Grain grain = Grain.Hourly;
            string sqlQueryString = @"SELECT * FROM c" +
                                                         $" WHERE c.datasetTestId='{datasetTestId}' and c.timeStamp='{timeStamp.ToString("s")}' and c.grain='{grain.ToString()}'";
            Console.WriteLine(sqlQueryString);
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(sqlQueryString)).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject);
            }
        }

        public static void QueryServiceMonitorDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "ServiceMonitor");

            string sqlQueryString = @"SELECT * FROM c where c.isEnabled = true";
            IList<JObject> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(sqlQueryString)).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject);
            }
        }

        public static void QueryTestRunCount()
        {
            int minute = 5;
            var startTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);

            var startDate = DateTime.UtcNow.AddDays(-1);

            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");

            for (int i = 0; i < 100; i++)
            {
                var endDate = startDate.AddMinutes(minute);
                var startTs = (startDate - startTime).TotalSeconds;
                var endTs = (endDate - startTime).TotalSeconds;
                startDate = endDate;
                string sqlQueryString = $"SELECT value count(1) FROM c where c._ts > {startTs} and c._ts < {endTs} and c.lastModifiedBy = 'AlertService'";
                IList<object> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<object>(new SqlQuerySpec(sqlQueryString)).Result;
                foreach (var count in list)
                {
                    Console.WriteLine($"{startDate:o}: {count}");
                }

            }
        }

        public enum Grain
        {
            /// <summary>
            /// Non-Value
            /// </summary>
            None = 0,

            /// <summary>
            /// Hourly Grain - Once per hour
            /// </summary>
            Hourly,

            /// <summary>
            /// Daily Grain - Once per day
            /// </summary>
            Daily,

            /// <summary>
            /// Weeekly Grain - Once per week
            /// </summary>
            Weekly,

            /// <summary>
            /// Monthly Grain - Once per month (always on the first of every month)
            /// </summary>
            Monthly,

            /// <summary>
            /// Monthly Grain - Once per month (always test at the end of every month)
            /// </summary>
            EndOfMonth,

            /// <summary>
            /// Yearly Grain - Once per year
            /// </summary>
            Yearly
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

        public static void DeleteTestRunById()
        {
            var datasetIds = new string[]
            {
                "7299ec58-75d2-4e99-9165-7231268f92c8",
                "918124d3-b87e-409f-ad1e-804475c04653",
                "84142b9b-f6d2-4d86-8303-b42cd3145bc3"
            };
            HashSet<string> datasetIdSet = new HashSet<string>(datasetIds);

            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasets = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.datasetId = '7299ec58-75d2-4e99-9165-7231268f92c8'")).Result;
            foreach (JObject dataset in datasets)
            {
                string id = dataset["id"].ToString();
                string partitionKey = dataset["partitionKey"].ToString();
                string datasetId = dataset["datasetId"].ToString();
                if (datasetIdSet.Contains(datasetId))
                {
                    string documentLink = UriFactory.CreateDocumentUri("DataCop", "Dataset", id).ToString();
                    var reqOptions = new RequestOptions { PartitionKey = new PartitionKey(partitionKey) };
                    ResourceResponse<Document> resource = azureCosmosDBClient.DeleteDocumentAsync(documentLink, reqOptions).Result;
                    Console.WriteLine(resource);
                }
            }
        }

        public static void DeleteWaitingTestRun()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            // Collation: asc and desc is ascending and descending
            IList<JObject> testRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT top 1000 * FROM c WHERE c.status = 'Waiting' and not contains(c.partitionKey, 'T00:00:00Z') order by c.createTime desc")).Result;
            foreach (JObject testRun in testRuns)
            {
                try
                {
                    string id = testRun["id"].ToString();
                    string partitionKey = testRun["partitionKey"].ToString();
                    string documentLink = UriFactory.CreateDocumentUri("DataCop", "PartitionedTestRun", id).ToString();
                    var reqOptions = new RequestOptions { PartitionKey = new PartitionKey(partitionKey) };
                    ResourceResponse<Document> resource = azureCosmosDBClient.DeleteDocumentAsync(documentLink, reqOptions).Result;
                    Console.WriteLine(resource);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            }
        }

        // Delete CosmosDB instance without partitionKey
        public static void DeleteAlertsWithoutIncidentId()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "Alert");
            // Collation: asc and desc is ascending and descending
            IList<JObject> alerts = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT top 1000 * FROM c WHERE not is_defined(c.incidentId)")).Result;
            while (alerts.Count > 0)
            {
                foreach (JObject alert in alerts)
                {
                    string id = alert["id"].ToString();
                    Console.WriteLine(id);
                    string documentLink = GetDocumentLink("DataCop", "Alert", id);
                    ResourceResponse<Document> resource = azureCosmosDBClient.DeleteDocumentAsync(documentLink).Result;
                    //Console.WriteLine(resource);
                }
                alerts = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT top 1000 * FROM c WHERE not is_defined(c.incidentId)")).Result;
            }
        }

        public static void UpdateAlertSettingToGitFolder()
        {
            AzureCosmosDBClient alertSettingsCosmosDBClient = new AzureCosmosDBClient("DataCop", "AlertSettings");

            // Filter out duplicates alertSettings
            IList<JObject> alertSettingJObjects = alertSettingsCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c")).Result;
            //var alertSettingDict = ClusterAlertSettings(alertSettingJObjects);
            //Console.WriteLine(alertSettingJObjects.Count);
            //Console.WriteLine(alertSettingDict.Count);
            //foreach (var item in alertSettingDict.Keys)
            //{
            //    Console.WriteLine(item);
            //}

            //// Filter out alertSetting identifier used by datasetTest
            //AzureCosmosDBClient datasetTestCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            //IList<JObject> datasetTestJObjects = datasetTestCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT distinct c.alertSettingId FROM c where c.status = 'Enabled' and is_defined(c.alertSettingId)")).Result;
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


            var filePaths = ReadFile.GetAllFile(@"C:\Users\jianjlv\source\repos\Ibiza\Source\DataCopMonitors\PROD");

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
                    SaveFile.FirstMethod(gitAlertSettingDict[id].Item1, alertSettingJObject.ToString());
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
                    SaveFile.FirstMethod(gitFilePath, alertSettingJObject.ToString());
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

        public static void DeleteCosmosTestRunByResultExpirePeriod()
        {
            string resultExpirePeriod = "2.00:00:00";
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            // Collation: asc and desc is ascending and descending
            IList<JObject> testRuns = azureCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec($@"SELECT * FROM c WHERE c.dataFabric= 'Cosmos' and c.resultExpirePeriod = '{resultExpirePeriod}'")).Result;
            foreach (JObject testRun in testRuns)
            {
                string id = testRun["id"].ToString();

                if (id != "6df33f69-706e-43bd-8736-b3cb04515ad9") continue;

                Console.WriteLine(testRun);
                Console.WriteLine(testRun["partitionKey"]);
                string partitionKey = testRun["partitionKey"].ToString();
                partitionKey = JsonConvert.SerializeObject(DateTime.Parse(testRun["partitionKey"].ToString())).Trim('"');
                string documentLink = UriFactory.CreateDocumentUri("DataCop", "Dataset", id).ToString();
                Console.WriteLine(id);
                Console.WriteLine(partitionKey);
                //var reqOptions = new RequestOptions { PartitionKey = new PartitionKey(partitionKey) };
                //ResourceResponse<Document> resource = azureCosmosDBClient.DeleteDocumentAsync(documentLink, reqOptions).Result;
                //Console.WriteLine(resource);
            }
        }
    }
}
