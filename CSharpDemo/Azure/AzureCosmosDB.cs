using AzureLib.KeyVault;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CSharpDemo.Azure
{
    class AzureCosmosDB
    {
        // "datacopdev","ideasdatacopppe", "datacopprod" or "csharpmvcwebapikeyvault"(csharpmvcwebapicosmosdb)
        public static string KeyVaultName = "csharpmvcwebapikeyvault";
        public static void MainMethod()
        {
            //UpdateAllAlertSettingsDemo();
            //DisableAllDataset();
            //EnableDataset();

            //QueryAlertSettingDemo();
            //QueryTestDemo();
            //GetLastTestDemo();

            //UpsertTestDemoToCosmosDB();
            //DeleteTestDemo();
            DeleteTestRun();

            //MigrateData("DatasetTest");

            //AddCompletenessMonitors4ADLS();

            //CheckDatasetTestIntegrity();
            //CheckPPEAlertsetting();
            //CheckAdlsConnectionInfoMappingCorrectness();
            //CheckCosmosConnectionInfoMappingCorrectness();
            //CheckDuplicatedEnabledDatasetTest();



        }

        public static void CheckDuplicatedEnabledDatasetTest()
        {
            KeyVaultName = "datacopprod";

            AzureCosmosDB azureDatasetTestCosmosDB = new AzureCosmosDB("DataCop", "DatasetTest");
            AzureCosmosDB azureDatasetCosmosDB = new AzureCosmosDB("DataCop", "Dataset");

            // Collation: asc and desc is ascending and descending
            IList<JObject> datasetList = azureDatasetCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.dataFabric = 'ADLS' and c.isEnabled = true")).Result;
            IList<JObject> availabilityDatasetTestList = azureDatasetTestCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.testContentType = 'AdlsCompleteness' and c.status = 'Enabled'")).Result;

            HashSet<string> datasetIdSet = new HashSet<string>();
            Dictionary<string, int> availabilityDatasetIdCountDict = new Dictionary<string, int>();
            Dictionary<string, string> availabilityDatasetIdNameDict = new Dictionary<string, string>();

            foreach (JObject jObject in datasetList)
            {
                string id = jObject["id"].ToString();
                datasetIdSet.Add(id);
            }

            foreach (JObject jObject in availabilityDatasetTestList)
            {
                string datasetId = jObject["datasetId"].ToString();
                string datasetTestName = jObject["name"].ToString();

                if (datasetIdSet.Contains(datasetId))
                {
                    //Console.WriteLine(datasetId);
                    if (!availabilityDatasetIdNameDict.ContainsKey(datasetId))
                    {
                        availabilityDatasetIdNameDict.Add(datasetId, "");
                    }
                    availabilityDatasetIdNameDict[datasetId] += "\t" + datasetTestName;

                    if (!availabilityDatasetIdCountDict.ContainsKey(datasetId))
                    {
                        availabilityDatasetIdCountDict.Add(datasetId, 0);
                    }
                    availabilityDatasetIdCountDict[datasetId]++;
                }

            }

            foreach (var availabilityDatasetIdCount in availabilityDatasetIdCountDict)
            {
                Console.WriteLine($"datasetId: {availabilityDatasetIdCount.Key}   availabilityDatasetIdCount: {availabilityDatasetIdCount.Value}");
                Console.WriteLine($"datasetTestName: {availabilityDatasetIdNameDict[availabilityDatasetIdCount.Key]}");
            }
        }

        public static void CheckAdlsConnectionInfoMappingCorrectness()
        {
            KeyVaultName = "datacopprod";
            AzureCosmosDB azureDatasetCosmosDB = new AzureCosmosDB("DataCop", "Dataset");
            AzureCosmosDB azureDatasetTestCosmosDB = new AzureCosmosDB("DataCop", "DatasetTest");
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
            KeyVaultName = "datacopprod";
            AzureCosmosDB azureDatasetCosmosDB = new AzureCosmosDB("DataCop", "Dataset");
            AzureCosmosDB azureDatasetTestCosmosDB = new AzureCosmosDB("DataCop", "DatasetTest");
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
            KeyVaultName = "ideasdatacopppe";
            string cfrId = "da71491f-c49a-475e-9d54-d2fde4a6403f";
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "AlertSettings");
            IList<JObject> alertSettings = azureCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c")).Result;
            foreach (var alertSetting in alertSettings)
            {
                if (alertSetting["id"].ToString().Equals(cfrId)) continue;
                if ((alertSetting["routingId"].ToString().Equals("IDEAS://IDEAsDataCopTest") || alertSetting["routingId"].ToString().Equals("IDEAs://IDEAsDataCopTest")) && alertSetting["owningTeamId"].ToString().Equals("IDEAS\\IDEAsDataCopTest")) continue;
                //Console.WriteLine(alertSetting["routingId"].ToString() + "\t" + alertSetting["owningTeamId"].ToString());
                Console.WriteLine(alertSetting);

                alertSetting["routingId"] = "IDEAS://IDEAsDataCopTest";
                alertSetting["owningTeamId"] = "IDEAS\\IDEAsDataCopTest";
                azureCosmosDB.UpsertDocumentAsync(alertSetting).Wait();
            }
        }

        public static void CheckDatasetTestIntegrity()
        {
            KeyVaultName = "datacopprod";
            AzureCosmosDB azureDatasetTestCosmosDB = new AzureCosmosDB("DataCop", "DatasetTest");
            AzureCosmosDB azureDatasetCosmosDB = new AzureCosmosDB("DataCop", "Dataset");
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
            KeyVaultName = "datacopprod";
            string dirPath = @"D:\data\company_work\M365\IDEAs\work_item_file\1161175\write\";
            string duplicateDirPath = @"D:\data\company_work\M365\IDEAs\work_item_file\1161175\duplicate\";


            AzureCosmosDB azureDatasetTestCosmosDB = new AzureCosmosDB("DataCop", "DatasetTest");
            AzureCosmosDB azureDatasetCosmosDB = new AzureCosmosDB("DataCop", "Dataset");
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

        private static bool CheckDatasetEnabled(AzureCosmosDB azureDatasetCosmosDB, string datasetId)
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

        public static void MigrateData(string collectionId)
        {
            KeyVaultName = "datacopprod";
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", collectionId);
            IList<JObject> list = azureCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c")).Result;

            // This is a funny thing. azureCosmosDB has not been changed.
            KeyVaultName = "datacopdev";
            azureCosmosDB = new AzureCosmosDB("DataCop", collectionId, CosmosDBDocumentClientMode.NoSingle);
            int count = 0;
            foreach (JObject json in list)
            {
                count++;
                try
                {
                    Console.WriteLine(json["id"]);
                    azureCosmosDB.UpsertDocumentAsync(json).Wait();
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
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "Alert");
            // Collation: asc and desc is ascending and descending
            IList<JObject> alerts = azureCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.testCategory = 'None' ORDER BY c.issuedOnDate ASC")).Result;
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
                    ResourceResponse<Document> resource = azureCosmosDB.UpsertDocumentAsync(alert).Result;
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
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "TestRun");
            // Collation: asc and desc is ascending and descending
            IList<JObject> list = azureCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec($@"SELECT * FROM c where c.id = '{testRunId}'")).Result;
            return list.Count > 0 ? list[0] : null;
        }

        public static void UpdateAllAlertSettingsDemo()
        {
            KeyVaultName = "datacopdev";

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

            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "AlertSettings");
            // Collation: asc and desc is ascending and descending
            IList<JObject> alertSettings = azureCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c")).Result;
            foreach (JObject alertSetting in alertSettings)
            {
                Console.WriteLine(alertSetting["id"].ToString());
                alertSetting["serviceCustomFieldNames"] = serviceCustomFieldNamesJArray;
                azureCosmosDB.UpsertDocumentAsync(alertSetting).Wait();
            }
        }

        public static void DisableAllDataset()
        {
            KeyVaultName = "datacopdev";

            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasets = azureCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c")).Result;
            foreach (JObject dataset in datasets)
            {
                Console.WriteLine(dataset["id"].ToString());
                dataset["isEnabled"] = false;
                azureCosmosDB.UpsertDocumentAsync(dataset).Wait();
            }
        }

        public static void EnableDataset()
        {
            KeyVaultName = "datacopdev";

            var datasetIds = new string[]
            {
                "7299ec58-75d2-4e99-9165-7231268f92c8",
                "918124d3-b87e-409f-ad1e-804475c04653",
                "84142b9b-f6d2-4d86-8303-b42cd3145bc3"
            };
            HashSet<string> datasetIdSet = new HashSet<string>(datasetIds);

            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasets = azureCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c")).Result;
            foreach (JObject dataset in datasets)
            {
                string id = dataset["id"].ToString();
                Console.WriteLine(id);
                if (datasetIdSet.Contains(id))
                {
                    dataset["isEnabled"] = true;
                    azureCosmosDB.UpsertDocumentAsync(dataset).Wait();
                }
            }
        }

        public static void QueryAlertSettingDemo()
        {
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "AlertSettings");
            // Collation: asc and desc is ascending and descending
            IList<JObject> list = azureCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT distinct c.owningTeamId FROM c")).Result;
            foreach (JObject jObject in list)
            {
                if (jObject["owningTeamId"] != null)
                    Console.WriteLine(jObject["owningTeamId"]);
            }
        }

        public static void QueryDataSetDemo()
        {
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "Dataset");
            // Collation: asc and desc is ascending and descending
            IList<JObject> list = azureCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.id = 'b3969342-a5ef-44a3-bb14-afbdefbf5aba'")).Result;
            foreach (JObject jObject in list)
            {
                Console.WriteLine(jObject);
            }
        }

        public static void QueryTestDemo()
        {
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("CosmosDBTest", "TestCollectionId");
            // Collation: asc and desc is ascending and descending
            IList<AzureCosmosDBTestClass> list = azureCosmosDB.GetAllDocumentsInQueryAsync<AzureCosmosDBTestClass>(new SqlQuerySpec(@"SELECT * FROM c order by c.timestampTicks asc")).Result;
            foreach (AzureCosmosDBTestClass t in list)
            {
                Console.WriteLine(t.Id);
                Console.WriteLine(JsonConvert.SerializeObject(t));
            }
        }

        // Through this code, I cant reproduce the error: 
        // Microsoft.Azure.Documents.DocumentClientException: Entity with the specified id does not exist in the system.
        public static void GetLastTestDemo()
        {
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("CosmosDBTest", "TestCollectionId");
            // Collation: asc and desc is ascending and descending
            JObject t = azureCosmosDB.FindFirstOrDefaultItemAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c WHERE c.id=1111 order by c.timestampTicks desc")).Result;
            if (t == null)
            {
                Console.WriteLine("null");
            }
            else
            {
                Console.WriteLine(t["id"]);
                Console.WriteLine(JsonConvert.SerializeObject(t));
            }
        }

        /*
        * For deleting operation, now I must set the PartitionKey, 
        * so I just can delete the document with PartitionKey.
        */
        public static void DeleteTestDemo()
        {
            KeyVaultName = "csharpmvcwebapikeyvault";
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("CosmosDBTest", "TestPartitionContains");

            string documentLink = UriFactory.CreateDocumentUri("CosmosDBTest", "TestPartitionContains", "34a4a888-dd51-496b-b21c-9d2fc91be01d").ToString();
            var reqOptions = new RequestOptions { PartitionKey = new PartitionKey("a") };
            ResourceResponse<Document> resource = azureCosmosDB.DeleteDocumentAsync(documentLink, reqOptions).Result;
            Console.WriteLine(resource);
        }

        public static void DeleteTestRun()
        {
            KeyVaultName = "datacopdev";

            var datasetIds = new string[]
            {
                "7299ec58-75d2-4e99-9165-7231268f92c8",
                "918124d3-b87e-409f-ad1e-804475c04653",
                "84142b9b-f6d2-4d86-8303-b42cd3145bc3"
            };
            HashSet<string> datasetIdSet = new HashSet<string>(datasetIds);

            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "PartitionedTestRun");
            // Collation: asc and desc is ascending and descending
            IList<JObject> datasets = azureCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.datasetId = '7299ec58-75d2-4e99-9165-7231268f92c8'")).Result;
            foreach (JObject dataset in datasets)
            {
                string id = dataset["id"].ToString();
                string partitionKey = dataset["partitionKey"].ToString();
                string datasetId = dataset["datasetId"].ToString();
                if (datasetIdSet.Contains(datasetId))
                {
                    string documentLink = UriFactory.CreateDocumentUri("DataCop", "Dataset", id).ToString();
                    var reqOptions = new RequestOptions { PartitionKey = new PartitionKey(partitionKey) };
                    ResourceResponse<Document> resource = azureCosmosDB.DeleteDocumentAsync(documentLink, reqOptions).Result;
                    Console.WriteLine(resource);
                }
            }
        }

        public static void UpsertTestDemoToCosmosDB()
        {
            KeyVaultName = "csharpmvcwebapikeyvault";
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("CosmosDBTest", "TestCollectionId");

            AzureCosmosDBTestClass t = new AzureCosmosDBTestClass();
            t.Id = Guid.NewGuid().ToString();
            t.TestA = "a";
            t.TestB = "b";
            t.TestC = "cc";
            t.TestHashSet = new HashSet<string>();
            t.TestHashSet.Add("1");
            t.TestHashSet.Add("2");
            t.TestHashSet.Add("3");
            t.TestHashSet.Add("4");
            // the value of long.MaxValue is 9223372036854775807
            // But in the Azure CosmosDB, this two value will be shown as 9223372036854776000
            // When you get it from Azure CosmosDB, you will get the value 9223372036854775807
            // But if you update the number as 9223372036854775807 in Azure CosmosDB portal, it will be shown and updated as 9223372036854776000,
            // When you get it from Azure CosmosDB, you will get the value 9223372036854776000, and there will be an exception thrown when convert it to a long property.
            t.TestLongMaxValueViaLong = long.MaxValue;
            t.TestLongMaxValueViaDouble = long.MaxValue;
            t.CreateDate = DateTime.Now;

            ResourceResponse<Document> resource = azureCosmosDB.UpsertDocumentAsync(t).Result;
            Console.WriteLine(resource);
        }

        public string Endpoint { get; set; }
        public string Key { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CosmosDBDocumentCollection"/> class.
        /// </summary>
        /// <param name="databaseId">The database identifier.</param>
        /// <param name="collectionId">The collection identifier.</param>
        public AzureCosmosDB(string databaseId, string collectionId, CosmosDBDocumentClientMode mode = CosmosDBDocumentClientMode.Single)
        {
            if (mode == CosmosDBDocumentClientMode.Single)
            {
                this.Client = CosmosDBDocumentClient.Instance;
            }
            else
            {
                this.Client = CosmosDBDocumentClient.NewInstance();
            }

            this.Client.CreateDatabaseIfNotExistsAsync(new Database() { Id = databaseId }).Wait();
            this.DatabaseLink = this.Client.GetDatabaseLink(databaseId);
            this.CollectionLink = this.Client.GetCollectionLink(databaseId, collectionId);

            this.DocumentCollection = this.Client.CreateDocumentCollectionIfNotExistsAsync(this.DatabaseLink, new DocumentCollection() { Id = collectionId }, null).Result;
        }

        /// <summary>
        /// Gets the client.
        /// </summary>
        /// <value>The client.</value>
        public CosmosDBDocumentClient Client;

        /// <summary>
        /// Gets the collection link.
        /// </summary>
        /// <value>The collection link.</value>
        public string CollectionLink;

        /// <summary>
        /// Gets the database link.
        /// </summary>
        /// <value>The database link.</value>
        public string DatabaseLink;

        /// <summary>
        /// Gets the document collection.
        /// </summary>
        /// <value>The document collection.</value>
        public DocumentCollection DocumentCollection;

        public async Task<DocumentResponse<T>> ReadDocumentAsync<T>(string documentLink, RequestOptions options = null)
        {
            return await this.Client.ReadDocumentAsync<T>(documentLink, options);
        }

        /// <summary>
        /// Deletes the document provided by looking up the id. document link should be of the form
        /// TODO - update.
        /// </summary>
        /// <param name="documentLink">documentLink of the form</param>
        /// <param name="requestOptions">options request options object that specifies CosmosDB to handle options.</param>
        /// <returns>A <see cref="Task" /> representing the asynchronous operation with the document being the task result.</returns>
        public async Task<ResourceResponse<Document>> DeleteDocumentAsync(string documentLink, RequestOptions requestOptions = null)
        {
            return await this.Client.DeleteDocumentAsync(documentLink, requestOptions);
        }

        /// <summary>
        /// Creates the document query.
        /// </summary>
        /// <typeparam name="T">The type parameter</typeparam>
        /// <param name="sqlExpression">The SQL expression.</param>
        /// <param name="feedOptions">The feed options.</param>
        /// <returns>IQueryable&lt;T&gt;.</returns>
        public IQueryable<T> CreateDocumentQuery<T>(string sqlExpression, FeedOptions feedOptions = null)
        {
            return this.Client.CreateDocumentQuery<T>(this.CollectionLink, sqlExpression, feedOptions);
        }

        /// <summary>
        /// get all documents in query as an asynchronous operation.
        /// </summary>
        /// <typeparam name="T">The type parameter</typeparam>
        /// <param name="collectionLink">The collection link.</param>
        /// <param name="sqlQuerySpec">The SQL query spec.</param>
        /// <returns>Task&lt;IList&lt;T&gt;&gt;.</returns>
        protected async Task<IList<T>> GetAllDocumentsInQueryAsync<T>(SqlQuerySpec sqlQuerySpec)
        {
            return await this.Client.GetAllDocumentsInQueryAsync<T>(this.CollectionLink, sqlQuerySpec);
        }

        /// <summary>
        /// create document if not exists as an asynchronous operation.
        /// </summary>
        /// <param name="document">The document.</param>
        /// <param name="options">The options.</param>
        /// <param name="disableAutomaticIdGeneration">if set to <c>true</c> [disable automatic identifier generation].</param>
        /// <returns>Task&lt;ResourceResponse&lt;Document&gt;&gt;.</returns>
        protected async Task<ResourceResponse<Document>> CreateDocumentIfNotExistsAsync(object document, RequestOptions options, bool disableAutomaticIdGeneration)
        {
            return await this.Client.CreateDocumentIfNotExistsAsync(this.CollectionLink, document, options, disableAutomaticIdGeneration);
        }

        /// <summary>
        /// find first or default item as an asynchronous operation.
        /// </summary>
        /// <typeparam name="T">The type parameter</typeparam>
        /// <param name="sqlQuerySpec">The SQL query spec.</param>
        /// <returns>Task&lt;T&gt;.</returns>
        protected async Task<T> FindFirstOrDefaultItemAsync<T>(SqlQuerySpec sqlQuerySpec)
        {
            return await this.Client.FindFirstOrDefaultItemAsync<T>(this.CollectionLink, sqlQuerySpec);
        }

        /// <summary>
        /// If document exist, run update operation; if not exist, run insert operation
        /// </summary>
        /// <param name="document">document object that needs to be upserted.</param>
        /// <returns>A <see cref="Task" /> representing the asynchronous operation with the document being the task result.</returns>
        public async Task<ResourceResponse<Document>> UpsertDocumentAsync(object document)
        {
            return await this.Client.UpsertDocumentAsync(this.CollectionLink, document);
        }
    }

    enum CosmosDBDocumentClientMode
    {
        Single = 0,
        NoSingle = 1
    }

    class CosmosDBDocumentClient
    {
        // http://csharpindepth.com/Articles/General/Singleton.aspx explains the thread safe singleton pattern that is followed here.
        // Fetching the value from config as we create a singleton document client. It is preferred to have
        // one document client instance for better per. this is as per Azure CosmosDB perf recommendations.
        // The other option is to let the callers create passing in the value for endpoint. That is contradictory
        // to have a singleton instance. So reading from config for the end point to create the client.

        /// <summary>
        /// The document client
        /// </summary>
        private static Lazy<CosmosDBDocumentClient> documentClient = new Lazy<CosmosDBDocumentClient>(() => new CosmosDBDocumentClient());

        /// <summary>
        /// The client
        /// </summary>
        private readonly DocumentClient client;

        /// <summary>
        /// The disposed value
        /// </summary>
        private bool disposedValue = false; // To detect redundant calls

        /// <summary>
        /// The databases
        /// </summary>
        private ConcurrentDictionary<string, ResourceResponse<Database>> databases = new ConcurrentDictionary<string, ResourceResponse<Database>>();

        /// <summary>
        /// Prevents a default instance of the <see cref="CosmosDBDocumentClient" /> class from being created.
        /// </summary>
        private CosmosDBDocumentClient()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;

            string endpoint = secretProvider.GetSecretAsync(AzureCosmosDB.KeyVaultName, "CosmosDBEndPoint").Result;
            string key = secretProvider.GetSecretAsync(AzureCosmosDB.KeyVaultName, "CosmosDBAuthKey").Result;

            // https://docs.microsoft.com/en-us/azure/cosmos-db/performance-tips has more details.
            ConnectionPolicy connectionPolicy = new ConnectionPolicy()
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp
            };

            // https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.documents.client.feedoptions.jsonserializersettings?view=azure-dotnet for more details.
            JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore };

            this.client = new DocumentClient(
                new Uri(endpoint),
                key,
                jsonSerializerSettings,
                connectionPolicy);

            this.client.OpenAsync().Wait();
        }

        private CosmosDBDocumentClient(string endpoint, string key)
        {
            // https://docs.microsoft.com/en-us/azure/cosmos-db/performance-tips has more details.
            ConnectionPolicy connectionPolicy = new ConnectionPolicy()
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp
            };

            // https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.documents.client.feedoptions.jsonserializersettings?view=azure-dotnet for more details.
            JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore };

            this.client = new DocumentClient(
                new Uri(endpoint),
                key,
                jsonSerializerSettings,
                connectionPolicy);

            this.client.OpenAsync().Wait();
        }

        /// <summary>
        /// Gets the instance.
        /// </summary>
        /// <value>The instance.</value>
        public static CosmosDBDocumentClient Instance => documentClient.Value;

        // Sometimes I need non-single mode
        public static CosmosDBDocumentClient NewInstance()
        {
            return new CosmosDBDocumentClient();
        }

        public static CosmosDBDocumentClient NewInstance(string endpoint, string key)
        {
            return new CosmosDBDocumentClient(endpoint, key);
        }

        /// <summary>
        /// Gets the document client.
        /// </summary>
        /// <returns>DocumentClient.</returns>
        public DocumentClient GetDocumentClient()
        {
            return this.client;
        }

        /// <summary>
        /// create database if not exists as an asynchronous operation.
        /// </summary>
        /// <param name="database">The database.</param>
        /// <returns>Task&lt;ResourceResponse&lt;Database&gt;&gt;.</returns>
        public async Task<ResourceResponse<Database>> CreateDatabaseIfNotExistsAsync(Database database)
        {
            if (this.databases.ContainsKey(database.Id))
            {
                return this.databases[database.Id];
            }
            else
            {
                var retval = await this.client.CreateDatabaseIfNotExistsAsync(database);
                this.databases.TryAdd(database.Id, retval);
                return retval;
            }
        }

        /// <summary>
        /// create document collection if not exists as an asynchronous operation.
        /// </summary>
        /// <param name="databaseLink">The database link.</param>
        /// <param name="documentCollection">The document collection.</param>
        /// <param name="requestOptions">The request options.</param>
        /// <returns>Task&lt;ResourceResponse&lt;DocumentCollection&gt;&gt;.</returns>
        public async Task<ResourceResponse<DocumentCollection>> CreateDocumentCollectionIfNotExistsAsync(
            string databaseLink,
            DocumentCollection documentCollection,
            RequestOptions requestOptions)
        {
            return await this.client.CreateDocumentCollectionIfNotExistsAsync(databaseLink, documentCollection, requestOptions);
        }

        /// <summary>
        /// find first or default item as an asynchronous operation.
        /// </summary>
        /// <typeparam name="T">The type parameter.</typeparam>
        /// <param name="collectionLink">The collection link.</param>
        /// <param name="sqlQuerySpec">The SQL query spec.</param>
        /// <returns>Task&lt;T&gt;.</returns>
        public async Task<T> FindFirstOrDefaultItemAsync<T>(string collectionLink, SqlQuerySpec sqlQuerySpec)
        {
            IDocumentQuery<T> query = this.client.CreateDocumentQuery<T>(collectionLink, sqlQuerySpec, new FeedOptions
            {
                MaxItemCount = 1,
                // This is a necessary settting, but we don't set it in DataCop, it also can run successfully, but cant here. I don't know why.
                EnableCrossPartitionQuery = true
            }).AsDocumentQuery<T>();

            return (await query.ExecuteNextAsync<T>()).FirstOrDefault();
        }

        /// <summary>
        /// Get all the documents which hit the query.
        /// </summary>
        /// <typeparam name="T">The data type</typeparam>
        /// <param name="collectionLink">The collection link</param>
        /// <param name="sqlQuerySpec">The query spec</param>
        /// <returns>List of documents. Note the documents count should not larger than 1000</returns>
        /// <exception cref="InvalidOperationException">Too many documents found in the query specified. Please break your query into smaller chunks.</exception>
        public async Task<IList<T>> GetAllDocumentsInQueryAsync<T>(string collectionLink, SqlQuerySpec sqlQuerySpec)
        {
            FeedOptions feedOptions = new FeedOptions
            {
                MaxItemCount = 100,
                EnableCrossPartitionQuery = true
            };

            List<T> allDocuments = new List<T>();
            IDocumentQuery<T> query = this.client.CreateDocumentQuery<T>(collectionLink, sqlQuerySpec, feedOptions).AsDocumentQuery<T>();
            while (query.HasMoreResults)
            {
                allDocuments.AddRange(await query.ExecuteNextAsync<T>());
                if (allDocuments.Count > 1000)
                {
                    throw new InvalidOperationException("Too many documents found in the query specified. Please break your query into smaller chunks.");
                }
            }

            return allDocuments;
        }

        /// <summary>
        /// delete document collection as an asynchronous operation.
        /// </summary>
        /// <param name="documentCollectionLink">The document collection link.</param>
        /// <param name="options">The options.</param>
        /// <returns>Task&lt;ResourceResponse&lt;DocumentCollection&gt;&gt;.</returns>
        public async Task<ResourceResponse<DocumentCollection>> DeleteDocumentCollectionAsync(string documentCollectionLink, RequestOptions options = null)
        {
            return await this.client.DeleteDocumentCollectionAsync(documentCollectionLink, options);
        }

        /// <summary>
        /// delete document as an asynchronous operation.
        /// </summary>
        /// <param name="documentLink">The document link.</param>
        /// <param name="options">The options.</param>
        /// <returns>Task&lt;ResourceResponse&lt;Document&gt;&gt;.</returns>
        public async Task<ResourceResponse<Document>> DeleteDocumentAsync(string documentLink, RequestOptions options = null)
        {
            return await this.client.DeleteDocumentAsync(documentLink, options);
        }

        /// <summary>
        /// Reads a document specified by the documentLink as an async operation.
        /// It is the most efficient way to look up a document in a collection.
        /// </summary>
        /// <typeparam name="T">The type parameter.</typeparam>
        /// <param name="documentLink">link to the document - /db/:dbid/colls/:collid/docs/:docid where docid is the unique id to the document.</param>
        /// <param name="options">request options object that specifies various update options.</param>
        /// <returns>Task&lt;DocumentResponse&lt;T&gt;&gt;.</returns>
        public async Task<DocumentResponse<T>> ReadDocumentAsync<T>(string documentLink, RequestOptions options = null)
        {
            return await this.client.ReadDocumentAsync<T>(documentLink, options);
        }

        /// <summary>
        /// If document exist, run update operation; if not exist, run insert operation
        /// </summary>
        /// <param name="documentsFeedOrDatabaseLink">the link to the documents feed</param>
        /// <param name="document">the actual document object that has to upserted</param>
        /// <returns>A <see cref="Task" /> representing the asynchronous operation with document in the task result.</returns>
        public async Task<ResourceResponse<Document>> UpsertDocumentAsync(string documentsFeedOrDatabaseLink, object document)
        {
            return await this.client.UpsertDocumentAsync(documentsFeedOrDatabaseLink, document);
        }

        /// <summary>
        /// create document if not exists as an asynchronous operation.
        /// </summary>
        /// <param name="documentsFeedOrDatabaseLink">The documents feed or database link.</param>
        /// <param name="document">The document.</param>
        /// <param name="options">The options.</param>
        /// <param name="disableAutomaticIdGeneration">if set to <c>true</c> [disable automatic identifier generation].</param>
        /// <returns>Task&lt;ResourceResponse&lt;Document&gt;&gt;.</returns>
        public async Task<ResourceResponse<Document>> CreateDocumentIfNotExistsAsync(string documentsFeedOrDatabaseLink, object document, RequestOptions options, bool disableAutomaticIdGeneration)
        {
            try
            {
                return await this.client.CreateDocumentAsync(documentsFeedOrDatabaseLink, document, options, disableAutomaticIdGeneration);
            }
            catch (DocumentClientException dce)
            {
                if (dce.StatusCode == HttpStatusCode.Conflict)
                {
                    return null;
                }

                throw;
            }
        }

        /// <summary>
        /// Creates the document collection query.
        /// </summary>
        /// <param name="databaseUri">The database URI.</param>
        /// <param name="feedOptions">The feed options.</param>
        /// <returns>IOrderedQueryable&lt;DocumentCollection&gt;.</returns>
        public IOrderedQueryable<DocumentCollection> CreateDocumentCollectionQuery(Uri databaseUri, FeedOptions feedOptions = null)
        {
            return this.client.CreateDocumentCollectionQuery(databaseUri, feedOptions);
        }

        /// <summary>
        /// Gets the database link.
        /// </summary>
        /// <param name="databaseName">Name of the database.</param>
        /// <returns>System.String.</returns>
        public string GetDatabaseLink(string databaseName) => $"/dbs/{databaseName}";

        /// <summary>
        /// Gets the document link.
        /// </summary>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="collectionName">Name of the collection.</param>
        /// <param name="documentId">The document identifier.</param>
        /// <returns>System.String.</returns>
        public string GetDocumentLink(
            string databaseName,
            string collectionName,
            string documentId) => this.GetCollectionLink(databaseName, collectionName) + $"docs/{documentId}";

        /// <summary>
        /// Gets the collection link.
        /// </summary>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="collectionName">Name of the collection.</param>
        /// <returns>System.String.</returns>
        public string GetCollectionLink(
            string databaseName,
            string collectionName) => $"/dbs/{databaseName}/colls/{collectionName}/";

        /// <summary>
        /// Creates the document query.
        /// </summary>
        /// <typeparam name="T">The type parameter.</typeparam>
        /// <param name="collectionLink">The collection link.</param>
        /// <param name="sqlExpression">The SQL expression.</param>
        /// <param name="feedOptions">The feed options.</param>
        /// <returns>IQueryable&lt;T&gt;.</returns>
        public IQueryable<T> CreateDocumentQuery<T>(string collectionLink, string sqlExpression, FeedOptions feedOptions = null)
        {
            return this.client.CreateDocumentQuery<T>(collectionLink, sqlExpression, feedOptions);
        }

        /// <summary>
        /// Disposes this instance.
        /// </summary>
        public void Dispose()
        {
            if (!this.disposedValue)
            {
                this.client.Dispose();
                this.disposedValue = true;
            }
        }
    }
}
