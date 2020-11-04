namespace CSharpDemo.IDEAs
{
    using AzureLib.KeyVault;
    using CSharpDemo.Azure.CosmosDB;
    using CSharpDemo.FileOperation;
    using Microsoft.Azure.Documents;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.IO;

    class DatasetJsonFileOperation
    {
        private static string FolderPath = @"D:\IDEAs\repos\Ibiza\Source\DataCopMonitors";


        public static void MainMethod()
        {
            //UpdateAllDatasetJsonForMergingADLSCosmos();
            //UpdateOldPathSchemaDatasetJsonForMergingADLSCosmos();
            //UpdateCFRDatasetJsonForMergingADLSCosmos();
            //DisableAllDatasetTests();
            //DisableSomeDatasets();
            //DisableAllDatasetTest();
            //UpdateAllSqlKeyVaultName();
            //UpdateSomeCFRToAdls();
            //UpdateCosmosViewScripts();
            UpdateCosmosVCToBuild();
        }
        public static void DisableAllDatasetTests()
        {
            string keyVaultName = "datacop-prod";
            var secretProvider = KeyVaultSecretProvider.Instance;
            string endpoint = secretProvider.GetSecretAsync(keyVaultName, "CosmosDBEndPoint").Result;
            string key = secretProvider.GetSecretAsync(keyVaultName, "CosmosDBAuthKey").Result;
            AzureCosmosDBClient.Endpoint = endpoint;
            AzureCosmosDBClient.Key = key;

            string fileFolder = FolderPath + @"\PROD\DataVC\Monitor";
            var filePaths = ReadFile.GetFolderSubPaths(fileFolder, ReadType.File, PathType.Absolute);
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "DatasetTest");
            foreach (var filePath in filePaths)
            {
                var fileContentString = ReadFile.ThirdMethod(filePath);
                var json = JObject.Parse(fileContentString);
                var id = json["id"].ToString();
                if (id.Equals("01883b87-5b20-40f4-90f2-2ce6cbdfa766"))
                {

                    var jObjectInCosmosDb = azureCosmosDBClient.FindFirstOrDefaultItemAsync<JObject>(new SqlQuerySpec($"SELECT * FROM c where c.id = '{id}'")).Result;
                    if (jObjectInCosmosDb != null)
                    {

                        jObjectInCosmosDb["status"] = "Disabled";
                        //jObjectInCosmosDb["testContent"]["isStreamSet"] = true;
                        Console.WriteLine(jObjectInCosmosDb);
                        //azureCosmosDBClient.UpsertDocumentAsync(json).Wait();
                    }
                }
            }
        }

        public static void DisableSomeDatasets()
        {
            var gerber = new HashSet<string>()
            {
                "d4e37837-c8ad-4d19-8508-8a75c4397cf0",
                "0c19f055-9db3-4029-83ad-61b6d632e856",
                "6b855ba7-328f-4aee-afda-cecb1887af23",
                "4f3fcf03-c412-468e-adec-c7ce91e0dd04",
                "f76890b6-8615-4c7c-8ccb-568acf63fc1e",
                "ecde6a83-b80a-48ea-9da8-98ba18e026f9",
            };

            var jiankun = new HashSet<string>()
            {
                "04BC25E4-6D3F-418D-8B90-182E957D5EE2",
                "E7575A0C-A7D4-45B7-9FA8-DCFB5ECBA47E",
                "00D3EA46-3658-43CD-8288-41C0F908FB50",
            };

            var isabela = new HashSet<string>()
            {
                "3bccf775-8bb9-424a-b2e7-abc9acb8da68",
                "8dd74f3b-4292-4226-b50a-be08b8342ea6",
                "0e502297-9fa9-4f1c-b24e-e031f65dc76e",
                "c5334ffc-74f7-46b4-9683-f165c336cf33",
                "b1690c6d-0053-4996-b5c7-2566b48326af",
                "2b9a1312-3a45-4c4e-a9ba-2a09c8bae380",
                "adaf57ca-249f-41a6-9cc5-e514b5a93a2c",
                "1335394c-5f5b-4d94-8e5c-cc680105a14e",
                "f571c4aa-04c1-4543-a385-8f4849eef884",
                "10c6846f-abed-46af-a207-fd34447e1741",
                "2939e351-2b44-411f-b706-cf3a07bd5be5",
                "b2d8c0c0-0cee-4dde-85b5-b0b5d13ec494",
                "69c30e25-a574-4eb0-9538-326b03125224",
                "eb5af06c-43c3-45d6-9226-1fb5e8ecac56",
                "521fe391-e36f-4ac2-846f-cd35eba6347e",
                "7f3da785-2ea7-46e7-baed-76ef974decc3",
                "e250d2c4-9b55-4a0a-a382-cf769d893fda",
                "a7752d68-23ae-4d0b-969d-da015d714d87",
                "94974b78-f4a4-49c6-a4aa-b1f38e898ac8",
                "abc892a1-5778-472a-b87f-d37921676808",
                "f9a56b61-9d88-48aa-8311-23a92b20c369",
                "e256560c-6481-4f40-8a76-47f92fe0ac19",
                "4d1e5fef-99cc-47d3-8d08-328f9d159bf2",
                "4401de99-d679-4dff-b69b-fe85266290ae",
                "125bafc3-e1f4-4184-bbde-dd6197db1e45",
                "65a8034c-88d5-4ed0-9141-32b17af0e1e0",
                "6b6dc999-c526-4759-9da7-d1e0fde094fb",
            };

            var carlos = new HashSet<string>()
            {
                "c78f84ea-2745-49a1-abe9-b27904af3acc",
                "ac2c140f-ad75-4fa5-b1d8-179685f5d928",
                "b80e0db6-788f-453d-9d0c-59a510deeede",
                "f99a878b-54a2-4ffa-b091-ad4e1fb95f5b",
                "5ef7debd-342d-479d-859f-efddf3f27be8",
                "39f5d173-12ea-48f2-b0c7-41cc51b797a5",
                "c41b4180-19a9-49b7-a459-cecf711c3f0f",
                "8ac50336-ffa6-4745-a25a-10420283bcbc",
                "99d9aaec-0654-4ee3-b952-bf5e6c3f70e1",
                "4cc040a1-bb69-45b9-af47-84eea7793dab",
                "47e57daa-116a-4b15-a008-9275a199fda0",
                "49530e02-4e2d-40f5-ae5a-9242536da003",
                "067cea9e-0367-45af-81fc-bb84f3bc3a31",
                "3c9028ae-07be-403b-b0c6-bd3e60228f59",
                "7995071a-a659-4b5f-938e-bba17e59525e",
                "0a622da3-d97b-4302-829a-3b7ba2f4a562",
                "845208ad-a677-4912-8a25-26f79d0753ed",
                "359fe73e-e383-40ee-8293-62b7cfc6847c",
                "becca739-64de-4889-abca-c75b095e97f5",
                "b3a8fcec-4b8b-44fc-a60b-44f36e2e281f",
                "97066f18-8087-4872-bc16-431178bb61ab",
                "795a050f-f3fd-443e-b87d-4c31081357c2",
                "7b19db4b-a425-4ebc-ab32-ef62949d5cdd",
            };
            DisableDatasets(jiankun);

        }
        private static void DisableDatasets(HashSet<string> datasetIds)
        {
            var datasetJsonFilePaths = ReadFile.GetAllFilePath(FolderPath);

            foreach (var datasetJsonFilePath in datasetJsonFilePaths)
            {
                if (datasetJsonFilePath.ToLower().Contains("dataset"))
                {
                    string fileContent = ReadFile.ThirdMethod(datasetJsonFilePath);
                    JsonSerializerSettings serializer = new JsonSerializerSettings
                    {
                        DateParseHandling = DateParseHandling.None
                    };
                    JObject gitDatasetJObject = JsonConvert.DeserializeObject<JObject>(fileContent, serializer);
                    string id = gitDatasetJObject["id"].ToString();
                    if (datasetIds.Contains(id))
                    {
                        Console.WriteLine(id);
                        gitDatasetJObject["isEnabled"] = false;
                        WriteFile.FirstMethod(datasetJsonFilePath, gitDatasetJObject.ToString());
                    }
                }
            }
        }

        public static void UpdateAllSqlKeyVaultName()
        {
            var datasetJsonFilePaths = ReadFile.GetAllFilePath(FolderPath);

            foreach (var datasetJsonFilePath in datasetJsonFilePaths)
            {
                if (datasetJsonFilePath.ToLower().Contains("dataset"))
                {
                    string fileContent = ReadFile.ThirdMethod(datasetJsonFilePath);
                    JsonSerializerSettings serializer = new JsonSerializerSettings
                    {
                        DateParseHandling = DateParseHandling.None
                    };
                    JObject gitDatasetJObject = JsonConvert.DeserializeObject<JObject>(fileContent, serializer);
                    if (gitDatasetJObject["dataFabric"]?.ToString().ToLower().Equals("sql") == true)
                    {
                        if (gitDatasetJObject["connectionInfo"]["auth"]["keyVaultName"]?.ToString().ToLower().Equals("datacopprod") == true)
                        {
                            gitDatasetJObject["connectionInfo"]["auth"]["keyVaultName"] = "datacop-prod";
                            WriteFile.FirstMethod(datasetJsonFilePath, gitDatasetJObject.ToString());
                            Console.WriteLine(datasetJsonFilePath);
                        }
                        else if (gitDatasetJObject["connectionInfo"]["auth"]["keyVaultName"]?.ToString().ToLower().Equals("datacop-prod") == true)
                        {
                        }
                        else
                        {
                            Console.WriteLine(gitDatasetJObject["id"]);
                            Console.WriteLine(gitDatasetJObject["connectionInfo"]["auth"]["keyVaultName"]);
                        }
                    }
                }
            }
        }

        public static void UpdateSomeCFRToAdls()
        {
            var datasetIds = new HashSet<string>()
            {
                "CFR_GAL_ReportUXWeekly",
                "CFR_WWPPE_Cubes_ProPlusUsage",
                "CFR_WW_ReportUXWeekly",
                "CFR_WW_ReportUXAndAPIWeekly",
                "CFR_GAL_ReportUXAndAPIWeekly",
                "CFR_CookedUXLog",
                "CFR_CookedKustoAPILog",
                "CFR_WWPPE_Cubes_FormsActivity",
                "CFR_WWPPE_Cubes_FormsProActivity",
                "CFR_ReportGraphAPIAndExportWeekly",
                "CFR_ReportGraphAPIAndExportMonthly",
                "CFR_GAL_ReportUXMonthly",
                "CFR_WW_ReportUXMonthly",
                "CFR_WW_ReportUXAndAPIMonthly",
                "CFR_GAL_ReportUXAndAPIMonthly",
                "CFR_WWPROD_Cubes_ProPlusUsage",
                "CFR_WWPROD_Cubes_FormsActivity",
                "CFR_WWPROD_Cubes_FormsProActivity",
            };

            var jsonFilePaths = ReadFile.GetAllFilePath(FolderPath);

            foreach (var jsonFilePath in jsonFilePaths)
            {
                if (jsonFilePath.ToLower().Contains(@"\datasets"))
                {
                    string fileContent = ReadFile.ThirdMethod(jsonFilePath);
                    JsonSerializerSettings serializer = new JsonSerializerSettings
                    {
                        DateParseHandling = DateParseHandling.None
                    };
                    JObject gitDatasetJObject = JsonConvert.DeserializeObject<JObject>(fileContent, serializer);
                    string id = gitDatasetJObject["id"].ToString();
                    if (datasetIds.Contains(id))
                    {
                        var connectionInfo = JObject.Parse(gitDatasetJObject["connectionInfo"].ToString());
                        connectionInfo.Remove("cosmosVC");
                        string streamPath = connectionInfo["streamPath"].ToString().Trim(new char[] { '/' });
                        var newConnectionInfo = new JObject();
                        if (streamPath.ToLower().StartsWith("shares/cfr.ppe"))
                        {
                            newConnectionInfo["dataLakeStore"] = "cfr-ppe-c14.azuredatalakestore.net";
                            newConnectionInfo["streamPath"] = streamPath.Substring(14);
                        }
                        else if (streamPath.ToLower().StartsWith("shares/cfr.prod"))
                        {
                            newConnectionInfo["dataLakeStore"] = "cfr-prod-c14.azuredatalakestore.net";
                            newConnectionInfo["streamPath"] = streamPath.Substring(15);
                        }
                        else
                        {
                            continue;
                        }

                        gitDatasetJObject["connectionInfo"] = newConnectionInfo;
                        gitDatasetJObject["dataFabric"] = "ADLS";

                        Console.WriteLine($"Update datasets: {id}");
                        WriteFile.FirstMethod(jsonFilePath, gitDatasetJObject.ToString());
                    }
                }

                // Update datasetTests
                if (jsonFilePath.ToLower().Contains(@"\monitors"))
                {
                    string fileContent = ReadFile.ThirdMethod(jsonFilePath);
                    JsonSerializerSettings serializer = new JsonSerializerSettings
                    {
                        DateParseHandling = DateParseHandling.None
                    };
                    JObject gitDatasetTestJObject = JsonConvert.DeserializeObject<JObject>(fileContent, serializer);
                    string id = gitDatasetTestJObject["id"].ToString();
                    string datasetId = gitDatasetTestJObject["datasetId"].ToString();
                    if (datasetIds.Contains(datasetId))
                    {
                        gitDatasetTestJObject["testContentType"] = "AdlsAvailability";
                        gitDatasetTestJObject["dataFabric"] = "ADLS";
                        Console.WriteLine($"Update datasetTests: {id}");
                        WriteFile.FirstMethod(jsonFilePath, gitDatasetTestJObject.ToString());
                    }
                }
            }
        }

        public static void UpdateCosmosViewScripts()
        {
            var datasetJsonFilePaths = ReadFile.GetAllFilePath(FolderPath);
            int count = 0;

            foreach (var datasetJsonFilePath in datasetJsonFilePaths)
            {
                if (datasetJsonFilePath.ToLower().Contains(@"\monitor"))
                {
                    string fileContent = ReadFile.ThirdMethod(datasetJsonFilePath);
                    JsonSerializerSettings serializer = new JsonSerializerSettings
                    {
                        DateParseHandling = DateParseHandling.None
                    };
                    JObject gitDatasetJObject = JsonConvert.DeserializeObject<JObject>(fileContent, serializer);
                    if (gitDatasetJObject["dataFabric"]?.ToString().ToLower().Equals("cosmosview") == true)
                    {
                        count++;
                        var cosmosScriptContent = gitDatasetJObject["testContent"]["cosmosScriptContent"].ToString();
                        var index = cosmosScriptContent.IndexOf("OUTPUT ViewSamples");
                        cosmosScriptContent = cosmosScriptContent.Substring(0, index);
                        cosmosScriptContent += " Count = SELECT COUNT() AS NumSessions FROM ViewSamples; OUTPUT Count TO SSTREAM \"/my/output.ss\"";
                        gitDatasetJObject["testContent"]["cosmosScriptContent"] = cosmosScriptContent;
                        WriteFile.FirstMethod(datasetJsonFilePath, gitDatasetJObject.ToString());
                    }
                }
            }
            Console.WriteLine($"count: {count}");
        }

        public static void UpdateCosmosVCToBuild()
        {
            var datasetJsonFilePaths = ReadFile.GetAllFilePath(FolderPath);

            foreach (var datasetJsonFilePath in datasetJsonFilePaths)
            {
                if (datasetJsonFilePath.ToLower().Contains(@"dataset"))
                {
                    string fileContent = ReadFile.ThirdMethod(datasetJsonFilePath);
                    JsonSerializerSettings serializer = new JsonSerializerSettings
                    {
                        DateParseHandling = DateParseHandling.None
                    };
                    JObject gitDatasetJObject = JsonConvert.DeserializeObject<JObject>(fileContent, serializer);
                    if (gitDatasetJObject["dataFabric"]?.ToString().ToLower().Equals("cosmosstream") == true)
                    {
                        if (gitDatasetJObject["connectionInfo"]["cosmosVC"]?.ToString().ToLower().Trim('/').Equals("https://cosmos14.osdinfra.net/cosmos/ideas.prod.build") == true)
                        {
                            continue;
                        }
                        else if (gitDatasetJObject["connectionInfo"]["cosmosVC"]?.ToString().ToLower().Trim('/').Equals("https://cosmos14.osdinfra.net/cosmos/ideas.prod") == true)
                        {
                            gitDatasetJObject["connectionInfo"]["cosmosVC"] = gitDatasetJObject["connectionInfo"]["cosmosVC"]?.ToString().Trim('/') + ".Build/";
                            WriteFile.FirstMethod(datasetJsonFilePath, gitDatasetJObject.ToString());
                            Console.WriteLine($"Update dataset: {gitDatasetJObject["id"]?.ToString()}");
                        }
                        else
                        {
                            gitDatasetJObject["connectionInfo"]["cosmosVC"] = @"https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod.Build/";
                            WriteFile.FirstMethod(datasetJsonFilePath, gitDatasetJObject.ToString());
                            Console.WriteLine(gitDatasetJObject["id"]?.ToString());
                            Console.WriteLine(gitDatasetJObject["connectionInfo"]["cosmosVC"]?.ToString());
                        }
                    }
                }
            }
        }

        public static void UpdateAllDatasetJsonForMergingADLSCosmos()
        {
            var datasetJsonFilePaths = ReadFile.GetAllFilePath(FolderPath);

            foreach (var datasetJsonFilePath in datasetJsonFilePaths)
            {
                Console.WriteLine(datasetJsonFilePath);
                UpdateDatasetConnectionInfoForMergingADLSCosmos(datasetJsonFilePath);
            }
        }

        public static void UpdateOldPathSchemaDatasetJsonForMergingADLSCosmos()
        {
            var folders = ReadFile.GetFolderSubPaths(FolderPath, ReadType.Directory, PathType.Absolute);
            int count = 0;
            foreach (var folder in folders)
            {
                Console.WriteLine(folder);
                string subFolderPath = folder + @"\Dataset";
                try
                {
                    var datasetJsonFiles = ReadFile.GetFolderSubPaths(subFolderPath, ReadType.File, PathType.Absolute);
                    foreach (var filePath in datasetJsonFiles)
                    {
                        UpdateDatasetConnectionInfoForMergingADLSCosmos(filePath);
                    }

                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            }
            Console.WriteLine(count);
        }

        public static void UpdateCFRDatasetJsonForMergingADLSCosmos()
        {
            string cfrFolder = Path.Combine(FolderPath, @"\CFR\Cooked\Datasets");
            try
            {
                var datasetJsonFiles = ReadFile.GetFolderSubPaths(cfrFolder, ReadType.File, PathType.Absolute);
                foreach (var filePath in datasetJsonFiles)
                {
                    UpdateDatasetConnectionInfoForMergingADLSCosmos(filePath);
                }

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        private static void UpdateDatasetConnectionInfoForMergingADLSCosmos(string filePath)
        {
            if (!filePath.EndsWith(".json"))
            {
                return;
            }
            string datasetString = ReadFile.ThirdMethod(filePath);
            JObject datasetJObject = JObject.Parse(datasetString);
            //Console.WriteLine(datasetJObject["dataFabric"]);
            if (datasetJObject["dataFabric"] != null && datasetJObject["dataFabric"].ToString() == "ADLS")
            {
                string curTime = DateTime.UtcNow.ToString("o");
                var connectionInfo = JObject.Parse(datasetJObject["connectionInfo"].ToString());
                datasetJObject["lastModifiedTime"] = curTime;
                datasetJObject["lastModifiedBy"] = "jianjlv";
                string streamPath = null;
                if (connectionInfo["streamPath"] != null)
                {
                    streamPath = connectionInfo["streamPath"].ToString();
                }
                connectionInfo["streamPath"] = string.IsNullOrEmpty(streamPath) ? connectionInfo["dataLakePath"] : streamPath;
                connectionInfo.Remove("cosmosPath");
                connectionInfo.Remove("cosmosVC");
                connectionInfo.Remove("dataLakePath");
                connectionInfo.Remove("auth");
                if (connectionInfo["dataLakeStore"] != null && connectionInfo["dataLakeStore"].ToString().Equals("ideas-prod-c14.azuredatalakestore.net"))
                {
                    connectionInfo["cosmosVC"] = "https://cosmos14.osdinfra.net/cosmos/Ideas.prod/";
                    string streamPathTemp = connectionInfo["streamPath"].ToString();
                    connectionInfo.Remove("streamPath");
                    connectionInfo["streamPath"] = streamPathTemp;
                }
                datasetJObject["connectionInfo"] = connectionInfo;
                if (datasetJObject["createdBy"] == null || string.IsNullOrEmpty(datasetJObject["createdBy"].ToString()))
                {
                    datasetJObject.Remove("lastModifiedBy");
                }
                WriteFile.FirstMethod(filePath, datasetJObject.ToString());
            }
        }

        public static void DisableAllDatasetTest()
        {
            string fileFolder = Path.Combine(FolderPath, @"PROD\ConsumerPaidSubscription\Monitors\");
            var filePaths = ReadFile.GetFolderSubPaths(fileFolder, ReadType.File, PathType.Absolute);
            foreach (var filePath in filePaths)
            {
                var fileContentString = ReadFile.ThirdMethod(filePath);
                var json = JsonConvert.DeserializeObject<JObject>(fileContentString, new JsonSerializerSettings
                {
                    // Default value is DateParseHandling.DateTime
                    DateParseHandling = DateParseHandling.None
                });
                var id = json["id"].ToString();
                json["status"] = "Disabled";
                WriteFile.FirstMethod(filePath, json.ToString());
                Console.WriteLine(id);
            }
        }
    }
}
