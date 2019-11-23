namespace CSharpDemo.IDEAs
{
    using CSharpDemo.Azure.CosmosDB;
    using CSharpDemo.FileOperation;
    using Microsoft.Azure.Documents;
    using Newtonsoft.Json.Linq;
    using System;

    class DatasetJsonFileOperation
    {
        private static string FolderPath = @"D:\IDEAs\Ibiza\Source\DataCopMonitors";


        public static void MainMethod()
        {
            //UpdateAllDatasetJsonForMergingADLSCosmos();
            //UpdateOldPathSchemaDatasetJsonForMergingADLSCosmos();
            //UpdateCFRDatasetJsonForMergingADLSCosmos();
            //DisableAllDatasets();
        }
        public static void DisableAllDatasets()
        {
            AzureCosmosDBClient.KeyVaultName = "datacopprod";
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

        public static void UpdateAllDatasetJsonForMergingADLSCosmos()
        {
            UpdateDatasetJsonForMergingADLSCosmosRecursively(FolderPath);
        }

        private static void UpdateDatasetJsonForMergingADLSCosmosRecursively(string folderPath)
        {
            var datasetJsonFiles = ReadFile.GetFolderSubPaths(folderPath, ReadType.File, PathType.Absolute);

            foreach (var datasetJsonFile in datasetJsonFiles)
            {
                Console.WriteLine(datasetJsonFile);
                UpdateDatasetConnectionInfoForMergingADLSCosmos(datasetJsonFile);
            }

            var subFolders = ReadFile.GetFolderSubPaths(folderPath, ReadType.Directory, PathType.Absolute);
            foreach (var subFolder in subFolders)
            {
                UpdateDatasetJsonForMergingADLSCosmosRecursively(subFolder);
            }
        }

        public static void UpdateOldPathSchemaDatasetJsonForMergingADLSCosmos()
        {
            var folders = ReadFile.GetFolderSubPaths(FolderPath, ReadType.Directory, PathType.Absolute);
            int count = 0;
            foreach (var folder in folders)
            {
                System.Console.WriteLine(folder);
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
                    System.Console.WriteLine(e.Message);
                }
            }
            Console.WriteLine(count);
        }

        public static void UpdateCFRDatasetJsonForMergingADLSCosmos()
        {
            string cfrFolder = FolderPath + @"\CFR\Cooked\Datasets";
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
                SaveFile.FirstMethod(filePath, datasetJObject.ToString());
            }
        }
    }
}
