namespace LogAnalyticsDemo
{
    using AzureLib.CosmosDB;
    using AzureLib.KeyVault;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    class DataCopLogQueryDemo
    {
        private static DataCopLogProvider datacopLogProvider;

        public static void MainMethod()
        {
            Console.WriteLine("Start Log Analytics demo process: ");
            Initialize(@"datacop-prod");

            //CosmosWorkerFallbackLogQuery();
            CosmosWorkerRetryLogQuery();
        }

        private static void CosmosWorkerFallbackLogQuery()
        {
            Console.WriteLine(@"Start querying CosmosWorkerFallbackLog...");
            AzureCosmosDBClient testRunCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            var minEndTime = DateTime.UtcNow.AddDays(-18);
            string queryString = "CosmosWorkerTrace_CL ";
            queryString += @"| where TagId_g == '1fb4b131-1a3d-45ce-920a-2ddb97291c82' or TagId_g == '1fb4b131-1a3d-45ce-920a-2ddb97291c82'";
            queryString += @"| where TimeGenerated > ago(15d)";
            queryString += @"| project Message";
            var queryResult = datacopLogProvider.GetQueryresult(queryString);
            var messageJsons = JArray.Parse(JsonConvert.SerializeObject(queryResult));
            Console.WriteLine("Output testRun messages: ");
            Dictionary<string, JObject> datasetDict = new Dictionary<string, JObject>();
            foreach (var messageJson in messageJsons)
            {
                var message = messageJson["Message"].ToString();
                int start = message.IndexOf("testRun '") + 9;
                int end = message.IndexOf("'", start);
                var testRunId = message.Substring(start, end - start);
                //Console.WriteLine(testRunId);
                var testRunQuery = $"SELECT * FROM c WHERE c.id = '{testRunId}'";
                var testRuns = testRunCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(testRunQuery).Result;
                var status = testRuns[0]["status"].ToString();
                Console.WriteLine(testRunId);
                Console.WriteLine(testRuns[0]["testContentType"].ToString());
                Console.WriteLine(status);
            }
        }

        private static void CosmosWorkerRetryLogQuery()
        {
            Console.WriteLine(@"Start querying CosmosWorkerRetryLog...");
            AzureCosmosDBClient testRunCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            string queryString = "CosmosWorkerTrace_CL ";
            queryString += @"| where TimeGenerated > ago(2d) ";
            queryString += @"| where TagId_g == 'fd98b873-fd99-4de8-b3b3-f2d6262ee3dc'";
            queryString += @"| extend Count = toint(substring(Message, 57))";
            queryString += @"| where Count > 0";
            queryString += @"| project Count, TestRunId_g";
            var queryResult = datacopLogProvider.GetQueryresult(queryString);
            var testRunIdJsons = JArray.Parse(JsonConvert.SerializeObject(queryResult));
            string joinStr = string.Join(",", testRunIdJsons.Select(t => "'" + t["TestRunId_g"] + "'").ToArray());
            var testRunQuery = $"SELECT c.id, c.status FROM c WHERE c.status != 'Failed' and c.id in ({joinStr})";
            Console.WriteLine("Output testRun messages: ");
            var testRuns = testRunCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(testRunQuery).Result;
            foreach (var testRun in testRuns)
            {
                var status = testRun["status"].ToString();
                if (!status.Equals("Failed"))
                {
                    Console.WriteLine(testRun["id"]);
                    Console.WriteLine(testRun["status"]);
                }
            }
        }

        private static void CosmosViewWorkerFallbackLogQuery()
        {
            Console.WriteLine(@"Start querying CosmosViewWorkerFallbackLog...");
            AzureCosmosDBClient testRunCosmosDBClient = new AzureCosmosDBClient("DataCop", "PartitionedTestRun");
            var minEndTime = DateTime.UtcNow.AddDays(-18);
            string queryString = "CosmosWorkerTrace_CL ";
            queryString += @"| where TagId_g == 'a83c16b8-c7c1-4676-b6b0-2dd8b4ca2a14' ";
            queryString += @"| where Message endswith 'Success\'.' ";
            queryString += @"| where TimeGenerated > ago(1d) ";
            queryString += @"| project TestRunId_g";
            var queryResult = datacopLogProvider.GetQueryresult(queryString);
            var testRunIdJsons = JArray.Parse(JsonConvert.SerializeObject(queryResult));
            Console.WriteLine("Output testRun messages: ");
            Dictionary<string, JObject> datasetDict = new Dictionary<string, JObject>();
            foreach (var testRunIdJson in testRunIdJsons)
            {
                JObject fallbackTestRunJson = new JObject();
                var testRunId = testRunIdJson["TestRunId_g"].ToString();
                fallbackTestRunJson["testRunId"] = testRunId;
                Console.WriteLine(testRunId);
                var messageQueryString = @"CosmosWorkerTrace_CL" +
                    @"| where TagId_g == 'e21c2015-72c2-4ff2-84b6-31b1aa441f14'" +
                    $"| where TestRunId_g == '{testRunId}'" +
                    @"| project Message";
                var messageQueryResult = datacopLogProvider.GetQueryresult(messageQueryString);
                var messageJsons = JArray.Parse(JsonConvert.SerializeObject(messageQueryResult));
                JArray messages = new JArray();
                foreach (var messageJson in messageJsons)
                {
                    messages.Add(messageJson["Message"]);
                }
                fallbackTestRunJson["messages"] = messages;

                var testRunQuery = $"SELECT * FROM c WHERE c.id = '{testRunId}'";
                var testRuns = testRunCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(testRunQuery).Result;
                var datasetId = testRuns[0]["datasetId"].ToString();
                if (!datasetDict.ContainsKey(datasetId))
                {
                    datasetDict.Add(datasetId, new JObject());
                    var succeedTestRunQuery = $"SELECT c.id FROM c WHERE c.datasetId = '{datasetId}' and c.endTime > '{minEndTime:o}' order by c.createTime desc";
                    var succeedTestRuns = testRunCosmosDBClient.GetAllDocumentsInQueryAsync<JObject>(succeedTestRunQuery).Result;
                    datasetDict[datasetId]["succeedTestRunIds"] = JArray.FromObject(succeedTestRuns.Select(s => s["id"].ToString()));
                    datasetDict[datasetId]["fallbackTestRuns"] = new JArray();
                }

                ((JArray)datasetDict[datasetId]["fallbackTestRuns"]).Add(fallbackTestRunJson);
                //jObject["datasetId"] = datasetId;


            }

            JArray result = new JArray();
            foreach (var datasetPair in datasetDict)
            {
                var jObject = new JObject();
                var datasetId = datasetPair.Key;
                var testRunsInfo = datasetPair.Value;

                var fallbackTestRuns = (JArray)testRunsInfo["fallbackTestRuns"];
                var succeedTestRunIds = testRunsInfo["succeedTestRunIds"];
                jObject["datasetId"] = datasetId;
                jObject["succeedTestRunIds"] = succeedTestRunIds;
                jObject["fallbackTestRunIds"] = new JArray();
                jObject["fallbackTestRunMessages"] = new JArray();
                foreach (var fallbackTestRun in fallbackTestRuns)
                {
                    ((JArray)jObject["fallbackTestRunIds"]).Add(fallbackTestRun["testRunId"]);
                    foreach (var message in (JArray)fallbackTestRun["messages"])
                    {
                        ((JArray)jObject["fallbackTestRunMessages"]).Add(message);
                    }
                }

                result.Add(jObject);
            }
            File.WriteAllText(@"D:\data\company_work\M365\IDEAs\datacop\cosmosworker\fallback.json", result.ToString());
        }

        private static void Initialize(string keyVaultName)
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string workspaceId = secretProvider.GetSecretAsync(keyVaultName, "DataCopLogMonitorWorkspaceId").Result;
            string clientId = secretProvider.GetSecretAsync(keyVaultName, "BuildLogMonitorClientId").Result;
            string clientKey = secretProvider.GetSecretAsync(keyVaultName, "BuildLogMonitorClientKey").Result;
            datacopLogProvider = new DataCopLogProvider(workspaceId, clientId, clientKey);

            string endpoint = secretProvider.GetSecretAsync(keyVaultName, "CosmosDBEndPoint").Result;
            string key = secretProvider.GetSecretAsync(keyVaultName, "CosmosDBAuthKey").Result;
            AzureCosmosDBClient.Endpoint = endpoint;
            AzureCosmosDBClient.Key = key;
        }

    }
}
