namespace CSharpDemo.Azure
{
    using CSharpDemo.Azure.CosmosDB;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;

    class AzureCosmosDB
    {
        public static void MainMethod()
        {
            // keyVault name csharpmvcwebapikeyvault for csharpmvcwebapicosmosdb
            AzureCosmosDBClient.KeyVaultName = "datacopdev";

            //QueryTestDemo();
            //GetLastTestDemo();

            UpsertTestDemoToCosmosDB();
            //ReadTestDemoTestLongMaxValueFromCosmosDB();

            //DeleteTestDemo();
        }

        public static void QueryTestDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("CosmosDBTest", "TestCollection");
            // Collation: asc and desc is ascending and descending
            IList<AzureCosmosDBTestClass> list = azureCosmosDBClient.GetAllDocumentsInQueryAsync<AzureCosmosDBTestClass>(new SqlQuerySpec(@"SELECT * FROM c order by c.timestampTicks asc")).Result;
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
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("CosmosDBTest", "TestCollection");
            // Collation: asc and desc is ascending and descending
            JObject t = azureCosmosDBClient.FindFirstOrDefaultItemAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c WHERE c.id=1111 order by c.timestampTicks desc")).Result;
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

        public static void UpsertTestDemoToCosmosDB()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCop", "MonitorReport");

            AzureCosmosDBTestClass t = new AzureCosmosDBTestClass();
            t.Id = Guid.NewGuid().ToString();
            t.PartitionKey = DateTime.UtcNow.ToString();
            Console.WriteLine(t.Id);
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
            t.TimeSpanTest = new TimeSpan(12, 0, 0);

            ResourceResponse<Document> resource = azureCosmosDBClient.UpsertDocumentAsync(t).Result;
            Console.WriteLine(resource);
        }

        public static void ReadTestDemoTestLongMaxValueFromCosmosDB()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("DataCopTest", "Test");
            AzureCosmosDBTestClass azureCosmosDBTestClass = azureCosmosDBClient.FindFirstOrDefaultItemAsync<AzureCosmosDBTestClass>(new SqlQuerySpec(@"SELECT * FROM c WHERE c.id='e4da3600-8520-481d-b3e5-9a05e8920731'")).Result;
            if (azureCosmosDBTestClass == null)
            {
                Console.WriteLine("null");
            }
            else
            {
                // Read long max value
                Console.WriteLine(azureCosmosDBTestClass.TestLongMaxValueViaLong);
                Console.WriteLine(azureCosmosDBTestClass.TestLongMaxValueViaDouble);
                Console.WriteLine(azureCosmosDBTestClass.TestLongMaxValueViaLong == azureCosmosDBTestClass.TestLongMaxValueViaDouble);

                Console.WriteLine();
            }
        }

        /*
        * For deleting operation, now I must set the PartitionKey, 
        * so I just can delete the document with PartitionKey.
        */
        public static void DeleteTestDemo()
        {
            AzureCosmosDBClient azureCosmosDBClient = new AzureCosmosDBClient("CosmosDBTest", "TestPartitionContains");

            string documentLink = UriFactory.CreateDocumentUri("CosmosDBTest", "TestPartitionContains", "34a4a888-dd51-496b-b21c-9d2fc91be01d").ToString();
            var reqOptions = new RequestOptions { PartitionKey = new PartitionKey("a") };
            ResourceResponse<Document> resource = azureCosmosDBClient.DeleteDocumentAsync(documentLink, reqOptions).Result;
            Console.WriteLine(resource);
        }
    }
}
