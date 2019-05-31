using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using CSharpDemo.Application;
using CSharpDemo.CosmosDBModel;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CSharpDemo.Azure
{
    class AzureCosmosDB
    {
        // "datacopdev", "datacopprod" or "csharpmvcwebapikeyvault"(csharpmvcwebapicosmosdb)
        public static string KeyVaultName = "csharpmvcwebapikeyvault";
        public static void MainMethod()
        {
            //UpdateAllAlertSettingsDemo();
            //QueryAlertSettingDemo();
            //QueryAlertDemo();
            //UpsertAlertDemoToDev();


            //UpsertTestDemoToCosmosDB();
            //QueryTestDemo();
            GetLastTestDemo();


            //UpsertDatasetDemoToDev();
            //UpsertDatcopScoreDemoToDev();
            //UpsertActiveAlertTrendToDev();
        }

        public static void UpdateNoneAlertTypeDemo()
        {
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "Alert");
            // Collation: asc and desc is ascending and descending
            IList<JObject> list = azureCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT * FROM c where c.testCategory = 'None' ORDER BY c.issuedOnDate ASC")).Result;
            foreach (JObject alert in list)
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
                    Console.WriteLine("Error");
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
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "AlertSettings");
            // Collation: asc and desc is ascending and descending
            IList<TestRunAlertSettings> list = azureCosmosDB.GetAllDocumentsInQueryAsync<TestRunAlertSettings>(new SqlQuerySpec(@"SELECT * FROM c")).Result;
            foreach (TestRunAlertSettings alert in list)
            {
                Console.WriteLine(alert.Id);
                alert.ServiceCustomFieldNames = new string[] {"DatasetId",
                                                            "AlertType",
                                                            "DisplayInSurface",
                                                            "BusinessOwner",
                                                            "TitleOverride"};
                ResourceResponse<Document> resource = azureCosmosDB.UpsertDocumentAsync(alert).Result;
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

        public static void QueryAlertDemo()
        {
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "Alert");
            // Collation: asc and desc is ascending and descending
            IList<JObject> list = azureCosmosDB.GetAllDocumentsInQueryAsync<JObject>(new SqlQuerySpec(@"SELECT c.issuedOnDate AS aaaa, c.impactedDate AS bbbb, time(c.issuedOnDate), time(c.impactedDate) AS cccc FROM c")).Result;
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
            TestRunAlert t = azureCosmosDB.FindFirstOrDefaultItemAsync<TestRunAlert>(new SqlQuerySpec(@"SELECT * FROM c WHERE c.id=1111 order by c.timestampTicks desc")).Result;
            if (t == null)
            {
                Console.WriteLine("null");
            }
            else
            {
                Console.WriteLine(t.Id);
                Console.WriteLine(JsonConvert.SerializeObject(t));
            }
        }

        public static void UpsertTestDemoToCosmosDB()
        {
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("CosmosDBTest", "TestCollectionId");

            AzureCosmosDBTestClass t = new AzureCosmosDBTestClass();
            t.TestA = "a";
            t.TestB = "b";
            t.TestC = "cc";
            t.Id = "1111";
            t.TestHashSet = new HashSet<string>();
            t.TestHashSet.Add("1234");

            ResourceResponse<Document> resource = azureCosmosDB.UpsertDocumentAsync(t).Result;
            Console.WriteLine(resource);
        }

        public static void UpsertAlertDemoToDev()
        {
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "Alert");

            string alertDemoString =
                @"{
                    'id': '46ef2a20-297a-42ca-a994-2024965017f5',
                    'status': 'Succeed',
                    'alertSettingId': '3f49b378-40a8-4930-b39d-595381f3fb44',
                    'severity': 4,
                    'title': 'ExternalO365CommercialServicesMAU_AllUp_RL28_MonthOverMonth: Test ExternalO365CommercialServicesMAU_AllUp_RL28_MonthOverMonth fails on 2/2/2019 because percentage difference of 5.61947771497777 is above positive threshold of 5.',
                    'description': 'The description',
                    'timestamp': '2019-02-14T00:01:10.6699545Z',
                    'timestampTicks': 636856992706699500,
                    'incidentId': 109849040,
                    'routingId': 'IDEAs://datacop',
                    'owningTeamId': 'IDEAS\\IDEAsDataCop',
                    'environment': 'Dev'
                }";

            TestRunAlert tr = JsonConvert.DeserializeObject<TestRunAlert>(alertDemoString);
            tr.ImpactedDates = new HashSet<DateTime>();
            tr.ImpactedDates.Add(DateTime.Parse("2019-04-12"));
            tr.ImpactedDates.Add(DateTime.Parse("2019-04-09"));
            tr.ImpactedDates.Add(DateTime.Parse("2019-04-19"));
            tr.ImpactedDates.Add(DateTime.Parse("2019-04-09"));
            Console.WriteLine(tr.ImpactedDates.Count);

            Console.WriteLine(tr.Id);

            ResourceResponse<Document> resource = azureCosmosDB.UpsertDocumentAsync(tr).Result;
            Console.WriteLine(resource);
        }

        public static void UpsertDatasetDemoToDev()
        {
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCopTest", "DataCopScore");

            string datasetDemoString =
                @"{
                    'id': 'fa8f1f0e-4831-46b3-8332-e433f4884444',
                    'name': 'Tenant Profile Extension - MS Sales Tenants Mapping ',
                    'createTime': '2019-02-25T03:38:49.4165653Z',
                    'lastModifiedTime': '0001-01-01T00:00:00',
                    'connectionInfo': '',
                    'dataFabric': 'ADLS',
                    'type': 'Core',
                    'category': 'Profile',
                    'startDate': '2019-02-20T00:00:00.0000000Z',
                    'rollingWindow': '5.00:00:00',
                    'grain': 'Daily',
                    'sla': '1.12:00:00',
                    'isEnabled': true
                }";

            Dataset ds = JsonConvert.DeserializeObject<Dataset>(datasetDemoString);
            List<string> children = new List<string>();
            //children.Add("0fc8063f-3b03-4223-81a0-c591cd1c409a");
            children.Add("fa8f1f0e-4831-46b3-8332-e433f48813b3");
            children.Add("5dbcf13b-a7c8-4a9f-9226-55a6980e1f00");
            children.Add("8d6e5188-3242-4065-89a9-51e48cefd7f8");
            ds.Children = children;

            ResourceResponse<Document> resource = azureCosmosDB.UpsertDocumentAsync(ds).Result;
        }
        public static void UpsertDatcopScoreDemoToDev()
        {
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCopTest", "DataCopScore");

            string datcopScoreDemoString =
                @"{
                    'id': 'InternalO365CommercialServicesMAU_PPE_20190307094515',
                    'name': 'InternalO365CommercialServicesMAU_PPE',
                    'datasetId': '8d6e5188-3242-4065-89a9-51e48cefd7f8',
                    'datasetCategory': 'Metric',
                    'scoreTime': '2019-02-19T09:45:13.4866178Z',
                    'score': 50,
                    'correctnessScore': 50,
                    'testRunIds': [
                        '250471d4-cfb0-40fd-8f0e-9cbd1869c39b',
                        '3d8161a2-4df9-4455-8dcd-b0532e1cd142'
                    ],
                    '_rid': 'et18AIUSqCsbAAAAAAAAAA==',
                    '_self': 'dbs/et18AA==/colls/et18AIUSqCs=/docs/et18AIUSqCsbAAAAAAAAAA==/',
                    '_etag': '\'07002fc8-0000-0700-0000-5c6bd02c0000\'',
                    '_attachments': 'attachments/',
                    '_ts': 1550569516
                }";
            DataCopScore dc = JsonConvert.DeserializeObject<DataCopScore>(datcopScoreDemoString);

            ResourceResponse<Document> resource2 = azureCosmosDB.UpsertDocumentAsync(dc).Result;
        }

        public static void UpsertActiveAlertTrendToDev()
        {
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "ActiveAlertTrend");
            DateTime d = DateTime.Now.AddMonths(-0);
            DateTime date = new DateTime(d.Year, d.Month + 1, 1).AddSeconds(-1);

            string activeAlertTrendString =
                "{" +
                    $@"'id': 'ActiveAlertTrend_{date.ToString("yyyyMM")}',
                    'timeStamp': '{date}',
                    'activeAlertCount': 45" +
                "}";
            ActiveAlertTrend aat = JsonConvert.DeserializeObject<ActiveAlertTrend>(activeAlertTrendString);
            aat = null;
            if (aat != null)
            {
                ResourceResponse<Document> resource2 = azureCosmosDB.UpsertDocumentAsync(aat).Result;
                Console.WriteLine(resource2);
            }
            else
            {
                Console.WriteLine("aat is null");
            }

        }

        public string Endpoint { get; set; }
        public string Key { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CosmosDBDocumentCollection"/> class.
        /// </summary>
        /// <param name="databaseId">The database identifier.</param>
        /// <param name="collectionId">The collection identifier.</param>
        public AzureCosmosDB(string databaseId, string collectionId)
        {
            this.Client = CosmosDBDocumentClient.Instance;

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
            return await this.Client.DeleteDocumentAsync(this.CollectionLink, requestOptions);
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

        /// <summary>
        /// Gets the instance.
        /// </summary>
        /// <value>The instance.</value>
        public static CosmosDBDocumentClient Instance => documentClient.Value;

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
