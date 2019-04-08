using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using CSharpDemo.Application;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CSharpDemo.Azure
{

    class AzureCosmosDBTestClass
    {
        [JsonProperty("Testa")]
        public string TestA { get; set; } = "1234";
        [JsonProperty("Testb")]
        public string TestB { get; set; }
        [JsonProperty("Testc")]
        public string TestC { get; set; }
        public bool ShouldSerializeTestC()
        {
            return this.TestC == "c";
        }
        [JsonProperty("Testd")]
        private string TestD { get; set; } = "private testd";
        [JsonProperty("id")]
        public string Id { get; set; }
        [JsonProperty("timestampTicks")]
        public long TimestampTicks
        {
            get { return DateTime.UtcNow.Ticks; }
        }
    }
    class AzureCosmosDB
    {
        public static void MainMethod()
        {
            //QueryAlertSettingDemo();
            //QueryTestDemo();
            //UpdateTestDemo();
            //UpsertAlertDemoToDev();
            //UpsertTestDemoToCosmosDB();
            //UpsertDatasetDemoToDev();
            //UpsertDatcopScoreDemoToDev();
            UpsertActiveAlertTrendToDev();
        }

        public static void QueryAlertSettingDemo()
        {
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "AlertSettings");
            // Collation: asc and desc is ascending and descending
            IList<JObject> list = azureCosmosDB.GetAllDocumentsInQueryAsync<JObject>(azureCosmosDB.collectionLink, new SqlQuerySpec(@"SELECT distinct c.owningTeamId FROM c")).Result;
            foreach (JObject jObject in list)
            {
                if (jObject["owningTeamId"] != null)
                    Console.WriteLine(jObject["owningTeamId"]);
            }
        }
        public static void QueryTestDemo()
        {
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("CosmosDBTest", "TestCollectionId");
            // Collation: asc and desc is ascending and descending
            IList<AzureCosmosDBTestClass> list = azureCosmosDB.GetAllDocumentsInQueryAsync<AzureCosmosDBTestClass>(azureCosmosDB.collectionLink, new SqlQuerySpec(@"SELECT * FROM c order by c.timestampTicks asc")).Result;
            foreach (AzureCosmosDBTestClass t in list)
            {
                Console.WriteLine(t.Id);
            }
        }

        public static void UpdateTestDemo()
        {
            AzureCosmosDBTestClass t = new AzureCosmosDBTestClass();
            t.TestA = "AAAA";
            t.Id = "1235";
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("CosmosDBTest", "TestCollectionId");
            string updateResult = azureCosmosDB.UpdateTestDemo(t.Id, t).Result;
            Console.WriteLine(updateResult);
        }

        public static void UpsertTestDemoToCosmosDB()
        {
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("CosmosDBTest", "TestCollectionId");

            AzureCosmosDBTestClass t = new AzureCosmosDBTestClass();
            t.TestA = "a";
            t.TestB = "b";
            t.TestC = "c";
            t.Id = "1235";

            ResourceResponse<Document> resource = azureCosmosDB.UpsertDocumentAsync(t).Result;
        }

        public static void UpsertAlertDemoToDev()
        {
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("DataCop", "Alert");

            string alertDemoString =
                @"{
                    'id': '46ef2a20-297a-42ca-a994-20249650aaaa',
                    'status': 'Failed',
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
            string dateStr = "2019-04-30 23:59:59";
            DateTime d = DateTime.Now;
            DateTime date = new DateTime(d.Year, d.Month + 1, 1).AddSeconds(-1);

            string activeAlertTrendString =
                "{" + 
                    $@"'id': 'ActiveAlertTrend_{date.ToString("yyyyMM")}',
                    'timeStamp': '{date}',
                    'activeAlertCount': 45" + 
                "}";
            ActiveAlertTrend aat = JsonConvert.DeserializeObject<ActiveAlertTrend>(activeAlertTrendString);

            ResourceResponse<Document> resource2 = azureCosmosDB.UpsertDocumentAsync(aat).Result;
        }

        // It is useless.
        public async Task<string> UpdateTestDemo(string testId, AzureCosmosDBTestClass t)
        {
            SqlQuerySpec sqlQuerySpec = new SqlQuerySpec($@"UPDATE c.Testa = '{t.TestA}' WHERE c.id='{testId}'");

            return await this.FindFirstOrDefaultItemAsync<string>(sqlQuerySpec);
        }

        public async Task<string> GetLatestAlertAsync(string datasetTestId)
        {
            SqlQuerySpec sqlQuerySpec = new SqlQuerySpec($@"SELECT * FROM c WHERE c.datasetTestId='{datasetTestId}' 
                                                            order by c.timestampTicks desc");

            return await this.FindFirstOrDefaultItemAsync<string>(sqlQuerySpec);
        }

        public string Endpoint { get; set; }
        public string Key { get; set; }

        private readonly CosmosDBDocumentClient client;

        /// <summary>
        /// The document collection
        /// </summary>
        private readonly DocumentCollection documentCollection;

        /// <summary>
        /// The collection identifier
        /// </summary>
        private readonly string collectionId;

        /// <summary>
        /// The database identifier
        /// </summary>
        private readonly string databaseId;

        /// <summary>
        /// The collection link
        /// </summary>
        private readonly string collectionLink;

        /// <summary>
        /// The database link
        /// </summary>
        private readonly string databaseLink;

        /// <summary>
        /// Initializes a new instance of the <see cref="CosmosDBDocumentCollection"/> class.
        /// </summary>
        /// <param name="databaseId">The database identifier.</param>
        /// <param name="collectionId">The collection identifier.</param>
        public AzureCosmosDB(string databaseId, string collectionId)
        {
            this.client = CosmosDBDocumentClient.Instance;

            this.client.CreateDatabaseIfNotExistsAsync(new Database() { Id = databaseId }).Wait();
            this.databaseId = databaseId;
            this.collectionId = collectionId;
            this.databaseLink = this.client.GetDatabaseLink(databaseId);
            this.collectionLink = this.client.GetCollectionLink(databaseId, collectionId);

            this.documentCollection = this.client.CreateDocumentCollectionIfNotExistsAsync(this.databaseLink, new DocumentCollection() { Id = collectionId }, null).Result;
        }

        /// <summary>
        /// Gets the client.
        /// </summary>
        /// <value>The client.</value>
        public CosmosDBDocumentClient Client => this.client;

        /// <summary>
        /// Gets the collection link.
        /// </summary>
        /// <value>The collection link.</value>
        public string CollectionLink => this.collectionLink;

        /// <summary>
        /// Gets the database link.
        /// </summary>
        /// <value>The database link.</value>
        public string DatabaseLink => this.DatabaseLink;

        /// <summary>
        /// Gets the document collection.
        /// </summary>
        /// <value>The document collection.</value>
        public DocumentCollection DocumentCollection => this.documentCollection;

        /// <summary>
        /// read document as an asynchronous operation.
        /// </summary>
        /// <typeparam name="T">The type parameter</typeparam>
        /// <param name="documentLink">The document link.</param>
        /// <param name="options">The options.</param>
        /// <returns>Task&lt;DocumentResponse&lt;T&gt;&gt;.</returns>
        public async Task<DocumentResponse<T>> ReadDocumentAsync<T>(string documentLink, RequestOptions options = null)
        {
            return await this.client.ReadDocumentAsync<T>(documentLink, options);
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
            return await this.client.DeleteDocumentAsync(documentLink, requestOptions);
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
            return this.client.CreateDocumentQuery<T>(this.CollectionLink, sqlExpression, feedOptions);
        }

        /// <summary>
        /// Gets the document link.
        /// </summary>
        /// <param name="documentId">The document identifier.</param>
        /// <returns>System.String.</returns>
        public string GetDocumentLink(string documentId)
            => this.Client.GetDocumentLink(this.databaseId, this.collectionId, documentId);

        /// <summary>
        /// get all documents in query as an asynchronous operation.
        /// </summary>
        /// <typeparam name="T">The type parameter</typeparam>
        /// <param name="collectionLink">The collection link.</param>
        /// <param name="sqlQuerySpec">The SQL query spec.</param>
        /// <returns>Task&lt;IList&lt;T&gt;&gt;.</returns>
        protected async Task<IList<T>> GetAllDocumentsInQueryAsync<T>(string collectionLink, SqlQuerySpec sqlQuerySpec)
        {
            return await this.client.GetAllDocumentsInQueryAsync<T>(collectionLink, sqlQuerySpec);
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
            return await this.client.CreateDocumentIfNotExistsAsync(this.CollectionLink, document, options, disableAutomaticIdGeneration);
        }

        /// <summary>
        /// find first or default item as an asynchronous operation.
        /// </summary>
        /// <typeparam name="T">The type parameter</typeparam>
        /// <param name="sqlQuerySpec">The SQL query spec.</param>
        /// <returns>Task&lt;T&gt;.</returns>
        protected async Task<T> FindFirstOrDefaultItemAsync<T>(SqlQuerySpec sqlQuerySpec)
        {
            return await this.client.FindFirstOrDefaultItemAsync<T>(this.collectionLink, sqlQuerySpec);
        }

        /// <summary>
        /// If document exist, run update operation; if not exist, run insert operation
        /// </summary>
        /// <param name="document">document object that needs to be upserted.</param>
        /// <returns>A <see cref="Task" /> representing the asynchronous operation with the document being the task result.</returns>
        public async Task<ResourceResponse<Document>> UpsertDocumentAsync(object document)
        {
            return await this.client.UpsertDocumentAsync(this.CollectionLink, document);
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

            // for datacopdev
            string endpoint = @"https://datacopdev.documents.azure.com:443/";
            string key = secretProvider.GetSecretAsync("datacopdev", "CosmosDBAuthKey").Result;
            // for csharpmvcwebapicosmosdb
            //string endpoint = @"https://csharpmvcwebapicosmosdb.documents.azure.com:443/";
            //string key = secretProvider.GetSecretAsync("csharpmvcwebapikeyvault", "CosmosDBAuthKey").Result;

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
            IDocumentQuery<T> query = this.client.CreateDocumentQuery<T>(collectionLink, sqlQuerySpec, new FeedOptions { MaxItemCount = 1 }).AsDocumentQuery<T>();

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
    class Dataset
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        /// <value>The identifier.</value>
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        /// <value>The name.</value>
        [JsonProperty(PropertyName = "name")]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the create time.
        /// </summary>
        /// <value>The create time.</value>
        [JsonProperty(PropertyName = "createTime")]
        public DateTime CreateTime { get; set; }

        /// <summary>
        /// Gets or sets the last modified time.
        /// </summary>
        /// <value>The last modified time.</value>
        [JsonProperty(PropertyName = "lastModifiedTime")]
        public DateTime LastModifiedTime { get; set; }

        /// <summary>
        /// Gets or sets the created by.
        /// </summary>
        /// <value>The created by.</value>
        [JsonProperty(PropertyName = "createdBy")]
        public string CreatedBy { get; set; }

        /// <summary>
        /// Gets or sets the last modified by.
        /// </summary>
        /// <value>The last modified by.</value>
        [JsonProperty(PropertyName = "lastModifiedBy")]
        public string LastModifiedBy { get; set; }

        /// <summary>
        /// Gets or sets the connection information.
        /// </summary>
        /// <value>The connection information.</value>
        [JsonProperty(PropertyName = "connectionInfo")]
        public JToken ConnectionInfo { get; set; }

        // TODO - Possible enum value.

        /// <summary>
        /// Gets or sets the state.
        /// </summary>
        /// <value>The state.</value>
        [JsonProperty(PropertyName = "state")]
        public string State { get; set; }

        /// <summary>
        /// Gets or sets the data fabric.
        /// </summary>
        /// <value>The data fabric.</value>
        [JsonProperty(PropertyName = "dataFabric")]
        public string DataFabric { get; set; }

        /// <summary>
        /// Gets or sets the type.
        /// </summary>
        /// <value>The type.</value>
        [JsonProperty(PropertyName = "type")]
        public string Type { get; set; }

        /// <summary>
        /// Gets or sets the category.
        /// </summary>
        /// <value>The category.</value>
        [JsonProperty(PropertyName = "category")]
        public string Category { get; set; }

        /// <summary>
        /// Gets or sets the start date.
        /// </summary>
        /// <value>The start date.</value>
        [JsonProperty(PropertyName = "startDate")]
        public string StartDate { get; set; }

        /// <summary>
        /// Gets or sets the end date.
        /// </summary>
        /// <value>The end date.</value>
        [JsonProperty(PropertyName = "endDate")]
        public string EndDate { get; set; }

        /// <summary>
        /// Gets or sets the start serial number.
        /// </summary>
        /// <value>The start serial number.</value>
        [JsonProperty(PropertyName = "startSerialNumber")]
        public int? StartSerialNumber { get; set; }

        /// <summary>
        /// Gets or sets the end serial number.
        /// </summary>
        /// <value>The end serial number.</value>
        [JsonProperty(PropertyName = "endSerialNumber")]
        public int? EndSerialNumber { get; set; }

        /// <summary>
        /// Gets or sets the rolling window.
        /// </summary>
        /// <value>The rolling window.</value>
        [JsonProperty(PropertyName = "rollingWindow")]
        public TimeSpan? RollingWindow { get; set; }

        /// <summary>
        /// Gets or sets the grain.
        /// </summary>
        /// <value>The grain.</value>
        [JsonProperty(PropertyName = "grain")]
        public string Grain { get; set; }

        /// <summary>
        /// Gets or sets the sla.
        /// </summary>
        /// <value>The sla.</value>
        [JsonProperty(PropertyName = "sla")]
        public string SLA { get; set; }

        /// <summary>
        /// Gets or sets the schema.
        /// </summary>
        /// <value>The schema.</value>
        [JsonProperty(PropertyName = "schema")]
        public string Schema { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this instance is enabled.
        /// </summary>
        /// <value><c>true</c> if this instance is enabled; otherwise, <c>false</c>.</value>
        [JsonProperty("isEnabled")]
        public bool IsEnabled { get; set; }

        /// <summary>
        /// Gets or sets the children.
        /// The list contains dataset Ids that the dataset depends on
        /// </summary>
        /// <value>The children.</value>
        /// Hashset is not directly serialisable. Since the size of the list will be small, using list is fine here.
        [JsonProperty("children")]
        public List<string> Children { get; set; }

    }

    class ActiveAlertTrend
    {
        [JsonProperty(PropertyName = "id")]
        public string Id
        {
            get; set;
        }
        [JsonProperty(PropertyName = "timeStamp")]
        public DateTime TimeStamp
        {
            get; set;
        }
        [JsonProperty(PropertyName = "activeAlertCount")]
        public long ActiveAlertCount
        {
            get; set;
        }
    }
    class DataCopScore
    {
        /// <summary>
        /// Gets the DataCopScore Id.
        /// The Id should be a combination of DataCopScore Name and ScoreTime to make sure for each workload/metric at one ScoreTime, just has one score
        /// </summary>
        /// <value>The identifier.</value>
        [JsonProperty(PropertyName = "id")]
        public string Id
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the DataCopScore name.
        /// The name should be workload or metric name
        /// </summary>
        /// <value>The name.</value>
        [JsonProperty("name")]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets Id of the dataset against which the score is computed.
        /// </summary>
        /// <value>The dataset identifier.</value>
        [JsonProperty("datasetId")]
        public string DatasetId { get; set; }

        /// <summary>
        /// Gets or sets category of the dataset. Persisting this to optimize for
        /// query performance and to avoid join while returning back
        /// in REST APIs.
        /// </summary>
        /// <value>The dataset category.</value>
        [JsonProperty("datasetCategory")]
        public string DatasetCategory { get; set; }

        /// <summary>
        /// Gets or sets the scoreTime - the time when the DQ score is computed.
        /// </summary>
        /// <value>The score time.</value>
        [JsonProperty(PropertyName = "scoreTime")]
        public DateTime ScoreTime { get; set; }

        /// <summary>
        /// Gets or sets the score.
        /// </summary>
        /// <value>The score.</value>
        [JsonProperty(PropertyName = "score")]
        public double? Score { get; set; }

        /// <summary>
        /// Gets or sets the availability score.
        /// </summary>
        /// <value>The availability score.</value>
        [JsonProperty(PropertyName = "availabilityScore")]
        public double? AvailabilityScore { get; set; }

        /// <summary>
        /// Gets or sets the latency score.
        /// </summary>
        /// <value>The latency score.</value>
        [JsonProperty(PropertyName = "latencyScore")]
        public double? LatencyScore { get; set; }

        /// <summary>
        /// Gets or sets the completeness score.
        /// </summary>
        /// <value>The completeness score.</value>
        [JsonProperty(PropertyName = "completenessScore")]
        public double? CompletenessScore { get; set; }

        /// <summary>
        /// Gets or sets the correctness score.
        /// </summary>
        /// <value>The correctness score.</value>
        [JsonProperty(PropertyName = "correctnessScore")]
        public double? CorrectnessScore { get; set; }

        /// <summary>
        /// Gets or sets the test run ids.
        /// </summary>
        /// <value>The test run ids.</value>
        [JsonProperty(PropertyName = "testRunIds")]
        public List<string> TestRunIds { get; set; } = new List<string>();
    }
    public class TestRunAlert
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TestRunAlert" /> class.
        /// Default ctor for Deserialize
        /// </summary>
        public TestRunAlert()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TestRunAlert"/> class.
        /// </summary>
        /// <param name="testRun">The test run.</param>

        /// <summary>
        /// Gets or sets the id of test run alert feed
        /// Should be equal to the Guid of the test run, as one test run can only have one test run alert feed/type
        /// </summary>
        /// <value>The identifier.</value>
        [JsonProperty("id")]
        public Guid Id { get; set; }

        /// <summary>
        /// Gets or sets the alias of owning contact
        /// </summary>
        /// <value>The alias of owning contact.</value>
        [JsonProperty(PropertyName = "owningContactAlias")]
        public string OwningContactAlias { get; set; }
        [JsonProperty(PropertyName = "description")]
        public string Description { get; set; }
        /// <summary>
        /// Gets or sets the id for this test type
        /// </summary>
        /// <value>The dataset test identifier.</value>
        [JsonProperty(PropertyName = "datasetTestId")]
        public string DatasetTestId { get; set; }

        /// <summary>
        /// Gets or sets the id for the dataset of this test
        /// </summary>
        /// <value>The dataset identifier.</value>
        [JsonProperty(PropertyName = "datasetId")]
        public string DatasetId { get; set; }

        /// <summary>
        /// Gets or sets the alert setting id, equals the test id and used to get the alert settings
        /// </summary>
        /// <value>The alert setting identifier.</value>
        [JsonProperty("alertSettingId")]
        public string AlertSettingId { get; set; }

        /// <summary>
        /// Gets or sets the severity.
        /// </summary>
        /// <value>The severity.</value>
        [JsonProperty("severity")]
        public int? Severity { get; set; }

        /// <summary>
        /// Gets or sets the title.
        /// </summary>
        /// <value>The title.</value>
        [JsonProperty("title")]
        public string Title { get; set; }

        /// <summary>
        /// Gets or sets the content shown in Surface
        /// </summary>
        /// <value>The content shown in Surface.</value>
        [JsonProperty(PropertyName = "showInSurface")]
        public string ShowInSurface { get; set; }

        /// <summary>
        /// Gets or sets the timestamp.
        /// </summary>
        /// <value>The timestamp.</value>
        [JsonProperty("timestamp")]
        public DateTime? Timestamp { get; set; }

        /// <summary>
        /// Gets the timestamp ticks.
        /// </summary>
        /// <value>The timestamp ticks.</value>
        [JsonProperty("timestampTicks")]
        public long TimestampTicks
        {
            get { return this.Timestamp.HasValue ? this.Timestamp.Value.Ticks : 0; }
        }

        /// <summary>
        /// Gets or sets the issuedOnDate.
        /// </summary>
        /// <value>The issuedOnDate.</value>
        [JsonProperty("issuedOnDate")]
        public DateTime? IssuedOnDate { get; set; }

        /// <summary>
        /// Gets or sets the impactedDate.
        /// </summary>
        /// <value>The impactedDate.</value>
        [JsonProperty("impactedDate")]
        public DateTime? ImpactedDate { get; set; }

        /// <summary>
        /// Gets or sets the resolvedDate.
        /// </summary>i
        /// <value>The resolvedDate.</value>
        [JsonProperty("resolvedDate")]
        public DateTime? ResolvedDate { get; set; }

        /// <summary>
        /// Gets or sets the lastUpdateDate.
        /// </summary>
        /// <value>The lastUpdateDate.</value>
        [JsonProperty("lastUpdateDate")]
        public DateTime LastUpdateDate { get; set; }

        /// <summary>
        /// Gets or sets the lastUpdate content.
        /// </summary>
        /// <value>The lastUpdate content.</value>
        [JsonProperty("lastUpdate")]
        public string LastUpdate { get; set; }

        /// <summary>
        /// Gets or sets the last sync from IcM date.
        /// </summary>
        /// <value>The lastUpdateDate.</value>
        [JsonProperty("lastSyncDate")]
        public DateTime LastSyncDate { get; set; }

        /// <summary>
        /// Gets or sets the incident identifier.
        /// </summary>
        /// <value>The incident identifier.</value>
        [JsonProperty("incidentId")]
        public long? IncidentId { get; set; }

        /// <summary>
        /// Gets or sets the routing identifier.
        /// </summary>
        /// <value>The routing identifier.</value>
        [JsonProperty("routingId")]
        public string RoutingId { get; set; }

        /// <summary>
        /// Gets or sets the owning team identifier.
        /// </summary>
        /// <value>The owning team identifier.</value>
        [JsonProperty("owningTeamId")]
        public string OwningTeamId { get; set; }

        /// <summary>
        /// Gets or sets the environment.
        /// </summary>
        /// <value>The environment.</value>
        [JsonProperty("environment")]
        public string Environment { get; set; }

        /// <summary>
        /// Gets or sets the alertStatus.
        /// </summary>
        /// <value>The alertStatus.</value>
        [JsonProperty("alertStatus")]
        public string AlertStatus { get; set; }

        /// <summary>
        /// Gets or sets the on call playbook link.
        /// </summary>
        /// <value>The on call playbook link.</value>
        [JsonProperty("onCallPlaybookLink")]
        public string OnCallPlaybookLink { get; set; }

        /// <summary>
        /// Gets or sets the datacop portal link.
        /// </summary>
        /// <value>The datacop portal link.</value>
        [JsonProperty("dataCopPortalLink")]
        public string DataCopPortalLink { get; set; }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            // For more info, see https://www.newtonsoft.com/json/help/html/T_Newtonsoft_Json_NullValueHandling.htm
            JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore };

            return JsonConvert.SerializeObject(this, jsonSerializerSettings);
        }
    }
}
