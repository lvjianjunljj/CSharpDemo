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

namespace CSharpDemo.Azure
{
    class Test
    {
        [JsonProperty("Testa")]
        public string TestA { get; set; }
        [JsonProperty("Testb")]
        public string TestB { get; set; }
        [JsonProperty("Testc")]
        public string TestC { get; set; }
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
            AzureCosmosDB azureCosmosDB = new AzureCosmosDB("CosmosDBTest", "TestCollectionId");
            //Test t = new Test();
            //t.TestA = "AAAa";
            //t.TestB = "BBBb";
            //t.TestC = "CCCc";
            //t.Id = "1235";
            //ResourceResponse<Document> resource = azureCosmosDB.UpsertDocumentAsync(t).Result;
            //Console.WriteLine(resource.ToString());

            // Collation: asc and desc is ascending and descending
            IList<Test> list = azureCosmosDB.GetAllDocumentsInQueryAsync<Test>(azureCosmosDB.collectionLink, new SqlQuerySpec(@"SELECT * FROM c order by c.timestampTicks asc")).Result;
            foreach (Test t in list)
            {
                Console.WriteLine(t.Id);
            }

        }
        public async Task<string> GetLatestAlertAsync(string datasetTestId)
        {
            SqlQuerySpec sqlQuerySpec = new SqlQuerySpec($@"SELECT * FROM c WHERE c.datasetTestId='{datasetTestId}' 
                                                            order by c.timestampTicks desc");

            return await this.FindFirstOrDefaultItemAsync<string>(sqlQuerySpec);
        }

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
            //string keyVaultName = ConfigurationManager.AppSettings.Get("KeyVaultName");
            //ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            //string endpoint = secretProvider.GetSecretAsync(keyVaultName, "CosmosDBEndPoint").Result;
            //string key = secretProvider.GetSecretAsync(keyVaultName, "CosmosDBAuthKey").Result;
            string endpoint = @"https://csharpmvcwebapicosmosdb.documents.azure.com:443/";
            string key = @"69PbtT20qRv16aHgHzibJLu9EFPCoS6CrnGQu0qlCprrB4oLCcuyCCulKSx6oDMobxR8i7LSf75pEGQBr02BvQ==";

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
}
