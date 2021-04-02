namespace AzureLib.CosmosDB
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;

    public class AzureCosmosDBClient
    {
        public static string Endpoint { get; set; }
        public static string Key { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CosmosDBDocumentCollection"/> class.
        /// </summary>
        /// <param name="databaseId">The database identifier.</param>
        /// <param name="collectionId">The collection identifier.</param>
        public AzureCosmosDBClient(string databaseId, string collectionId, CosmosDBDocumentClientMode mode = CosmosDBDocumentClientMode.Single)
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
        private readonly CosmosDBDocumentClient Client;

        /// <summary>
        /// Gets the collection link.
        /// </summary>
        /// <value>The collection link.</value>
        private readonly string CollectionLink;

        /// <summary>
        /// Gets the database link.
        /// </summary>
        /// <value>The database link.</value>
        private readonly string DatabaseLink;

        /// <summary>
        /// Gets the document collection.
        /// </summary>
        /// <value>The document collection.</value>
        private readonly DocumentCollection DocumentCollection;

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
        public async Task<HttpStatusCode> DeleteDocumentAsync(string documentLink, string partitionKey = null)
        {
            RequestOptions requestOptions = string.IsNullOrEmpty(partitionKey) ? null : new RequestOptions { PartitionKey = new PartitionKey(partitionKey) };
            var response = await this.Client.DeleteDocumentAsync(documentLink, requestOptions);
            return response.StatusCode;
        }

        public async Task<HttpStatusCode> DeleteDocumentAsync(string databaseId, string collectionId, string documentId, string partitionKey = null)
        {
            string documentLink = UriFactory.CreateDocumentUri(databaseId, collectionId, documentId).ToString();
            RequestOptions requestOptions = string.IsNullOrEmpty(partitionKey) ? null : new RequestOptions { PartitionKey = new PartitionKey(partitionKey) };
            var response = await this.Client.DeleteDocumentAsync(documentLink, requestOptions);
            return response.StatusCode;
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
        public async Task<IList<T>> GetAllDocumentsInQueryAsync<T>(string sqlQuery)
        {
            return await this.Client.GetAllDocumentsInQueryAsync<T>(this.CollectionLink, new SqlQuerySpec(sqlQuery));
        }

        /// <summary>
        /// create document if not exists as an asynchronous operation.
        /// </summary>
        /// <param name="document">The document.</param>
        /// <param name="options">The options.</param>
        /// <param name="disableAutomaticIdGeneration">if set to <c>true</c> [disable automatic identifier generation].</param>
        /// <returns>Task&lt;ResourceResponse&lt;Document&gt;&gt;.</returns>
        public async Task<ResourceResponse<Document>> CreateDocumentIfNotExistsAsync(object document, RequestOptions options, bool disableAutomaticIdGeneration)
        {
            return await this.Client.CreateDocumentIfNotExistsAsync(this.CollectionLink, document, options, disableAutomaticIdGeneration);
        }

        /// <summary>
        /// find first or default item as an asynchronous operation.
        /// </summary>
        /// <typeparam name="T">The type parameter</typeparam>
        /// <param name="sqlQuerySpec">The SQL query spec.</param>
        /// <returns>Task&lt;T&gt;.</returns>
        public async Task<T> FindFirstOrDefaultItemAsync<T>(string sqlQuery)
        {
            return await this.Client.FindFirstOrDefaultItemAsync<T>(this.CollectionLink, new SqlQuerySpec(sqlQuery));
        }

        /// <summary>
        /// If document exist, run update operation; if not exist, run insert operation
        /// </summary>
        /// <param name="document">document object that needs to be upserted.</param>
        /// <returns>A <see cref="Task" /> representing the asynchronous operation with the document being the task result.</returns>
        public async Task<HttpStatusCode> UpsertDocumentAsync(object document)
        {
            var response = await this.Client.UpsertDocumentAsync(this.CollectionLink, document);
            return response.StatusCode;
        }
    }

    public enum CosmosDBDocumentClientMode
    {
        Single = 0,
        NoSingle = 1
    }
}
