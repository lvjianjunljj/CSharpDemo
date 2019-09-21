namespace AdlsDemo
{
    using System;
    using System.Net;

    using Microsoft.Azure.DataLake.Store;
    using Microsoft.Rest;
    using Microsoft.Rest.Azure.Authentication;
    using System.Threading;
    using System.Collections.Generic;
    using Newtonsoft.Json.Linq;
    using System.Linq;

    public class DataLakeClient
    {
        /// <summary>
        /// Microsoft Tenant Id for Active Directory
        /// </summary>
        const string TenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";

        private string clientId, clientKey;

        /// <summary>
        /// Initializes a new instance of the <see cref="DataLakeClient"/> class.
        /// </summary>
        /// <param name="client">The client.</param>
        public DataLakeClient(string clientId, string clientKey)
        {
            this.clientId = clientId;
            this.clientKey = clientKey;
        }

        /// <summary>
        /// Checks whether the path maps to a file and the file exists.
        /// ToDo: update the method to expose the failure reason
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns><c>true</c> if XXXX, <c>false</c> otherwise.</returns>
        public bool CheckExists(string store, string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null or empty");
            }

            try
            {
                // https://github.com/Azure/azure-data-lake-store-net/blob/a067d7c1d572c96c3b323cd7167132a19efe7235/AdlsDotNetSDK/ADLSClient.cs#L1462
                var client = CreateAdlsClient(store, clientId, clientKey);
                var entity = client.GetDirectoryEntry(path);
                if (entity.Type == DirectoryEntryType.FILE)
                {
                    // Only return true when the entity is a file
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (AdlsException e)
            {
                if (e.HttpStatus == HttpStatusCode.NotFound)
                {
                    return false;
                }

                throw;
            }
        }

        /// <summary>
        /// Gets the size of the file.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns>System.Int64.</returns>
        public long? GetFileSize(string store, string path)
        {
            try
            {
                var client = CreateAdlsClient(store, clientId, clientKey);
                return client.GetDirectoryEntry(path).Length;
            }
            catch (AdlsException adlsException)
            {
                Console.WriteLine($"Get directory entry from ADLS failed, error message:{adlsException.Message}");
                return null;
            }
        }

        public IEnumerable<JObject> EnumerateAdlsMetadataEntity(string store, string path)
        {
            var client = CreateAdlsClient(store, clientId, clientKey);
            var entities = client.EnumerateDirectory(path);

            try
            {
                Console.WriteLine(entities.Any());
            }
            catch (NullReferenceException ex)
            {
                Console.WriteLine($"NullReferenceException in function EnumerateAdlsMetadataCacheEntity, error message: {ex.Message}");
                yield break;
            }
            foreach (var entity in entities)
            {
                if (entity.Type == DirectoryEntryType.FILE)
                {
                    var fileEntity = new JObject();

                    fileEntity["FullName"] = entity.FullName;
                    fileEntity["Name"] = entity.Name;
                    fileEntity["Length"] = entity.Length;
                    fileEntity["LastModifiedTime"] = entity.LastModifiedTime;
                    fileEntity["LastAccessTime"] = entity.LastAccessTime;
                    fileEntity["ExpiryTime"] = entity.ExpiryTime;
                    yield return fileEntity;
                }
                else
                {
                    foreach (var adlsMetadataEntity in this.EnumerateAdlsMetadataEntity(store, entity.FullName))
                    {
                        yield return adlsMetadataEntity;
                    }
                }
            }
        }

        /// <summary>
        /// Creates the adls client.
        /// </summary>
        /// <param name="dataLakeStore">The data lake store.</param>
        /// <param name="clientId">The client identifier.</param>
        /// <param name="clientKey">The client key.</param>
        /// <returns>AdlsClient.</returns>
        private static AdlsClient CreateAdlsClient(string dataLakeStore, string clientId, string clientKey)
        {
            var adlsCreds = GetAdlsCreds(clientId, clientKey);
            var client = AdlsClient.CreateClient(dataLakeStore, adlsCreds);
            // TODO - Cache the ADLS client to make it performant
            // This is tracked by https://o365exchange.visualstudio.com/O365%20Core/_workitems/edit/947943.
            return client;
        }

        /// <summary>
        /// Gets the adls creds.
        /// </summary>
        /// <param name="clientId">The client identifier.</param>
        /// <param name="clientKey">The client key.</param>
        /// <returns>ServiceClientCredentials.</returns>
        private static ServiceClientCredentials GetAdlsCreds(string clientId, string clientKey)
        {
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            var serviceSettings = ActiveDirectoryServiceSettings.Azure;
            serviceSettings.TokenAudience = new Uri(@"https://datalake.azure.net/");

            var creds =
                ApplicationTokenProvider.LoginSilentAsync(
                 TenantId,
                 clientId,
                 clientKey,
                 serviceSettings).GetAwaiter().GetResult();

            return creds;
        }
    }
}
