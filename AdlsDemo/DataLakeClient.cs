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

    public class DataLakeClient
    {
        private string tenantId, clientId, clientKey;

        /// <summary>
        /// Initializes a new instance of the <see cref="DataLakeClient"/> class.
        /// </summary>
        /// <param name="client">The client.</param>
        public DataLakeClient(string tenantId, string clientId, string clientKey)
        {
            this.tenantId = tenantId;
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
                var client = CreateAdlsClient(store, tenantId, clientId, clientKey);
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

        public bool CheckDirectoryExists(string store, string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null or empty");
            }

            try
            {
                // https://github.com/Azure/azure-data-lake-store-net/blob/a067d7c1d572c96c3b323cd7167132a19efe7235/AdlsDotNetSDK/ADLSClient.cs#L1462
                var client = CreateAdlsClient(store, tenantId, clientId, clientKey);
                var entity = client.GetDirectoryEntry(path);
                if (entity.Type == DirectoryEntryType.DIRECTORY)
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

        public bool CheckPermission(string store, string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null or empty");
            }

            try
            {
                var client = CreateAdlsClient(store, tenantId, clientId, clientKey);
                client.GetDirectoryEntry(path);
                return true;
            }
            catch (AdlsException e)
            {
                if (e.HttpStatus == HttpStatusCode.NotFound)
                {
                    return true;
                }

                if (e.HttpStatus == HttpStatusCode.Forbidden)
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
                var client = CreateAdlsClient(store, tenantId, clientId, clientKey);
                return client.GetDirectoryEntry(path).Length;
            }
            catch (AdlsException adlsException)
            {
                Console.WriteLine($"Get directory entry from ADLS failed, error message:{adlsException.Message}");
                return null;
            }
        }

        public void CreateFile(string store, string path, string content)
        {
            try
            {
                var client = CreateAdlsClient(store, tenantId, clientId, clientKey);
                var stream = client.CreateFile(path, IfExists.Overwrite);
                // CanRead is false but function GetFileSize works for this path.
                //Console.WriteLine(stream.CanRead);
                byte[] data = System.Text.Encoding.Default.GetBytes(content);
                //开始写入
                stream.Write(data, 0, data.Length);
                stream.Flush();
                stream.Close();
            }
            catch (AdlsException adlsException)
            {
                Console.WriteLine($"Create ADLS file failed, error message:{adlsException.Message}");
            }
        }

        public void DeleteFile(string store, string path)
        {
            try
            {
                var client = CreateAdlsClient(store, tenantId, clientId, clientKey);
                var stream = client.Delete(path);
            }
            catch (AdlsException adlsException)
            {
                Console.WriteLine($"Create ADLS file failed, error message:{adlsException.Message}");
            }
        }

        public IEnumerable<JObject> EnumerateAdlsMetadataEntity(string store, string path)
        {
            var client = CreateAdlsClient(store, tenantId, clientId, clientKey);
            IEnumerator<DirectoryEntry> entities;
            try
            {
                entities = client.EnumerateDirectory(path).GetEnumerator();
            }
            catch (ArgumentException ae)
            {
                Console.WriteLine(ae.Message);
                yield break;
            }

            while (true)
            {
                DirectoryEntry entity;
                try
                {
                    if (!entities.MoveNext())
                    {
                        break;
                    }
                    entity = entities.Current;
                }
                catch (AdlsException e)
                {
                    if (e.HttpStatus == HttpStatusCode.NotFound)
                    {
                        Console.WriteLine("Not Found Error!!!");
                        break;
                    }
                    throw;
                }
                var fileEntity = new JObject();
                if (entity.Type == DirectoryEntryType.FILE)
                {
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

        public IEnumerable<JObject> EnumerateAdlsMetadataEntityFor(string store, string path)
        {
            var client = CreateAdlsClient(store, tenantId, clientId, clientKey);
            var entities = client.EnumerateDirectory(path);

            foreach (var entity in client.EnumerateDirectory(path))
            {
                var fileEntity = new JObject();
                if (entity.Type == DirectoryEntryType.FILE)
                {
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
                    foreach (var adlsMetadataEntity in this.EnumerateAdlsMetadataEntityFor(store, entity.FullName))
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
        private static AdlsClient CreateAdlsClient(string dataLakeStore, string tenantId, string clientId, string clientKey)
        {
            var adlsCreds = GetAdlsCreds(tenantId, clientId, clientKey);
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
        private static ServiceClientCredentials GetAdlsCreds(string tenantId, string clientId, string clientKey)
        {
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            var serviceSettings = ActiveDirectoryServiceSettings.Azure;
            serviceSettings.TokenAudience = new Uri(@"https://datalake.azure.net/");

            var creds =
                ApplicationTokenProvider.LoginSilentAsync(
                 tenantId,
                 clientId,
                 clientKey,
                 serviceSettings).GetAwaiter().GetResult();

            return creds;
        }
    }
}
