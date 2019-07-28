using AzureLib.KeyVault;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace CSharpDemo.Azure
{
    class AzureMonitorLog
    {
        // The doc link: https://docs.microsoft.com/en-us/azure/azure-monitor/platform/data-collector-api
        // https://docs.microsoft.com/en-us/azure/azure-monitor/log-query/log-query-overview
        // Query doc: https://docs.microsoft.com/en-us/azure/azure-monitor/log-query/get-started-portal
        // Query url: https://ms.portal.azure.com/#@microsoft.onmicrosoft.com/resource/subscriptions/6f7fbe56-fb42-4e46-b14d-dfc86acf0e0f/resourceGroups/CSharpMVCWebAPIApplicationResourceGroup/providers/Microsoft.OperationalInsights/workspaces/CSharpMVCWebAPIApplication/logs
        // It will cost a long time to create a table when you send a log request to a new table first.
        public static void MainMethod()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string workspaceId = secretProvider.GetSecretAsync("csharpmvcwebapikeyvault", "LogAnalyticsWorkspaceId").Result;
            string primaryKey = secretProvider.GetSecretAsync("csharpmvcwebapikeyvault", "LogAnalyticsPrimaryKey").Result;
            AzureMonitorLog azureMonitorLog = new AzureMonitorLog(workspaceId, primaryKey);

            // tagId is for separating different log locations
            string tagId = "9491c703-f581-4635-98b3-c2781ae59de2";
            string message = "This is a test error log";
            azureMonitorLog.LogErrorAsync(tagId, message).Wait();
        }

        /// Update customerId to your Log Analytics workspace ID
        private readonly string customerId;

        /// For sharedKey, use either the primary or the secondary Connected Sources client authentication key
        private readonly string sharedKey;

        private readonly string traceTableName;

        private const string dataCopDefaultTraceTableName = "csharpmvcwebapiapplication";

        public AzureMonitorLog(string customerId, string sharedKey) : this(customerId, sharedKey, dataCopDefaultTraceTableName)
        {
        }

        public AzureMonitorLog(string customerId, string sharedKey, string traceTableName)
        {
            this.customerId = customerId;
            this.sharedKey = sharedKey;
            this.traceTableName = traceTableName;
        }

        /// <summary>
        /// Log the custormize schema entity to Azure Log Analytics with table name, which can't be traceTable
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">The custormize schema entity.</param>
        /// <param name="customTableName">The customize tableName.</param>
        public async Task<HttpResponseMessage> LogEntityAsync<T>(T entity, string customTableName)
        {
            if (customTableName == this.traceTableName || customTableName == dataCopDefaultTraceTableName)
            {
                throw new InvalidOperationException($"Data write into the traceTable should only use LogInfoAsync(), LogWarningAsync() or LogErrorAsync()");
            }

            return await this.PostDataAsync(JsonConvert.SerializeObject(entity), customTableName);
        }

        /// <summary>
        /// Log trace info to traceTable.
        /// </summary>
        /// <param name="tagId">The identify of the logger.</param>
        /// <param name="message">The message.</param>
        public async Task<HttpResponseMessage> LogInfoAsync(string tagId, string message)
        {
            return await this.SendDataCopTraceAsync(tagId, message, TraceEventType.Information);
        }

        /// <summary>
        /// Log trace warning to traceTable.
        /// </summary>
        /// <param name="tagId">The identify of the logger.</param>
        /// <param name="message">The message.</param>
        public async Task<HttpResponseMessage> LogWarningAsync(string tagId, string message)
        {
            return await this.SendDataCopTraceAsync(tagId, message, TraceEventType.Warning);
        }

        /// <summary>
        /// Log trace error to traceTable.
        /// </summary>
        /// <param name="tagId">The identify of the logger.</param>
        /// <param name="message">The message.</param>
        public async Task<HttpResponseMessage> LogErrorAsync(string tagId, string message)
        {
            return await this.SendDataCopTraceAsync(tagId, message, TraceEventType.Error);
        }

        /// <summary>
        /// Integrate the message into DataCopTraceEntity and send to Log Analytics
        /// </summary>
        /// <param name="tagId">The identify of the logger.</param>
        /// <param name="message">The message.</param>
        /// <param name="severityLevel">The severitylevel of the logger</param>
        private async Task<HttpResponseMessage> SendDataCopTraceAsync(string tagId, string message, TraceEventType traceEventType)
        {
            DataCopTraceEntity traceEntity = new DataCopTraceEntity
            {
                TagId = tagId,
                Message = message,
                TraceEventType = traceEventType,
            };

            return await this.PostDataAsync(traceEntity.ToString(), this.traceTableName);
        }

        /// Send a request to the POST API endpoint
        /// Refernce document: https://docs.microsoft.com/en-us/azure/azure-monitor/platform/data-collector-api
        private async Task<HttpResponseMessage> PostDataAsync(string json, string tableName)
        {
            // Create a hash for the API signature
            string date = DateTime.UtcNow.ToString("r");
            string url = "https://" + this.customerId + ".ods.opinsights.azure.com/api/logs?api-version=2016-04-01";

            try
            {
                HttpClient client = new HttpClient();
                client.DefaultRequestHeaders.Add("Accept", "application/json");
                client.DefaultRequestHeaders.Add("Log-Type", tableName);
                client.DefaultRequestHeaders.Add("Authorization", this.BuildSignature(json, date));
                client.DefaultRequestHeaders.Add("x-ms-date", date);

                HttpContent httpContent = new StringContent(json, Encoding.UTF8);
                httpContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");

                HttpResponseMessage httpResponseMessage = await client.PostAsync(new Uri(url), httpContent);
                if (httpResponseMessage.StatusCode != HttpStatusCode.OK)
                {
                    httpResponseMessage = await client.PostAsync(new Uri(url), httpContent);
                    if (httpResponseMessage.StatusCode != HttpStatusCode.OK)
                    {
                        Console.WriteLine($"Post {json} to {tableName} failed , with status code {httpResponseMessage.StatusCode}");
                    }
                }
                Console.WriteLine(httpResponseMessage);
                return httpResponseMessage;
            }
            catch (Exception excep)
            {
                Console.WriteLine($"API Post Exception: {excep.Message}");
                HttpResponseMessage httpResponseMessage = new HttpResponseMessage()
                {
                    StatusCode = HttpStatusCode.InternalServerError,
                    Content = new StringContent($"API Post Exception: {excep.Message}")
                };
                return httpResponseMessage;
            }
        }

        /// Build the API signature
        private string BuildSignature(string json, string date)
        {
            string stringToHash = "POST\n" + Encoding.UTF8.GetBytes(json).Length + "\napplication/json\n" + "x-ms-date:" + date + "\n/api/logs";
            byte[] messageBytes = (new ASCIIEncoding()).GetBytes(stringToHash);
            using (var hmacsha256 = new HMACSHA256(Convert.FromBase64String(this.sharedKey)))
            {
                return "SharedKey " + this.customerId + ":" + Convert.ToBase64String(hmacsha256.ComputeHash(messageBytes));
            }
        }

        private class DataCopTraceEntity
        {
            public string TagId { get; set; }

            public string Message { get; set; }

            [JsonConverter(typeof(StringEnumConverter))]
            public TraceEventType TraceEventType { get; set; }

            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }
    }
}
