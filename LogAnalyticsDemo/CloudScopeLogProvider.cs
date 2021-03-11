namespace LogAnalyticsDemo
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using Kusto.Cloud.Platform.Utils;
    using Microsoft.Azure.OperationalInsights;
    using Microsoft.Rest.Azure.Authentication;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public class CloudScopeLogProvider
    {
        /// <summary>
        /// The domain.  Should probably make this driven by a keyvault secret, but this isn't too likely to change again and this'
        /// change is needed right away.
        /// </summary>
        private const string Domain = "prdtrs01.prod.outlook.com";
        /// <summary>
        /// The authentication endpoint
        /// </summary>
        private const string AuthEndpoint = "https://login.microsoftonline.com";
        /// <summary>
        /// The token audience
        /// </summary>
        private const string TokenAudience = "https://api.loganalytics.io/";

        /// <summary>
        /// The workspace identifier
        /// </summary>
        private readonly string workspaceId = null;
        /// <summary>
        /// The client identifier
        /// </summary>
        private readonly string clientId = null;
        /// <summary>
        /// The client secret
        /// </summary>
        private readonly string clientSecret = null;

        /// <summary>
        /// Initializes a new instance of the <see cref=CloudScopeLogProvider" /> class.
        /// </summary>
        /// <param name="workspaceId">The workspace identifier.</param>
        /// <param name="clientId">The client identifier.</param>
        /// <param name="clientSecret">The client secret.</param>
        public CloudScopeLogProvider(
            string workspaceId,
            string clientId,
            string clientSecret)
        {
            this.workspaceId = workspaceId;
            this.clientId = clientId;
            this.clientSecret = clientSecret;
        }

        /// <summary>
        /// Authenticate to the Kusto Log Analytics instance for build.
        /// </summary>
        /// <returns></returns>
        private OperationalInsightsDataClient GetClient()
        {
            // Authenticate to log analytics.
            var adSettings = new ActiveDirectoryServiceSettings
            {
                AuthenticationEndpoint = new Uri(AuthEndpoint),
                TokenAudience = new Uri(TokenAudience),
                ValidateAuthority = true
            };
            var creds = ApplicationTokenProvider.LoginSilentAsync(Domain, clientId, clientSecret, adSettings).GetAwaiter().GetResult();
            var client = new OperationalInsightsDataClient(creds)
            {
                WorkspaceId = workspaceId
            };

            return client;
        }

        /// <summary>
        /// Get the last pipeline run and its status based on buildEntityId.
        /// </summary>
        /// <param name="buildEntityId">The buildEntityId.</param>
        /// <param name="startTime">The first time to scan for WindowStart.</param>
        /// <param name="endTime">The last time to scan for WindowStart.</param>
        /// <param name="status">The status output.</param>
        /// <returns>The last entry for the log.</returns>
        public List<IDictionary<string, string>> GetTraces(string pipelineRunId)
        {
            // Get the client to do the query.
            var client = GetClient();
            // Construct and execute the query to get all triggers whose last run failed recently.
            string queryString = "traces ";
            queryString += $"| where timestamp > ago(1h) ";
            queryString += $"| where customDimensions contains \"53d48be7-bd87-4517-a3a4-25f4c5a5eb72\" ";

            var results = client.Query(query: queryString);
            return results.Results.ToList();
        }

        private const string URL =
        "https://api.applicationinsights.io/v1/apps/{0}/{1}/{2}?{3}";

        public static string GetTelemetry(string appid, string apikey,
                string queryType, string queryPath, string parameterString)
        {
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Add("x-api-key", apikey);
            var req = string.Format(URL, appid, queryType, queryPath, parameterString);
            HttpResponseMessage response = client.GetAsync(req).Result;
            if (response.IsSuccessStatusCode)
            {
                return response.Content.ReadAsStringAsync().Result;
            }
            else
            {
                return response.ReasonPhrase;
            }
        }
    }
}
