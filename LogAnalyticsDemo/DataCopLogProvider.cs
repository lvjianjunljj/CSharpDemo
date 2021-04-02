namespace LogAnalyticsDemo
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Kusto.Cloud.Platform.Utils;
    using Microsoft.Azure.OperationalInsights;
    using Microsoft.Rest.Azure.Authentication;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public class DataCopLogProvider
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
        /// Initializes a new instance of the <see cref=BuildLogProvider" /> class.
        /// </summary>
        /// <param name="workspaceId">The workspace identifier.</param>
        /// <param name="clientId">The client identifier.</param>
        /// <param name="clientSecret">The client secret.</param>
        public DataCopLogProvider(
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
        public List<IDictionary<string, string>> GetQueryresult(string queryString)
        {
            // Get the client to do the query.
            var client = GetClient();
            var results = client.Query(query: queryString);
            return results.Results.ToList();
        }
    }
}
