namespace LogAnalyticsDemo
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using Kusto.Cloud.Platform.Utils;
    using Microsoft.Azure.ApplicationInsights;
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
        private string Domain = "prdtrs01.onmicrosoft.com";
        /// <summary>
        /// The authentication endpoint
        /// </summary>
        private const string AuthEndpoint = "https://login.microsoftonline.com";
        /// <summary>
        /// The token audience
        /// </summary>
        private const string TokenAudience = "https://api.loganalytics.io/";

        /// <summary>
        /// The application identifier
        /// </summary>
        private readonly string applicationId = null;
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
            string applicationId,
            string clientId,
            string clientSecret)
        {
            this.applicationId = applicationId;
            this.clientId = clientId;
            this.clientSecret = clientSecret;
        }

        private ApplicationInsightsDataClient GetClient()
        {
            // Authenticate to log analytics.
            var adSettings = new ActiveDirectoryServiceSettings
            {
                AuthenticationEndpoint = new Uri(AuthEndpoint),
                TokenAudience = new Uri(TokenAudience),
                ValidateAuthority = true
            };
            var creds = ApplicationTokenProvider.LoginSilentAsync(Domain, clientId, clientSecret, adSettings).Result;
            return new ApplicationInsightsDataClient(creds)
            {
                AppId = applicationId
            };
        }

        public IList<IDictionary<string, string>> GetTraces()
        {
            // Get the client to do the query.
            var client = GetClient();
            // Construct and execute the query to get all triggers whose last run failed recently.
            string queryString = "traces ";
            queryString += $"| where timestamp > ago(1h) ";
            queryString += $"| where customDimensions contains \"53d48be7-bd87-4517-a3a4-25f4c5a5eb72\" ";

            var results = client.QueryAsync(query: queryString).Result.Results.ToList();
            return results;
        }
    }
}
