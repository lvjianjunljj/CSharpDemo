namespace AdlaDemo
{
    using Microsoft.Azure.Management.DataLake.Analytics;
    using Microsoft.Rest;
    using System;

    class Program
    {

        static void Main(string[] args)
        {
            Console.WriteLine(1234);
            DataLakeAnalyticsAccountManagementClient client = new DataLakeAnalyticsAccountManagementClient();

            string _tenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";

            string _clientId = " {clientId} ";

            string _clientSecret = " {clientSecret} ";

            string _adlaAccountName = "sandbox-c08";

            string _adlsAccountName = "sandbox-c08";

            string _accountSuffix = "azuredatalakestore.net";

            // Authenticate

            var authContext = new AuthenticationContext(string.Format("https://login.windows.net/{0}", _tenantId));

            // swap these lines to switch between certificate and secret authentication

            var clientCredentials = new ClientCredential(_clientId, _clientSecret);

            //var clientCredentials = new ClientAssertionCertificate(_clientId, Utility.GetCertificateFromStore(_clientCertificateThumbprint, "")); // *

            var authResult = authContext.AcquireTokenAsync("https://management.core.windows.net/", clientCredentials).GetAwaiter().GetResult();

            if (authResult == null)

                throw new InvalidOperationException("Failed to obtain the JWT token");

            // Get access token based upon access request.

            var tokenCred = new TokenCredentials(authResult.AccessToken);

            var internalJobClient = new DataLakeInternalAnalyticsScopeJobManagementClient(tokenCred);

            var publicJobClient = new DataLakeAnalyticsJobManagementClient(tokenCred);

            var publicStoreClient = new DataLakeStoreFileSystemManagementClient(tokenCred);

            var privateStorageClient = Microsoft.Azure.DataLake.Store.AdlsClient.CreateClient(accountFqdn: string.Format("{0}.{1}", _adlsAccountName, _accountSuffix), token: authResult.AccessToken);
        }

    }
}
