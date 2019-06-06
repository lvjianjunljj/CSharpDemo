namespace CSharpDemo.IcMTest
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;

    class MultiRequestError
    {
        private const string EditIncidentDescriptionEntryContent = @"
                {
                  'NewDescriptionEntry' : {
                    'ChangedBy' : 'DataCopAlert',
                    'SubmittedBy' : 'DataCopAlert',
                    'Text' : 'Suppress the alert:<br/>impactedDate: 2/8/2019 12:00:00 AM', 
                    'RenderType' : 'Html',
                    'Cause' : 'Transferred'
                  }
                }";

        private const long IncidentId = 116781127;

        public static void MainMethod()
        {
            HttpWebRequestDemo();
        }

        // This funtion won't throw the exception in function HttpWebRequestDemo().
        // The general consensus is that you do not (should not) need to dispose of HttpClient.
        // Doc link: https://stackoverflow.com/questions/15705092/do-httpclient-and-httpclienthandler-have-to-be-disposed
        private static void HttpClientDemo()
        {
            for (int i = 0; i < 10; i++)
            {
                Console.WriteLine($"Start round: {i + 1}");

                WebRequestHandler handler = new WebRequestHandler();
                X509Certificate certificate = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
                handler.ClientCertificates.Add(certificate);
                HttpClient client = new HttpClient(handler);

                byte[] buffer = System.Text.Encoding.UTF8.GetBytes(EditIncidentDescriptionEntryContent);
                ByteArrayContent content = new ByteArrayContent(buffer);
                content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

                HttpRequestMessage request = new HttpRequestMessage(new HttpMethod("PATCH"), $"https://icm.ad.msft.net/api/cert/incidents({IncidentId})");
                request.Content = content;
                client.SendAsync(request).Wait();

                Console.WriteLine($"Finish round: {i + 1}");
            }
        }


        private static void HttpWebRequestDemo()
        {
            for (int i = 0; i < 10; i++)
            {
                Console.WriteLine($"Start round: {i + 1}");

                // Must set the certificate in current machine.
                X509Certificate2 certificate = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");

                string url = $"https://icm.ad.msft.net/api/cert/incidents({IncidentId})";

                HttpWebRequest req = WebRequest.CreateHttp(url);
                req.ClientCertificates.Add(certificate);
                req.Method = "PATCH";

                byte[] buffer = Encoding.UTF8.GetBytes(EditIncidentDescriptionEntryContent);

                req.ContentType = "application/json";
                req.ContentLength = buffer.Length;
                using (Stream reqStream = req.GetRequestStream())
                {
                    reqStream.Write(buffer, 0, buffer.Length);
                }

                // The Timeout setting is useless
                req.Timeout = 3000;
                req.AllowWriteStreamBuffering = false;

                // Will hang in here and finally throw timeout exception in GetRequestStream() after running this program several times if we dont run the dispose method.
                WebResponse response = req.GetResponse();
                //response.Dispose();

                Console.WriteLine($"Finish round: {i + 1}");
            }
        }

        private async Task<Dictionary<string, string>> GetDataCopRequestHeaders()
        {
            //var authenticationContext = new AuthenticationContext(this.defaultAadInstance, TokenCache.DefaultShared);
            string microsoftTenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";
            string clientId = "83ac8948-e5e1-4bbd-97ea-798a13dc8bc6";
            string clientSecret = "IKLBuG0PqH?ZrpNmQu1ta.3Q==3:*I+A";
            var authenticationContext = new AuthenticationContext($"https://login.microsoftonline.com/{microsoftTenantId}/authorize/", TokenCache.DefaultShared);
            //var authenticationResult = await authenticationContext.AcquireTokenAsync(
            //    this.dataCopAadClientId,
            //    this.dataCopAadClientId,
            //    new Uri("https://ideasdatacop.azure.activedirectory.com"),
            //    new PlatformParameters(PromptBehavior.RefreshSession));


            ClientCredential clientCred = new ClientCredential(clientId, clientSecret);
            var authenticationResult = await authenticationContext.AcquireTokenAsync(clientId, clientCred);
            Console.WriteLine(authenticationResult.AccessToken);

            return new Dictionary<string, string>
            {
                { "Authorization", $"Bearer {authenticationResult.AccessToken}" },
            };
        }

        public static X509Certificate2 GetCert(string certId)
        {

            if (string.IsNullOrEmpty(certId))
            {
                return null;
            }

            return GetCert(certId, StoreLocation.CurrentUser) ??
                   GetCert(certId, StoreLocation.LocalMachine);
        }
        private static X509Certificate2 GetCert(string certId, StoreLocation location)
        {
            X509Certificate2 result = null;
            X509Store certStore;
            certStore = new X509Store(StoreName.My, location);
            certStore.Open(OpenFlags.OpenExistingOnly | OpenFlags.ReadOnly);
            try
            {
                X509Certificate2Collection set = certStore.Certificates.Find(
                    X509FindType.FindByThumbprint, certId, true);
                if (set.Count > 0 && set[0] != null && set[0].HasPrivateKey)
                {
                    result = set[0];
                }
            }
            finally
            {
                certStore.Close();
            }
            return result;
        }
    }
}
