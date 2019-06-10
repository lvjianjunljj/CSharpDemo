using CSharpDemo.Application;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace CSharpDemo.Azure
{
    class AzureActiveDirectoryToken
    {
        public static void MainMethod()
        {
            //HttpClientDemo();
            HttpWebRequestDemo();
        }
        private static void HttpClientDemo()
        {
            Console.WriteLine($"Start HttpClient request demo");

            WebRequestHandler handler = new WebRequestHandler();
            HttpClient client = new HttpClient(handler);

            HttpRequestMessage request = new HttpRequestMessage(new HttpMethod("GET"), $"https://datacopdevfe.azurewebsites.net/datacop/api/v1/alerts");

            Dictionary<string, string> headers = GetDataCopRequestHeaders().Result;
            foreach (var header in headers)
            {
                request.Headers.Add(header.Key, header.Value);
            }
            using (HttpResponseMessage response = client.SendAsync(request).Result)
            {
                string responseString = response.Content.ReadAsStringAsync().Result;
                Console.WriteLine(responseString);
            }

            Console.WriteLine($"Finish HttpClient request demo");
        }


        private static void HttpWebRequestDemo()
        {
            Console.WriteLine($"Start HttpWebRequest request demo");

            string url = $"https://datacopdevfe.azurewebsites.net/datacop/api/v1/alerts";

            HttpWebRequest req = WebRequest.CreateHttp(url);
            req.Method = "GET";

            Dictionary<string, string> headers = GetDataCopRequestHeaders().Result;
            foreach (var header in headers)
            {
                req.Headers.Add(header.Key, header.Value);
            }

            req.ContentType = "application/json";

            using (HttpWebResponse resp = (HttpWebResponse)req.GetResponse())
            {
                using (Stream data = resp.GetResponseStream())
                {
                    if (data != null)
                    {
                        TextReader tr = new StreamReader(data);
                        string respString = tr.ReadToEnd();
                        Console.WriteLine(respString);
                    }
                }
            }

            Console.WriteLine($"Finish HttpWebRequest request demo");
        }
        private static async Task<Dictionary<string, string>> GetDataCopRequestHeaders()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string microsoftTenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";
            string clientId = "83ac8948-e5e1-4bbd-97ea-798a13dc8bc6";
            string clientSecret = secretProvider.GetSecretAsync("datacopdev", "AADDataCopClientSecret").Result;
            string resource = "83ac8948-e5e1-4bbd-97ea-798a13dc8bc6";
            var authenticationContext = new AuthenticationContext($"https://login.microsoftonline.com/{microsoftTenantId}", TokenCache.DefaultShared);

            ClientCredential clientCred = new ClientCredential(clientId, clientSecret);
            // Function AcquireTokenAsync() has multiple overloads
            var authenticationResult = await authenticationContext.AcquireTokenAsync(resource, clientCred);

            return new Dictionary<string, string>
            {
                { "Authorization", $"Bearer {authenticationResult.AccessToken}" }
            };
        }
    }
}
