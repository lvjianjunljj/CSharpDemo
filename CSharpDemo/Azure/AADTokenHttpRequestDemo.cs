namespace CSharpDemo.Azure
{
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.IO;
    using System.Net;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Text;

    // Doc link: https://stackoverflow.com/questions/4015324/how-to-make-http-post-web-request
    // OneNote link: https://microsoftapc-my.sharepoint.com/personal/jianjlv_microsoft_com/_layouts/OneNote.aspx?id=%2Fpersonal%2Fjianjlv_microsoft_com%2FDocuments%2FJianjun%20%40%20Microsoft&wd=target%28Develop%20Lang.one%7C5B97F099-2C06-420B-BE08-9DDC5BC0F237%2FWebClient%20vs%20HttpClient%20vs%20HttpWebRequest%7CF9FAC99F-2F78-4A46-B12E-A797E52FBA9C%2F%29
    // Here we use getting token from AAD as sample.
    class AADTokenHttpRequestDemo
    {
        public static void MainMethod()
        {
            //GetTokenByAADLib();

            //GetTokenByHttpWebRequest();

            //GetTokenByHttpClientKeyValueBody();
            //GetTokenByHttpClientStringBody();

            //GetTokenByWebClientKeyValueBody();
        }

        // We don't need this property when we get token by AAD lib.
        private static string grant_type = @"client_credentials";

        // This is the tenant id for Microsoft
        private static string microsoftTenantId = @"";
        private static string client_id = @"";
        private static string client_secret = @"";
        private static string resource = @"";
        private static string tokenUrl = $"https://login.microsoftonline.com/{microsoftTenantId}/oauth2/token";

        public static void GetTokenByAADLib()
        {
            var authenticationContext = new AuthenticationContext($"https://login.microsoftonline.com/{microsoftTenantId}", TokenCache.DefaultShared);

            ClientCredential clientCred = new ClientCredential(client_id, client_secret);
            // Function AcquireTokenAsync() has multiple overloads
            var authenticationResult = authenticationContext.AcquireTokenAsync(resource, clientCred).Result;

            Console.WriteLine(authenticationResult.AccessToken);
        }


        public static void GetTokenByHttpWebRequest()
        {
            var postData = "grant_type=" + Uri.EscapeDataString(grant_type);
            postData += "&client_id=" + Uri.EscapeDataString(client_id);
            postData += "&client_secret=" + Uri.EscapeDataString(client_secret);
            postData += "&resource=" + Uri.EscapeDataString(resource);

            HttpWebRequest req = WebRequest.CreateHttp(tokenUrl);
            req.Method = "POST";

            req.ContentType = "application/x-www-form-urlencoded";

            byte[] buffer = Encoding.ASCII.GetBytes(postData);

            req.ContentLength = buffer.Length;
            Stream reqStream = req.GetRequestStream();
            reqStream.Write(buffer, 0, buffer.Length);
            reqStream.Close();
            using (HttpWebResponse resp = (HttpWebResponse)req.GetResponse())
            {
                using (Stream data = resp.GetResponseStream())
                {
                    if (data != null)
                    {
                        TextReader tr = new StreamReader(data);
                        string respString = tr.ReadToEnd();
                        JObject j = JObject.Parse(respString);
                        Console.WriteLine(j["access_token"]);
                    }
                }
            }
        }

        public static void GetTokenByHttpClientKeyValueBody()
        {
            HttpClient client = new HttpClient();
            var values = new Dictionary<string, string>
            {
                { "grant_type", grant_type },
                { "client_id", client_id},
                { "client_secret", client_secret },
                { "resource", resource }
            };
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, tokenUrl);

            request.Content = new FormUrlEncodedContent(values);
            var response = client.SendAsync(request).Result;

            var responseString = response.Content.ReadAsStringAsync().Result;
            JObject j = JObject.Parse(responseString);
            Console.WriteLine(j["access_token"]);
        }

        public static void GetTokenByHttpClientStringBody()
        {
            HttpClient client = new HttpClient();
            var postData = "grant_type=" + Uri.EscapeDataString(grant_type);
            postData += "&client_id=" + Uri.EscapeDataString(client_id);
            postData += "&client_secret=" + Uri.EscapeDataString(client_secret);
            postData += "&resource=" + Uri.EscapeDataString(resource);
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, tokenUrl);

            request.Content = new StringContent(postData,
                                    Encoding.UTF8,
                                    "application/x-www-form-urlencoded");
            var response = client.SendAsync(request).Result;

            var responseString = response.Content.ReadAsStringAsync().Result;
            JObject j = JObject.Parse(responseString);
            Console.WriteLine(j["access_token"]);
        }

        public static void GetTokenByWebClientKeyValueBody()
        {
            using (var client = new WebClient())
            {
                var values = new NameValueCollection();
                values["grant_type"] = grant_type;
                values["client_id"] = client_id;
                values["client_secret"] = client_secret;
                values["resource"] = resource;

                var response = client.UploadValues(tokenUrl, values);

                var responseString = Encoding.Default.GetString(response);
                Console.WriteLine(responseString);
            }
        }
    }
}
