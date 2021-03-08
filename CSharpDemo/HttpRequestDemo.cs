namespace CSharpDemo
{
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.IO;
    using System.Net;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Text;

    class HttpRequestDemo
    {
        public static void MainMethod()
        {
            //SendHttpWebRequest();
            SendSubmitJobRequestWithDictBodyDemo();
        }

        private static void SendSubmitJobRequestWithDictBodyDemo()
        {
            string scriptStr = File.ReadAllText(@"D:\data\company_work\M365\IDEAs\cloudscope\employees_test_script.script");

            string url = "http://localhost:2189/api/datalake/sandbox-c08/submitJob";
            HttpClient client = new HttpClient();
            var values = new Dictionary<string, string>
            {
                { "Script", scriptStr},
                {"PipelineRunId", Guid.NewGuid().ToString()},
                {"Compression", "false"},
                {"ScopeJobName", "Submit Scope Example by api(jianjlv)"},
                {"Priority", "1000"},
                {"TokenAllocation","0"},
                {"VirtualClusterPercentAllocation","0"},
                {"NebulaCommandLineArgs"," -on adl "},

            };
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            //client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", "Your Oauth token");
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, url);

            request.Content = new FormUrlEncodedContent(values);
            var response = client.SendAsync(request).Result;

            var responseString = response.Content.ReadAsStringAsync().Result;
            Console.WriteLine(responseString);
        }

        public static void SendHttpWebRequest()
        {
            string url = @"http://www.sdubbs.cn/showbbs.asp?id=54682&totable=1";
            HttpWebRequest req = WebRequest.CreateHttp(url);
            req.Method = "GET";

            req.ContentType = "application/x-www-form-urlencoded";

            //byte[] buffer = Encoding.ASCII.GetBytes(postData);

            //req.ContentLength = buffer.Length;
            //Stream reqStream = req.GetRequestStream();
            //reqStream.Write(buffer, 0, buffer.Length);
            //reqStream.Close();
            Console.WriteLine("Start...");
            using (HttpWebResponse resp = (HttpWebResponse)req.GetResponse())
            {
                Console.WriteLine(resp.StatusCode);
                using (Stream data = resp.GetResponseStream())
                {
                    if (data != null)
                    {
                        TextReader tr = new StreamReader(data);
                        string respString = tr.ReadToEnd();
                        //JObject j = JObject.Parse(respString);
                        //Console.WriteLine(j["access_token"]);
                        Console.WriteLine(respString);
                    }
                }
            }
        }

        public static void SendHttpClientRequestWithDictBody()
        {
            string url = "";
            HttpClient client = new HttpClient();
            var values = new Dictionary<string, string>
            {
                { "foo","foo"}
            };
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, url);

            request.Content = new FormUrlEncodedContent(values);
            var response = client.SendAsync(request).Result;

            var responseString = response.Content.ReadAsStringAsync().Result;
            JObject j = JObject.Parse(responseString);
            Console.WriteLine(j["access_token"]);
        }

        public static void SendHttpClientRequestWithStringBody()
        {
            string url = "";
            HttpClient client = new HttpClient();
            var postData = "foo=" + Uri.EscapeDataString("foo");
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, url);

            request.Content = new StringContent(postData,
                                    Encoding.UTF8,
                                    "application/x-www-form-urlencoded");
            var response = client.SendAsync(request).Result;

            var responseString = response.Content.ReadAsStringAsync().Result;
            JObject j = JObject.Parse(responseString);
            Console.WriteLine(j["access_token"]);
        }

        public static void SendWebClientRequest()
        {
            string url = "";
            using (var client = new WebClient())
            {
                var values = new NameValueCollection();
                values["foo"] = "foo";

                var response = client.UploadValues(url, values);

                var responseString = Encoding.Default.GetString(response);
                Console.WriteLine(responseString);
            }
        }

        public static string GetAccessTokenV1(string tenantId, string clientId, string clientSecret, string resource)
        {
            // URL "https://login.microsoftonline.com/cdc5aeea-15c5-4db6-b079-fcadd2505dc2/oauth2/v2.0/token" is for the v2 token,
            // the 'scope' is like this "api://ead06413-cb7c-408e-a533-2cdbe58bf3a6/.default" (The parameters is also differnet)
            // This url is for the v1 token, the resource is like this "api://ead06413-cb7c-408e-a533-2cdbe58bf3a6"
            string url = $"https://login.microsoftonline.com/{tenantId}/oauth2/token";
            HttpClient client = new HttpClient();
            var values = new Dictionary<string, string>
            {
                { "grant_type","client_credentials"},
                { "client_id",clientId},
                { "client_secret",clientSecret},
                { "resource",resource},
            };
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, url);

            request.Content = new FormUrlEncodedContent(values);
            var response = client.SendAsync(request).Result;

            var responseString = response.Content.ReadAsStringAsync().Result;
            JObject json = JObject.Parse(responseString);
            return json["access_token"].ToString();
        }
    }
}
