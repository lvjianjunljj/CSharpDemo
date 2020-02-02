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
    }
}
