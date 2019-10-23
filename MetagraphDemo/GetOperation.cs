namespace MetagraphDemo
{
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;

    class GetOperation
    {
        public static void MainMethod()
        {
            //GetMetagraphDemodata();

            string interfaceId = "a75b373c-0b21-428c-a44e-207a2883872c";
            string interfaceFeature = "identifier,dataFabric";
            GetIDEAsDataInterface0(interfaceId, interfaceFeature);
            GetIDEAsDataInterface1(interfaceId, interfaceFeature);
            GetIDEAsDataInterface2(interfaceId);
            GetIDEAsDataInterface2(interfaceId, interfaceFeature);


            string onBoardRequestId = "40dc20a6-3769-4271-816b-9172d7d09241";
            GetIDEAsOnBoardRequest0(onBoardRequestId);


            GetIDEAsDataInterfaceExpand(interfaceId);
            GetIDEAsDataOnBoardRequestExpand(onBoardRequestId);
        }

        private static string metagraphRootUrl = Constant.METAGRAPH_ROOT_URL;

        // This is the tenant id for Microsoft
        private static string microsoftTenantId = Constant.MICROSOFT_TENANT_ID;
        private static string client_id = Constant.CLINT_ID;
        private static string client_secret = Constant.CLINT_SECRET;
        private static string resource = Constant.METAGRAPH_RESOURCE;


        // Use a old way to send http request.
        private static void GetMetagraphDemodata()
        {
            string url = $"{metagraphRootUrl}Applications?$top=25&skip=0&$count=true&$orderby=Name asc";
            url = $"{metagraphRootUrl}IDEAsDataInterfaces";
            url = $"{metagraphRootUrl}IDEAsOnboardRequests";

            HttpWebRequest req = WebRequest.CreateHttp(url);
            req.Method = "GET";

            Dictionary<string, string> headers = GetRequestHeaders().Result;
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
                        JObject respJObject = JObject.Parse(respString);

                        JArray array = (JArray)respJObject["value"];
                        foreach (JToken jTokenChildContent in respJObject["value"])
                        {
                            Console.WriteLine(jTokenChildContent["identifier"]);
                        }
                        Console.WriteLine(array.Count);
                    }
                }
            }
        }

        // Same as function GetIDEAsDataInterface1(), just use different way to send http request.
        private static void GetIDEAsDataInterface0(string id, string features = null)
        {
            Console.WriteLine($"Get IDEAsDataInterface {id}");
            string url = $"{metagraphRootUrl}IDEAsDataInterfaces({id})";

            HttpWebRequest req = WebRequest.CreateHttp(url);
            req.Method = "GET";

            Dictionary<string, string> headers = GetRequestHeaders().Result;
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
                        JObject respJObject = JObject.Parse(respString);
                        if (!string.IsNullOrEmpty(features))
                        {
                            foreach (var feature in features.Split(new char[] { ',' }))
                                Console.WriteLine(respJObject[feature]);
                        }
                        else
                        {
                            Console.WriteLine(respJObject);
                        }
                    }
                }
            }
        }

        private static void GetIDEAsDataInterfaceByName(string name)
        {
            Console.WriteLine($"Get IDEAsDataInterface by name: {name}");
            string url = $"{metagraphRootUrl}IDEAsDataInterfaces(Name={name})";

            string respString = SendRequestString(url, "GET", null).Result;

            if (string.IsNullOrEmpty(respString))
            {
                return;
            }
            JObject respJObject = JObject.Parse(respString);
            Console.WriteLine(respJObject);
        }

        private static void GetIDEAsDataInterface1(string id, string features = null)
        {
            Console.WriteLine($"Get IDEAsDataInterface {id}");
            string url = $"{metagraphRootUrl}IDEAsDataInterfaces({id})";

            string respString = SendRequestString(url, "GET", null).Result;

            if (string.IsNullOrEmpty(respString))
            {
                return;
            }

            JObject respJObject = JObject.Parse(respString);
            if (!string.IsNullOrEmpty(features))
            {
                foreach (var feature in features.Split(new char[] { ',' }))
                    Console.WriteLine(respJObject[feature]);
            }
            else
            {
                Console.WriteLine(respJObject);
            }
        }

        private static void GetIDEAsDataInterface2(string id, string features = null)
        {
            Console.WriteLine($"Get IDEAsDataInterface {id}");
            string url = $"{metagraphRootUrl}IDEAsDataInterfaces({id})";
            if (!string.IsNullOrEmpty(features))
            {
                url += $"?$select={features}";
            }

            string respString = SendRequestString(url, "GET", null).Result;
            if (!string.IsNullOrEmpty(respString))
            {
                JObject respJObject = JObject.Parse(respString);
                Console.WriteLine(respJObject);
            }

        }

        private static void GetIDEAsOnBoardRequestByName(string name)
        {
            Console.WriteLine($"Get IDEAsDataInterface by name: {name}");
            string url = $"{metagraphRootUrl}IDEAsOnboardRequests(Name={name})";

            string respString = SendRequestString(url, "GET", null).Result;

            if (string.IsNullOrEmpty(respString))
            {
                return;
            }
            JObject respJObject = JObject.Parse(respString);
            Console.WriteLine(respJObject);
        }

        private static void GetIDEAsOnBoardRequest0(string id)
        {
            Console.WriteLine($"Get IDEAsOnBoardRequest {id}");
            string url = $"{metagraphRootUrl}IDEAsOnboardRequests({id})";

            string respString = SendRequestString(url, "GET", null).Result;
            if (!string.IsNullOrEmpty(respString))
            {
                JObject respJObject = JObject.Parse(respString);
                Console.WriteLine(respJObject);
            }
        }

        private static void GetIDEAsDataInterfaceExpand(string id)
        {
            Console.WriteLine($"Get IDEAsDataInterfaceExpand {id}");
            string url = $"{metagraphRootUrl}IDEAsDataInterfaces({id})?$expand=OnboardedByIDEAsOnboardRequest";

            string respString = SendRequestString(url, "GET", null).Result;
            JObject respJObject = JObject.Parse(respString);
            //Console.WriteLine(respJObject);
            // Sensitive to letter case, must be "onboardedByIDEAsOnboardRequest"
            Console.WriteLine(respJObject["onboardedByIDEAsOnboardRequest"]);
        }

        private static void GetIDEAsDataOnBoardRequestExpand(string id)
        {
            Console.WriteLine($"Get IDEAsDataonBoardRequestExpand {id}");
            string url = $"{metagraphRootUrl}IDEAsOnboardRequests({id})?$expand=IDEAsDatasetOnboardedBy";

            string respString = SendRequestString(url, "GET", null).Result;
            JObject respJObject = JObject.Parse(respString);
            //Console.WriteLine(respJObject);

            // "ideAsDatasetOnboardedBy" is so weird
            Console.WriteLine(respJObject["ideAsDatasetOnboardedBy"]);
        }

        private static async Task<Dictionary<string, string>> GetRequestHeaders()
        {
            var authenticationContext = new AuthenticationContext($"https://login.microsoftonline.com/{microsoftTenantId}", TokenCache.DefaultShared);

            ClientCredential clientCred = new ClientCredential(client_id, client_secret);
            // Function AcquireTokenAsync() has multiple overloads
            var authenticationResult = await authenticationContext.AcquireTokenAsync(resource, clientCred);

            return new Dictionary<string, string>
            {
                { "Authorization", $"Bearer {authenticationResult.AccessToken}" }
            };
        }

        private static async Task<string> SendRequestString(string url, string requestMethod, string reqeustContent = null)
        {
            HttpClient httpClient = new HttpClient();
            HttpRequestMessage request = new HttpRequestMessage(new HttpMethod(requestMethod), url);

            if (!string.IsNullOrEmpty(reqeustContent))
            {
                request.Content = new StringContent(reqeustContent,
                                   Encoding.UTF8,
                                   "application/json");
            }

            var token = GetToken();
            request.Headers.Add("Authorization", $"Bearer {token}");
            using (HttpResponseMessage response = httpClient.SendAsync(request).Result)
            {
                string respString = await response.Content.ReadAsStringAsync();
                return respString;
            }
        }

        private static string GetToken()
        {
            var authenticationContext = new AuthenticationContext($"https://login.microsoftonline.com/{microsoftTenantId}", TokenCache.DefaultShared);

            ClientCredential clientCred = new ClientCredential(client_id, client_secret);
            // Function AcquireTokenAsync() has multiple overloads
            var authenticationResult = authenticationContext.AcquireTokenAsync(resource, clientCred).Result;
            return authenticationResult.AccessToken;
        }


        // Pulic method to be used in other class(Put, Post and so no)
        public static JObject GetIDEAsDataInterface(string id)
        {
            string url = $"{metagraphRootUrl}IDEAsDataInterfaces({id})";

            string respString = SendRequestString(url, "GET", null).Result;

            if (string.IsNullOrEmpty(respString))
            {
                return new JObject();
            }

            return JObject.Parse(respString);
        }

        public static JObject GetIDEAsOnBoardRequest(string id)
        {
            Console.WriteLine($"Get IDEAsOnBoardRequest {id}");
            string url = $"{metagraphRootUrl}IDEAsOnboardRequests({id})";

            string respString = SendRequestString(url, "GET", null).Result;
            if (string.IsNullOrEmpty(respString))
            {
                return new JObject();
            }

            return JObject.Parse(respString);
        }
    }
}