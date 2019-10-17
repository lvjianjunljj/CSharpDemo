using AzureLib.KeyVault;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace MetagraphDemo
{
    class Program
    {
        // The OneNote for access the metagraph data:


        static void Main(string[] args)
        {

            GetMetagraphDemodata();

            //string interfaceId = "a75b373c-0b21-428c-a44e-207a2883872c";
            //string interfaceFeature = "identifier,dataFabric";
            //GetIDEAsDataInterface1(interfaceId, interfaceFeature);
            //GetIDEAsDataInterface2(interfaceId);
            //GetIDEAsDataInterface2(interfaceId, interfaceFeature);


            //string onBoardRequestId = "40dc20a6-3769-4271-816b-9172d7d09241";
            //GetIDEAsOnBoardRequest(onBoardRequestId);


            //GetIDEAsDataInterfaceExpand(interfaceId);
            //GetIDEAsDataonBoardRequestIdExpand(onBoardRequestId);
            Console.ReadKey();
        }

        private static string metagraphRootUrl = "https://api.metagraph.officeppe.net/2.0/";

        // This is the tenant id for Microsoft
        private static string microsoftTenantId = @"";
        private static string client_id = @"";
        private static string client_secret = @"";
        private static string resource = @"https://microsoft.onmicrosoft.com/metagraphapi";

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

        private static void GetIDEAsDataInterface1(string id, string features = null)
        {
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
                        //Console.WriteLine(respJObject);
                        if (!string.IsNullOrEmpty(features))
                        {
                            foreach (var feature in features.Split(new char[] { ',' }))
                                Console.WriteLine(respJObject[feature]);

                        }
                    }
                }
            }
        }

        private static void GetIDEAsDataInterface2(string id, string features = null)
        {
            string url = $"{metagraphRootUrl}IDEAsDataInterfaces({id})";
            if (!string.IsNullOrEmpty(features))
            {
                url += $"?$select={features}";
            }

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
                        Console.WriteLine(respJObject);
                    }
                }
            }
        }


        private static void GetIDEAsOnBoardRequest(string id)
        {
            string url = $"{metagraphRootUrl}IDEAsOnboardRequests({id})";

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
                        Console.WriteLine(respJObject);
                    }
                }
            }
        }

        private static void GetIDEAsDataInterfaceExpand(string id)
        {
            string url = $"{metagraphRootUrl}IDEAsDataInterfaces({id})?$expand=OnboardedByIDEAsOnboardRequest";

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
                        //Console.WriteLine(respJObject);
                        // Sensitive to letter case, must be "onboardedByIDEAsOnboardRequest"
                        Console.WriteLine(respJObject["onboardedByIDEAsOnboardRequest"]);
                    }
                }
            }
        }

        private static void GetIDEAsDataonBoardRequestIdExpand(string id)
        {
            string url = $"{metagraphRootUrl}IDEAsOnboardRequests({id})?$expand=IDEAsDatasetOnboardedBy";

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
                        //Console.WriteLine(respJObject);

                        // "ideAsDatasetOnboardedBy" is so weird
                        Console.WriteLine(respJObject["ideAsDatasetOnboardedBy"]);
                    }
                }
            }
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
    }
}
