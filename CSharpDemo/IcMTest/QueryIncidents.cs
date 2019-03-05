using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography.X509Certificates;
using System.Web.Script.Serialization; //lib name in Assemblies: System.Web.Extensions.

using Microsoft.AzureAd.Icm.Types;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CSharpDemo.IcMTest
{
    class QueryIncidents
    {
        public static void MainMethod()
        {
            // ignore all cert errors in this sample
            ServicePointManager.ServerCertificateValidationCallback = (a, b, c, d) => true;
            GetIncident(7252196);
        }

        class CustomFields
        {
            public List<CustomFieldGroup> CustomFieldGroups { get; set; }
        }
        class CustomFieldGroup
        {
            public string PublicId { get; set; }
            public string ContainerId { get; set; }
            public string GroupType { get; set; }
            public List<CustomField> CustomFields { get; set; }
        }
        class CustomField
        {
            public string Name { get; set; }
            public string DisplayName { get; set; }
            public string Value { get; set; }
            public string Type { get; set; }
        }

        public static void EditIncidentCustomFields()
        {
            WebRequestHandler handler = new WebRequestHandler();
            X509Certificate2 certificate = QueryIncidents.GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
            handler.ClientCertificates.Add(certificate);
            HttpClient client = new HttpClient(handler);

            CustomField customField = new CustomField
            {
                Name = "DatasetId",
                DisplayName = "Dataset Id",
                Value = "Test",
                Type = "ShortString"
            };
            CustomFieldGroup customFieldGroup = new CustomFieldGroup
            {
                PublicId = "00000000-0000-0000-0000-000000000000",
                ContainerId = "54671",
                GroupType = "Team",
                CustomFields = new List<CustomField>()
            };
            customFieldGroup.CustomFields.Add(customField);
            CustomFields customFields = new CustomFields
            {
                CustomFieldGroups = new List<CustomFieldGroup>()
            };
            customFields.CustomFieldGroups.Add(customFieldGroup);

            string myContent = JsonConvert.SerializeObject(customFields);
            Console.WriteLine(myContent);
            byte[] buffer = System.Text.Encoding.UTF8.GetBytes(myContent);
            ByteArrayContent content = new ByteArrayContent(buffer);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            HttpRequestMessage request = new HttpRequestMessage(new HttpMethod("PATCH"), "https://icm.ad.msft.net/api/cert/incidents(107215017)");
            request.Content = content;
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            HttpResponseMessage response = client.SendAsync(request).Result;

            string responseString = response.Content.ReadAsStringAsync().Result;
            Console.WriteLine(responseString);
        }

        class Test
        {
            public string Category { get; set; }
            public string Description { get; set; }
            public string Title { get; set; }
        }
        public static void CreateRootCause()
        {
            WebRequestHandler handler = new WebRequestHandler();
            X509Certificate2 certificate = QueryIncidents.GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
            handler.ClientCertificates.Add(certificate);
            HttpClient client = new HttpClient(handler);
            Dictionary<string, string> values = new Dictionary<string, string>
                {
                   { "Category", "Other" },
                   { "Description", "description" },
                   { "Title", "title" }
                };

            //FormUrlEncodedContent content = new FormUrlEncodedContent(values);

            Test data = new Test();
            data.Category = "Other";
            data.Description = "description";
            data.Title = "Title";
            string myContent = "{ \"Category\": \"Other\" , \"Description\": \"description\" , \"Title\": \"title\" }";
            //string myContent = JsonConvert.SerializeObject(data);
            Console.WriteLine(myContent);
            byte[] buffer = System.Text.Encoding.UTF8.GetBytes(myContent);
            ByteArrayContent content = new ByteArrayContent(buffer);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");


            HttpResponseMessage response = client.PostAsync("https://icm.ad.msft.net/api/cert/incidents(107246528)/RootCause", content).Result;

            string responseString = response.Content.ReadAsStringAsync().Result;
            Console.WriteLine(responseString);
        }

        // It does not work
        public static void LinkRootCause()
        {
            WebRequestHandler handler = new WebRequestHandler();
            X509Certificate2 certificate = QueryIncidents.GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
            handler.ClientCertificates.Add(certificate);
            HttpClient client = new HttpClient(handler);
            Dictionary<string, string> values = new Dictionary<string, string>
                {
                   { "url", "https://icm.ad.msft.net/api/cert/RootCause(1434604)" }
                };

            FormUrlEncodedContent content = new FormUrlEncodedContent(values);

            HttpResponseMessage response = client.PostAsync(@"https://icm.ad.msft.net/api/cert/incidents(107065717)/$links/RootCause", content).Result;

            string responseString = response.Content.ReadAsStringAsync().Result;
            Console.WriteLine(responseString);
        }

        public static string GetRootCause(long id)
        {
            WebRequestHandler handler = new WebRequestHandler();
            X509Certificate2 certificate = QueryIncidents.GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
            handler.ClientCertificates.Add(certificate);
            HttpClient client = new HttpClient(handler);

            HttpResponseMessage response = client.GetAsync($"https://icm.ad.msft.net/api/cert/incidents({id})/RootCause").Result;

            string responseString = response.Content.ReadAsStringAsync().Result;
            return responseString;
        }

        public static string GetTenants()
        {
            WebRequestHandler handler = new WebRequestHandler();
            X509Certificate2 certificate = QueryIncidents.GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
            handler.ClientCertificates.Add(certificate);
            HttpClient client = new HttpClient(handler);

            HttpResponseMessage response = client.GetAsync(@"https://icm.ad.msft.net/api/user/oncall/tenants").Result;

            string responseString = response.Content.ReadAsStringAsync().Result;
            return responseString;
        }

        public static void AddChildIncident()
        {

        }

        public static string GetIncident(long id)
        {

            JavaScriptSerializer serializer = new JavaScriptSerializer();
            HttpWebResponse result;
            HttpWebRequest req;
            string json = null;
            string url;

            // build the URL we'll hit
            // "icm.ad.msft.net" is the OdataServiceBaseUri
            //url = string.Format("https://{0}/api/cert/incidents({1})", "icm.ad.msoppe.msft.net", id);
            //url = @"https://icm.ad.msft.net/api/cert/incidents?$filter=OwningTeamId eq 'IDEAS\IDEAsDataCopTest' and Id eq 106845925";
            //url = @"https://icm.ad.msft.net/api/cert/incidents?$filter=Id eq 106843121";
            //url = string.Format("https://{0}/api/cert/incidents({1})", "icm.ad.msft.net", id);

            url = @"https://icm.ad.msft.net/api/cert/incidents?$filter=OwningTeamId eq 'IDEAS\IDEAsDataCopTest' and 
                CustomFieldGroups/any(cfg:cfg/CustomFields/any
                (cf:cf/Name eq 'DatasetId' and cf/Type eq 'ShortString' and cf/Value eq 'Test')
                ) and  Status eq 'ACTIVE'";

            // create the request
            req = WebRequest.CreateHttp(url);
            // add in the cert we'll authenticate with
            // "87a1331eac328ec321578c10ebc8cc4c356b005f" is the CertThumbprint
            try
            {
                X509Certificate cert = QueryIncidents.GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
                if (cert == null)
                {
                    Console.WriteLine("cert is null");
                    return "cert is null";
                }
                req.ClientCertificates.Add(cert);
                // submit the request
                result = (HttpWebResponse)req.GetResponse();

                // read out the response stream as text
                using (Stream data = result.GetResponseStream())
                {
                    if (data != null)
                    {
                        TextReader tr = new StreamReader(data);
                        json = tr.ReadToEnd();
                    }
                }

                // deserialize it into an incident object
                //Incident incident = serializer.Deserialize<Incident>(json);

                JObject jObject = JObject.Parse(json);
                List<Incident> incidentList = new List<Incident>();
                foreach (JToken jToken in jObject.GetValue("value"))
                {
                    incidentList.Add(serializer.Deserialize<Incident>(jToken.ToString()));
                }


                // TODO: do something with the incident
                foreach (Incident inci in incidentList)
                {
                    Console.WriteLine(inci.IncidentLocation.ServiceInstanceId);
                }

                return json;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
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

        /// <summary>Gets the cert</summary>
        /// <param name="certId">cert identifier</param>
        /// <returns>resulting value</returns>
        private static X509Certificate2 GetCert(string certId)
        {

            if (string.IsNullOrEmpty(certId))
            {
                return null;
            }

            return QueryIncidents.GetCert(certId, StoreLocation.CurrentUser) ??
                   QueryIncidents.GetCert(certId, StoreLocation.LocalMachine);
        }
    }
}

