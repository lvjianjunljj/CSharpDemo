using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Web.Script.Serialization; //lib name in Assemblies: System.Web.Extensions.
using CSharpDemo.FileOperation;
using Microsoft.AzureAd.Icm.Types;
using Microsoft.AzureAd.Icm.Types.Api;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CSharpDemo.IcMTest
{
    // The doc link: https://icmdocs.azurewebsites.net/
    class QueryIncidents
    {
        public static void MainMethod()
        {
            // ignore all cert errors in this sample
            //ServicePointManager.ServerCertificateValidationCallback = (a, b, c, d) => true;
            //string jsonString = GetIncident();
            //SaveFile.FirstMethod(@"D:\data\company_work\IDEAs\IcMWork\test\incident2_test.txt", jsonString);


            //LinkRootCause();
            //CreateRootCause();
            //string rootCauseString = .GetRootCause(107063448);
            //Console.WriteLine(rootCauseString);
            //SaveFile.FirstMethod(@"D:\data\company_work\IDEAs\IcMWork\root_cause_test.txt", rootCauseString);

            //SaveFile.FirstMethod(@"D:\data\company_work\IDEAs\IcMWork\tenants_list.html", GetTenants());

            //EditIncidentCustomFieldsSimple();

            //AddDescriptionEntry();
            //string descriptionEntriesJsonString = GetDescriptionEntries();
            //SaveFile.FirstMethod(@"D:\data\company_work\IDEAs\IcMWork\test\description_entries_test.txt", descriptionEntriesJsonString);

            //Thread[] threads = new Thread[10];
            //for (int i = 0; i < 10; i++)
            //{
            //    threads[i] = new Thread(OneThread);
            //    threads[i].Start();
            //}
            //for (int i = 0; i < 10; i++)
            //{
            //    threads[i].Join();
            //}

            //EditIncidentCustomFields();
            GetIncidentTeamCustomField(110867568);
        }
        static void OneThread()
        {
            double totalTimeCost = 0;
            for (int i = 0; i < 10; i++)
            {
                DateTime beforDT = System.DateTime.Now;
                List<long> activeIncidentIdList = GetIncidentIdList(@"IDEAS\IDEAsDataCopTest");
                DateTime afterDT = System.DateTime.Now;
                TimeSpan ts = afterDT.Subtract(beforDT);
                //Console.WriteLine(activeIncidentIdList.Count);
                //Console.WriteLine("get incident list time cost: {0}ms.", ts.TotalMilliseconds);
                totalTimeCost += ts.TotalMilliseconds;
            }
            Console.WriteLine(totalTimeCost / 10);
        }

        public static void AddDescriptionEntry()
        {
            string EditIncidentDescriptionEntryContent = @"
                {
                  'NewDescriptionEntry' : { 
                    'Text' : 'add new description entry test', 
                    'RenderType' : 'Html',
                    'Cause' : 'Transferred'
                  }
                }";
            NewDescriptionEntry newDescriptionEntry = new NewDescriptionEntry
            {
                Text = "add new descriptionEntry",

            };
            Incident editIncident = new Incident
            {
                NewDescriptionEntry = newDescriptionEntry,
                Status = IncidentStatus.Active
            };

            //Dictionary<string, NewDescriptionEntry> descriptionEntryDict = new Dictionary<string, NewDescriptionEntry>();
            //descriptionEntryDict.Add("NewDescriptionEntry", newDescriptionEntry);
            //JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore };
            //string EditIncidentDescriptionEntryContent = JsonConvert.SerializeObject(descriptionEntryDict, jsonSerializerSettings).ToString();


            Console.WriteLine(EditIncidentDescriptionEntryContent);
            // Must set the certificate in current machine.
            X509Certificate2 certificate = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");

            string url = "https://icm.ad.msft.net/api/cert/incidents(108097160)";
            HttpWebRequest req = WebRequest.CreateHttp(url);
            req.ClientCertificates.Add(certificate);
            req.Method = "PATCH";

            byte[] buffer = Encoding.UTF8.GetBytes(EditIncidentDescriptionEntryContent);

            req.ContentType = "application/json";
            req.ContentLength = buffer.Length;
            Stream reqStream = req.GetRequestStream();
            reqStream.Write(buffer, 0, buffer.Length);
            reqStream.Close();

            req.GetResponse();
        }

        public static void EditIncidentCustomFieldsSimple()
        {
            string EditIncidentCustomFieldsContent = @"
                {
                    'CustomFieldGroups':[
                        {
                            'PublicId':'00000000-0000-0000-0000-000000000000',
                            'ContainerId':'54671',
                            'GroupType':'Team',
                            'CustomFields':[
                                {
                                    'Name':'DatasetId',
                                    'DisplayName':'Dataset Id',
                                    'Value':'1111',
                                    'Type':'ShortString'
                                }
                            ]
                        }
                    ]
                }";
            X509Certificate2 certificate = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");

            string url = "https://icm.ad.msft.net/api/cert/incidents(108097160)";
            HttpWebRequest req = WebRequest.CreateHttp(url);
            req.ClientCertificates.Add(certificate);
            req.Method = "PATCH";

            byte[] buffer = Encoding.UTF8.GetBytes(EditIncidentCustomFieldsContent);

            req.ContentType = "application/json";
            req.ContentLength = buffer.Length;
            Stream reqStream = req.GetRequestStream();
            reqStream.Write(buffer, 0, buffer.Length);
            reqStream.Close();

            HttpWebResponse resp = (HttpWebResponse)req.GetResponse();
            Stream respStream = resp.GetResponseStream();
            StreamReader reader = new StreamReader(respStream, Encoding.UTF8);
            string responseString = reader.ReadToEnd();
            reader.Close();
            Console.WriteLine(responseString);
        }

        //class CustomFields
        //{
        //    public List<CustomFieldGroup> CustomFieldGroups { get; set; }
        //}
        //class CustomFieldGroup
        //{
        //    public string PublicId { get; set; }
        //    public string ContainerId { get; set; }
        //    public string GroupType { get; set; }
        //    public List<CustomField> CustomFields { get; set; }
        //}
        //class CustomField
        //{
        //    public string Name { get; set; }
        //    public string DisplayName { get; set; }
        //    public string Value { get; set; }
        //    public string Type { get; set; }
        //}

        public static void EditIncidentCustomFields()
        {
            string EditIncidentCustomFieldsContent = @"{'CustomFieldGroups':[{'PublicId':'00000000-0000-0000-0000-000000000000','ContainerId':'54671','GroupType':'Team','CustomFields':[{'Name':'DatasetId','DisplayName':'Dataset Id','Value':'12341234','Type':'ShortString'}]}]}";
            WebRequestHandler handler = new WebRequestHandler();
            X509Certificate2 certificate = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
            handler.ClientCertificates.Add(certificate);
            HttpClient client = new HttpClient(handler);

            //CustomField customField = new CustomField
            //{
            //    Name = "DatasetId",
            //    DisplayName = "Dataset Id",
            //    Value = "Test",
            //    Type = "ShortString"
            //};
            //CustomFieldGroup customFieldGroup = new CustomFieldGroup
            //{
            //    PublicId = "00000000-0000-0000-0000-000000000000",
            //    ContainerId = "54671",
            //    GroupType = "Team",
            //    CustomFields = new List<CustomField>()
            //};
            //customFieldGroup.CustomFields.Add(customField);
            //CustomFields customFields = new CustomFields
            //{
            //    CustomFieldGroups = new List<CustomFieldGroup>()
            //};
            //customFields.CustomFieldGroups.Add(customFieldGroup);

            //List<CustomFieldGroup> customFieldGroups = new List<CustomFieldGroup>();
            //customFieldGroups.Add(customFieldGroup);
            //string myContent = JsonConvert.SerializeObject(customFields);
            //Console.WriteLine(myContent);

            byte[] buffer = System.Text.Encoding.UTF8.GetBytes(EditIncidentCustomFieldsContent);
            ByteArrayContent content = new ByteArrayContent(buffer);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            HttpRequestMessage request = new HttpRequestMessage(new HttpMethod("PATCH"), "https://icm.ad.msft.net/api/cert/incidents(109578527)");
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
            X509Certificate2 certificate = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
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
            X509Certificate2 certificate = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
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
            X509Certificate2 certificate = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
            handler.ClientCertificates.Add(certificate);
            HttpClient client = new HttpClient(handler);

            HttpResponseMessage response = client.GetAsync($"https://icm.ad.msft.net/api/cert/incidents({id})/RootCause").Result;

            string responseString = response.Content.ReadAsStringAsync().Result;
            return responseString;
        }

        public static string GetTenants()
        {
            WebRequestHandler handler = new WebRequestHandler();
            X509Certificate2 certificate = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
            handler.ClientCertificates.Add(certificate);
            HttpClient client = new HttpClient(handler);

            HttpResponseMessage response = client.GetAsync(@"https://icm.ad.msft.net/api/user/oncall/tenants").Result;

            string responseString = response.Content.ReadAsStringAsync().Result;
            return responseString;
        }

        public static void AddChildIncident()
        {
        }

        public static string GetDescriptionEntries()
        {
            JavaScriptSerializer serializer = new JavaScriptSerializer();
            HttpWebResponse result;
            HttpWebRequest req;
            string json = null;
            string url;

            url = @"https://icm.ad.msft.net/api/cert/incidents(108097160)/DescriptionEntries?$inlinecount=allpages";

            // create the request
            req = WebRequest.CreateHttp(url);
            //req.Method
            // add in the cert we'll authenticate with
            // "87a1331eac328ec321578c10ebc8cc4c356b005f" is the CertThumbprint
            try
            {
                X509Certificate cert = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
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
                Console.WriteLine(json);
                List<DescriptionEntry> enrtyList = new List<DescriptionEntry>();

                return json;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
        }

        public static List<long> GetIncidentIdList(string owningTeamId, string incidentStatus = null)
        {
            HttpWebResponse result;
            HttpWebRequest req;
            string json = null;
            string url;

            url = $@"https://icm.ad.msft.net/api/cert/incidents?&$filter=OwningTeamId eq '{owningTeamId}'";
            List<long> incidentIdList = new List<long>();
            while (url != null)
            {
                // ACTIVE
                if (incidentStatus != null)
                {
                    url += $@" and Status eq '{incidentStatus}'";
                }
                req = WebRequest.CreateHttp(url);
                url = null;
                try
                {
                    X509Certificate cert = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
                    if (cert == null)
                    {
                        Console.WriteLine("cert is null");
                        return incidentIdList;
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
                    JObject jsonObject = JObject.Parse(json);
                    //Console.WriteLine(json);
                    List<Incident> incidentList = JsonConvert.DeserializeObject<List<Incident>>(jsonObject["value"].ToString());
                    foreach (Incident i in incidentList) incidentIdList.Add(i.Id);
                    if (jsonObject["odata.nextLink"] != null)
                    {
                        url = jsonObject["odata.nextLink"].ToString();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    return incidentIdList;
                }
            }
            //Console.WriteLine(incidentIdList.Count);
            return incidentIdList;
        }
        public static void AcknowledgeIncident(long incidentId)
        {
            string EditIncidentDescriptionEntryContent = @"
                {
                  'NewDescriptionEntry' : { 
                    'Text' : 'add new description entry test', 
                    'RenderType' : 'Html',
                    'Cause' : 'Transferred'
                  }
                }";
            HttpWebRequest req;
            string url;

            url = $"https://icm.ad.msft.net/api/cert/incidents({incidentId})/AcknowledgeIncident";
            req = WebRequest.CreateHttp(url);
            req.Method = "POST";

            try
            {
                X509Certificate cert = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
                if (cert == null)
                {
                    Console.WriteLine("cert is null");
                }
                req.ClientCertificates.Add(cert);
                byte[] buffer = Encoding.UTF8.GetBytes(EditIncidentDescriptionEntryContent);

                req.ContentType = "application/json";
                req.ContentLength = 0;
                //Stream reqStream = req.GetRequestStream();
                //reqStream.Write(buffer, 0, buffer.Length);
                //reqStream.Close();
                req.GetResponse();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        public static void MitigateIncident(long incidentId)
        {
            string MitigateIncidentContent = @"
                {
                  'MitigateParameters' : { 
                    'IsCustomerImpacting' : 'True', 
                    'IsNoise' : 'False',
                    'Mitigation' : 'just mitigate the incident this is purely for demo sake',
                    'HowFixed' : 'Fixed with Hotfix',
                    'MitigateContactAlias' : 'jianjlv'
                  }
                }";
            HttpWebRequest req;
            string url;

            url = $"https://icm.ad.msft.net/api/cert/incidents({incidentId})/MitigateIncident";
            req = WebRequest.CreateHttp(url);
            req.Method = "POST";

            try
            {
                X509Certificate cert = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
                if (cert == null)
                {
                    Console.WriteLine("cert is null");
                }

                req.ClientCertificates.Add(cert);
                byte[] buffer = Encoding.UTF8.GetBytes(MitigateIncidentContent);

                req.ContentType = "application/json";
                req.ContentLength = buffer.Length;
                Stream reqStream = req.GetRequestStream();
                reqStream.Write(buffer, 0, buffer.Length);
                reqStream.Close();
                req.GetResponse();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
        public static void ResolveIncident(long incidentId)
        {
            string MitigateIncidentContent = @"
                {
                  'ResolveParameters' : { 
                    'IsCustomerImpacting' : 'False', 
                    'IsNoise' : 'True',
                    'Description' : {
                        'Text' : 'just resolve the incident....check it out',
                        'RenderType' : 'Plaintext'
                    },
                    'ResolveContactAlias' : 'jianjlv'
                  }
                }";
            HttpWebRequest req;
            string url;

            url = $"https://icm.ad.msft.net/api/cert/incidents({incidentId})/ResolveIncident";
            req = WebRequest.CreateHttp(url);
            req.Method = "POST";

            try
            {
                X509Certificate cert = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
                if (cert == null)
                {
                    Console.WriteLine("cert is null");
                }

                req.ClientCertificates.Add(cert);
                byte[] buffer = Encoding.UTF8.GetBytes(MitigateIncidentContent);

                req.ContentType = "application/json";
                req.ContentLength = buffer.Length;
                Stream reqStream = req.GetRequestStream();
                reqStream.Write(buffer, 0, buffer.Length);
                reqStream.Close();
                req.GetResponse();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        public static string GetIncident(long incidentId)
        {
            HttpWebResponse result;
            HttpWebRequest req;
            string json = null;
            string url;

            url = $@"https://icm.ad.msft.net/api/cert/incidents({incidentId})";
            req = WebRequest.CreateHttp(url);
            try
            {
                X509Certificate cert = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
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
                Incident incident = JsonConvert.DeserializeObject<Incident>(json);
                return json;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
        }

        public static string GetIncidentTeamCustomField(long incidentId)
        {
            HttpWebResponse result;
            HttpWebRequest req;
            string json = null;
            string url;

            url = $@"https://icm.ad.msft.net/api/cert/incidents({incidentId})";
            req = WebRequest.CreateHttp(url);
            try
            {
                X509Certificate cert = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
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
                Incident incident = JsonConvert.DeserializeObject<Incident>(json);
                ICollection<CustomFieldGroup> customFieldGroups = incident.CustomFieldGroups;
                foreach (CustomFieldGroup customFieldGroup in customFieldGroups)
                {
                    Console.WriteLine($"PublicId: {customFieldGroup.PublicId}");
                    Console.WriteLine($"ContainerId: {customFieldGroup.ContainerId}");
                    Console.WriteLine($"GroupType: {customFieldGroup.GroupType}");
                    ICollection<CustomField> customFields = customFieldGroup.CustomFields;
                    foreach (CustomField customField in customFields)
                    {
                        Console.WriteLine("{0}\t{1}\t{2}\t{3}", customField.Name, customField.DisplayName, customField.Value, customField.Type);
                    }
                    Console.WriteLine();
                }

                return "";
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
        }

        public static string GetIncident()
        {
            //JavaScriptSerializer serializer = new JavaScriptSerializer();
            HttpWebResponse result;
            HttpWebRequest req;
            string json = null;
            string url;

            // build the URL we'll hit
            // "icm.ad.msft.net" is the OdataServiceBaseUri
            //url = string.Format("https://{0}/api/cert/incidents({1})", "icm.ad.msoppe.msft.net", id);
            //url = @"https://icm.ad.msft.net/api/cert/incidents?$filter=OwningTeamId eq 
            //    'IDEAS\IDEAsDataCopTest' and Id eq 108097160";
            string LastSyncTimeString = DateTime.UtcNow.AddHours(-20).ToString("s");
            url = $@"https://icm.ad.msft.net/api/cert/incidents?$filter=OwningTeamId eq 'IDEAS\IDEAsDataCopTest' and Id eq 109578527 and ModifiedDate ge datetime'{LastSyncTimeString}'";
            Console.WriteLine(url);
            //url = @"https://icm.ad.msft.net/api/cert/incidents(108097160)";
            //url = string.Format("https://{0}/api/cert/incidents({1})", "icm.ad.msft.net", id);

            //url = @"https://icm.ad.msft.net/api/cert/incidents?$filter=OwningTeamId eq 'IDEAS\IDEAsDataCopTest' and 
            //    CustomFieldGroups/any(cfg:cfg/CustomFields/any
            //    (cf:cf/Name eq 'DatasetId' and cf/Type eq 'ShortString' and cf/Value eq 'Test')
            //    ) and  Status eq 'RESOLVED'";

            // create the request
            req = WebRequest.CreateHttp(url);
            // add in the cert we'll authenticate with
            // "87a1331eac328ec321578c10ebc8cc4c356b005f" is the CertThumbprint
            try
            {
                X509Certificate cert = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
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
                //Console.WriteLine(json);
                // deserialize it into an incident object
                //Incident incident = serializer.Deserialize<Incident>(json);

                //JObject jObject = JObject.Parse(json);
                //List<Incident> incidentList = new List<Incident>();
                //foreach (JToken jToken in jObject.GetValue("value"))
                //{
                //    incidentList.Add(serializer.Deserialize<Incident>(jToken.ToString()));
                //}


                //// TODO: do something with the incident
                //foreach (Incident inci in incidentList)
                //{
                //    Console.WriteLine(inci.IncidentLocation.ServiceInstanceId);
                //}
                JObject jsonObject = JObject.Parse(json);
                List<Incident> i = JsonConvert.DeserializeObject<List<Incident>>(jsonObject["value"].ToString());
                Console.WriteLine(i.Count);
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

            return GetCert(certId, StoreLocation.CurrentUser) ??
                   GetCert(certId, StoreLocation.LocalMachine);
        }
    }
}

