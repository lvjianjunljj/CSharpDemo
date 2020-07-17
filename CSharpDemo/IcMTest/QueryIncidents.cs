using System;
using System.Collections.Generic;
using System.IO;
namespace CSharpDemo.IcMTest
{
    using System.Net;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Web.Script.Serialization; //lib name in Assemblies: System.Web.Extensions.
    using CSharpDemo.FileOperation;
    using Microsoft.AzureAd.Icm.Types;
    using Microsoft.AzureAd.Icm.Types.Api;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    // The doc link: https://icmdocs.azurewebsites.net/
    class QueryIncidents
    {
        public static void MainMethod()
        {
            // ignore all cert errors in this sample
            //ServicePointManager.ServerCertificateValidationCallback = (a, b, c, d) => true;

            //LinkRootCause();
            //CreateRootCause();

            //SaveFile.FirstMethod(@"D:\data\company_work\IDEAs\IcMWork\root_cause_test.txt", GetRootCause(107063448));
            //SaveFile.FirstMethod(@"D:\data\company_work\IDEAs\IcMWork\tenants_list.html", GetTenants());
            //SaveFile.FirstMethod(@"D:\data\company_work\IDEAs\IcMWork\test\description_entries_test.txt", GetDescriptionEntries());

            //AddDescriptionEntry();


            //EditIncidentCustomFields();
            //EditIncidentCustomFieldsSimple();
            //EditIncidentInfo();


            //GetIncidentTeamCustomField(162426570);


            //ResloveDevActiveAlert();
            //ResolveDemoAlert();

            //CompareTeamAlertBaseCreated();

            GetIncident();
            //GetCFRIncident();
            //GetCurrentOnCallDemo();

        }


        /* For this function, we cant call it very frequently.
        * For example, if we call it in a for loop:
            for (int i = 0; i < 10; i++)
            {
                AddDescriptionEntry();
            }
        * sometimes we just can call it successfully for two times, and in the third call, We will never be able to get a response and always stuck there
        * */

        public static void AddDescriptionEntry()
        {
            string EditIncidentDescriptionEntryContent = @"
                {
                  'NewDescriptionEntry' : {
                    'ChangedBy' : 'DataCopAlert',
                    'SubmittedBy' : 'DataCopAlert',
                    'Text' : 'Suppress the alert:<br/>impactedDate: 2/8/2019 12:00:00 AM', 
                    'RenderType' : 'Html',
                    'Cause' : 'Transferred'
                  }
                }";

            // Must set the certificate in current machine.
            X509Certificate2 certificate = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");

            string url = "https://icm.ad.msft.net/api/cert/incidents(116786922)";

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

            // The Timeout setting is useless, so the below retry policy is useless
            req.Timeout = 3000;
            req.AllowWriteStreamBuffering = false;
            int sleepMillisecondsTimeout = 1000;
            for (int i = 0; i < 10; i++)
            {
                try
                {
                    req.GetResponse();
                    return;
                }
                catch (WebException e)
                {
                    Console.WriteLine(e.Message);
                    Thread.Sleep(sleepMillisecondsTimeout);
                    sleepMillisecondsTimeout *= 2;
                }
            }
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
            EditIncidentCustomFieldsContent = @"
                {
                   'CustomFieldGroups':[
                        {
                            'PublicId':'965b31d9-e7e4-45bf-85d3-39810e289096',
                            'GroupType':'Tenant',
                            'CustomFields':[
                                {
                                    'Name':'DatasetId',
                                    'Value':'4321',
                                    'Type':'ShortString'
                               },
                                {
                                    'Name':'ImpactedDates',
                                    'Value':'1\n2\r3\n\r4',
                                    'Type':'BigString'
                                }
                            ]
                        }
                    ]
                }";

            X509Certificate2 certificate = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");

            string url = "https://icm.ad.msft.net/api/cert/incidents(116786922)";
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

        public static void EditIncidentCustomFields()
        {
            string EditIncidentCustomFieldsContent = @"{'CustomFieldGroups':[{'PublicId':'00000000-0000-0000-0000-000000000000','ContainerId':'54671','GroupType':'Team','CustomFields':[{'Name':'DatasetId','DisplayName':'Dataset Id','Value':'12341234','Type':'ShortString'}]}]}";
            WebRequestHandler handler = new WebRequestHandler();
            X509Certificate2 certificate = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
            handler.ClientCertificates.Add(certificate);
            HttpClient client = new HttpClient(handler);

            byte[] buffer = Encoding.UTF8.GetBytes(EditIncidentCustomFieldsContent);
            ByteArrayContent content = new ByteArrayContent(buffer);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            HttpRequestMessage request = new HttpRequestMessage(new HttpMethod("PATCH"), "https://icm.ad.msft.net/api/cert/incidents(109578527)")
            {
                Content = content
            };
            HttpResponseMessage response = client.SendAsync(request).Result;

            string responseString = response.Content.ReadAsStringAsync().Result;
            Console.WriteLine(responseString);
        }

        public static void EditIncidentInfo()
        {
            // The schema of ImpactStartDate is very fixed, we just can use ToString("o") for updating it
            string EditIncidentCustomFieldsContent = $@"{{'ImpactStartDate': '{DateTime.UtcNow.ToString("o")}'}}";

            //EditIncidentCustomFieldsContent = $@"{{'Title': 'Test', 'Summary': 'Just for summary', 'ImpactStartDate' : '2015-09-10T19:00:00.0000000Z'}}";

            WebRequestHandler handler = new WebRequestHandler();
            X509Certificate2 certificate = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
            handler.ClientCertificates.Add(certificate);
            HttpClient client = new HttpClient(handler);

            byte[] buffer = Encoding.UTF8.GetBytes(EditIncidentCustomFieldsContent);
            ByteArrayContent content = new ByteArrayContent(buffer);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            HttpRequestMessage request = new HttpRequestMessage(new HttpMethod("PATCH"), "https://icm.ad.msft.net/api/cert/incidents(137852320)")
            {
                Content = content
            };
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

            Test data = new Test
            {
                Category = "Other",
                Description = "description",
                Title = "Title"
            };
            string myContent = "{ \"Category\": \"Other\" , \"Description\": \"description\" , \"Title\": \"title\" }";
            //string myContent = JsonConvert.SerializeObject(data);
            Console.WriteLine(myContent);
            byte[] buffer = Encoding.UTF8.GetBytes(myContent);
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

        public static List<long> GetIncidentIdList(string owningTeamId, string incidentStatus = null, string environment = null)
        {
            string url = $@"https://icm.ad.msft.net/api/cert/incidents?&$filter=OwningTeamId eq '{owningTeamId}'";
            if (incidentStatus != null)
            {
                url += $@" and Status eq '{incidentStatus}'";
            }
            if (environment != null)
            {
                url += $@" and IncidentLocation/Environment eq '{environment}'";
            }
            List<long> incidentIdList = new List<long>();
            IEnumerable<Incident> incidents = GetIncidentListStatic<Incident>(url);
            foreach (Incident i in incidents) incidentIdList.Add(i.Id);
            return incidentIdList;
        }

        public static void AcknowledgeIncident(long incidentId)
        {
            string acknowledgementParametersEntryContent = @"
                {
                  'AcknowledgementParameters' : { 
                    'AcknowledgeContactAlias' : 'administrator'
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
                byte[] buffer = Encoding.UTF8.GetBytes(acknowledgementParametersEntryContent);

                req.ContentType = "application/json";
                req.ContentLength = 0;
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

        public static void ResolveDemoAlert()
        {
            long incidnetId = 159689066;
            MitigateIncident(incidnetId);
            ResolveIncident(incidnetId);
            // Cannot work
            //UpdateMitigationData(incidnetId);
        }

        public static void ResloveDevActiveAlert()
        {
            //Reslove all the active alert for test in DEV
            List<long> activeIncidentIdList = GetIncidentIdList(@"IDEAS\IDEAsDataCopTest", "ACTIVE", "DEV");
            foreach (long incidnetId in activeIncidentIdList)
            {
                Console.WriteLine(incidnetId);
                MitigateIncident(incidnetId);
                ResolveIncident(incidnetId);
            }
            Console.WriteLine(activeIncidentIdList.Count);
        }

        public static void MitigateIncident(long incidentId)
        {
            string MitigateIncidentContent = @"
                {
                  'MitigateParameters' : { 
                    'IsCustomerImpacting' : 'True', 
                    'IsNoise' : 'False',
                    'Mitigation' : 'just mitigate the incident for test in DEV',
                    'HowFixed' : 'Fixed with Hotfix',
                    'MitigateContactAlias' : 'jianjlv'
                  }
                }";
            MitigateIncidentContent = string.Format("{{'MitigateParameters':{{'IsCustomerImpacting':'True','IsNoise':'False','Mitigation':'Mitigate the test incident','HowFixed':'Fixed By Automation','MitigateContactAlias':'{0}'}}}}", "jianjlv");
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
                // Will return 400 bad reqeust error for mitigated incident.
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
                        'Text' : 'just resolve the incident for test in DEV',
                        'RenderType' : 'Plaintext'
                    },
                    'ResolveContactAlias' : 'jianjlv'
                  }
                }";
            MitigateIncidentContent = string.Format("{{'ResolveParameters':{{'IsCustomerImpacting':'False','IsNoise':'True','Description':{{'Text':'Resolve the test incident','RenderType':'Plaintext'}},'ResolveContactAlias':'{0}'}}}}", "jianjlv");
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

        // Failed to edit mitigation info for an incident.
        public static void UpdateMitigationData(long incidentId)
        {
            string editIncidentDescriptionEntryContent = @"
                {
                  'MitigationData' : { 
                    'ChangedBy' : 'IDEAS\IDEAsDataCopTest', 
                    }
                  },
                  'ResolutionData' : { 
                    'ChangedBy' : 'IDEAS\IDEAsDataCopTest', 
                    }
                  }
                }";

            HttpWebRequest req;
            string url;

            url = $"https://icm.ad.msft.net/api/cert/incidents({incidentId})";
            req = WebRequest.CreateHttp(url);
            req.Method = "PATCH";

            try
            {
                X509Certificate cert = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
                if (cert == null)
                {
                    Console.WriteLine("cert is null");
                }

                req.ClientCertificates.Add(cert);
                byte[] buffer = Encoding.UTF8.GetBytes(editIncidentDescriptionEntryContent);

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

        public static void GetIncident()
        {
            string url;
            // build the URL we'll hit
            string LastSyncTimeString = DateTime.UtcNow.AddHours(-100).ToString("s");
            url = $@"https://icm.ad.msft.net/api/cert/incidents?$filter=OwningTeamId eq 'IDEAS\ScorecardTest' and ModifiedDate ge datetime'{LastSyncTimeString}'";
            url = $@"https://prod.microsofticm.com/api/cert/incidents?$filter=OwningTeamId eq 'IDEAS\Test' and ModifiedDate ge datetime'{LastSyncTimeString}'";
            // CreatedBy is not a valid filter in the query url
            // and CreatedBy eq 'DataCopMonitor'

            //url = $@"https://icm.ad.msft.net/api/cert/incidents?$filter=IncidentLocation/ServiceInstanceId eq 'DataCopAlertMicroService' and Status eq 'Active' and IncidentLocation/Environment eq 'prod'";

            // "icm.ad.msft.net" is the OdataServiceBaseUri
            //url = string.Format("https://{0}/api/cert/incidents({1})", "icm.ad.msoppe.msft.net", id);

            //url = @"https://icm.ad.msft.net/api/cert/incidents?$filter=OwningTeamId eq 'IDEAS\IDEAsDataCopTest' and Id eq 175353420";

            // an error query
            //url = @"https://icm.ad.msft.net/api/cert/incidents?&$filter=OwningTeamId eq '<The SQL oncall team>' and ModifiedDate ge datetime'2019-04-11T15:24:41'";

            //url = $@"https://icm.ad.msft.net/api/cert/incidents(108097160)";

            //url = @"https://icm.ad.msft.net/api/cert/incidents?$filter=OwningContactAlias eq 'jianjlv'";

            //url = string.Format("https://{0}/api/cert/incidents({1})", "icm.ad.msft.net", 145576096);

            //url = @"https://icm.ad.msft.net/api/cert/incidents?$filter=OwningTeamId eq 'IDEAS\IDEAsDataCopTest' and 
            //    CustomFieldGroups/any(cfg:cfg/CustomFields/any
            //    (cf:cf/Name eq 'DatasetId' and cf/Type eq 'ShortString' and cf/Value eq 'Test')
            //    ) and  Status eq 'RESOLVED'";


            //url = @"https://prod.microsofticm.com/api/cert/incidents?$filter=OwningTeamId eq 'IDEAS\IDEAsDataCopTest' and Id eq 175353420";
            IEnumerable<JToken> incidents = GetIncidentListStatic<JToken>(url);
            int count = 0;
            foreach (var incident in incidents)
            {
                count++;
                Console.WriteLine(incident["Id"]);
                Console.WriteLine(incident["Title"]);
                Console.WriteLine(incident["Status"]);
                Console.WriteLine(incident);
            }
            Console.WriteLine($"count: {count}");
        }

        public static void CompareTeamAlertBaseCreated()
        {
            string[] teamIds = new string[]
            {
                @"IDEAS\ConsumerGraphTeam",
                @"IDEAS\CustomerExperience-ProductEngagement",
                @"IDEAS\DigitalAnalytics",
                @"IDEAS\DigitalAnalyticsTest",
                //@"IDEAS\Explore",
                @"IDEAS\FieldMetrics",
                @"IDEAS\FieldTest",
                @"IDEAS\Growth",
                @"IDEAS\IDEAsDataCop",
                @"IDEAS\IDEAsDataCopTest",
                @"IDEAS\IDEASDataModelTeam",
                @"IDEAS\IncidentManager",
                @"IDEAS\Scorecard",
                @"IDEAS\ScorecardTest",
                @"IDEAS\SRTTest",
                @"IDEAS\Triage",

                @"CUSTOMERINSIGHTANDANALYSIS\CIA",
                @"CAPSENSEMONITORING\Triage"
            };

            foreach (string teamId in teamIds)
            {
                // query the last month alerts
                string LastSyncTimeString = DateTime.UtcNow.AddMonths(-1).ToString("s");

                string url = $@"https://icm.ad.msft.net/api/cert/incidents?$filter=OwningTeamId eq '{teamId}' and ModifiedDate ge datetime'{LastSyncTimeString}' and IncidentLocation/Environment eq 'prod'";
                int count = 0, count2 = 0;
                IEnumerable<JToken> incidents = GetIncidentListStatic<JToken>(url);
                foreach (var incident in incidents)
                {
                    count++;
                    if (string.Equals(incident?["Source"]?["CreatedBy"]?.ToString(), "DataCopMonitor"))
                        count2++;
                }


                Console.WriteLine($"teamId: {teamId}, count: {count}, count2: {count2}");
            }
        }


        public static void GetCFRIncident()
        {
            //JavaScriptSerializer serializer = new JavaScriptSerializer();
            HttpWebResponse result;
            HttpWebRequest req;
            string json = null;
            string url;

            //url = @"https://icm.ad.msft.net/api/cert/incidents?$filter=OwningContactAlias eq 'limji' and OwningTeamId eq 'CUSTOMERINSIGHTANDANALYSIS\CIA'";

            url = @"https://icm.ad.msft.net/api/cert/incidents?$filter=Id eq 126785415";
            url = @"https://icm.ad.msft.net/api/cert/incidents?$filter=Id eq 137886726";
            url = @"https://icm.ad.msft.net/api/cert/incidents(138422659)";
            url = "https://icm.ad.msft.net/api/cert/incidents?&$filter=Id eq 138422659";
            req = WebRequest.CreateHttp(url);
            // "87a1331eac328ec321578c10ebc8cc4c356b005f" is the CertThumbprint
            try
            {
                X509Certificate cert = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
                if (cert == null)
                {
                    Console.WriteLine("cert is null");
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

                JObject jsonObject = JObject.Parse(json);

                // Not reference Microsoft.AzureAd.Icm
                JArray incidents = JsonConvert.DeserializeObject<JArray>(jsonObject["value"].ToString());
                foreach (var incident in incidents)
                {
                    Console.WriteLine(incident["Id"]);
                    Console.WriteLine(incident["Title"]);
                    Console.WriteLine(incident["Status"]);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        // Doc link: https://icmdocs.azurewebsites.net/developers/OnCall/DirectoryService.html
        public static void GetCurrentOnCallDemo()
        {
            IList<long> teamIds = new List<long>(new long[] { 54671, 53839 });
            IList<string> onCallAliases = GetCurrentOnCallAlias(teamIds);
            foreach (var onCallAlias in onCallAliases)
            {
                Console.WriteLine(onCallAlias);
            }
        }

        public static IList<string> GetCurrentOnCallAlias(IList<long> teamIds)
        {
            IList<string> onCallAliases = new List<string>();
            string oncallRequstBodyContent = $@"
                {{
                  'TeamIds': {JsonConvert.SerializeObject(teamIds)}
                }}";
            HttpWebRequest req;
            string url;

            url = $"https://oncallapi.prod.microsofticm.com/Directory/GetCurrentOnCallForCurrentShiftForTeams";
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
                byte[] buffer = Encoding.UTF8.GetBytes(oncallRequstBodyContent);

                req.ContentType = "application/json";
                req.ContentLength = buffer.Length;
                Stream reqStream = req.GetRequestStream();
                reqStream.Write(buffer, 0, buffer.Length);
                reqStream.Close();
                HttpWebResponse result = (HttpWebResponse)req.GetResponse();

                // read out the response stream as text
                using (Stream data = result.GetResponseStream())
                {
                    if (data != null)
                    {
                        TextReader tr = new StreamReader(data);
                        var json = tr.ReadToEnd();
                        Console.WriteLine(JObject.Parse(json)["value"][0]["ShiftCurrentOnCalls"][0]["CurrentOnCallContacts"][0]["Alias"]);
                        Console.WriteLine(json);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return onCallAliases;
        }

        public static void GetTeamIdByPublicId()
        {

        }

        private static IEnumerable<T> GetIncidentListStatic<T>(string queryUrl)
        {
            QueryIncidents queryInstance = new QueryIncidents();
            return queryInstance.GetIncidentList<T>(queryUrl);
        }

        public IEnumerable<T> GetIncidentList<T>(string owningTeamId, DateTime cutOffTime)
        {
            string icMRESTAPIQueryByTeamUriTemplate = "https://icm.ad.msft.net/api/cert/incidents?&$filter=OwningTeamId eq '{0}' and ModifiedDate ge datetime'{1}' and IncidentLocation/Environment eq '{2}'";
            // build the URL we'll hit
            string url = string.Format(icMRESTAPIQueryByTeamUriTemplate, owningTeamId, cutOffTime.ToString("s"), "PROD");
            return GetIncidentList<T>(url);
        }

        private IEnumerable<T> GetIncidentList<T>(string queryUrl)
        {
            while (queryUrl != null)
            {
                // create the request
                HttpWebRequest req = WebRequest.CreateHttp(queryUrl);

                // set url null first to avoid falling into an infinite loop
                queryUrl = null;

                // add in the cert we'll authenticate with

                X509Certificate cert = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
                if (cert == null)
                {
                    Console.WriteLine("cert is null");
                }
                req.ClientCertificates.Add(cert);

                // submit the request
                HttpWebResponse result = (HttpWebResponse)(req.GetResponseAsync().Result);

                //TODO: Log the response status and request time usage to record the IcM OData API throttling strategy.

                // read out the response stream as text
                using (Stream data = result.GetResponseStream())
                {
                    if (data != null)
                    {
                        TextReader tr = new StreamReader(data);
                        string json = tr.ReadToEnd();
                        JObject jsonObject = JObject.Parse(json);
                        //JArray incidents = JsonConvert.DeserializeObject<JArray>(jsonObject["value"].ToString());
                        List<T> incidentList = JsonConvert.DeserializeObject<List<T>>(jsonObject["value"].ToString());
                        foreach (T incident in incidentList)
                        {
                            yield return incident;
                        }
                        if (jsonObject["odata.nextLink"] != null)
                        {
                            queryUrl = jsonObject["odata.nextLink"].ToString();
                        }
                    }
                }
            }
        }

        /// <summary>Gets the cert</summary>
        /// <param name="certId">cert identifier</param>
        /// <returns>resulting value</returns>
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

