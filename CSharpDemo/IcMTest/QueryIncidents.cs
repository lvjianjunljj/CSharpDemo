using System;
using System.IO;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Web.Script.Serialization; //lib name in Assemblies: System.Web.Extensions.

using Microsoft.AzureAd.Icm.Types;
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

        public static string GetIncident(long id)
        {

            JavaScriptSerializer serializer = new JavaScriptSerializer();

            HttpWebResponse result;

            HttpWebRequest req;

            Incident incident;

            string json = null;

            string url;


            // build the URL we'll hit
            // "icm.ad.msft.net" is the OdataServiceBaseUri
            //url = string.Format("https://{0}/api/cert/incidents({1})", "icm.ad.msoppe.msft.net", id);
            //url = @"https://icm.ad.msft.net/api/cert/incidents?$filter=OwningTeamId eq 'IDEAS\IDEAsDataCopTest' and Id eq 106845925";
            //url = @"https://icm.ad.msft.net/api/cert/incidents?$filter=Id eq 106845925";

            url = string.Format("https://{0}/api/cert/incidents({1})", "icm.ad.msft.net", id);


            // create the request

            req = WebRequest.CreateHttp(url);



            // add in the cert we'll authenticate with
            // "87a1331eac328ec321578c10ebc8cc4c356b005f" is the CertThumbprint
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

            incident = serializer.Deserialize<Incident>(json);



            // TODO: do something with the incident



            Console.WriteLine(json);
            return json;
        }

        private static X509Certificate2 GetCert(

            string certId,

            StoreLocation location)
        {
            Console.WriteLine(location);
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
        private static X509Certificate2 GetCert(

            string certId)
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

