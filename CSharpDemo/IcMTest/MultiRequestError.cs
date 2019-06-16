namespace CSharpDemo.IcMTest
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading.Tasks;
    using CSharpDemo.Application;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;

    class MultiRequestError
    {
        private const string EditIncidentDescriptionEntryContent = @"
                {
                  'NewDescriptionEntry' : {
                    'ChangedBy' : 'DataCopAlert',
                    'SubmittedBy' : 'DataCopAlert',
                    'Text' : 'Suppress the alert:<br/>impactedDate: 2/8/2019 12:00:00 AM', 
                    'RenderType' : 'Html',
                    'Cause' : 'Transferred'
                  }
                }";

        private const long IncidentId = 116781127;

        public static void MainMethod()
        {
            //HttpClientDemo();
            HttpWebRequestDemo();
        }

        // This funtion won't throw the exception in function HttpWebRequestDemo().
        // The general consensus is that you do not (should not) need to dispose of HttpClient.
        // Doc link: https://stackoverflow.com/questions/15705092/do-httpclient-and-httpclienthandler-have-to-be-disposed
        private static void HttpClientDemo()
        {
            for (int i = 0; i < 10; i++)
            {
                Console.WriteLine($"Start round: {i + 1}");

                WebRequestHandler handler = new WebRequestHandler();
                X509Certificate certificate = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");
                handler.ClientCertificates.Add(certificate);
                HttpClient client = new HttpClient(handler);

                byte[] buffer = Encoding.UTF8.GetBytes(EditIncidentDescriptionEntryContent);
                ByteArrayContent content = new ByteArrayContent(buffer);
                content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

                HttpRequestMessage request = new HttpRequestMessage(new HttpMethod("PATCH"), $"https://icm.ad.msft.net/api/cert/incidents({IncidentId})");
                request.Content = content;
                client.SendAsync(request).Wait();

                Console.WriteLine($"Finish round: {i + 1}");
            }
        }


        private static void HttpWebRequestDemo()
        {
            for (int i = 0; i < 10; i++)
            {
                Console.WriteLine($"Start round: {i + 1}");

                // Must set the certificate in current machine.
                X509Certificate2 certificate = GetCert("87a1331eac328ec321578c10ebc8cc4c356b005f");

                string url = $"https://icm.ad.msft.net/api/cert/incidents({IncidentId})";

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

                // The Timeout setting is useless
                req.Timeout = 3000;
                req.AllowWriteStreamBuffering = false;

                // Will hang in here and finally throw timeout exception in GetRequestStream() after running this program several times if we dont run the dispose method.
                HttpWebResponse result = (HttpWebResponse)req.GetResponse();

                // read out the response stream as text
                Stream data = result.GetResponseStream();
                if (data != null)
                {
                    TextReader tr = new StreamReader(data);
                    string resultString = tr.ReadToEnd();
                    Console.WriteLine(resultString);
                }

                /* All the below three lines can solve this problem.
                 * Or we can use this two line to solve this problem, the principle is the same
                 * using (HttpWebResponse result = (HttpWebResponse)req.GetResponse())
                 * using (Stream data = result.GetResponseStream())
                 * Do some test and get the call hierarchy: data.Close() => data.Disponse() => response.Dispose()
                 * We can see it in the doc: https://docs.microsoft.com/en-us/dotnet/api/system.io.stream.close?view=netframework-4.8
                 * Stream.Close Method: This method calls Dispose
                 */
                //data.Close();
                //data.Dispose();
                //response.Dispose();

                Console.WriteLine($"Finish round: {i + 1}");
            }
        }

        private static X509Certificate2 GetCert(string certId)
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
