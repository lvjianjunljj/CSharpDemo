namespace CosmosDemo
{
    using System;
    using System.Security.Cryptography;
    using System.Security.Cryptography.X509Certificates;

    class Program
    {
        static void Main(string[] args)
        {
            // You can get the sample stram path and thumbprint from the doc: https://microsoftapc-my.sharepoint.com/personal/jianjlv_microsoft_com/_layouts/OneNote.aspx?id=%2Fpersonal%2Fjianjlv_microsoft_com%2FDocuments%2FJianjun%20%40%20Microsoft&wd=target%28Work.one%7C08C13A75-D69C-49FE-8D53-8DBF6710CCF0%2FSample%20Code%7CF29C765D-F05A-4516-8F35-08DCE5847D4C%2F%29

            string stream = "";

            var settings = new Microsoft.Cosmos.ExportClient.ScopeExportSettings();
            settings.Path = stream;
            settings.ClientCertificate = GetCertificateByThumbprint("");

            var exportClient = new Microsoft.Cosmos.ExportClient.ExportClient(settings);

            long rowCount = exportClient.GetRowCount(null).Result;

            Console.WriteLine(rowCount);

            Console.ReadKey();
        }

        // <summary>
        /// Gets the certificate by thumbprint.
        /// </summary>
        /// <param name="thumbprint">The thumbprint.</param>
        /// <returns>X509Certificate2.</returns>
        /// <exception cref="CryptographicException"></exception>
        public static X509Certificate2 GetCertificateByThumbprint(string thumbprint, StoreLocation storeLocation = StoreLocation.CurrentUser)
        {
            X509Store store = null;
            try
            {
                store = new X509Store(StoreName.My, storeLocation);
                store.Open(OpenFlags.ReadOnly);
                X509Certificate2Collection certificates = store.Certificates.Find(X509FindType.FindByThumbprint, thumbprint, false);
                if (certificates.Count > 0)
                {
                    return certificates[0];
                }
                else
                {
                    throw new CryptographicException(string.Format("Cannot find certificate from CurrentUser with thumbprint {0}", thumbprint));
                }
            }
            finally
            {
                if (store != null)
                {
                    store.Close();
                }
            }
        }
    }
}
