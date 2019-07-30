﻿namespace CosmosDemo
{
    using System;
    using System.Collections.Generic;
    using System.Security.Cryptography;
    using System.Security.Cryptography.X509Certificates;
    using VcClient;

    class Program
    {
        const string Thumbprint = "7C3B9FAC23D24DB1313E7F985BB820FEF862A284";
        static void Main(string[] args)
        {
            // You can get the sample stram path from the doc: https://microsoftapc-my.sharepoint.com/personal/jianjlv_microsoft_com/_layouts/OneNote.aspx?id=%2Fpersonal%2Fjianjlv_microsoft_com%2FDocuments%2FJianjun%20%40%20Microsoft&wd=target%28Work.one%7C08C13A75-D69C-49FE-8D53-8DBF6710CCF0%2FSample%20Code%7CF29C765D-F05A-4516-8F35-08DCE5847D4C%2F%29


            //CheckExists1();

            //GetRowCountIteratively("2019-07-10T00:00:00.0000000Z");
            Console.WriteLine(GetRowCount("https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Users/2019/07/25/MsodsUsersHistory_2019_07_25.ss"));

            Console.ReadKey();
        }

        // Function VcClient.VC.StreamExists(string streamName) just can check the existing of file not directory.
        public static void CheckExists1()
        {
            string stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants/2019/07/12/TenantsHistory_2019_07_12.ss";
            //stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants/TenantsHistory.ss";
            stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants";
            stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Ppe/local/ParquetConverter.py";
            var certificate = GetCertificateByThumbprint(Thumbprint);
            VC.Setup(null, certificate);

            // Function VC.StreamExists will return false if the input is path of a directory.
            Console.WriteLine(VC.StreamExists(stream));
        }

        // This function also just can check the existing of file not directory.
        public static void CheckExists2()
        {
            string stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants/2019/07/12/TenantsHistory_2019_07_12.ss";
            stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants/TenantsHistory.ss";
            //stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants";

            var certificate = GetCertificateByThumbprint(Thumbprint);
            var settings = new Microsoft.Cosmos.ExportClient.ScopeExportSettings();
            settings.Path = stream;
            settings.ClientCertificate = certificate;

            var exportClient = new Microsoft.Cosmos.ExportClient.ExportClient(settings);

            try
            {
                //List<int> indices = exportClient.GetAllPartitionIndices(null).Result;
                //foreach (var item in indices)
                //{
                //    Console.WriteLine(item);
                //}

                long rowCount = exportClient.GetRowCount(null).Result;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Message: {e.Message}; Type: {e.GetType()}");
            }
        }

        public static void GetRowCountIteratively(string startDateString)
        {
            DateTime date = DateTime.Parse(startDateString);
            while (date < DateTime.Now)
            {
                Console.WriteLine(date);

                var year = date.Year.ToString();
                var month = date.Month.ToString("00");
                var day = date.Day.ToString("00");

                // For function GetRowCount, we just can get the row count value for file whose name ends with .ss, or it will throw an exception. Such as the below steam path, it will throw exception. But for function VC.StreamExists is OK.
                //string stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Ppe/local/ParquetConverter.py";

                string stream = $"https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants/{year}/{month}/{day}/TenantsHistory_{year}_{month}_{day}.ss";

                long? rowCount = GetRowCount(stream);
                Console.WriteLine(rowCount);
                date = date.AddDays(1);
            }
        }

        public static long? GetRowCount(string stream)
        {
            var certificate = GetCertificateByThumbprint(Thumbprint);
            var settings = new Microsoft.Cosmos.ExportClient.ScopeExportSettings();
            settings.Path = stream;
            settings.ClientCertificate = certificate;

            var exportClient = new Microsoft.Cosmos.ExportClient.ExportClient(settings);

            try
            {
                return exportClient.GetRowCount(null).Result;
            }
            // cannot catch the exception "CosmosFileNotFoundException", 
            // and we either cannot use this exception "CosmosFileNotFoundException.
            //catch (Microsoft.Cosmos.CosmosUriException e)
            catch (Exception e)
            {
                Console.WriteLine($"Message: {e.Message}; Type: {e.GetType()}");
                return null;
            }
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
