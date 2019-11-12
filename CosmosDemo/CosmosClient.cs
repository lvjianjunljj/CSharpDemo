namespace CosmosDemo
{
    using Microsoft.Cosmos.FrontEnd.Contract;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using VcClient;

    class CosmosClient
    {
        // Function VcClient.VC.StreamExists(string streamName) just can check the existance of file not directory.
        public static bool CheckStreamExists(string streamPath)
        {
            var certificate = CertificateGenerator.GetCertificateByThumbprint();
            VC.Setup(null, certificate);

            // Function VC.StreamExists will return false if the input is path of a directory.
            return VC.StreamExists(streamPath);
        }

        // We can use VcClient.VC.DirectoryExists(string directoryName) to check the existance of directory.
        public static bool CheckDirectoryExists(string directoryPath)
        {
            var certificate = CertificateGenerator.GetCertificateByThumbprint();
            VC.Setup(null, certificate);

            // Function VC.DirectoryExists will return true if the input is path of a directory.
            // The input parameter name is directoryName, same as StreamInfo.StreamName . It is just the path.
            return VC.DirectoryExists(directoryPath);
        }

        // This function also just can check the existing of file not directory.
        public static bool CheckExists(string streamPath, out long rowCount)
        {
            var certificate = CertificateGenerator.GetCertificateByThumbprint();
            var settings = new Microsoft.Cosmos.ExportClient.ScopeExportSettings();
            settings.Path = streamPath;
            settings.ClientCertificate = certificate;

            var exportClient = new Microsoft.Cosmos.ExportClient.ExportClient(settings);

            try
            {
                rowCount = exportClient.GetRowCount(null).Result;
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Message: {e.Message}; Type: {e.GetType()}");
                rowCount = -1;
                return false;
            }
        }

        public static long? GetRowCount(string streamPath)
        {
            var certificate = CertificateGenerator.GetCertificateByThumbprint();
            var settings = new Microsoft.Cosmos.ExportClient.ScopeExportSettings();
            settings.Path = streamPath;
            settings.ClientCertificate = certificate;

            var exportClient = new Microsoft.Cosmos.ExportClient.ExportClient(settings);

            try
            {
                return exportClient.GetRowCount(null).Result;
            }
            // we cannot catch the exception "CosmosFileNotFoundException" by using CosmosUriException, 
            //catch (Microsoft.Cosmos.CosmosUriException e)

            // and we either cannot use this exception "CosmosFileNotFoundException".
            // For the asynchronous function GetRowCount(We can see its return is a subclass of Task), its exception will be packaged as AggregateException, we cannot directly catch it.
            catch (CosmosFileNotFoundException e)
            {
                return null;
            }
            // We just can catch AggregateException and judge it in AggregateException.InnerException
            // AggregateException: Represents one or more errors that occur during application execution.
            // Doc link: https://docs.microsoft.com/en-us/dotnet/api/system.aggregateexception?view=netframework-4.8
            catch (AggregateException e)
            {
                // Here we just want to catch the Exception "CosmosFileNotFoundException", if it is not the reason, we will throw the exception.
                if (e.InnerException.GetType() == typeof(CosmosFileNotFoundException))
                {
                    Console.WriteLine("Cannot find the file...");
                    return null;
                }
                if (e.InnerException.GetType() == typeof(CosmosDirectoryNotFoundException))
                {
                    Console.WriteLine("Cannot find the directory...");
                    return null;
                }
                if (e.InnerException.GetType() == typeof(CosmosArgumentException))
                {
                    Console.WriteLine($"CosmosArgumentException...{e.InnerException.Message}");
                    return null;
                }
                throw;
            }
            // This can catch any exception..
            catch (Exception e)
            {
                Console.WriteLine($"Message: {e}; Type: {e.GetType()}");
                return null;
            }
        }

        public static List<JToken> GetStreamInfos(string directoryPath)
        {
            var certificate = CertificateGenerator.GetCertificateByThumbprint();
            VC.Setup(null, certificate);

            List<JToken> streamInfoJTokens = new List<JToken>();
            List<StreamInfo> streamInfos = VC.GetDirectoryInfo(directoryPath, true);
            foreach (var streamInfo in streamInfos)
            {
                streamInfoJTokens.Add(JToken.Parse(JsonConvert.SerializeObject(streamInfo)));
            }
            return streamInfoJTokens;
        }
    }
}
