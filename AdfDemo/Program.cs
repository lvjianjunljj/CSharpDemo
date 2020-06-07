﻿namespace AdfDemo
{
    using System;
    using Microsoft.Azure;
    using Microsoft.Azure.Management.DataFactories;
    using Microsoft.Azure.Management.DataFactories.Models;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Newtonsoft.Json;

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Start...");

            string tenant_id = "";
            string client_id = "";
            string client_key = "";
            string subscription_id = "";
            string resourceGroup = "";
            string dataFactory = "";
            string pipelineName = "";
            string datasetName = "";
            string linkedServiceName = "";

            DateTime slice = new DateTime(2020, 5, 4);

            AuthenticationContext authenticationContext = new AuthenticationContext($"https://login.windows.net/{tenant_id}");
            var credential = new ClientCredential(clientId: client_id, clientSecret: client_key);
            var result = authenticationContext.AcquireTokenAsync(resource: "https://management.core.windows.net/", clientCredential: credential).Result;

            if (result == null)
            {
                throw new InvalidOperationException("Failed to obtain the JWT token");
            }

            var token = result.AccessToken;

            var _credentials = new TokenCloudCredentials(subscription_id, token);
            var client = new DataFactoryManagementClient(_credentials);

            var pipeline = client.Pipelines.Get(resourceGroup, dataFactory, pipelineName);
            //pipeline.Pipeline.Properties.Start = DateTime.Parse($"{slice.Date:yyyy-MM-dd}T00:00:00Z");
            //pipeline.Pipeline.Properties.End = DateTime.Parse($"{slice.Date:yyyy-MM-dd}T23:59:59Z");
            //pipeline.Pipeline.Properties.IsPaused = false;
            //client.Pipelines.CreateOrUpdate(resourceGroup, dataFactory, new PipelineCreateOrUpdateParameters()
            //{
            //    Pipeline = pipeline.Pipeline
            //});

            var dataset = client.Datasets.Get(resourceGroup, dataFactory, datasetName);
            var linkedService = client.LinkedServices.Get(resourceGroup, dataFactory, linkedServiceName);

            Console.WriteLine(JsonConvert.SerializeObject(pipeline));
            Console.WriteLine(JsonConvert.SerializeObject(dataset));
            Console.WriteLine(JsonConvert.SerializeObject(linkedService));

            Console.ReadKey();
        }
    }
}
