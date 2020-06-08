namespace AdfV2Demo
{
    using Microsoft.Azure.Management.DataFactory;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Microsoft.Rest;
    using Newtonsoft.Json;
    using System;

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
            //string linkedServiceName = "";

            /*
             * Sample v2 azure data factory in ideas-ppe
             * Microsoft.Azure.Management.DataFactories is the sdk for ADF v1,
             * we cannot use it to find the Data factory v2.
             */
            dataFactory = "fcm-adf";
            pipelineName = "invokeBricks";
            datasetName = "blobIn";

            // Not sure if there is linked service list in ADF v2
            //linkedServiceName = "CosmosLinkedService";


            DateTime slice = new DateTime(2020, 5, 4);

            AuthenticationContext authenticationContext = new AuthenticationContext($"https://login.windows.net/{tenant_id}");
            var credential = new ClientCredential(clientId: client_id, clientSecret: client_key);
            var result = authenticationContext.AcquireTokenAsync(resource: "https://management.core.windows.net/", clientCredential: credential).Result;

            if (result == null)
            {
                throw new InvalidOperationException("Failed to obtain the JWT token");
            }

            var token = result.AccessToken;

            ServiceClientCredentials _credentials = new TokenCredentials(token);
            var client = new DataFactoryManagementClient(_credentials)
            {
                SubscriptionId = subscription_id
            };

            var pipeline = client.Pipelines.Get(resourceGroup, dataFactory, pipelineName);
            //pipeline.Pipeline.Properties.Start = DateTime.Parse($"{slice.Date:yyyy-MM-dd}T00:00:00Z");
            //pipeline.Pipeline.Properties.End = DateTime.Parse($"{slice.Date:yyyy-MM-dd}T23:59:59Z");
            //pipeline.Pipeline.Properties.IsPaused = false;
            //client.Pipelines.CreateOrUpdate(resourceGroup, dataFactory, new PipelineCreateOrUpdateParameters()
            //{
            //    Pipeline = pipeline.Pipeline
            //});

            var dataset = client.Datasets.Get(resourceGroup, dataFactory, datasetName);
            //var linkedService = client.LinkedServices.Get(resourceGroup, dataFactory, linkedServiceName);

            Console.WriteLine(JsonConvert.SerializeObject(pipeline));
            Console.WriteLine(JsonConvert.SerializeObject(dataset));
            //Console.WriteLine(JsonConvert.SerializeObject(linkedService));

            //Pipeline pipelineInput = new Pipeline
            //{
            //    Properties = new PipelineProperties
            //    {

            //    }
            //};
            //client.Pipelines.CreateOrUpdate(resourceGroup, dataFactory, new PipelineCreateOrUpdateParameters()
            //{
            //    Pipeline = pipelineInput
            //});

            Console.WriteLine("End...");
            Console.ReadKey();
        }
    }
}
