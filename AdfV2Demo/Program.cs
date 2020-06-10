namespace AdfV2Demo
{
    using Microsoft.Azure.Management.DataFactory;
    using Microsoft.Azure.Management.DataFactory.Models;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Microsoft.Rest;
    using Newtonsoft.Json;
    using System;
    using System.Threading;

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

            /*
             * Sample v2 azure data factory in ideas-ppe
             * Microsoft.Azure.Management.DataFactories is the sdk for ADF v1,
             * we cannot use it to find the Data factory v2.
             */
            dataFactory = "fcm-adf";
            pipelineName = "scope_test_pipeline";
            datasetName = "blobIn";
            linkedServiceName = "adla_sandbox_c08";


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
            var linkedService = client.LinkedServices.Get(resourceGroup, dataFactory, linkedServiceName);
            var response = client.Pipelines.CreateRun(resourceGroup, dataFactory, pipelineName);

            var run = client.PipelineRuns.Get(resourceGroup, dataFactory, response.RunId);
            while (run.Status == "InProgress")
            {
                Thread.Sleep(5000);
                run = client.PipelineRuns.Get(resourceGroup, dataFactory, response.RunId);
            }


            Console.WriteLine(JsonConvert.SerializeObject(pipeline));
            Console.WriteLine(JsonConvert.SerializeObject(dataset));
            Console.WriteLine(JsonConvert.SerializeObject(linkedService));
            Console.WriteLine(JsonConvert.SerializeObject(response));
            Console.WriteLine(JsonConvert.SerializeObject(run));
            Console.WriteLine(run.Status);
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

            //string azureCosmosDBConnString = "";
            //string cosmosDbLinkedServiceName = "cosmosDbLinkedService";
            //LinkedServiceResource cosmosDbLinkedService = new LinkedServiceResource(
            //    new CosmosDbLinkedService
            //    {
            //        ConnectionString = new SecureString(azureCosmosDBConnString),
            //    }
            //);

            //LinkedServiceResource cosmosLinkedService = new LinkedServiceResource(
            //);
            //client.LinkedServices.CreateOrUpdate(resourceGroup, dataFactory, cosmosDbLinkedServiceName, cosmosDbLinkedService);
            //Console.WriteLine(JsonConvert.SerializeObject(cosmosDbLinkedService, client.SerializationSettings));

            Console.WriteLine("End...");
            Console.WriteLine("Press any key to exit");
            Console.ReadKey();
        }
    }
}
