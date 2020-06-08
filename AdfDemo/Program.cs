// Sample code for testing ADF v1 sdk: Microsoft.Azure.Management.DataFactories
namespace AdfDemo
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

            /*
             * Sample v1 azure data factory in ideas-ppe
             * ADF v1 service will be shut down for Microsoft internal customers on June 30, 2020.
             * You won’t be able to create ADF v1 data factories, pipelines after June 30, 2020.
             * The existing ADF v1 pipelines will automatically shut down and you will not be able to view,
             * execute your existing ADF v1 pipelines. Migrate your existing ADF v1 pipelines to ADF v2
             */
            dataFactory = "ideas-ppe-adf-sandbox";
            pipelineName = "CosmosToBlob_UserTypeDim";
            datasetName = "Cosmos_UserTypeDim";
            linkedServiceName = "CosmosLinkedService";

            /*
             * Sample v2 azure data factory in ideas-ppe
             * Microsoft.Azure.Management.DataFactories is the sdk for ADF v1,
             * we cannot use it to find the Data factory v2.
             */
            dataFactory = "fcm-adf";
            pipelineName = "invokeBricks";
            datasetName = "blobIn";
            // There should not be linked service list in ADF v2
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
