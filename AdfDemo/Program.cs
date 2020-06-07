namespace AdfDemo
{
    using System;
    using Microsoft.Azure;
    using Microsoft.Azure.Management.DataFactories;
    using Microsoft.Azure.Management.DataFactories.Models;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(1234);

            string tenant_id = "";
            string client_id = "";
            string client_key = "";
            string subscription_id = "";
            string pipelineName = "";
            string resourceGroup = "";
            string dataFactory = "";

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
            pipeline.Pipeline.Properties.Start = DateTime.Parse($"{slice.Date:yyyy-MM-dd}T00:00:00Z");
            pipeline.Pipeline.Properties.End = DateTime.Parse($"{slice.Date:yyyy-MM-dd}T23:59:59Z");
            pipeline.Pipeline.Properties.IsPaused = false;
            client.Pipelines.CreateOrUpdate(resourceGroup, dataFactory, new PipelineCreateOrUpdateParameters()
            {
                Pipeline = pipeline.Pipeline
            });


            Console.ReadKey();
        }

        private static DataFactoryManagementClient Create_adf_client()
        {
            string tenant_id = "";
            string client_id = "a3bdd828-cb26-41e7-8437-e5d73ced7ab9";
            string client_key = "";
            string subscription_id = "";

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
            return client;
        }

        private static void StartPipeline(DataFactoryManagementClient client, string resourceGroup, string dataFactory, string pipelineName, DateTime slice)
        {
            var pipeline = client.Pipelines.Get(resourceGroup, dataFactory, pipelineName);
            pipeline.Pipeline.Properties.Start = DateTime.Parse($"{slice.Date:yyyy-MM-dd}T00:00:00Z");
            pipeline.Pipeline.Properties.End = DateTime.Parse($"{slice.Date:yyyy-MM-dd}T23:59:59Z");
            pipeline.Pipeline.Properties.IsPaused = false;
            client.Pipelines.CreateOrUpdate(resourceGroup, dataFactory, new PipelineCreateOrUpdateParameters()
            {
                Pipeline = pipeline.Pipeline
            });
        }
    }
}
