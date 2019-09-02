namespace CSharpDemo.Azure
{
    using AzureLib.KeyVault;
    using Microsoft.Azure.ServiceBus;
    using System;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    class AzureServiceBus
    {
        public static void MainMethod()
        {
            //string keyVaultName = "datacopdev";
            string keyVaultName = "ideasdatacopppe";
            //string queueName = "cosmostest";
            //string queueName = "alert";
            string queueName = "onboardingrequest";
            //string queueName = "onboardingresponse";
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string serviceBusConnectionString = secretProvider.GetSecretAsync(keyVaultName, "ServiceBusConnectionString").Result;
            IQueueClient queueClient = new QueueClient(serviceBusConnectionString, queueName);


            //Send message to the queue.
            //Create a new message to send to the queue.
            string requestId = Guid.NewGuid().ToString();
            string datasetId = Guid.NewGuid().ToString();

            // Message for merging adls and cosmos onboarding test
            string messageBody = "{\"requestId\":\"" + requestId + "\",\"requestType\":\"Create\",\"dataset\":{\"id\":\"" + datasetId + "\",\"name\":\"ExcelActiveDevices\",\"createTime\":\"0001-01-01T00:00:00\",\"lastModifiedTime\":\"0001-01-01T00:00:00\",\"connectionInfo\":{\"cosmosVC\":\"https://cosmos14.osdinfra.net/cosmos/Ideas.prod/\",\"dataLakeStore\":\"ideas-prod-c14.azuredatalakestore.net\",\"streamPath\":\"local/Partner/PreRelease/dev/activeusage/excelcommercialdevice/%Y/%m/ExcelActiveDevices_%Y_%m_%d.ss\"},\"dataFabric\":\"ADLS\",\"category\":\"None\",\"startDate\":\"2019-06-04T00:00:00Z\",\"rollingWindow\":\"120.00:00:00\",\"grain\":\"Daily\",\"sla\":\"6.0:0:0\",\"isEnabled\":true,\"ttl\":-1},\"alert\":{\"targetIcMConnector\":\"64c4853b-78cd-41ff-8eff-65083863a2e1\",\"containerPublicId\":\"965b31d9-e7e4-45bf-85d3-39810e289096\",\"serviceCustomFieldNames\":[\"DatasetId\",\"AlertType\",\"DisplayInSurface\",\"BusinessOwner\"],\"severity\":4,\"alertThreshold\":0,\"alertIntervalMins\":0,\"isAlertSuppressionEnabled\":true,\"suppressionMins\":60,\"routingId\":\"IDEAS://IDEAsDataCopTest\",\"owningTeamId\":\"IDEAS\\\\IDEAsDataCopTest\",\"ttl\":-1}}";

            //messageBody = "{\"requestId\":\"" + requestId + "\",\"requestType\":\"Create\",\"dataset\":{\"id\":\"" + datasetId + "\",\"name\":\"ExcelActiveDevices\",\"createTime\":\"0001-01-01T00:00:00\",\"lastModifiedTime\":\"0001-01-01T00:00:00\",\"connectionInfo\":{\"dataLakeStore\":\"ideas-prod-c14.azuredatalakestore.net\",\"streamPath\":\"local/Partner/PreRelease/dev/activeusage/excelcommercialdevice/%Y/%m/ExcelActiveDevices_%Y_%m_%d.ss\"},\"dataFabric\":\"ADLS\",\"category\":\"None\",\"startDate\":\"2019-06-04T00:00:00Z\",\"rollingWindow\":\"120.00:00:00\",\"grain\":\"Daily\",\"sla\":\"6.0:0:0\",\"isEnabled\":true,\"ttl\":-1},\"alert\":{\"targetIcMConnector\":\"64c4853b-78cd-41ff-8eff-65083863a2e1\",\"containerPublicId\":\"965b31d9-e7e4-45bf-85d3-39810e289096\",\"serviceCustomFieldNames\":[\"DatasetId\",\"AlertType\",\"DisplayInSurface\",\"BusinessOwner\"],\"severity\":4,\"alertThreshold\":0,\"alertIntervalMins\":0,\"isAlertSuppressionEnabled\":true,\"suppressionMins\":60,\"routingId\":\"IDEAS://IDEAsDataCopTest\",\"owningTeamId\":\"IDEAS\\\\IDEAsDataCopTest\",\"ttl\":-1}}";

            //messageBody = "{\"requestId\":\"" + requestId + "\",\"requestType\":\"Create\",\"dataset\":{\"id\":\"" + datasetId + "\",\"name\":\"ExcelActiveDevices\",\"createTime\":\"0001-01-01T00:00:00\",\"lastModifiedTime\":\"0001-01-01T00:00:00\",\"connectionInfo\":{\"cosmosVC\":\"https://cosmos14.osdinfra.net/cosmos/Ideas.prod/\",\"streamPath\":\"local/Partner/PreRelease/dev/activeusage/excelcommercialdevice/%Y/%m/ExcelActiveDevices_%Y_%m_%d.ss\"},\"dataFabric\":\"Cosmos\",\"category\":\"None\",\"startDate\":\"2019-06-04T00:00:00Z\",\"rollingWindow\":\"120.00:00:00\",\"grain\":\"Daily\",\"sla\":\"6.0:0:0\",\"isEnabled\":true,\"ttl\":-1},\"alert\":{\"targetIcMConnector\":\"64c4853b-78cd-41ff-8eff-65083863a2e1\",\"containerPublicId\":\"965b31d9-e7e4-45bf-85d3-39810e289096\",\"serviceCustomFieldNames\":[\"DatasetId\",\"AlertType\",\"DisplayInSurface\",\"BusinessOwner\"],\"severity\":4,\"alertThreshold\":0,\"alertIntervalMins\":0,\"isAlertSuppressionEnabled\":true,\"suppressionMins\":60,\"routingId\":\"IDEAS://IDEAsDataCopTest\",\"owningTeamId\":\"IDEAS\\\\IDEAsDataCopTest\",\"ttl\":-1}}";

            messageBody = "{\"requestId\":\"" + requestId + "\",\"requestType\":\"Adhoc\",\"dataset\":{\"id\":\"" + datasetId + "\",\"name\":\"dataForKatana\",\"createTime\":\"0001-01-01T00:00:00\",\"lastModifiedTime\":\"0001-01-01T00:00:00\",\"connectionInfo\":{\"cosmosVC\":\"https://cosmos14.osdinfra.net/cosmos/Ideas.prod/\",\"streamPath\":\"shares/AD_DataAnalytics/AD_DataAnalytics/Data/PublishedForKatana/IDEA/%Y/%m/%d/IDEA.DNA.Public.UserActivity.Misc_MAU_Daily/dataForKatana.ss\"},\"dataFabric\":\"Cosmos\",\"category\":\"None\",\"startDate\":\"2019-08-22T00:00:00Z\",\"endDate\":\"2019-08-25T00:00:00Z\",\"grain\":\"Daily\",\"sla\":\"3.0:0:0\",\"isEnabled\":false,\"ttl\":-1}}";


            Message message = new Message(Encoding.UTF8.GetBytes(messageBody));
            message.MessageId = Guid.NewGuid().ToString();


            queueClient.SendAsync(message).Wait();
            Console.WriteLine(datasetId);
            Console.WriteLine("End...");


            // Recieve message from the queue
            //AzureServiceBus serviceBus = new AzureServiceBus();
            //serviceBus.RegisterOnMessageHandlerAndReceiveMessages(queueClient);
        }


        public void RegisterOnMessageHandlerAndReceiveMessages(IQueueClient queueClient)
        {
            // Configure the MessageHandler Options in terms of exception handling, number of concurrent messages to deliver etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of Concurrent calls to the callback `ProcessMessagesAsync`, set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 1,

                // Indicates whether MessagePump should automatically complete the messages after returning from User Callback.
                // False below indicates the Complete will be handled by the User Callback as in `ProcessMessagesAsync` below.
                // For my most scenarios, I think setting it true is better.
                AutoComplete = true
            };

            // Register the function that will process messages
            queueClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        private async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            // Process the message
            Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)} Id: {message.MessageId}");

            // Complete the message so that it is not received again.
            // This can be done only if the queueClient is created in ReceiveMode.PeekLock mode (which is default).
            //await queueClient.CompleteAsync(message.SystemProperties.LockToken);

            // Note: Use the cancellationToken passed as necessary to determine if the queueClient has already been closed.
            // If queueClient has already been Closed, you may chose to not call CompleteAsync() or AbandonAsync() etc. calls 
            // to avoid unnecessary exceptions.
        }

        private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }
    }
}
