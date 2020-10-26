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
            string keyVaultName = "datacopdev";
            //string keyVaultName = "datacop-ppe";
            //string keyVaultName = "datacop-prod";
            string queueName = "cosmostest";
            //string queueName = "alert";
            //string queueName = "onboardingrequest";
            //string queueName = "orchestratortrigger";
            //string queueName = "onboardingresponse";
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string serviceBusConnectionString = secretProvider.GetSecretAsync(keyVaultName, "ServiceBusConnectionString").Result;
            IQueueClient queueClient = new QueueClient(serviceBusConnectionString, queueName);


            //Send message to the queue.
            //Create a new message to send to the queue.
            string requestId = Guid.NewGuid().ToString();
            string datasetId = Guid.NewGuid().ToString();

            // Message for merging adls and cosmos onboarding test
            //string messageBody = "{\"requestId\":\"" + requestId + "\",\"requestType\":\"Create\",\"dataset\":{\"id\":\"" + datasetId + "\",\"name\":\"ExcelActiveDevices\",\"createTime\":\"0001-01-01T00:00:00\",\"lastModifiedTime\":\"0001-01-01T00:00:00\",\"connectionInfo\":{\"cosmosVC\":\"https://cosmos14.osdinfra.net/cosmos/Ideas.prod/\",\"dataLakeStore\":\"ideas-prod-c14.azuredatalakestore.net\",\"streamPath\":\"local/Partner/PreRelease/dev/activeusage/excelcommercialdevice/%Y/%m/ExcelActiveDevices_%Y_%m_%d.ss\"},\"dataFabric\":\"ADLS\",\"category\":\"None\",\"startDate\":\"2019-06-04T00:00:00Z\",\"rollingWindow\":\"120.00:00:00\",\"grain\":\"Daily\",\"sla\":\"6.0:0:0\",\"isEnabled\":true,\"ttl\":-1},\"alert\":{\"targetIcMConnector\":\"64c4853b-78cd-41ff-8eff-65083863a2e1\",\"containerPublicId\":\"965b31d9-e7e4-45bf-85d3-39810e289096\",\"serviceCustomFieldNames\":[\"DatasetId\",\"AlertType\",\"DisplayInSurface\",\"BusinessOwner\"],\"severity\":4,\"alertThreshold\":0,\"alertIntervalMins\":0,\"isAlertSuppressionEnabled\":true,\"suppressionMins\":60,\"routingId\":\"IDEAS://IDEAsDataCopTest\",\"owningTeamId\":\"IDEAS\\\\IDEAsDataCopTest\",\"ttl\":-1}}";

            //messageBody = "{\"requestId\":\"" + requestId + "\",\"requestType\":\"Create\",\"dataset\":{\"id\":\"" + datasetId + "\",\"name\":\"ExcelActiveDevices\",\"createTime\":\"0001-01-01T00:00:00\",\"lastModifiedTime\":\"0001-01-01T00:00:00\",\"connectionInfo\":{\"dataLakeStore\":\"ideas-prod-c14.azuredatalakestore.net\",\"streamPath\":\"local/Partner/PreRelease/dev/activeusage/excelcommercialdevice/%Y/%m/ExcelActiveDevices_%Y_%m_%d.ss\"},\"dataFabric\":\"ADLS\",\"category\":\"None\",\"startDate\":\"2019-06-04T00:00:00Z\",\"rollingWindow\":\"120.00:00:00\",\"grain\":\"Daily\",\"sla\":\"6.0:0:0\",\"isEnabled\":true,\"ttl\":-1},\"alert\":{\"targetIcMConnector\":\"64c4853b-78cd-41ff-8eff-65083863a2e1\",\"containerPublicId\":\"965b31d9-e7e4-45bf-85d3-39810e289096\",\"serviceCustomFieldNames\":[\"DatasetId\",\"AlertType\",\"DisplayInSurface\",\"BusinessOwner\"],\"severity\":4,\"alertThreshold\":0,\"alertIntervalMins\":0,\"isAlertSuppressionEnabled\":true,\"suppressionMins\":60,\"routingId\":\"IDEAS://IDEAsDataCopTest\",\"owningTeamId\":\"IDEAS\\\\IDEAsDataCopTest\",\"ttl\":-1}}";

            //messageBody = "{\"requestId\":\"" + requestId + "\",\"requestType\":\"Create\",\"dataset\":{\"id\":\"" + datasetId + "\",\"name\":\"ExcelActiveDevices\",\"createTime\":\"0001-01-01T00:00:00\",\"lastModifiedTime\":\"0001-01-01T00:00:00\",\"connectionInfo\":{\"cosmosVC\":\"https://cosmos14.osdinfra.net/cosmos/Ideas.prod/\",\"streamPath\":\"local/Partner/PreRelease/dev/activeusage/excelcommercialdevice/%Y/%m/ExcelActiveDevices_%Y_%m_%d.ss\"},\"dataFabric\":\"Cosmos\",\"category\":\"None\",\"startDate\":\"2019-06-04T00:00:00Z\",\"rollingWindow\":\"120.00:00:00\",\"grain\":\"Daily\",\"sla\":\"6.0:0:0\",\"isEnabled\":true,\"ttl\":-1},\"alert\":{\"targetIcMConnector\":\"64c4853b-78cd-41ff-8eff-65083863a2e1\",\"containerPublicId\":\"965b31d9-e7e4-45bf-85d3-39810e289096\",\"serviceCustomFieldNames\":[\"DatasetId\",\"AlertType\",\"DisplayInSurface\",\"BusinessOwner\"],\"severity\":4,\"alertThreshold\":0,\"alertIntervalMins\":0,\"isAlertSuppressionEnabled\":true,\"suppressionMins\":60,\"routingId\":\"IDEAS://IDEAsDataCopTest\",\"owningTeamId\":\"IDEAS\\\\IDEAsDataCopTest\",\"ttl\":-1}}";

            //messageBody = "{\"requestId\":\"" + requestId + "\",\"requestType\":\"Adhoc\",\"dataset\":{\"id\":\"" + datasetId + "\",\"name\":\"dataForKatana\",\"createTime\":\"0001-01-01T00:00:00\",\"lastModifiedTime\":\"0001-01-01T00:00:00\",\"connectionInfo\":{\"cosmosVC\":\"https://cosmos14.osdinfra.net/cosmos/Ideas.prod/\",\"streamPath\":\"shares/AD_DataAnalytics/AD_DataAnalytics/Data/PublishedForKatana/IDEA/%Y/%m/%d/IDEA.DNA.Public.UserActivity.Misc_MAU_Daily/dataForKatana.ss\"},\"dataFabric\":\"Cosmos\",\"category\":\"None\",\"startDate\":\"2019-08-22T00:00:00Z\",\"endDate\":\"2019-08-25T00:00:00Z\",\"grain\":\"Daily\",\"sla\":\"3.0:0:0\",\"isEnabled\":false,\"ttl\":-1}}";


            //string requestsStr = FileOperation.ReadFile.ThirdMethod(@"D:\data\company_work\M365\IDEAs\request.txt");
            //string[] requestsStrSplits = requestsStr.Split(new char[] { '\n' });

            //foreach (var requestsStrSplit in requestsStrSplits)
            for (int i = 1; i < 7; i++)
            {
                //string requestsStrSplit = FileOperation.ReadFile.ThirdMethod($@"D:\data\company_work\M365\IDEAs\request\{i}.txt");
                //Message message = new Message(Encoding.UTF8.GetBytes(requestsStrSplit));
                //message.MessageId = Guid.NewGuid().ToString();

                //Console.WriteLine(requestsStrSplit);
                //queueClient.SendAsync(message).Wait();
                break;

            }


            //string response = "{\"requestId\":\"c7c864d1-9a5e-4a02-afa4-fb2bf3399273\",\"datasetId\":\"7e118c1e-f2b3-462d-97ce-f45bc7a378f8\",\"status\":200,\"scores\":{\"scoreTime\":\"2019-09-19T08:37:25.1608155Z\",\"score\":100.0,\"measures\":[{\"type\":\"Availability\",\"score\":100.0,\"errors\":[]}]},\"requestor\":\"OnboardRequest\"}";
            //string messageStr = "{\"requestId\":\"136503cc-745f-4c6e-a0f3-ebf4555b1f14\",\"requestType\":\"Adhoc\",\"dataset\":{\"id\":\"f4967631-884d-41a5-92ff-16be925bdf1b\",\"name\":\"SandCOATPRaw\",\"createTime\":\"0001-01-01T00:00:00\",\"lastModifiedTime\":\"0001-01-01T00:00:00\",\"connectionInfo\":{\"cosmosVC\":\"https://cosmos14.osdinfra.net/cosmos/Ideas.prod/\",\"dataLakeStore\":\"\",\"streamPath\":\"shares/ffo.antispam/SafeAttachment/%Y/%m/%d/ATPUsage.ss\"},\"dataFabric\":\"Cosmos\",\"category\":\"None\",\"startDate\":\"2019-05-01T00:00:00Z\",\"rollingWindow\":\"60.00:00:00\",\"grain\":\"Daily\",\"sla\":\"3.0:0:0\",\"isEnabled\":false,\"ttl\":-1}}";
            //for (int i = 0; i < 10; i++)
            //{
            //    Message message = new Message(Encoding.UTF8.GetBytes(messageStr));
            //    message.MessageId = Guid.NewGuid().ToString();

            //    queueClient.SendAsync(message).Wait();
            //}

            //Console.WriteLine(datasetId);
            //Console.WriteLine("End...");
            // Recieve message from the queue
            AzureServiceBus serviceBus = new AzureServiceBus();
            serviceBus.QueueClient = queueClient;
            serviceBus.RegisterOnMessageHandlerAndReceiveMessages();
        }

        public IQueueClient QueueClient;


        public void RegisterOnMessageHandlerAndReceiveMessages()
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
                AutoComplete = false,
                MaxAutoRenewDuration = TimeSpan.FromSeconds(5),
            };

            // Register the function that will process messages
            this.QueueClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        private async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            // Process the message
            Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)} Id: {message.MessageId}");

            // Process will throw Exception if the running time of function ProcessMessagesAsync is longer than MaxAutoRenewDuration value.
            //await Task.Delay(60 * 1000);
            Console.WriteLine("ProcessMessagesAsync end...");
            //FileOperation.SaveFile.FirstMethod($@"D:\data\company_work\M365\IDEAs\request\{message.MessageId}", message.Body.ToString());
            // Complete the message so that it is not received again.
            // This can be done only if the queueClient is created in ReceiveMode.PeekLock mode (which is default).
            //await queueClient.CompleteAsync(message.SystemProperties.LockToken);

            // Note: Use the cancellationToken passed as necessary to determine if the queueClient has already been closed.
            // If queueClient has already been Closed, you may chose to not call CompleteAsync() or AbandonAsync() etc. calls 
            // to avoid unnecessary exceptions.
            await this.QueueClient.CompleteAsync(message.GetLockToken());
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

    public static class MessageExtensions
    {
        public static int? GetDeliveryCount(this Message message)
        {
            return message.SystemProperties.IsReceived ? message.SystemProperties.DeliveryCount : (int?)null;
        }

        public static string GetLockToken(this Message message)
        {
            return message.SystemProperties.IsLockTokenSet ? message.SystemProperties.LockToken : null;
        }

    }
}
