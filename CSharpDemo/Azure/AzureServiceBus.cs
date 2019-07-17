namespace CSharpDemo.Azure
{
    using CSharpDemo.Application;
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
            //string keyVaultName = "ideasdatacopppe";
            //string queueName = "onboardingrequest";
            //string queueName = "cosmostest";
            string queueName = "alert";
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string serviceBusConnectionString = secretProvider.GetSecretAsync(keyVaultName, "ServiceBusConnectionString").Result;
            IQueueClient queueClient = new QueueClient(serviceBusConnectionString, queueName);


            //Send message to the queue.
            //Create a new message to send to the queue.
            //string messageBody = "{\"requestId\":\"7e295d1b-9887-412e-bc37-c6404f6a2998\",\"requestType\":\"Create\",\"dataset\":{\"id\":\"d42a4841-85ab-40c9-95c3-48cc3ce6eb20\",\"name\":\"Sharepoint Active Usage\",\"createTime\":\"0001-01-01T00:00:00\",\"lastModifiedTime\":\"0001-01-01T00:00:00\",\"connectionInfo\":{\"dataLakeStore\":\"ideas-prod-c14.azuredatalakestore.net\",\"dataLakePath\":\"local/Partner/PreRelease/dev/activeusage/sharepointcommercial/%Y/%m/SharepointActiveUsage_%Y_%m_%d.ss\"},\"dataFabric\":\"ADLS\",\"category\":\"None\",\"startDate\":\"2017-07-01T00:00:00Z\",\"grain\":\"Daily\",\"sla\":\"6.0:0:0\",\"isEnabled\":false,\"ttl\":-1}}";


            //Message message = new Message(Encoding.UTF8.GetBytes(messageBody));
            //message.MessageId = "7e295d1b-9887-412e-bc37-c6404f6a2998";


            //queueClient.SendAsync(message);
            //Console.WriteLine("End...");


            // Recieve message from the queue
            AzureServiceBus serviceBus = new AzureServiceBus();
            serviceBus.RegisterOnMessageHandlerAndReceiveMessages(queueClient);
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
                AutoComplete = false
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
