using CSharpDemo.Application;
using Microsoft.Azure.ServiceBus;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CSharpDemo.Azure.ServiceBus
{
    //AzureServiceBusMultiThread azureServiceBusMultiThread = new AzureServiceBusMultiThread();
    //azureServiceBusMultiThread.SendMainAsync().GetAwaiter().GetResult();
    //azureServiceBusMultiThread.ReceiveMainAsync().GetAwaiter().GetResult();
    class AzureServiceBusMultiThread
    {
        const string KeyVaultName = "csharpmvcwebapikeyvault";
        const string QueueName = "queue_test_single_client";
        private IQueueClient queueClient;
        public AzureServiceBusMultiThread()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string serviceBusConnectionString = secretProvider.GetSecretAsync(KeyVaultName, "ServiceBusConnectionString").Result;
            queueClient = new QueueClient(serviceBusConnectionString, QueueName);
        }
        public async Task SendMainAsync()
        {
            const int numberOfMessages = 20;
            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after sending all the messages.");
            Console.WriteLine("======================================================");

            // Send messages.
            await SendMessagesAsync(numberOfMessages);

            //await queueClient.CloseAsync();
        }
        private async Task SendMessagesAsync(int numberOfMessagesToSend)
        {
            try
            {
                Task[] taskList = new Task[numberOfMessagesToSend];
                for (var i = 0; i < numberOfMessagesToSend; i++)
                {
                    // Create a new message to send to the queue.
                    string messageBody = $"Message {i}";
                    Message message = new Message(Encoding.UTF8.GetBytes(messageBody));

                    message.MessageId = $"{i}";
                    // Write the body of the message to the console.
                    //Console.WriteLine($"Sending message: {messageBody}, id: {i}");

                    // Send the message to the queue.
                    taskList[i] = queueClient.SendAsync(message).ContinueWith((t) =>
                   {
                       Console.WriteLine($"Sending message: {messageBody}, id: {i}");
                   }); ;
                }
                Task.WaitAll(taskList);
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }
        public async Task ReceiveMainAsync()
        {

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after receiving all the messages.");
            Console.WriteLine("======================================================");

            // Register QueueClient's MessageHandler and receive messages in a loop
            await RegisterOnMessageHandlerAndReceiveMessages();

            //Console.ReadKey();
            //Thread.Sleep(3000);
            //await queueClient.CloseAsync();
        }

        private async Task RegisterOnMessageHandlerAndReceiveMessages()
        {
            // Configure the MessageHandler Options in terms of exception handling, number of concurrent messages to deliver etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of Concurrent calls to the callback `ProcessMessagesAsync`, set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 1,

                // Indicates whether MessagePump should automatically complete the messages after returning from User Callback.
                // False below indicates the Complete will be handled by the User Callback as in `ProcessMessagesAsync` below.
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
