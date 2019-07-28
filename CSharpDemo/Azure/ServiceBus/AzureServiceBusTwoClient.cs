using AzureLib.KeyVault;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

namespace CSharpDemo.Azure.ServiceBus
{
    class AzureServiceBusTwoClient
    {
        // AzureServiceBusTwoClient.NumberOfMessages = 10;
        // AzureServiceBusTwoClient.SendMainAsync().GetAwaiter().GetResult();
        // AzureServiceBusTwoClient.ReceiveMainAsync().GetAwaiter().GetResult();

        public static int NumberOfMessages { set; get; }
        const string KeyVaultName = "csharpmvcwebapikeyvault";
        const string QueueName = "queue_test_multi_client";
        static IQueueClient queueClient;
        public static async Task SendMainAsync()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string serviceBusConnectionString = secretProvider.GetSecretAsync(KeyVaultName, "ServiceBusConnectionString").Result;
            queueClient = new QueueClient(serviceBusConnectionString, QueueName);

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after sending all the messages.");
            Console.WriteLine("======================================================");

            // Send messages.
            await SendMessagesAsync();

            await queueClient.CloseAsync();
        }
        static async Task SendMessagesAsync()
        {
            try
            {
                for (var i = 0; i < NumberOfMessages; i++)
                {
                    // Create a new message to send to the queue.
                    string messageBody = $"Message {i}";
                    Message message = new Message(Encoding.UTF8.GetBytes(messageBody));

                    message.MessageId = $"{i}";
                    // Write the body of the message to the console.
                    Console.WriteLine($"Sending message: {messageBody}");

                    DateTime beforDT = System.DateTime.Now;
                    // Send the message to the queue.
                    await queueClient.SendAsync(message);
                    DateTime afterDT = System.DateTime.Now;
                    TimeSpan ts = afterDT.Subtract(beforDT);
                    Console.WriteLine("sned message time cost: {0}ms.", ts.TotalMilliseconds);
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }
        public static async Task ReceiveMainAsync()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string serviceBusConnectionString = secretProvider.GetSecretAsync(KeyVaultName, "ServiceBusConnectionString").Result;
            queueClient = new QueueClient(serviceBusConnectionString, QueueName);

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after receiving all the messages.");
            Console.WriteLine("======================================================");

            // Register QueueClient's MessageHandler and receive messages in a loop
            await RegisterOnMessageHandlerAndReceiveMessages();

            Console.ReadKey();
            //Thread.Sleep(3000);
            await queueClient.CloseAsync();
        }

        static async Task RegisterOnMessageHandlerAndReceiveMessages()
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

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            // Process the message
            Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)} Id: {message.MessageId}");

            // Complete the message so that it is not received again.
            // This can be done only if the queueClient is created in ReceiveMode.PeekLock mode (which is default).
            await queueClient.CompleteAsync(message.SystemProperties.LockToken);

            // Note: Use the cancellationToken passed as necessary to determine if the queueClient has already been closed.
            // If queueClient has already been Closed, you may chose to not call CompleteAsync() or AbandonAsync() etc. calls 
            // to avoid unnecessary exceptions.
        }

        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
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
