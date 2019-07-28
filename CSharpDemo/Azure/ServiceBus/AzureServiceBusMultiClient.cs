using AzureLib.KeyVault;
using Microsoft.Azure.ServiceBus;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CSharpDemo.Azure.ServiceBus
{
    class AzureServiceBusMultiClient
    {
        //AzureServiceBusMultiClient.NumberOfMessages = 10;
        //AzureServiceBusMultiClient.SendMainAsync().GetAwaiter().GetResult();
        //AzureServiceBusMultiClient.ReceiveMainAsync().GetAwaiter().GetResult();

        const string KeyVaultName = "csharpmvcwebapikeyvault";
        const string QueueName = "queue_test_multi_client";

        public static int NumberOfMessages { get; set; }

        public static async Task SendMainAsync()
        {

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after sending all the messages.");
            Console.WriteLine("======================================================");

            // Send messages.
            await SendMessagesAsync();
        }
        static async Task SendMessagesAsync()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string serviceBusConnectionString = secretProvider.GetSecretAsync(KeyVaultName, "ServiceBusConnectionString").Result;
            try
            {
                for (var i = 0; i < NumberOfMessages; i++)
                {
                    DateTime beforDT = System.DateTime.Now;

                    //耗时巨大的代码
                    IQueueClient queueClient = new QueueClient(serviceBusConnectionString, QueueName);

                    DateTime afterDT = System.DateTime.Now;
                    TimeSpan ts = afterDT.Subtract(beforDT);
                    beforDT = afterDT;
                    Console.WriteLine("create queue client time cost: {0}ms.", ts.TotalMilliseconds);
                    // Create a new message to send to the queue.
                    string messageBody = $"Message {i}";
                    Message message = new Message(Encoding.UTF8.GetBytes(messageBody));

                    message.MessageId = $"{i}";
                    // Write the body of the message to the console.
                    Console.WriteLine($"Sending message: {messageBody}");

                    beforDT = System.DateTime.Now;
                    // Send the message to the queue.
                    await queueClient.SendAsync(message);
                    afterDT = System.DateTime.Now;
                    ts = afterDT.Subtract(beforDT);
                    Console.WriteLine("sned message time cost: {0}ms.", ts.TotalMilliseconds);
                    await queueClient.CloseAsync();
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }
        public static async Task ReceiveMainAsync()
        {

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after receiving all the messages.");
            Console.WriteLine("======================================================");

            // Register QueueClient's MessageHandler and receive messages in a loop
            await RegisterOnMessageHandlerAndReceiveMessages();

            Console.ReadKey();
        }

        static async Task RegisterOnMessageHandlerAndReceiveMessages()
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string serviceBusConnectionString = secretProvider.GetSecretAsync(KeyVaultName, "ServiceBusConnectionString").Result;
            // Configure the MessageHandler Options in terms of exception handling, number of concurrent messages to deliver etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of Concurrent calls to the callback `ProcessMessagesAsync`, set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 1,

                // Indicates whether MessagePump should automatically complete the messages after returning from User Callback.
                // False below indicates the Complete will be handled by the User Callback as in `ProcessMessagesAsync` below.
                AutoComplete = true
            };

            IQueueClient queueClient = new QueueClient(serviceBusConnectionString, QueueName);
            // Register the function that will process messages
            queueClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);

            //await queueClient.CloseAsync();
        }

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
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
