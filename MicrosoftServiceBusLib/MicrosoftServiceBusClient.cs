namespace MicrosoftServiceBusLib
{
    using Microsoft.ServiceBus;
    using System.Collections.Generic;

    /* We need to use lib Microsoft.ServiceBus when we want to get the more information of ServiceBus message.
     * Such as message count and so on.
     * Doc link: 
     * https://docs.microsoft.com/en-us/azure/service-bus-messaging/message-counters
     * https://docs.microsoft.com/en-us/dotnet/api/microsoft.servicebus.messaging
     */
    public class MicrosoftServiceBusClient
    {
        public static long GetMessageCount(string connectionString, string queueName)
        {
            var nsmgr = NamespaceManager.CreateFromConnectionString(connectionString);
            long count = nsmgr.GetQueue(queueName).MessageCount;
            return count;
        }

        public static Dictionary<string, long> GetMessageCountDetails(string connectionString, string queueName)
        {
            Dictionary<string, long> dict = new Dictionary<string, long>();
            var nsmgr = NamespaceManager.CreateFromConnectionString(connectionString);
            var messageCountDetails = nsmgr.GetQueue(queueName).MessageCountDetails;

            dict.Add("activeCount", messageCountDetails.ActiveMessageCount);
            dict.Add("scheduledMessageCount", messageCountDetails.ScheduledMessageCount);
            dict.Add("deadLetterMessageCount", messageCountDetails.DeadLetterMessageCount);
            dict.Add("transferDeadLetterMessageCount", messageCountDetails.TransferDeadLetterMessageCount);
            dict.Add("transferMessageCountg", messageCountDetails.TransferMessageCount);

            return dict;
        }
    }
}
