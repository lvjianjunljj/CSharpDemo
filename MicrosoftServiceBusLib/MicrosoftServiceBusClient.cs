namespace MicrosoftServiceBusLib
{
    using Microsoft.ServiceBus;

    public class MicrosoftServiceBusClient
    {
        public static long GetMessageCount(string connectionString, string queueName)
        {
            var nsmgr = NamespaceManager.CreateFromConnectionString("");
            long count = nsmgr.GetQueue(queueName).MessageCount;
            return count;
        }
    }
}
