namespace LogAnalyticsDemo
{
    using AzureLib.KeyVault;
    using System;
    using System.Diagnostics;

    class Program
    {
        static void Main(string[] args)
        {
            //BuildLogQueryDemo.MainMethod();
            //CloudScopeLogQueryDemo.MainMethod();
            DataCopLogQueryDemo.MainMethod();

            if (Debugger.IsAttached)
            {
                Console.WriteLine("press enter key to exit...");
                Console.ReadLine();
            }
        }
    }
}
