namespace AzureSparkDemo
{
    using System;
    using System.Diagnostics;

    class Program
    {
        static void Main(string[] args)
        {
            SparkMetricDemo.MainMethod();

            if (Debugger.IsAttached)
            {
                Console.WriteLine("press enter key to exit...");
                Console.ReadLine();
            }
        }
    }
}
