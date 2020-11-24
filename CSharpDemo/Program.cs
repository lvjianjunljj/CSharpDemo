namespace CSharpDemo
{
    using CSharpDemo.IDEAs;
    using System;
    using System.Diagnostics;

    class Program
    {
        static void Main(string[] args)
        {
            //AzureCosmosDBOperation.MainMethod();
            //AzureServiceBus.MainMethod();
            //AzureCosmosDB.MainMethod();

            //QueryIncidents.MainMethod();
            //FireIncident.MainMethod();

            DatasetJsonFileOperation.MainMethod();
            //AzureActiveDirectoryToken.MainMethod();
            //AzureKeyVaultDemo.MainMethod();

            if (Debugger.IsAttached)
            {
                Console.WriteLine("press enter key to exit...");
                Console.ReadLine();
            }
        }
    }
}
