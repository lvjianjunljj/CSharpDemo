namespace CSharpDemo
{
    using CSharpDemo.IDEAs;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;

    class Program
    {
        static void Main(string[] args)
        {
            //AzureCosmosDBOperation.MainMethod();
            //AzureServiceBus.MainMethod();

            //QueryIncidents.MainMethod();
            //FireIncident.MainMethod();

            //DataCopJsonFileOperation.MainMethod();
            //DataBuildJsonFileOperation.MainMethod();
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
