namespace CSharpDemo
{
    using AzureLib.KeyVault;
    using CSharpDemo.FileOperation;
    using CSharpDemo.IDEAs;
    using CSharpDemo.InvalidCastExceptionDemo;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Text.RegularExpressions;

    class Program
    {
        static void Main(string[] args)
        {
            AzureCosmosDBOperation.MainMethod();
            //AzureServiceBus.MainMethod();

            //QueryIncidents.MainMethod();
            //FireIncident.MainMethod();

            //DataCopJsonFileOperation.MainMethod();
            //DataBuildJsonFileOperation.MainMethod();
            //AzureActiveDirectoryToken.MainMethod();
            //AzureKeyVaultDemo.MainMethod();
            //CsprojCheck.MainMethod();
            CosmosViewErrorMessageOperation.MainMethod();

            //CloudScopeOperation.MainMethod();


            if (Debugger.IsAttached)
            {
                Console.WriteLine("press enter key to exit...");
                Console.ReadLine();
            }
        }
    }
}
