namespace CSharpDemo
{
    using AzureLib.KeyVault;
    using CSharpDemo.Application;
    using CSharpDemo.Azure;
    using CSharpDemo.Azure.CosmosDB;
    using CSharpDemo.Concurrent;
    using CSharpDemo.FileOperation;
    using CSharpDemo.IcMTest;
    using CSharpDemo.IDEAs;
    using CSharpDemo.Json;
    using CSharpDemo.LINQ;
    using CSharpDemo.Parallel;
    using CSharpDemo.ReflectionDemo;
    using CSharpDemo.RetrierDir;
    using CSharpDemoAux;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.SystemFunctions;
    using Microsoft.Azure.ServiceBus;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Configuration;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Reflection;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;

    class Program
    {
        static void Main(string[] args)
        {
            //AzureCosmosDBOperation.MainMethod();
            //AzureServiceBus.MainMethod();
            //AzureCosmosDB.MainMethod();

            //QueryIncidents.MainMethod();
            //FireIncident.MainMethod();

            //DatasetJsonFileOperation.MainMethod();
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
