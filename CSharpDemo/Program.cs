namespace CSharpDemo
{
    using AzureLib.KeyVault;
    using CSharpDemo.Application;
    using CSharpDemo.Azure;
    using CSharpDemo.Azure.CosmosDB;
    using CSharpDemo.CSharpInDepth.Part1;
    using CSharpDemo.CSharpInDepth.Part2.CSharp2;
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
    using Microsoft.Azure.ServiceBus;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
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
            //AzureCosmosDBClientOperation.MainMethod();
            //AzureServiceBus.MainMethod();
            //AzureCosmosDB.MainMethod();

            //QueryIncidents.MainMethod();
            //FireIncident.MainMethod();

            //DatasetJsonFileOperation.MainMethod();
            //AzureActiveDirectoryToken.MainMethod();
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;


            IList<string> list = new List<string>();
            list.Add("11");
            list.Add("22");
            list.Add("33");
            var dict = list.ToDictionary(l => l.Substring(0, 1));
            foreach (var item in dict)
            {
                Console.WriteLine($"{item.Key}\t{item.Value}");
            }

            Console.WriteLine(dict.TryGetValue("12", out string ss));
            Console.WriteLine(ss);
            HashSet<string> has = new HashSet<string>(new string[] { "1", "3" });
            has.UnionWith(new HashSet<string>(new string[] { "1", "2" }));

            foreach (var item in has)
            {
                Console.WriteLine(item);
            }
            Console.ReadKey();
        }
    }
}
