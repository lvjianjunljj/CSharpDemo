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


namespace CSharpDemo
{
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

            //JsonCovertDemo.MainMethod();

            //string folderPath = @"D:\data\company_work\M365\IDEAs\DataCopServiceMonitor\datacop_service_monitor_test_file\";
            //for (int i = 1; i < 11; i++)
            //{
            //    string fileName = "datacop_service_monitor_test_2019_12_" + (i > 9 ? $"{i}" : $"0{i}");
            //    SaveFile.FirstMethod(folderPath + fileName + ".ss", fileName);
            //}
            //Console.WriteLine(DateTime.UtcNow.ToString("o"));


            TestA a = new TestA();
            a.TestB = new TestB
            {
                A = "BA"
            };
            a.Dict = new Dictionary<string, string>();
            a.Dict.Add("A", "A");

            var aa = a.GetClone();
            aa.TestB.A = "AA";
            aa.Dict["A"] = "B";
            Console.WriteLine(a.TestB.A);
            Console.WriteLine(a.Dict["A"]);

            Dictionary<string, string> dict = new Dictionary<string, string>();
            Console.WriteLine(dict.ContainsKey(""));
            Console.WriteLine(dict.ContainsKey(null));


            Console.ReadKey();
        }
    }
    public class TestA
    {
        public string StringA { get; set; }
        public string StringB { get; set; }
        public TestB TestB { get; set; }
        public Dictionary<string, string> Dict { get; set; }
        public TestA GetClone()
        {
            return (TestA)this.MemberwiseClone();
        }
    }

    public class TestB
    {
        public string A { get; set; }
    }
}