using AzureLib.KeyVault;
using CSharpDemo.Azure;
using CSharpDemo.Azure.CosmosDB;
using CSharpDemo.CSharpInDepth.Part1;
using CSharpDemo.CSharpInDepth.Part2.CSharp2;
using CSharpDemo.FileOperation;
using CSharpDemo.IcMTest;
using CSharpDemo.IDEAs;
using CSharpDemo.Json;
using CSharpDemo.Parallel;
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
using System.IO;
using System.Net;
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
            AzureCosmosDBClientOperation.MainMethod();
            //AzureServiceBus.MainMethod();

            //QueryIncidents.MainMethod();
            //FireIncident.MainMethod();

            //DatasetJsonFileOperation.MainMethod();


            Console.WriteLine(JsonConvert.SerializeObject(DateTime.Parse("2019-06-01").ToString("u")).Trim('"'));



            //TimeSpan t1 = JsonConvert.DeserializeObject<TimeSpan>("48:00:00");
            //TimeSpan t2 = JsonConvert.DeserializeObject<TimeSpan>("48.00:00:00");

            TimeSpan t1 = TimeSpan.Parse("24:00:00");
            TimeSpan t2 = new TimeSpan(24, 0, 0, 0);
            Console.WriteLine(t1 == t2);

            Console.WriteLine(t1);
            Console.WriteLine(t2);


            string timeSpanTestClassStr = "{TimeSpanTest : '24:00:00'}";
            JObject timeSpanTestClassJObject = JObject.Parse(timeSpanTestClassStr);
            Console.WriteLine(timeSpanTestClassJObject["TimeSpanTest"]);
            TimeSpanTestClass timeSpanTestClass = timeSpanTestClassJObject.ToObject<TimeSpanTestClass>();
            Console.WriteLine(timeSpanTestClass.TimeSpanTest);


            Console.WriteLine(JsonConvert.SerializeObject(timeSpanTestClass));
            Console.ReadKey();

        }

        class TimeSpanTestClass
        {
            public TimeSpan TimeSpanTest { get; set; }
        }
    }
}

