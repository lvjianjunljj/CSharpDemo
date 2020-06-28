namespace CSharpDemo
{
    using AzureLib.KeyVault;
    using CSharpDemo.Application;
    using CSharpDemo.Azure;
    using CSharpDemo.Azure.CosmosDB;
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


            ReflectionTest test = new ReflectionTest();
            test.Set = new HashSet<string> { "1", "2" };
            test.Dict = new Dictionary<string, string>
            {
                ["1"] = "1",
                ["2"] = "2",
                ["3"] = null,
            };
            Console.WriteLine(test.Dict.ContainsKey("3"));
            test.Array = new string[] { "1", "2" };
            test.Char = '1';
            test.Boolean = true;
            test.Enum = ReflectionEnum.One;
            var properties = test.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.GetProperty);

            Console.WriteLine(JsonConvert.SerializeObject(test));
            Console.WriteLine("---------------");

            //object property = setProperty.GetValue(test);

            foreach (var property in properties)
            {
                var type = property?.GetValue(test)?.GetType();
                string propertyStr = ConvertPropertyValueToString(property?.GetValue(test));

                Console.WriteLine(type);
                Console.WriteLine(property?.GetValue(test)?.ToString());
                Console.WriteLine(propertyStr);
                Console.WriteLine("-------------------");
            }




            string serial = JsonConvert.SerializeObject(test);

            Console.WriteLine(serial);
            var tes = JsonConvert.DeserializeObject<ReflectionTest>(serial);
            Console.WriteLine(tes.Int);
            var array = JsonConvert.DeserializeObject<string[]>("");
            // value cannot be null
            //array = JsonConvert.DeserializeObject<string[]>(null);
            Console.ReadKey();
        }

        private static string ConvertPropertyValueToString(object propertyValueObject)
        {
            if (propertyValueObject == null)
            {
                return string.Empty;
            }

            if (propertyValueObject is string || propertyValueObject is char || propertyValueObject is Enum)
            {
                return propertyValueObject.ToString();
            }

            return JsonConvert.SerializeObject(propertyValueObject);
        }
    }

    class ReflectionTest
    {
        public HashSet<string> Set { get; set; }
        public Dictionary<string, string> Dict { get; set; }
        public string[] Array { get; set; }
        public string Str { get; set; }
        public char Char { get; set; }
        public bool Boolean { get; set; }
        public float Float { get; set; }
        public int Int { get; set; }
        public ReflectionEnum Enum { get; set; }
    }

    enum ReflectionEnum
    {
        One = 1,
        Two = 2,
        Threee = 3
    }
}
