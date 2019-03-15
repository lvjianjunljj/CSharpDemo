using CSharpDemo.Azure;
using CSharpDemo.FileOperation;
using CSharpDemo.IcMTest;
using CSharpDemo.Json;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace CSharpDemo
{

    class Program
    {
        static void Main(string[] args)
        {
            //QueryIncidents.MainMethod();
            AzureCosmosDB.MainMethod();

            Console.ReadKey();
        }
    }
}
