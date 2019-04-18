using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using CSharpDemo.Azure;
using CSharpDemo.IcMTest;
using CSharpDemo.Parallel;
using CSharpDemo.SingletonDemo;
using Microsoft.AzureAd.Icm.Types;

namespace CSharpDemo
{
    class Test
    {
        public string A { get; set; }
        public string B { get; set; }
    }
    class Program
    {
        static void Main(string[] args)
        {
            //IcMKustoDemo.MainMethod();
            //AzureCosmosDB.MainMethod();
            //IcMKustoDemo.MainMethod();
            //QueryIncidents.MainMethod();

            Console.ReadKey();
        }
    }
}
