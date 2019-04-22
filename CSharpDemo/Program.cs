using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using CSharpDemo.Azure;
using CSharpDemo.IcMTest;
using CSharpDemo.Parallel;
using CSharpDemo.ReflectionDemo;
using CSharpDemo.SingletonDemo;
using CSharpDemoAux;
using Microsoft.AzureAd.Icm.Types;

namespace CSharpDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            //IcMKustoDemo.MainMethod();
            //AzureCosmosDB.MainMethod();
            //QueryIncidents.MainMethod();

            //ReferenceDemoAux referenceDemoAux = new ReferenceDemoAux();
            //referenceDemoAux.TestReferenceError();


            AzureServiceBusSingleClient azureServiceBusSingleClient = new AzureServiceBusSingleClient();
            azureServiceBusSingleClient.NumberOfMessages = 10;
            azureServiceBusSingleClient.SendMainAsync().GetAwaiter().GetResult();
            azureServiceBusSingleClient.ReceiveMainAsync().GetAwaiter().GetResult();



            Console.ReadKey();
        }
    }
}
