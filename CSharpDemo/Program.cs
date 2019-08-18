﻿using AzureLib.KeyVault;
using CSharpDemo.Azure;
using CSharpDemo.CSharpInDepth.Part1;
using CSharpDemo.CSharpInDepth.Part2.CSharp2;
using CSharpDemo.FileOperation;
using CSharpDemo.IcMTest;
using CSharpDemo.Json;
using CSharpDemo.RetrierDir;
using CSharpDemoAux;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Net;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace CSharpDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            //AzureCosmosDB.MainMethod();
            //QueryIncidents.MainMethod();

            AzureServiceBus.MainMethod();
            //FireIncident.MainMethod();

            //RetrierDemo.MainMethod();

            Console.ReadKey();
        }

    }


}

