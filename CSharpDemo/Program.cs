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
using CSharpDemo.Application;
using CSharpDemo.Azure;
using CSharpDemo.CSharpInDepth.Part1;
using CSharpDemo.CSharpInDepth.Part2.CSharp2;
using CSharpDemo.FileOperation;
using CSharpDemo.IcMTest;
using CSharpDemoAux;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace CSharpDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            //AzureCosmosDB.MainMethod();
            //QueryIncidents.MainMethod();

            //AzureServiceBus.MainMethod();


            DateTime date = DateTime.Parse("2019-01-10T00:00:00.0000000Z");
            while (date < DateTime.Now)
            {
                Console.WriteLine(date);
                date = date.AddDays(1);
            }

            Console.ReadKey();
        }

    }
}

