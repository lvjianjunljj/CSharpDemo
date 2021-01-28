﻿namespace CSharpDemo
{
    using CSharpDemo.FileOperation;
    using CSharpDemo.IDEAs;
    using CSharpDemo.InvalidCastExceptionDemo;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;

    class Program
    {
        static void Main(string[] args)
        {
            //RegexDemo.MainMethod();
            //AzureCosmosDBOperation.MainMethod();
            //AzureServiceBus.MainMethod();

            //QueryIncidents.MainMethod();
            //FireIncident.MainMethod();

            //DataCopJsonFileOperation.MainMethod();
            //DataBuildJsonFileOperation.MainMethod();
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
