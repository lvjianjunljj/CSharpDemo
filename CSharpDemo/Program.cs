using CSharpDemo.Azure;
using CSharpDemo.FileOperation;
using CSharpDemo.IcMTest;
using CSharpDemo.Json;
using CSharpDemo.Parallel;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CSharpDemo
{

    class Program
    {
        static void Main(string[] args)
        {
            //QueryIncidents.MainMethod();
            //AzureCosmosDB.MainMethod();

            string filePath = @"D:\data\company_work\IDEAs\IcMWork\test\IcMKustoIncident.txt";
            StreamReader sr = new StreamReader(filePath, Encoding.UTF8);
            String line;
            List<string[]> strsList = new List<string[]>();
            while ((line = sr.ReadLine()) != null)
            {
                strsList.Add(line.Split(new char[] { '\t' }));
            }
            for (int i = 0; i < strsList.Count; i++)
            {
                Console.WriteLine(strsList[i].Length);
            }
            for (int i = 0; i < strsList[0].Length; i++)
            {
                if (strsList[1][i] != strsList[2][i] || strsList[1][i] != strsList[3][i])
                {
                    Console.WriteLine(strsList[0][i]);
                    Console.WriteLine(strsList[1][i]);
                    Console.WriteLine(strsList[2][i]);
                    Console.WriteLine(strsList[3][i]);
                }
            }

            Console.ReadKey();
        }
    }
}
