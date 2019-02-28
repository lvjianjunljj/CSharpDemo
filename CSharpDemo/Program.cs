using CSharpDemo.Azure;
using CSharpDemo.FileOperation;
using CSharpDemo.IcMTest;
using System;

namespace CSharpDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            string jsonString = QueryIncidents.GetIncident(107035078);
            Console.WriteLine(jsonString);
            SaveFile.FirstMethod(@"D:\data\company_work\IDEAs\IcMWork\test.txt", jsonString);
            Console.ReadKey();
        }

    }
}
