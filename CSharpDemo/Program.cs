using CSharpDemo.Azure;
using CSharpDemo.IcMTest;
using CSharpDemo.Json;
using Newtonsoft.Json;
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
            //string jsonString = QueryIncidents.GetIncident(107035078);
            //SaveFile.FirstMethod(@"D:\data\company_work\IDEAs\IcMWork\incident_test.txt", jsonString);

            //QueryIncidents.LinkRootCause();
            //QueryIncidents.CreateRootCause();
            //string rootCauseString = QueryIncidents.GetRootCause(107063448);
            //Console.WriteLine(rootCauseString);
            //SaveFile.FirstMethod(@"D:\data\company_work\IDEAs\IcMWork\root_cause_test.txt", rootCauseString);

            //SaveFile.FirstMethod(@"D:\data\company_work\IDEAs\IcMWork\tenants_list.html", QueryIncidents.GetTenants());

            //QueryIncidents.GetIncident(1234);


            //AzureCosmosDB.MainMethod();


            Console.ReadKey();
        }
    }
}
