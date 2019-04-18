using CSharpDemo.IcMTest;
using Microsoft.AzureAd.Icm.Types;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CSharpDemo
{
    class ExceptionDemo
    {
        static void MainMethod()
        {
            try
            {
                ExceptionDemo exceptionDemo = new ExceptionDemo();
                Task t = exceptionDemo.TestMethod();
                t.Wait();
            }
            catch (Exception e)
            {
                while (e.InnerException != null)
                {
                    Console.WriteLine($"exception message: {e.Message}");
                    e = e.InnerException;
                }
                Console.WriteLine($"fianl exception message: {e.Message}");
            }

            Console.ReadKey();
        }
        async Task TestMethod()
        {
            await Task.Delay(TimeSpan.FromSeconds(1));
            IEnumerable<Incident> incidentList;
            //throw new AggregateException("test exception");
            QueryIncidents queryIncidents = new QueryIncidents();
            incidentList = queryIncidents.GetIncidentList<Incident>("<The SQL oncall team>", DateTime.UtcNow.AddDays(-1));
            foreach (Incident incident in incidentList) Console.WriteLine(incident.OwningTeamId);
        }
    }
}
