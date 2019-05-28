using CSharpDemo.IcMTest;
using Microsoft.AzureAd.Icm.Types;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CSharpDemo
{
    class ExceptionDemo
    {
        public static void MainMethod()
        {
            InnerExceptionDemo();
            ThrowKeyWordDemo();
        }

        static void ThrowKeyWordDemo()
        {
            try
            {
                int i = 0;
                try
                {
                    int b = 2 / i;
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Before throw in try block, error message: {e.Message}");
                    // Key word throw just can be used in catch block, and throw the same exception.
                    // In catch block, before this key word, we usually log the error message.
                    throw;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        static void InnerExceptionDemo()
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
