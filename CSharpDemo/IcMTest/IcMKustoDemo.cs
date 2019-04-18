using System;
using System.Collections.Generic;
using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Net.Client;
using System.Data;

/*
 * We need to install Microsoft.Azure.Kusto.Data from Nuget 
 */
namespace CSharpDemo.IcMTest
{
    class IcMKustoDemo
    {
        public static void MainMethod()
        {
            string query = @"let idx = 0;
                        let cutoffDate = datetime_add('month',-idx , endofmonth(now()));
                        Incidents
                        | where ModifiedDate <= cutoffDate and SourceCreatedBy == 'DataCopMonitor'
                        | summarize arg_max(ModifiedDate, OwningContactAlias, Status, CreateDate, ImpactStartDate, ResolveDate) by IncidentId
                        | where Status == 'ACTIVE'
                        | distinct IncidentId
                        | count";

            string url = @"https://icmcluster.kusto.windows.net/IcmDataWarehouse";
            var kustoConnectionStringBuilder2 = new KustoConnectionStringBuilder(url)
            {
                FederatedSecurity = true,
                ApplicationClientId = "",
                ApplicationKey = "",
                Authority = ""
            };

            var client = KustoClientFactory.CreateCslQueryProvider(kustoConnectionStringBuilder2);
            Console.WriteLine(client.DefaultDatabaseName);
            client.DefaultDatabaseName = "IcmDataWarehouse";
            IDataReader reader = client.ExecuteQuery(query);
            IList<string[]> a = reader.ToStringColumns(false);
            Console.WriteLine(a.Count);
            foreach (string[] strs in a)
            {
                foreach (string str in strs)
                {
                    Console.Write(str + "\t");
                }
                Console.WriteLine();
            }
        }
    }
}
