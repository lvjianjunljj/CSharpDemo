using System;
using System.Collections.Generic;
using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Net.Client;
using System.Data;

/*
 * We need to install Microsoft.Azure.Kusto.Data from Nuget 
 * You can get the config value from the doc(It's my own doc, you don't have permission to access it, HAAAAAAAAA): https://microsoftapc-my.sharepoint.com/personal/jianjlv_microsoft_com/_layouts/OneNote.aspx?id=%2Fpersonal%2Fjianjlv_microsoft_com%2FDocuments%2FJianjun%20%40%20Microsoft&wd=target%28Work.one%7C08C13A75-D69C-49FE-8D53-8DBF6710CCF0%2FKusto%7CAA3603CD-57C4-4547-B6A2-EAE9A7063F31%2F%29
onenote:https://microsoftapc-my.sharepoint.com/personal/jianjlv_microsoft_com/Documents/Jianjun%20@%20Microsoft/Work.one#Kusto&section-id={08C13A75-D69C-49FE-8D53-8DBF6710CCF0}&page-id={AA3603CD-57C4-4547-B6A2-EAE9A7063F31}&end
 */
namespace AzureKustoDemo
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
                ApplicationClientId = "83ac8948-e5e1-4bbd-97ea-798a13dc8bc6",
                ApplicationKey = "1d4r1I+DZATVj+qk3xZCfwoCQcBySATs1bntEN7aBNM=",
                Authority = "72f988bf-86f1-41af-91ab-2d7cd011db47"
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
