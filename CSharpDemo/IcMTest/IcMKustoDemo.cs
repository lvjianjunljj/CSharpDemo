using System;
using System.Collections.Generic;
using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Common;
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
            // All the failed test demo.

            //var client = Kusto.Data.Net.Client.KustoClientFactory.CreateCslQueryProvider("https://help.kusto.windows.net/Samples;Fed=true");
            //var reader = client.ExecuteQuery("demo_make_series1| limit 10");
            //Console.WriteLine(reader["TimeStamp"]);

            //ICslQueryProvider provider = KustoClientFactory.CreateCslQueryProvider("https://icmclusterlb.kustomfa.windows.net");
            //IDataReader queryKusto = QueryKusto(provider, "IcmDataWarehouse", "IncidentDescriptions | where IncidentId == 109301355 | distinct  DescriptionId, Date, ChangedBy, Text");

            //        var kcsb = new KustoConnectionStringBuilder("https://icmclusterlb.kustomfa.windows.net/IcmDataWarehouse").WithAadUserPromptAuthentication();

            //        var kustoConnectionStringBuilder =
            //new KustoConnectionStringBuilder(@"https://icmclusterlb.kusto.windows.net/")
            //{
            //    FederatedSecurity = true,
            //    ApplicationClientId = "DataCop",
            //    ApplicationKey = "1d4r1I+DZATVj+qk3xZCfwoCQcBySATs1bntEN7aBNM=",
            //    Authority = "cc6b8d55-ff41-44f8-abe6-057b650dad95"
            //};

            //using (var client = KustoClientFactory.CreateCslAdminProvider(kustoConnectionStringBuilder))
            //{
            //    var diagnosticsCommand = CslCommandGenerator.GenerateShowDatabasePrincipalsCommand("IcmDataWarehouse");
            //    var objectReader = new ObjectReader<DiagnosticsShowCommandResult>(
            //        client.ExecuteControlCommand(diagnosticsCommand));
            //    DiagnosticsShowCommandResult diagResult = objectReader.ToList().FirstOrDefault();
            //    Console.WriteLine(diagResult);
            //    // DO something with the diagResult    
            //}

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
            IDataReader reader = client.ExecuteQuery(query);
            //SaveFile.FirstMethod(@"D:\data\company_work\IDEAs\IcMWork\test\KustoTest.txt", reader.ToText());
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
