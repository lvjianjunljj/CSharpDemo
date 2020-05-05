namespace DocFileDemo
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using CsvHelper;
    using CsvHelper.Configuration;
    using CsvHelper.Configuration.Attributes;

    public class CsvFileOperationDemo
    {
        public static void MainMethod()
        {
            //SortRowByAssetId();
            //AddDisplayName();
            AddCategoryColumn();
        }

        private static void SortRowByAssetId()
        {
            Dictionary<string, string> dict = new Dictionary<string, string>();
            string readPath = @"D:\data\company_work\M365\IDEAs\accesscontrol\output\AssetReference.tsv";
            string writePath = @"D:\data\company_work\M365\IDEAs\accesscontrol\output\AssetReference2.tsv";
            Configuration cf = new Configuration()
            {
                Delimiter = "\t",
                HeaderValidated = null,
                HasHeaderRecord = false
            };
            StreamReader sr = new StreamReader(readPath);
            CsvReader csv = new CsvReader(sr, cf);
            StreamWriter sw = new StreamWriter(writePath);
            foreach (Row row in csv.GetRecords<Row>())
            {
                string ai = row.AssetIdentity?.Trim();
                string an = row.AssetDisplayName?.Trim();
                string af = row.AssetFabric?.Trim();
                string ag = row.AssetSecurityGroup?.Trim();
                string ad = row.AssetDependencies?.Trim();
                dict.Add(ai, $"\"{ai}\"\t\"{an}\"\t\"{af}\"\t\"{ag}\"\t\"{ad}\"");

            }
            List<string> ais = new List<string>(dict.Keys);
            ais.Sort();
            foreach (var ai in ais)
            {
                Console.WriteLine(ai);
                sw.WriteLine(dict[ai]);
            }
            sw.Flush();
        }


        private static void AddDisplayName()
        {
            string readPath = @"D:\IDEAs\Ibiza\Source\DataCompliance\CreateAssetMapping\AssetReference2.tsv";
            string writePath = @"D:\IDEAs\Ibiza\Source\DataCompliance\CreateAssetMapping\AssetReference.tsv";
            Configuration cf = new Configuration()
            {
                Delimiter = "\t",
                HeaderValidated = null,
                HasHeaderRecord = false
            };
            StreamReader sr = new StreamReader(readPath);
            CsvReader csv = new CsvReader(sr, cf);
            StreamWriter sw = new StreamWriter(writePath);
            foreach (Row row in csv.GetRecords<Row>())
            {
                string ai = row.AssetIdentity?.Trim();
                string af = row.AssetFabric?.Trim();
                string ag = row.AssetSecurityGroup?.Trim();
                string ad = row.AssetDependencies?.Trim();
                Console.WriteLine(ai);

                sw.WriteLine($"\"{ai}\"\t\"{ai}\"\t\"{af}\"\t\"{ag}\"\t\"{ad}\"");
            }
            sw.Flush();
        }


        private static void AddCategoryColumn()
        {
            string readPath = @"D:\IDEAs\repo\Ibiza\Source\DataCompliance\CreateAssetMapping\AssetReference2.tsv";
            string writePath = @"D:\IDEAs\repo\Ibiza\Source\DataCompliance\CreateAssetMapping\AssetReference.tsv";
            Configuration cf = new Configuration()
            {
                Delimiter = "\t",
                HeaderValidated = null,
                HasHeaderRecord = false
            };
            StreamReader sr = new StreamReader(readPath);
            CsvReader csv = new CsvReader(sr, cf);
            StreamWriter sw = new StreamWriter(writePath);
            foreach (Row row in csv.GetRecords<Row>())
            {
                string ai = row.AssetIdentity?.Trim();
                string an = row.AssetDisplayName?.Trim();
                string af = row.AssetFabric?.Trim();
                string ag = row.AssetSecurityGroup?.Trim();
                string ad = row.AssetDependencies?.Trim();
                Console.WriteLine(ai);
                string category = GetCategory(ai);

                sw.WriteLine($"\"{ai}\"\t\"{an}\"\t\"{af}\"\t\"{ag}\"\t\"{category}\"\t\"{ad}\"");
            }
            sw.Flush();
        }

        private static string GetCategory(string assetIdentity)
        {
            if (assetIdentity.StartsWith(@"Profiles"))
            {
                return "Profiles";
            }
            if (assetIdentity.StartsWith(@"Usage/ActiveUsage"))
            {
                return "ActiveUsage";
            }
            if (assetIdentity.StartsWith(@"Usage/CountedActions"))
            {
                return "CountedActionsUsage";
            }
            if (assetIdentity.StartsWith(@"Usage/TenantActiveUsage"))
            {
                return "TenantActiveUsage";
            }
            if (assetIdentity.StartsWith(@"WIND"))
            {
                return "WIND";
            }
            if (assetIdentity.StartsWith(@"Usage/CustomerHealth"))
            {
                return "CustomerHealth";
            }
            if (assetIdentity.StartsWith(@"Iris"))
            {
                return "Iris";
            }
            if (assetIdentity.StartsWith(@"CDS"))
            {
                return "FieldCDS";
            }
            if (assetIdentity.StartsWith(@"Cube"))
            {
                return "ConsumerCubes";
            }
            return string.Empty;
        }

        private class Row
        {
            /// <summary>
            /// A distinct asset identifier
            /// </summary>
            [Index(0)]
            public string AssetIdentity { get; set; }

            /// <summary>
            /// The asset display name
            /// </summary>
            [Index(1)]
            public string AssetDisplayName { get; set; }

            /// <summary>
            /// Indication of Cosmos, SQL, etc.
            /// </summary>
            [Index(2)]
            public string AssetFabric { get; set; }

            /// <summary>
            /// The asset security group name
            /// </summary>
            [Index(3)]
            public string AssetSecurityGroup { get; set; }

            /// <summary>
            /// Zero or more dependencies to the same asset
            /// </summary>
            [Index(4)]
            public string AssetDependencies { get; set; }
        }
    }
}
