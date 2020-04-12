namespace DocFileDemo
{
    using System;
    using System.IO;
    using CsvHelper;
    using CsvHelper.Configuration;
    using CsvHelper.Configuration.Attributes;

    public class CsvFileOperationDemo
    {
        public static void MainMethod()
        {
            AddDisplayName();
            Console.ReadKey();
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
                string category = GetCategory(ai);

                sw.WriteLine($"\"{ai}\"\t\"{af}\"\t\"{ag}\"\t\"{ad}\"\t\"{category}\"");
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
            return "None";
        }

        class Row
        {
            /// <summary>
            /// A distinct asset identifier
            /// </summary>
            [Index(0)]
            public string AssetIdentity { get; set; }

            /// <summary>
            /// Indication of Cosmos, SQL, etc.
            /// </summary>
            [Index(1)]
            public string AssetFabric { get; set; }

            /// <summary>
            /// Zero or more aliases referring to the same asset
            /// </summary>
            [Index(2)]
            public string AssetSecurityGroup { get; set; }

            /// <summary>
            /// Zero or more aliases referring to the same asset
            /// </summary>
            [Index(3)]
            public string AssetDependencies { get; set; }
        }
    }
}
