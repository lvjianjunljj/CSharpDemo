namespace DocFileDemo
{
    using System;
    using System.IO;
    using CsvHelper;
    using CsvHelper.Configuration;
    using CsvHelper.Configuration.Attributes;
    using CsvHelper.TypeConversion;

    class Program
    {
        static void Main(string[] args)
        {
            string readPath = @"C:\Users\jianjlv\source\repos\Ibiza\Source\DataCompliance\CreateAssetMapping\AssetReference.tsv";
            string writePath = @"C:\Users\jianjlv\source\repos\Ibiza\Source\DataCompliance\CreateAssetMapping\AssetReference2.tsv";
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
                string aa = row.AssetAliases?.Trim();
                string ag = row.AssetSecurityGroup?.Trim();
                string ad = row.AssetDependencies?.Trim();
                Console.WriteLine(ai);
                sw.WriteLine(ai);
            }
            Console.ReadKey();
        }
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
        public string AssetAliases { get; set; }

        /// <summary>
        /// Zero or more aliases referring to the same asset
        /// </summary>
        [Index(3)]
        public string AssetSecurityGroup { get; set; }

        /// <summary>
        /// Zero or more aliases referring to the same asset
        /// </summary>
        [Index(4)]
        public string AssetDependencies { get; set; }
    }
}
