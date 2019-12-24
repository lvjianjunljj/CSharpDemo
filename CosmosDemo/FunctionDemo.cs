namespace CosmosDemo
{
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;

    public class FunctionDemo
    {
        public static void MainMethod()
        {
            CheckStreamExists();
            //CheckDirectoryExists();

            //GetRowCountIteratively("2019-07-10T00:00:00.0000000Z");

            //GetStreamInfosDemo();

            //CompareStreamInfoRowCount();
        }

        public static void CheckStreamExists()
        {
            string stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants/2019/07/12/TenantsHistory_2019_07_12.ss";
            //stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants/TenantsHistory.ss";
            stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants";
            //stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Ppe/local/ParquetConverter.py";

            // This link is not able to be accessed
            //stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Ppe/shares/CFR.ppe/local/Cooked/StateUserDirectory/StateUserDirectory_2019_07_24.ss";InnerException

            stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Private.Data//shares/IDEAs.Prod.Data/Private/Profiles/Tenant/Commercial/IDEAsPrivateTenantProfile/Streams/v1/2019/10/IDEAsPrivateTenantProfile_History_2019_10_01.ss";
            Console.WriteLine(CosmosClient.CheckStreamExists(stream));
        }

        public static void CheckDirectoryExists()
        {
            string directoryPath = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants/";

            Console.WriteLine(CosmosClient.CheckDirectoryExists(directoryPath));
        }

        public static void GetRowCountIteratively(string startDateString)
        {
            DateTime date = DateTime.Parse(startDateString);
            while (date < DateTime.Now)
            {
                Console.WriteLine(date);

                var year = date.Year.ToString();
                var month = date.Month.ToString("00");
                var day = date.Day.ToString("00");

                // For function GetRowCount, we just can get the row count value for file whose name ends with .ss, or it will throw an exception. Such as the below steam path, it will throw exception. But for function VC.StreamExists is OK.
                //string stream = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Ppe/local/ParquetConverter.py";

                string stream = $"https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod/local/Scheduled/Datasets/Public/Profiles/Tenants/{year}/{month}/{day}/TenantsHistory_{year}_{month}_{day}.ss";

                long? rowCount = CosmosClient.GetRowCount(stream);
                Console.WriteLine(rowCount);
                date = date.AddDays(1);
            }
        }

        public static void GetStreamInfosDemo()
        {
            string directoryPath = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod//local/Partner/PreRelease/dev/activeusage/sharepointcommercial/2019/08/";

            List<JToken> streamInfos = CosmosClient.GetStreamInfos(directoryPath);
            foreach (var streamInfo in streamInfos)
            {
                Console.WriteLine(streamInfo);
            }

            directoryPath = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod//local/Partner/PreRelease/dev/activeusage/sharepointcommercial/2019/";
            streamInfos = CosmosClient.GetStreamInfos(directoryPath);
            foreach (var streamInfo in streamInfos)
            {
                Console.WriteLine(streamInfo);
            }
        }

        public static void CompareStreamInfoRowCount()
        {
            string directoryPath = "https://cosmos14.osdinfra.net/cosmos/IDEAs.Prod//local/Partner/PreRelease/dev/activeusage/sharepointcommercial/2019/08/";

            List<JToken> streamInfos = CosmosClient.GetStreamInfos(directoryPath);
            foreach (var streamInfo in streamInfos)
            {
                long? rowCount = CosmosClient.GetRowCount(streamInfo["StreamName"].ToString());
                // They are different...So sad.
                Console.WriteLine(streamInfo["Length"]);
                Console.WriteLine(rowCount);
            }
        }
    }
}