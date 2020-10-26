namespace SQLDemo
{
    using AzureLib.KeyVault;
    using SQLDemo.DataAccessLayer;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    class Program
    {
        static void Main(string[] args)
        {
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            //string connectionString = secretProvider.GetSecretAsync("datacop-prod", "IDEAsPortalPPEConnectionString").Result;
            string connectionString = secretProvider.GetSecretAsync("csharpmvcwebapikeyvault", "SQLConnectionString").Result;
            var sqlDal = new SqlDataAccessLayer(connectionString);
            //string cmdText = "SELECT Date, CONVERT(decimal, SUM([Value])) AS MetricValue FROM [DataQuality].[CommercialActiveUsageStats] WITH (NOLOCK) WHERE Date = @Date AND DataRefreshState = 'Stable' AND DateAggregationType = 'RL28' AND [Key] IN ('Platform_web_Count') AND [Workload] IN ('Exchange') GROUP BY Date ORDER BY Date DESC";
            string cmdText = @"SELECT count([userid]) as MetricValue
                                  FROM[dbo].[user_info]
                                where[userid] > 3";
            //Console.WriteLine(GetMetricValueAsDouble(sqlDal, "testName", CommandType.Text, cmdText, DateTime.UtcNow.AddDays(-1), 300).Result);
            cmdText = @"SELECT CAST(1 AS BIT) as MetricValue
                           FROM[dbo].[user_info]
                        where[userid] > 6";
            cmdText = @"SELECT CASE WHEN EXISTS (
                            SELECT *
                            FROM[dbo].[user_info]
                            where[userid] = 2
                        )
                        THEN CAST(1 AS BIT)
                        ELSE CAST(0 AS BIT) END
                        as MetricValue";
            Console.WriteLine(GetMetricValueAsBool(sqlDal, "testName", CommandType.Text, cmdText, DateTime.UtcNow.AddDays(-1), 300).Result);

            Console.ReadKey();
        }


        /// <summary>
        /// Private helper to get metric value.
        /// </summary>
        /// <param name="sqlDal">The SQL data access layer that will communicate with the SQL database.</param>
        /// <param name="testName">The name of test for logging purposes.</param>
        /// <param name="cmdType">The type of the commmand (text, stored procedure, or table direct).</param>
        /// <param name="cmdText">command to run.</param>
        /// <param name="date">The date of the metric.</param>
        /// <param name="cmdTimeoutInSeconds">The command timeout in seconds.</param>
        /// <returns>Task&lt;System.Double&gt;.</returns>
        /// <exception cref="ArgumentException"></exception>
        private static async Task<double> GetMetricValueAsDouble(ISqlDataAccessLayer sqlDal, string testName, CommandType cmdType,
            string cmdText, DateTime date, int cmdTimeoutInSeconds)
        {
            var dateParam = new SqlParameter("@Date", SqlDbType.DateTime)
            {
                Value = date
            };

            var sqlCommand = sqlDal.CreateSqlCommand(cmdType, cmdText, new List<SqlParameter> { dateParam }, cmdTimeoutInSeconds);
            var metricValue = await sqlDal.GetQueryResultAsDouble(sqlCommand, "MetricValue");
            //var metricValue = await sqlDal.GetQueryResult(sqlCommand, "MetricValue", false);
            //var metricValue = await sqlDal.GetQueryResult(sqlCommand, "MetricValue", Double.NaN);


            // Throw an exception when either of the dates returns no metric value (Double.NaN)
            if (Double.IsNaN(metricValue))
            {
                var msg = $"The database does not have metrics for the test {testName} for date {date}.";
                throw new Exception(msg);
            }

            return metricValue;
        }

        /// <summary>
        /// Private helper to get metric value.
        /// </summary>
        /// <param name="sqlDal">The SQL data access layer that will communicate with the SQL database.</param>
        /// <param name="testName">The name of test for logging purposes.</param>
        /// <param name="cmdType">The type of the commmand (text, stored procedure, or table direct).</param>
        /// <param name="cmdText">command to run.</param>
        /// <param name="date">The date of the metric.</param>
        /// <param name="cmdTimeoutInSeconds">The command timeout in seconds.</param>
        /// <returns>Task&lt;System.Double&gt;.</returns>
        /// <exception cref="ArgumentException"></exception>
        private static async Task<bool> GetMetricValueAsBool(ISqlDataAccessLayer sqlDal, string testName, CommandType cmdType,
            string cmdText, DateTime date, int cmdTimeoutInSeconds)
        {
            var dateParam = new SqlParameter("@Date", SqlDbType.DateTime)
            {
                Value = date
            };

            var sqlCommand = sqlDal.CreateSqlCommand(cmdType, cmdText, new List<SqlParameter> { dateParam }, cmdTimeoutInSeconds);
            var metricValue = await sqlDal.GetQueryResultAsBool(sqlCommand, "MetricValue");

            return metricValue;
        }
    }
}
