namespace SQLDemo
{
    using AzureLib.KeyVault;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(1234);
            ISecretProvider secretProvider = KeyVaultSecretProvider.Instance;
            string connectionString = secretProvider.GetSecretAsync("datacopprod",
                "IDEAsPortalPPEConnectionString").Result;
            Console.WriteLine(connectionString);
            var sqlDal = new SqlDataAccessLayer(connectionString);
            string cmdText = "SELECT Date, CONVERT(decimal, SUM([Value])) AS MetricValue FROM [DataQuality].[CommercialActiveUsageStats] WITH (NOLOCK) WHERE Date = @Date AND DataRefreshState = 'Stable' AND DateAggregationType = 'RL28' AND [Key] IN ('Platform_web_Count') AND [Workload] IN ('Exchange') GROUP BY Date ORDER BY Date DESC";
            Console.WriteLine(GetMetricValue(sqlDal, "testName", CommandType.Text, cmdText, DateTime.UtcNow.AddDays(-1), 300).Result);
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
        private static async Task<double> GetMetricValue(ISqlDataAccessLayer sqlDal, string testName, CommandType cmdType,
            string cmdText, DateTime date, int cmdTimeoutInSeconds)
        {
            var dateParam = new SqlParameter("@Date", SqlDbType.DateTime);
            dateParam.Value = date;

            var sqlCommand = sqlDal.CreateSqlCommand(cmdType, cmdText, new List<SqlParameter> { dateParam }, cmdTimeoutInSeconds);
            var metricValue = await sqlDal.GetQueryResultAsDouble(sqlCommand, "MetricValue");

            // Throw an exception when either of the dates returns no metric value (Double.NaN)
            if (Double.IsNaN(metricValue))
            {
                var msg = $"The database does not have metrics for the test {testName} for date {date}.";
                throw new Exception(msg);
            }

            return metricValue;
        }
    }

    public interface ISqlDataAccessLayer : IDisposable
    {
        /// <summary>
        /// Asynchronously run the given command in the SQL database.
        /// </summary>
        /// <param name="sqlCommand">The command to be run by SQL data access layer.</param>
        /// <param name="columnName">The column name that has a double to be extracted.</param>
        /// <returns>The double in the first row of the given column or Double.NaN otherwise.</returns>
        Task<double> GetQueryResultAsDouble(SqlCommand sqlCommand, string columnName);

        /// <summary>
        /// Asynchronously check if the given command will return any row in SQL.
        /// </summary>
        /// <param name="sqlCommand">The command to be run by SQL data access layer.</param>
        /// <returns>whether the given command return any row.</returns>
        Task<bool> HasRows(SqlCommand sqlCommand);

        /// <summary>
        /// Private helper method to create a SqlCommand.
        /// </summary>
        /// <param name="cmdType">The type of the command (text, stored procedure, or table direct).</param>
        /// <param name="cmdText">The command to run.</param>
        /// <param name="parameters">The list of parameters required by the command.</param>
        /// <param name="cmdTimeoutInSeconds">The command timeout in seconds.</param>
        /// <returns></returns>
        SqlCommand CreateSqlCommand(CommandType cmdType, string cmdText, List<SqlParameter> parameters, int cmdTimeoutInSeconds = 60);
    }

    public sealed class SqlDataAccessLayer : ISqlDataAccessLayer
    {
        /// <summary>
        /// The number of retries on failure
        /// </summary>
        private const short MAX_RETRIES = 3;

        /// <summary>
        /// The base time in seconds for thread to sleep.
        /// This number will be multiplied for each failure.
        /// </summary>
        private const short SLEEP_TIME_ON_RETRY_IN_SECONDS = 5;

        /// <summary>
        /// The transient error codes
        /// https://docs.microsoft.com/en-us/azure/sql-database/sql-database-develop-error-messages
        /// </summary>
        private static readonly ISet<int> TransientErrorNumbers = new HashSet<int> { -2, 4060, 40501, 40613, 49918, 49919, 49920, 4221 };

        /// <summary>
        /// The disposed
        /// </summary>
        private bool disposed;
        /// <summary>
        /// The connection
        /// </summary>
        private SqlConnection connection;

        /// <summary>
        /// Constructs a SQL data access layer that will communicate with a database using the given connection string.
        /// </summary>
        /// <param name="connectionString">The connection string that allows communication with a certain SQL database.</param>
        /// <exception cref="ArgumentException"></exception>
        public SqlDataAccessLayer(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                var msg = "Invalid parameter passed to SqlDataAccessLayer ctor - connectionString cannot be null or empty.";
                throw new ArgumentException(msg);
            }

            this.disposed = false;
            this.connection = new SqlConnection(connectionString);

            //if (connectionString.IndexOf("Password", StringComparison.OrdinalIgnoreCase) < 0 && connectionString.IndexOf("PWD", StringComparison.OrdinalIgnoreCase) < 0)
            //{
            //    var tokenProvider = new AzureServiceTokenProvider();
            //    connection.AccessToken = tokenProvider.GetAccessTokenAsync("https://database.windows.net/").Result;
            //}
        }

        /// <summary>
        /// Asynchronously run the given command in the SQL database.
        /// An exception will be thrown if there are multiple rows returned.
        /// </summary>
        /// <param name="sqlCommand">The command to be run by SQL data access layer.</param>
        /// <param name="columnName">The column name that has a double to be extracted.</param>
        /// <returns>The double in the first row of the given column if any or Double.NaN otherwise.</returns>
        /// <exception cref="ArgumentException">
        /// </exception>
        public async Task<double> GetQueryResultAsDouble(SqlCommand sqlCommand, string columnName)
        {
            if (string.IsNullOrEmpty(columnName))
            {
                var msg = "Invalid parameter passed to GetQueryResultAsDouble - columnName cannot be null or empty.";
                throw new ArgumentException(msg);
            }

            double result = Double.NaN;
            try
            {
                using (var sqlDataReader = await ResilientlyExecuteCommand(sqlCommand))
                {
                    if (sqlDataReader.Read())
                    {
                        // The metric value types in the databases currently are bigint and numeric.
                        // In both cases, the limit of double before it's losing precision of integers is 2^53.
                        // That number is way beyond the number that metric value will ever hit.
                        result = double.Parse(sqlDataReader[columnName].ToString());

                        // check for duplicate rows after reading the first row
                        if (sqlDataReader.Read())
                        {
                            var msg = "Invalid cmdText passed to GetQueryResultAsDouble - database returns more than one row.";
                            throw new Exception(msg);
                        }
                    }
                }
            }
            finally
            {
                this.connection.Close();
            }

            return result;
        }

        /// <summary>
        /// Asynchronously check if the given command will return any row in SQL.
        /// </summary>
        /// <param name="sqlCommand">The command to be run by SQL data access layer.</param>
        /// <returns>whether the given command return any row.</returns>
        public async Task<bool> HasRows(SqlCommand sqlCommand)
        {
            try
            {
                using (var sqlDataReader = await ResilientlyExecuteCommand(sqlCommand))
                {
                    return sqlDataReader.HasRows;
                }
            }
            finally
            {
                this.connection.Close();
            }
        }

        /// <summary>
        /// Private helper method to create a SqlCommand.
        /// </summary>
        /// <param name="cmdType">The type of the command (text, stored procedure, or table direct).</param>
        /// <param name="cmdText">The command to run.</param>
        /// <param name="parameters">The list of parameters required by the command.</param>
        /// <param name="cmdTimeoutInSeconds">The command timeout in seconds.</param>
        /// <returns>SqlCommand.</returns>
        /// <exception cref="ArgumentException"></exception>
        public SqlCommand CreateSqlCommand(CommandType cmdType, string cmdText, List<SqlParameter> parameters = null,
            int cmdTimeoutInSeconds = 60)
        {
            if (string.IsNullOrEmpty(cmdText))
            {
                var msg = "Invalid parameter passed to SqlDataAccessLayer.GetQueryResultAsDouble - cmdText cannot be null or empty.";
                throw new ArgumentException(msg);
            }

            // TODO : FxCop warning, this variable cmdText is susceptible to SQL injection. 
            // Consider using a stored proc or parameterized SQL query instead of building the query with string concatenations. 
            var sqlCommand = new SqlCommand(cmdText, this.connection);
            sqlCommand.CommandType = cmdType;
            sqlCommand.CommandTimeout = cmdTimeoutInSeconds;
            if (parameters != null)
            {
                parameters.ForEach(delegate (SqlParameter parameter) { sqlCommand.Parameters.Add(parameter); });
            }

            return sqlCommand;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            InternalDispose();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Internals the dispose.
        /// </summary>
        private void InternalDispose()
        {
            if (!this.disposed)
            {
                this.connection.Dispose();
                this.disposed = true;
            }
        }

        /// <summary>
        /// Resiliently try to execute command MAX_RETRIES times when timeout or transient error happens
        /// </summary>
        /// <param name="sqlCommand">The command to be executed</param>
        /// <returns></returns>
        private async Task<SqlDataReader> ResilientlyExecuteCommand(SqlCommand sqlCommand)
        {
            for (int i = 0; i < MAX_RETRIES; i++)
            {
                try
                {
                    this.connection.Open();
                    return await sqlCommand.ExecuteReaderAsync();
                }
                catch (SqlException ex)
                {
                    // Sleep then retry when it's a timeout or a transient error
                    // Closing and reopening fresh connection per SQL retry best practices
                    if (TransientErrorNumbers.Contains(ex.Number))
                    {
                        this.connection.Close();
                        Thread.Sleep((int)Math.Pow(SLEEP_TIME_ON_RETRY_IN_SECONDS, i + 1) * 1000);
                        continue;
                    }
                    else
                    {
                        throw ex;
                    }
                }
            }

            this.connection.Open();
            return await sqlCommand.ExecuteReaderAsync();
        }

        /// <summary>
        /// Finalizes an instance of the <see cref="SqlDataAccessLayer"/> class.
        /// </summary>
        ~SqlDataAccessLayer()
        {
            InternalDispose();
        }
    }
}
