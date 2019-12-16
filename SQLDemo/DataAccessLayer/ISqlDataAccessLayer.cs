namespace SQLDemo.DataAccessLayer
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Threading.Tasks;

    /// <summary>
    /// ISqlDataAccessLayer is an interface for all SQL data access layers that will communicate a SQL database.
    /// </summary>
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
        /// Asynchronously run the given command in the SQL database.
        /// </summary>
        /// <param name="sqlCommand">The command to be run by SQL data access layer.</param>
        /// <param name="columnName">The column name that has a double to be extracted.</param>
        /// <returns>The bit value in the first row of the given column or false otherwise.</returns>
        Task<bool> GetQueryResultAsBool(SqlCommand sqlCommand, string columnName);

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
}
