//------------------------------------------------------
// <copyright file="filename" company="Microsoft">
//      Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//
// @owner kebocksr
//
// @description The SqlComposable operator for executing queries in SQL Server.
//
//------------------------------------------------------

#pragma once

#include "ScopeCommonError.h"
#include "ScopeContainers.h"
#include "ScopeEngine.h"

#include <sql.h>
#include <sqlext.h>

#include <map>
#include <memory>
#include <string>
#include <thread>

namespace ScopeEngine
{
    class IncrementalAllocator;

    /// <summary>
    ///   C++ exception wrapping ODBC errors.
    /// </summary>
    class OdbcException : public ExceptionWithStack
    {
    public:
        /// <summary>
        ///   Verify the return code of a ODBC function call.
        ///   Logs any information messages or throws an exception on error.
        /// </summary>
        SCOPE_ENGINE_API static void VerifyOdbcReturn(SQLRETURN rc, SQLSMALLINT handleType, SQLHANDLE handle);

        SCOPE_ENGINE_API OdbcException();
        SCOPE_ENGINE_API OdbcException(SQLSMALLINT handleType, SQLHANDLE handle);
        SCOPE_ENGINE_API OdbcException(const OdbcException& other);
        SCOPE_ENGINE_API virtual ~OdbcException() override;
        SCOPE_ENGINE_API virtual ExceptionWithStack* Clone() const override;

        SCOPE_ENGINE_API virtual const char* what() const override;
    };

    /// <summary>
    ///   C++ wrapper for ODBC statement handles.
    /// </summary>
    class OdbcStatement
    {
        SQLHSTMT m_statement{ SQL_NULL_HANDLE };

    public:
        OdbcStatement() = default;
        OdbcStatement(SQLHDBC connection);
        OdbcStatement(const OdbcStatement& other) = delete;
        OdbcStatement(OdbcStatement&& other)
            : m_statement(other.m_statement)
        {
            other.m_statement = SQL_NULL_HANDLE;
        }
        ~OdbcStatement();

        OdbcStatement& operator=(const OdbcStatement& other) = delete;

        OdbcStatement& operator=(OdbcStatement&& other);

        /// <summary>
        ///   Execute the given query string.
        ///   The result set will be positioned at the row before the first row. Before retrieving the first row
        ///   OdbcStatement::Next() has to be called. Before executing a new statement any previously opened result
        ///   pointers have to be closed.
        /// </summary>
        ///
        /// <returns>Number of columns available in the result set.</returns>
        int Execute(const std::wstring& query);

        /// <summary>
        ///   Closes the result set.
        ///   After closing the result set the statement can be reused for the next query.
        /// </summary>
        void Close();

        /// <summary>
        ///  Moves the result set pointer to the next row. A result set must have been opened before.
        /// </summary>
        ///
        /// <returns>True in case a new row was fetched, false otherwise.</returns>
        bool Next();

        /// <summary>
        ///   Reads the data from the column into the C type. The result set pointer must be pointing to a valid row.
        /// </summary>
        void ReadData(SQLUSMALLINT columnId, SQLSMALLINT targetType, SQLPOINTER targetValue, SQLLEN bufferLength,
            SQLLEN* outLength);
    };

    inline bool OdbcStatement::Next()
    {
        auto rc = SQLFetch(m_statement);
        if (rc == SQL_NO_DATA)
        {
            return false;
        }
        if (rc != SQL_SUCCESS)
        {
            OdbcException::VerifyOdbcReturn(rc, SQL_HANDLE_STMT, m_statement);
        }
        return true;
    }

    inline void OdbcStatement::ReadData(SQLUSMALLINT columnId, SQLSMALLINT targetType, SQLPOINTER targetValue,
        SQLLEN bufferLength, SQLLEN* outLength)
    {
        auto rc = SQLGetData(m_statement, columnId, targetType, targetValue, bufferLength, outLength);
        // Do not log SQL_SUCCESS_WITH_INFO messages.
        // SQLGetData returns SQL_SUCCESS_WITH_INFO when the value is too large to completely fit into the target object
        // (i.e. result string to large to fit into allocated string).
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        {
            OdbcException::VerifyOdbcReturn(rc, SQL_HANDLE_STMT, m_statement);
        }
    }

    /// <summary>
    ///   C++ wrapper for ODBC connection handles.
    /// </summary>
    class OdbcConnection
    {
        SQLHDBC m_connection{ SQL_NULL_HANDLE };

    public:
        OdbcConnection() = default;

        OdbcConnection(SQLHENV environment);

        OdbcConnection(const OdbcConnection& other) = delete;

        OdbcConnection(OdbcConnection&& other)
            : m_connection(other.m_connection)
        {
            other.m_connection = SQL_NULL_HANDLE;
        }

        ~OdbcConnection();

        OdbcConnection& operator=(const OdbcConnection& other) = delete;

        OdbcConnection& operator=(OdbcConnection&& other);

        /// <summary>
        ///   Connects to the data source specified in the connection string.
        /// </summary>
        void Connect(const std::wstring& connectionString);

        /// <summary>
        ///   Disconnects from the database.
        /// </summary>
        void Disconnect();

        /// <summary>
        ///   Creates a new statement object associated with this connection.
        /// </summary>
        OdbcStatement CreateStatement()
        {
            return OdbcStatement(m_connection);
        }
    };

    /// <summary>
    ///   C++ wrapper for ODBC environment handles.
    /// </summary>
    class OdbcEnvironment
    {
        SQLHENV m_environment;

    public:
        OdbcEnvironment();

        OdbcEnvironment(const OdbcEnvironment& other) = delete;

        OdbcEnvironment(OdbcEnvironment&& other)
            : m_environment(other.m_environment)
        {
            other.m_environment = SQL_NULL_HANDLE;
        }

        ~OdbcEnvironment();

        OdbcEnvironment& operator=(const OdbcEnvironment& other) = delete;

        OdbcEnvironment& operator=(OdbcEnvironment&& other);

        /// <summary>
        ///   Creates a new connection object associated with this environment.
        /// </summary>
        OdbcConnection CreateConnection()
        {
            return OdbcConnection(m_environment);
        }
    };

    /// <summary>
    ///   External shared memory table in SQL Server.
    /// </summary>
    class SharedMemoryTable
    {
        /// <summary>
        ///   SQL schema of the shared memory table.
        /// </summary>
        std::wstring m_schema;

        /// <summary>
        ///   Name of the external shared memory table (generated GUID at runtime).
        /// </summary>
        std::wstring m_tableName;

    public:
        SharedMemoryTable(std::wstring&& schema);

        /// <summary>
        ///   Create the external table and setup the shared memory channel.
        /// </summary>
        void Init(OdbcStatement& statement);

        /// <summary>
        ///   Shutdown the shared memory channel and destroy the external table.
        /// </summary>
        void Close(OdbcStatement& statement);

        const std::wstring& GetSchema() const
        {
            return m_schema;
        }

        const std::wstring& GetTableName() const
        {
            return m_tableName;
        }

    private:
        /// <summary>
        ///   Generate the SQL string to create the external shared memory table.
        /// </summary>
        std::wstring GenerateCreateTableStatement() const;

        /// <summary>
        ///   Generate the SQL string to drop the external shared memory table.
        /// </summary>
        std::wstring GenerateDropTableStatement() const;

        /// <summary>
        ///   Generate the SQL string to mark the table as streaming.
        /// </summary>
        std::wstring GenerateStreamingPropertyStatement() const;
    };

    /// <summary>
    ///   Base class to any SQL Server input.
    /// </summary>
    ///
    /// <remarks>
    ///   Every channel represents one input table.
    /// </remarks>
    class SqlInputChannel
    {
    public:
        SCOPE_ENGINE_API virtual ~SqlInputChannel();

        /// <summary>
        ///   Setup the input table in SQL Server.
        /// </summary>
        ///
        /// <returns>The SQL string to substitute the query's placeholder with.</returns>
        SCOPE_ENGINE_API virtual std::wstring Init(OdbcStatement& statement) = 0;

        /// <summary>
        ///   Close the input table in SQL Server.
        /// </summary>
        SCOPE_ENGINE_API virtual void Close(OdbcStatement& statement) = 0;
    };

    /// <summary>
    ///   Base class to write a Scope operator into SQL Server via shared memory.
    /// </summary>
    ///
    /// <remarks>
    ///   Contains the logic that is not dependend on a concrete Scope operator.
    /// </remarks>
    class ShmSqlInputChannelBase : public SqlInputChannel
    {
        SharedMemoryTable m_table;

        std::thread m_writerThread;

    public:
        SCOPE_ENGINE_API ShmSqlInputChannelBase(std::wstring&& schema);
        SCOPE_ENGINE_API virtual ~ShmSqlInputChannelBase() override;

        /// <summary>
        ///   Initializes the shared memory input channel.
        /// </summary>
        ///
        /// <remarks>
        ///   Spawns a writer thread responsible for writing the scope operator output rowset to shared memory.
        /// </remarks>
        ///
        /// <returns>The shared memory table name to substitute the query's placeholder with.</returns>
        SCOPE_ENGINE_API virtual std::wstring Init(OdbcStatement& statement) override;

        /// <summary>
        ///   Closes the shared memory input channel.
        /// </summary>
        SCOPE_ENGINE_API virtual void Close(OdbcStatement& statement) override;

        /// <summary>
        ///   SQL schema of the input table.
        /// </summary>
        const std::wstring& GetSchema() const
        {
            return m_table.GetSchema();
        }

        /// <summary>
        ///   Write the data from the current column to the result set.
        /// </summary>
        ///
        /// <remarks>
        ///   Template specialization for every supported type.
        /// </remarks>
        template <typename T>
        void Write(const T& value)
        {
            // TODO TFS 7312268/6593609 Use shared memory to write data
        }

    protected:
        /// <summary>
        ///   Advances the operator's result cursor to the next row.
        /// </summary>
        void Next()
        {
            // TODO TFS 7312268/6593609 Use shared memory to write data
            throw std::logic_error("Not implemented");
        }

    private:
        /// <summary>
        ///   Function invoked in the writer thread to write the scope operator output rowset to shared memory.
        /// </summary>
        virtual void PerformWrite() = 0;

        /// <summary>
        ///   Writer thread entry function responsible for setting up and writing to shared memory.
        /// </summary>
        void WriterThread();
    };

    /// <summary>
    ///   SQL input channel to write a concrete Scope operator into SQL Server via shared memory.
    /// </summary>
    template<typename InputOperator, typename InputSchema, int UID>
    class ShmSqlInputChannel : public ShmSqlInputChannelBase
    {
        InputOperator* m_input;

    public:
        /// <summary>
        ///   Creates a new shared memory input channel.
        /// </summary>
        ///
        /// <param name = "input">Pointer to the scope input operator.</param>
        /// <param name = "schema">SQL schema of the input operator.</param>
        ShmSqlInputChannel(InputOperator* input, std::wstring&& schema)
            : ShmSqlInputChannelBase(std::move(schema)),
              m_input(input)
        {}

    private:
        virtual void PerformWrite() override;
    };

	template<typename InputOperator, typename InputSchema, int UID>
    void ShmSqlInputChannel<InputOperator, InputSchema, UID>::PerformWrite()
    {
        m_input->Init();

        InputSchema row;
        while (m_input->GetNextRow(row))
        {
            // Write the row to shared memory
            SqlOutputPolicy<InputSchema, UID>::Serialize(this, row);

            Next();
        }

        m_input->Close();
    }

    /// <summary>
    ///   Wraps the shared memory input channel in a temporary table.
    /// </summary>
    std::unique_ptr<SqlInputChannel> CreateMaterializeInputChannel(std::unique_ptr<ShmSqlInputChannelBase> input);

    /// <summary>
    ///   Constructor parameters for SqlComposable.
    /// </summary>
    struct SqlComposableParameters
    {
        /// <summary>
        ///   Input tables sorted by their placeholder index.
        /// </summary>
        std::map<int, std::unique_ptr<SqlInputChannel>> input;

        /// <summary>
        ///   SQL query template to execute.
        /// </summary>
        std::wstring query;

        /// <summary>
        ///   SQL schema of the output rowset.
        /// </summary>
        std::wstring schema;
    };

    /// <summary>
    ///   Scope operator for executing queries in SQL Server.
    /// </summary>
    class SqlComposable
    {
        /// <summary>
        ///   Regex to match the encoded information in the stream label for column store tables.
        ///   The label has the form "@FileGroup_<operatorID>_<tableID> [<partitionID>,<distributionID>,<sequenceID>]".
        /// </summary>
        static const char* const c_streamLabelRegex;

        /// <summary>
        ///   Connection string for connecting to the locally running SQL Server instance.
        /// </summary>
        static const wchar_t* const c_connectionString;

        IncrementalAllocator* m_allocator;

        SIZE_T m_bufSize;
        int m_bufCount;

        std::wstring m_query;

        /// <summary>
        ///   Input tables sorted by their placeholder index.
        /// </summary>
        std::map<int, std::unique_ptr<SqlInputChannel>> m_inputTables;

        /// <summary>
        ///   Output table for reading results back via shared memory.
        /// </summary>
        SharedMemoryTable m_outputTable;

        OdbcEnvironment m_environment;
        OdbcConnection m_connection;
        OdbcStatement m_statement;

        SQLUSMALLINT m_columnIndex{ 0 };

        /// <summary>
        ///   Read the non-nullable data type from the current row and column. Moves to the next column.
        /// </summary>
        template <typename T>
        void Read(SQLSMALLINT type, T& value)
        {
            m_statement.ReadData(m_columnIndex, type, &value, sizeof(value), nullptr);
            ++m_columnIndex;
        }

        /// <summary>
        ///   Read the nullable data type from the current row and column. Moves to the next column.
        /// </summary>
        template <typename T>
        void ReadNullable(SQLSMALLINT type, NativeNullable<T>& value)
        {
            SQLLEN length = 0;
            m_statement.ReadData(m_columnIndex, type, &value.get(), sizeof(value.get()), &length);
            if (length == SQL_NULL_DATA)
            {
                value.SetNull();
            }
            else
            {
                value.ClearNull();
            }
            ++m_columnIndex;
        }

    public:
        /// <summary>
        ///   Constructs a new SQL composable operator to execute a query in SQL Server.
        /// </summary>
        ///
        /// <remarks>
        ///   Currently all column store files are passed as one file input group. Shared memory input channels are
        ///   passed via the parameters struct. This will change with TFS 7295026 so that every input is treated as one
        ///   operator.
        ///</remarks>
        SCOPE_ENGINE_API SqlComposable(const InputFileInfo& input, IncrementalAllocator* allocator, SIZE_T bufSize,
            int bufCount, SqlComposableParameters&& parameters);

        SCOPE_ENGINE_API ~SqlComposable();

        /// <summary>
        ///   Handles setup for the operator
        ///   - Connects to the locally running SQL Server
        ///   - Initializes every input channel
        ///   - Rewrites and executes the user query
        /// </summary>
        SCOPE_ENGINE_API void Init();

        /// <summary>
        ///   Shuts the operator down
        ///   - Closes the SQL Server result set
        ///   - Closes all input channels
        /// </summary>
        SCOPE_ENGINE_API void Close();

        PartitionMetadata* ReadMetadata()
        {
            return nullptr;
        }

        void DiscardMetadata()
        {}

        void WriteRuntimeStats(TreeNode& root)
        {}

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }

        LONGLONG GetTotalIoWaitTime()
        {
            return 0;
        }

        LONGLONG GetInclusiveTimeMillisecond()
        {
            return 0;
        }

        /// <summary>
        ///   Advances the operator's result cursor to the next row.
        /// </summary>
        ///
        /// <returns>False if cursor reached the end and no data is available</returns>
        bool Next()
        {
            m_columnIndex = 1;
            return m_statement.Next();
        }

        /// <summary>
        ///   Reads the data from the current column from the result set.
        /// </summary>
        ///
        /// <remarks>
        ///   Template specialization for every supported type.
        /// </remarks>
        template <typename T>
        void Read(T& value)
        {}

        template <>
        void Read(short& value)
        {
            Read(SQL_C_SSHORT, value);
        }

        template <>
        void Read(NativeNullable<short>& value)
        {
            ReadNullable(SQL_C_SSHORT, value);
        }

        template <>
        void Read(int& value)
        {
            Read(SQL_C_SLONG, value);
        }

        template <>
        void Read(NativeNullable<int>& value)
        {
            ReadNullable(SQL_C_SLONG, value);
        }

        template <>
        void Read(__int64& value)
        {
            Read(SQL_C_SBIGINT, value);
        }

        template <>
        void Read(NativeNullable<__int64>& value)
        {
            ReadNullable(SQL_C_SBIGINT, value);
        }

    private:
        /// <summary>
        ///   Parses the encoded information in the stream label and adds the table to the input tables set.
        /// </summary>
        void ParseStreamInfo(int groupId);
    };

    /// <summary>
    ///   Template to define schema SQL Server deserialization.
    /// </summary>
    template<typename Schema, int = -1>
    class SqlExtractPolicy
    {
    public:
        /// <summary>
        ///   SQL Server deserialization routine (from SQL Server ODBC format).
        /// </summary>
        static bool Deserialize(void* sqlInputStream, Schema& row);
    };

    /// <summary>
    ///   Template to define schema SQL Server serialization.
    /// </summary>
    template<typename Schema, int = -1>
    class SqlOutputPolicy
    {
    public:
        /// <summary>
        ///   SQL Server serialization routine (to SQL Server ODBC format).
        /// </summary>
        static void Serialize(ShmSqlInputChannelBase* sqlOutputStream, const Schema& row);
    };
}
