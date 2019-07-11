// This is a generated file - DO NOT EDIT !!
namespace ScopeEngine
{
#pragma once
    enum ErrorNumber : unsigned int
    {
        E_USER_ERROR = 0xbad0000,
        E_TEST_ERROR = 0xbad0001,
        E_USER_OUT_OF_MEMORY = 0xbad0002,
        E_USER_OPERATOR_OUT_OF_MEMORY_QUOTA = 0xbad0003,
        E_USER_DECIMAL_ERROR = 0xbad0004,
        E_USER_AGGREGATE_OVERFLOW = 0xbad0005,
        E_INTERNAL_EXTRACT_ERROR = 0xbad0006,
        E_USER_ROW_TOO_BIG = 0xbad0007,
        E_USER_ONDISKROW_TOO_BIG = 0xbad0008,
        E_USER_EXPRESSION_ERROR = 0xbad0009,
        E_USER_TOO_MANY_RANGES = 0xbad000a,
        E_USER_INDEX_KEY_TOO_LONG = 0xbad000b,
        E_USER_KEY_TOO_BIG = 0xbad000c,
        E_USER_COLUMN_TOO_BIG = 0xbad000d,
        E_USER_STRING_TOO_BIG = 0xbad000e,
        E_USER_BINARY_TOO_BIG = 0xbad000f,
        E_USER_INVALID_CTI = 0xbad0010,
        E_USER_MISMATCH_ROW = 0xbad0011,
        E_USER_ROWSET_TOO_BIG = 0xbad0012,
        E_EXTRACT_STREAM = 0xbad0013,
        E_EXTRACT_ROW_TOO_LONG = 0xbad0014,
        E_EXTRACT_INPUT_ENCODING_MISALIGNED = 0xbad0015,
        E_EXTRACT_ENCODING_MISMATCH = 0xbad0016,
        E_EXTRACT_INVALID_CHARACTER = 0xbad0017,
        E_EXTRACT_UNEXPECTED_NUMBER_COLUMNS = 0xbad0018,
        E_EXTRACT_COLUMN_CONVERSION_UNDEFINED_ERROR = 0xbad0019,
        E_EXTRACT_COLUMN_CONVERSION_OUTOFRANGE_ERROR = 0xbad001a,
        E_EXTRACT_COLUMN_CONVERSION_INVALID_ERROR = 0xbad001b,
        E_EXTRACT_COLUMN_CONVERSION_EMPTY_ERROR = 0xbad001c,
        E_EXTRACT_COLUMN_CONVERSION_NULL_ERROR = 0xbad001d,
        E_EXTRACT_COLUMN_CONVERSION_INVALID_LENGTH = 0xbad001e,
        E_EXTRACT_COLUMN_CONVERSION_TOO_LONG = 0xbad001f,
        E_EXTRACT_INVALID_CHARACTER_AFTER_TEXTQUALIFIER = 0xbad0020,
        E_USER_SUBSTRING_OUT_RANGE = 0xbad0021,
        E_USER_JOIN_CROSS_LIMIT_EXCEEDED = 0xbad0022,
        E_UNHANDLED_EXCEPTION_FROM_USER_CODE = 0xbad0023,
        E_UNHANDLED_EXCEPTION_FROM_USER_CODE_UNKNOWN_METHOD = 0xbad0024,
        E_USER_BINARY_CONVERSION = 0xbad0025,
        E_USER_NULL_VALUE = 0xbad0026,
        E_USER_INVALID_GUID = 0xbad0027,
        E_USER_NULL_KEY_IN_MAP = 0xbad0028,
        E_USER_ARRAY_INDEX = 0xbad0029,
        E_RUNTIME_USER_HTTP_ERROR = 0xbad002a,
        E_DQ_USER_SECURITY_ERROR = 0xbad002b,
        E_DQ_USER_SECURE_HANDLER_ERROR = 0xbad002c,
        E_DQ_USER_REMOTE_CONNECTION_ERROR = 0xbad002d,
        E_DQ_USER_COLUMNTOOBIG = 0xbad002e,
        E_DQ_USER_UNSUPPORTED_OUTPUT_DATATYPE = 0xbad002f,
        E_DQ_USER_UNSUPPORTED_CONVERSION = 0xbad0030,
        E_DQ_USER_NOTNULL_CONSTRAINT_VIOLATION = 0xbad0031,
        E_DQ_USER_ROWTOOBIG = 0xbad0032,
        E_DQ_USER_SCHEMA_MISMATCH = 0xbad0033,
        E_DQ_USER_SQL_EXEC_SECURITY_ERROR = 0xbad0034,
        E_DQ_USER_SQL_EXEC_ERROR = 0xbad0035,
        E_USER_GZIP_ERROR = 0xbad0036,
        E_CATALOGVIEW_USER_ERROR_UNSUPPORTED_ITEM = 0xbad0037,
        E_SKIP_FIRST_ROWS_UNEXPECTED_EOF = 0xbad0038,
        E_EXTRACT_COLUMN_CONVERSION_TEXT_TO_SQLDATETIME2 = 0xbad0039,
        E_SYSTEM_ERROR = 0xbad003a,
        E_SYSTEM_ASSERT_FAILED = 0xbad003b,
        E_COSMOS_STORE_ERROR = 0xbad003c,
        E_SYSTEM_METADATA_ERROR = 0xbad003d,
        E_SYSTEM_INTERNAL_ERROR = 0xbad003e,
        E_SYSTEM_CORRUPT_SS = 0xbad003f,
        E_SYSTEM_STREAMCACHE_ERROR = 0xbad0040,
        E_SYSTEM_HEAP_OUT_OF_MEMORY = 0xbad0041,
        E_SYSTEM_DATA_UNAVAILABLE = 0xbad0042,
        E_SYSTEM_RESOURCE_OPEN = 0xbad0043,
        E_SYSTEM_RESOURCE_READ = 0xbad0044,
        E_SYSTEM_CRT_ERROR = 0xbad0045,
        E_RUNTIME_SYSTEM_HTTP_ERROR = 0xbad0046,
        E_DQ_SYSTEM_ERROR = 0xbad0047,
        E_DQ_SYSTEM_ERROR_UNSUPPORTED_VERSION = 0xbad0048,
        E_DQ_SYSTEM_ERROR_UNKNOWN_VERSION = 0xbad0049,
        E_DQ_SYSTEM_ERROR_BAD_REQUEST = 0xbad004a,
        E_DQ_SYSTEM_SECURITY_ERROR = 0xbad004b,
        E_DQ_SYSTEM_REMOTE_CONNECTION_ERROR = 0xbad004c,
        E_DQ_SYSTEM_THROTTLING_HOSTID_ERROR = 0xbad004d,
        E_DQ_SYSTEM_THROTTLING_JOBID_ERROR = 0xbad004e,
        E_DQ_SYSTEM_SQL_EXEC_ERROR = 0xbad004f,
        E_RUNTIME_SYSTEM_HASHJOIN_EXCEEDED_SPILL_LIMIT = 0xbad0050,
        E_SYSTEM_NOT_SUPPORTED = 0xbad0051,
        E_SYSTEM_GZIP_ERROR = 0xbad0052,
        E_CATALOGVIEW_SYSTEM_ERROR_BAD_REQUEST = 0xbad0053,
        E_STREAM_SPLIT_UNEXPECTED_EOF = 0xbad0054,
        E_SYSTEM_ODBC_ERROR = 0xbad0055,
        E_INVALID_MAGIC_IN_SS_TAIL = 0xbad0056,
        E_INVALID_MAGIC_IN_SS_DATAUNIT = 0xbad0057,
        E_INVALID_MAGIC_IN_TABLE_TAIL = 0xbad0058,
        E_INVALID_MAGIC_IN_TABLE_DATAUNIT = 0xbad0059,
    };
}
