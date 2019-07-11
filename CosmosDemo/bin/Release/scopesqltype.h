// Head file that inluces the implmentation of SqlTypes
#pragma once
#include "scopecontainers.h"
namespace ScopeEngine
{
    namespace ScopeSqlType
    {
        template <int N>
        class SqlStringFaceted;
        class SqlDecimal;
        class SqlDateTime;
        template <class I, typename T>
        class ArithmeticSqlType;
        class SqlInt64Imp;
        class SqlInt32Imp;
        class SqlInt16Imp;
        class SqlByteImp;
        class SqlBitImp;
        class SqlSingleImp;
        class SqlDoubleImp;
        template<class _Ty>
        struct is_sql_type;

#ifdef PLUGIN_TYPE_SYSTEM_NAMESPACE
        //Define the Nullable template for SqlTypes.
        //Native implementation is needed.
        // For testing purpose only
        template <typename T>
        class SqlNativeNullable : public PluginType::NativeNullable<T>
        {
        public:
            SqlNativeNullable() : PluginType::NativeNullable<T>()
            {
            }

            SqlNativeNullable<T>(const ArgumentType& val) : PluginType::NativeNullable<T>(val)
            {
            }

            template<typename O>
            SqlNativeNullable(const O& val) : PluginType::NativeNullable<T>(val)
            {
            }
        };

        // TODO: Existing SQL native implementation with T-SQL semantics need to be extended with
        //       plugin type system support, and then replace Scope types used here.
        typedef PluginType::NativeNullable<__int64>                                                                 SqlType_Int64;     // BIGINT
        typedef ScopeEngine::ScopeSqlType::SqlNativeNullable<int>                                                   SqlType_Int32;     // INT
        typedef PluginType::NativeNullable<short>                                                                   SqlType_Int16;     // SMALLINT
        typedef PluginType::NativeNullable<unsigned __int8>                                                         SqlType_Byte;      // TINYINT
        typedef PluginType::NativeNullable<__int8>                                                                  SqlType_Bit;       // BIT
        typedef PluginType::NativeNullable<PluginType::ScopeDecimal>                                                SqlType_Decimal;   // DECIMAL
        typedef PluginType::NativeNullable<PluginType::ScopeDateTime>                                               SqlType_Date;      // DATETIME2
        template <int N>
        using SqlType_MaxString = ScopeEngine::ScopeSqlType::SqlNativeNullable<ScopeEngine::ScopeSqlType::SqlStringFaceted<N>>;        // NVARCHAR(MAX)

#else
        // Define the Nullable template for SqlTypes. It supports ANSI NULLS OFF semantics.
        //
        // NOTE: All comparison operators should return false if either argument is NULL.
        // Sorting should be performed by using CompareTo method.
        //
        template <typename T>
        class SqlNativeNullable : public ScopeEngine::NativeNullable<T>
        {
        public:
            SqlNativeNullable()
            {
            }

            SqlNativeNullable(const ArgumentType& val) : NativeNullable(val)
            {
            }

            SqlNativeNullable(T && val) : NativeNullable(std::move(val))
            {
            }

            SqlNativeNullable(const SqlNativeNullable<T>& val) : NativeNullable((NativeNullable<T> &)val)
            {
            }

            template<typename O>
            SqlNativeNullable(const SqlNativeNullable<O>& val) : NativeNullable<O>((NativeNullable<O> &)val)
            {
            }

            template<typename O>
            SqlNativeNullable(const O& val) : NativeNullable<T>(val)
            {
            }

            SqlNativeNullable(nullptr_t) : NativeNullable(nullptr)
            {
            }

            SqlNativeNullable(const ArgumentType& val, IncrementalAllocator * alloc) : NativeNullable<T>(val, alloc)
            {
            }

            SqlNativeNullable(const NativeNullable<T>& val, IncrementalAllocator * alloc) : NativeNullable<T>(val, alloc)
            {
            }

            template<typename U>
            int CompareTo(NativeNullable<U> const& y) const
            {
                if (IsNull())
                    return y.IsNull() ? 0 : -1;
                if (y.IsNull())
                    return 1;

                return ScopeTypeCompare<typename ScopeCommonType<T, U>::type>(get(), y.get());
            }

            template<typename U>
            bool operator==(NativeNullable<U> const& y) const
            {
                if (IsAnyNull(*this, y))
                {
                    return false;
                }

                return ScopeTypeCompare<typename ScopeCommonType<T, U>::type>(get(), y.get()) == 0;
            }

            // If either value is null, the result is false; otherwise compare the elements.
            template<typename U>
            bool operator<(NativeNullable<U> const& y) const
            {
                if (IsAnyNull(*this, y))
                {
                    return false;
                }

                return ScopeTypeCompare<typename ScopeCommonType<T, U>::type>(get(), y.get()) < 0;
            }

            template<typename U>
            bool operator!=(NativeNullable<U> const& y) const
            {
                if (IsAnyNull(*this, y))
                {
                    return false;
                }

                return ScopeTypeCompare<typename ScopeCommonType<T, U>::type>(get(), y.get()) != 0;
            }

            template<typename U>
            bool operator>(NativeNullable<U> const& y) const
            {
                if (IsAnyNull(*this, y))
                {
                    return false;
                }

                return ScopeTypeCompare<typename ScopeCommonType<T, U>::type>(get(), y.get()) > 0;
            }

            template<typename U>
            bool operator<=(NativeNullable<U> const& y) const
            {
                if (IsAnyNull(*this, y))
                {
                    return false;
                }

                return ScopeTypeCompare<typename ScopeCommonType<T, U>::type>(get(), y.get()) <= 0;
            }

            template<typename U>
            bool operator>=(NativeNullable<U> const& y) const
            {
                if (IsAnyNull(*this, y))
                {
                    return false;
                }

                return ScopeTypeCompare<typename ScopeCommonType<T, U>::type>(get(), y.get()) >= 0;
            }
        };

        // TODO: The NativeNullable<T> implementation below will be replaced by a
        //       SQL scalar native implementation matching T-SQL semantics.
        typedef SqlNativeNullable<ArithmeticSqlType < SqlInt64Imp, __int64>>          SqlType_Int64;     // BIGINT
        typedef SqlNativeNullable<ArithmeticSqlType < SqlInt32Imp, int >>             SqlType_Int32;     // INT
        typedef SqlNativeNullable<ArithmeticSqlType < SqlInt16Imp, short>>            SqlType_Int16;     // SMALLINT
        typedef SqlNativeNullable<ArithmeticSqlType < SqlByteImp, unsigned __int8>>   SqlType_Byte;      // TINYINT
        typedef SqlNativeNullable<ArithmeticSqlType < SqlBitImp, __int8>>             SqlType_Bit;       // BIT
        typedef SqlNativeNullable<ArithmeticSqlType < SqlSingleImp, float>>           SqlType_Single;    // REAL
        typedef SqlNativeNullable<ArithmeticSqlType < SqlDoubleImp, double>>          SqlType_Double;    // FLOAT
        typedef ScopeEngine::NativeNullable<SqlDecimal>                               SqlType_Decimal;     // DECIMAL
        typedef ScopeEngine::NativeNullable<SqlDateTime>                              SqlType_Date;        // DATETIME2
        template <int N>
        using SqlType_MaxString = ScopeEngine::ScopeSqlType::SqlNativeNullable<ScopeEngine::ScopeSqlType::SqlStringFaceted<N>>;  // NVARCHAR(MAX)

#endif

        // Checks whether either argument is null.
        //
        template <typename T, typename U>
        inline bool IsAnyNull(const ScopeEngine::NativeNullable<T>& arg1, const ScopeEngine::NativeNullable<U>& arg2)
        {
            return arg1.IsNull() || arg2.IsNull();
        }

#pragma region SqlDateTime

        class SqlDateTimeImp;

        class SqlDateTime
        {
            std::unique_ptr<SqlDateTimeImp> m_imp;

            SCOPE_ENGINE_API SqlDateTime(const SqlDateTimeImp&);

        public:

            SCOPE_ENGINE_API SqlDateTime();

            SCOPE_ENGINE_API SqlDateTime(const SqlDateTime & c);

            SCOPE_ENGINE_API SqlDateTime(int year, int month, int day, int hour, int min, int sec, long fractional);

            SCOPE_ENGINE_API ~SqlDateTime();

            SCOPE_ENGINE_API __int64 ToBinaryTime() const;

            SCOPE_ENGINE_API long ToBinaryDate() const;

            SCOPE_ENGINE_API static SqlDateTime FromBinary(__int64 binaryTime, long binaryDate);

            SCOPE_ENGINE_API static bool TryParseStr(const char *str, SqlDateTime &sdt);

            SCOPE_ENGINE_API int ToString(char *outBuf, int bufSize) const;

            SCOPE_ENGINE_API SqlDateTime & operator= (SqlDateTime const& rhs);

            SCOPE_ENGINE_API bool operator < (const SqlDateTime & t) const;

            SCOPE_ENGINE_API bool operator == (const SqlDateTime & t) const;

            SCOPE_ENGINE_API bool operator != (const SqlDateTime & t) const;

            SCOPE_ENGINE_API bool operator > (const SqlDateTime & t) const;

            SCOPE_ENGINE_API bool operator <= (const SqlDateTime & t) const;

            SCOPE_ENGINE_API bool operator >= (const SqlDateTime & t) const;

            SCOPE_ENGINE_API int GetScopeHashCode() const;

            SCOPE_ENGINE_API unsigned __int64 GetCRC32Hash(unsigned __int64 crc) const;

            SCOPE_ENGINE_API int getYear() const;

            SCOPE_ENGINE_API int getMonth() const;

            SCOPE_ENGINE_API int getDay() const;

            SCOPE_ENGINE_API int getHour() const;

            SCOPE_ENGINE_API int getMinute() const;

            SCOPE_ENGINE_API int getSecond() const;

            SCOPE_ENGINE_API long getFractional() const;
        };
#pragma endregion SqlDateTime

#pragma region SqlArithmeticType

        template <class I, typename T>
        class ArithmeticSqlType
        {
        public:
            std::shared_ptr<I> m_imp;

            SCOPE_ENGINE_API ArithmeticSqlType(const I& newImp);

        public:
            SCOPE_ENGINE_API ArithmeticSqlType();

            SCOPE_ENGINE_API ArithmeticSqlType(const ArithmeticSqlType<I, T>&);

            SCOPE_ENGINE_API ArithmeticSqlType(const T&);

            SCOPE_ENGINE_API ~ArithmeticSqlType();

            SCOPE_ENGINE_API inline T getValue() const;

            SCOPE_ENGINE_API inline void setValue(T other);

            SCOPE_ENGINE_API ArithmeticSqlType & operator= (T other);

            SCOPE_ENGINE_API ArithmeticSqlType & operator= (const ArithmeticSqlType & other);

            // unary negation operator for ArithmeticSqlType
            SCOPE_ENGINE_API ArithmeticSqlType operator -() const;

            // unary plus operator for ArithmeticSqlType
            SCOPE_ENGINE_API ArithmeticSqlType operator +() const;

            // addition operator for ArithmeticSqlType
            SCOPE_ENGINE_API ArithmeticSqlType operator+ (const ArithmeticSqlType & other) const;

            // subtraction operator for ArithmeticSqlType
            SCOPE_ENGINE_API ArithmeticSqlType operator- (const ArithmeticSqlType & other) const;

            // multiplication operator for ArithmeticSqlType
            SCOPE_ENGINE_API ArithmeticSqlType operator* (const ArithmeticSqlType & other) const;

            // divide operator for ArithmeticSqlType
            SCOPE_ENGINE_API ArithmeticSqlType operator/ (const ArithmeticSqlType & other) const;

            SCOPE_ENGINE_API bool operator < (const ArithmeticSqlType & t) const;

            SCOPE_ENGINE_API bool operator == (const ArithmeticSqlType & t) const;

            SCOPE_ENGINE_API bool operator != (const ArithmeticSqlType & t) const;

            SCOPE_ENGINE_API bool operator >(const ArithmeticSqlType & t) const;

            SCOPE_ENGINE_API bool operator <= (const ArithmeticSqlType & t) const;

            SCOPE_ENGINE_API bool operator >= (const ArithmeticSqlType & t) const;

            // compute 32 bit hash for SqlDecimal
            SCOPE_ENGINE_API int GetScopeHashCode() const;

            SCOPE_ENGINE_API unsigned __int64 GetCRC32Hash(unsigned __int64 crc) const;
        };

        // Exception class that can be used for all sql types
        class SqlTypesException : public ScopeEngineException
        {
        public:
            // We should fix this to be multiple different errors and give more context, but for now match current behaviour
            SCOPE_ENGINE_API SqlTypesException(long error);
            SCOPE_ENGINE_API SqlTypesException(const SqlTypesException& c);
            SCOPE_ENGINE_API SqlTypesException& operator= (const SqlTypesException& c) throw();
            SCOPE_ENGINE_API virtual ~SqlTypesException() throw() override;
            SCOPE_ENGINE_API virtual ExceptionWithStack* Clone() const override;

            SCOPE_ENGINE_API long Error() const;

        private:
            static const char * Reason(long error);

            long m_error;
        };

        // TEMPLATE CLASS _Is_sql_type
        template<class _Ty>
        struct _Is_sql_type
            : false_type
        {   // determine whether _Ty is sql type
        };

        template<>
        struct _Is_sql_type<ArithmeticSqlType < SqlInt64Imp, __int64>>
            : true_type
        {   // determine whether _Ty is sql type
        };

        template<>
        struct _Is_sql_type<ArithmeticSqlType < SqlInt32Imp, int >>
            : true_type
        {   // determine whether _Ty is sql type
        };

        template<>
        struct _Is_sql_type<ArithmeticSqlType < SqlInt16Imp, short>>
            : true_type
        {   // determine whether _Ty is sql type
        };

        template<>
        struct _Is_sql_type<ArithmeticSqlType < SqlByteImp, unsigned __int8>>
            : true_type
        {   // determine whether _Ty is sql type
        };

        template<>
        struct _Is_sql_type<ArithmeticSqlType < SqlSingleImp, float>>
            : true_type
        {   // determine whether _Ty is sql type
        };

        template<>
        struct _Is_sql_type<ArithmeticSqlType < SqlDoubleImp, double>>
            : true_type
        {   // determine whether _Ty is sql type
        };

        // TEMPLATE CLASS is_sql_type
        template<class _Ty>
        struct is_sql_type
            : _Is_sql_type<typename remove_cv<_Ty>::type>
        {   // determine whether _Ty is sql type
        };

#pragma endregion SqlArithmeticType

#pragma region SqlDecimal

        class SqlDecimalImp;

        class SqlDecimal
        {
        private:
            std::unique_ptr<SqlDecimalImp> m_imp;

        public:
            SCOPE_ENGINE_API SqlDecimal();

            SCOPE_ENGINE_API SqlDecimal(const SqlDecimal & c);

            SCOPE_ENGINE_API SqlDecimal(const SqlDecimalImp&);

            SCOPE_ENGINE_API ~SqlDecimal();

            SCOPE_ENGINE_API inline bool FPositive() const;

            SCOPE_ENGINE_API inline BYTE BScale() const;

            SCOPE_ENGINE_API inline BYTE BPrec() const;

            SCOPE_ENGINE_API inline ULONG UlData(int i) const;

            SCOPE_ENGINE_API inline void SetValue(
                ULONG data0,
                ULONG data1,
                ULONG data2,
                ULONG data3,
                BYTE bPrec,
                BYTE bScale,
                bool fPositive);

            // converts a digit string to decimal
            SCOPE_ENGINE_API static SqlDecimal Parse(const char * pwchStr, unsigned int cbStr);

            SCOPE_ENGINE_API void ToString(char *pchStr, int *pcbStr) const;

            SCOPE_ENGINE_API SqlDecimal & operator= (SqlDecimal const& other);

            /* unary negation operator for SqlDecimal*/
            SCOPE_ENGINE_API SqlDecimal operator -() const;

            /* unary plus operator for SqlDecimal*/
            SCOPE_ENGINE_API SqlDecimal operator +() const;

            /* addition operator for SqlDecimal*/
            SCOPE_ENGINE_API SqlDecimal operator+ (const SqlDecimal & other) const;

            /* subtraction operator for SqlDecimal*/
            SCOPE_ENGINE_API SqlDecimal operator- (const SqlDecimal & other) const;

            /* multiplication operator for SqlDecimal*/
            SCOPE_ENGINE_API SqlDecimal operator* (const SqlDecimal & other) const;

            /* divide operator for SqlDecimal*/
            SCOPE_ENGINE_API SqlDecimal operator/ (const SqlDecimal & other) const;

            /* modulo operator for SqlDecimal*/
            SCOPE_ENGINE_API SqlDecimal operator% (const SqlDecimal & other) const;

            SCOPE_ENGINE_API bool operator < (const SqlDecimal & t) const;

            SCOPE_ENGINE_API bool operator == (const SqlDecimal & t) const;

            SCOPE_ENGINE_API bool operator != (const SqlDecimal & t) const;

            SCOPE_ENGINE_API bool operator > (const SqlDecimal & t) const;

            SCOPE_ENGINE_API bool operator <= (const SqlDecimal & t) const;

            SCOPE_ENGINE_API bool operator >= (const SqlDecimal & t) const;

            // compute 32 bit hash for SqlDecimal
            SCOPE_ENGINE_API int GetScopeHashCode() const;

            SCOPE_ENGINE_API unsigned __int64 GetCRC32Hash(unsigned __int64 crc) const;
        };

#pragma endregion SqlDecimal

#pragma region SqlString
        class SqlStringImp;

        class SqlString
        {
            std::unique_ptr<SqlStringImp> m_imp;

            SCOPE_ENGINE_API SqlString(const SqlStringImp & s);

        public:
            SCOPE_ENGINE_API SqlString(int len);

            SCOPE_ENGINE_API SqlString(const std::string & str, IncrementalAllocator * alloc, int len);

            // Allocater is needed to make a deep copy of the FString.
            SCOPE_ENGINE_API SqlString(const FString & str, IncrementalAllocator * alloc, int len);

            // Move constructor from FString. Allocator is actually not needed here, but is provided for the purpose of aligning with copy constructor from FString.
            SCOPE_ENGINE_API SqlString(FString && str, IncrementalAllocator * alloc, int len);

            SCOPE_ENGINE_API SqlString(const SqlString & c);

            SCOPE_ENGINE_API SqlString(SqlString && c);

            SCOPE_ENGINE_API SqlString(const SqlString & c, IncrementalAllocator * alloc);

            SCOPE_ENGINE_API ~SqlString();

            SCOPE_ENGINE_API SqlString & operator= (SqlString const& rhs);

            SCOPE_ENGINE_API bool operator < (const SqlString & rhs) const;

            SCOPE_ENGINE_API bool operator == (const SqlString & rhs) const;

            SCOPE_ENGINE_API bool operator != (const SqlString & rhs) const;

            SCOPE_ENGINE_API bool operator > (const SqlString & rhs) const;

            SCOPE_ENGINE_API bool operator <= (const SqlString & rhs) const;

            SCOPE_ENGINE_API bool operator >= (const SqlString & rhs) const;

            SCOPE_ENGINE_API int GetScopeHashCode() const;

            SCOPE_ENGINE_API unsigned __int64 GetCRC32Hash(unsigned __int64 crc) const;

            SCOPE_ENGINE_API ConvertResult ConvertFrom(FStringWithNull & str);

            SCOPE_ENGINE_API ConvertResult ConvertFrom(FString & str);

            SCOPE_ENGINE_API void SetEmpty();

            SCOPE_ENGINE_API const char * Buffer() const;

            SCOPE_ENGINE_API UINT Size() const;

            // Allocate memory for the data
            SCOPE_ENGINE_API SAFE_BUFFERS char * Reserve(size_t size, IncrementalAllocator * alloc);

            SCOPE_ENGINE_API static SqlString Concat(const SqlString & s1, const SqlString & s2, IncrementalAllocator * alloc);

            SCOPE_ENGINE_API SqlString Upper(IncrementalAllocator * alloc) const;

            SCOPE_ENGINE_API bool Like(const SqlString & pattern) const;
        };
#pragma endregion SqlString

#pragma region SqlStringFaceted
        template <int N>
        class SqlStringFaceted
        {
            SqlString m_imp;

            template <int K>
            friend class SqlStringFaceted;
            friend class SqlStringUtils;
        public:
            SqlStringFaceted() : m_imp(N)
            {
            }

            SqlStringFaceted(const SqlString & s) : m_imp(s)
            {
            }

            SqlStringFaceted(const std::string & str, IncrementalAllocator * alloc) : m_imp(str, alloc, N)
            {
            }

            SqlStringFaceted(const FString & str, IncrementalAllocator * alloc) : m_imp(str, alloc, N)
            {
            }

            SqlStringFaceted(FString && str, IncrementalAllocator * alloc) : m_imp(std::move(str), alloc, N)
            {
            }

            SqlStringFaceted(const SqlStringFaceted & c) : m_imp(c.m_imp)
            {
            }

            SqlStringFaceted(SqlStringFaceted && c) : m_imp(std::move(c.m_imp))
            {
            }

            SqlStringFaceted(const SqlStringFaceted & c, IncrementalAllocator * alloc) : m_imp(c.m_imp, alloc)
            {
            }

            SqlStringFaceted & operator= (SqlStringFaceted const& rhs)
            {
                m_imp = rhs.m_imp;
                return *this;
            }

            template <int K>
            bool operator < (const SqlStringFaceted<K> & rhs) const
            {
                return m_imp < rhs.m_imp;
            }

            template <int K>
            bool operator == (const SqlStringFaceted<K> & rhs) const
            {
                return m_imp == rhs.m_imp;
            }

            template <int K>
            bool operator != (const SqlStringFaceted<K> & rhs) const
            {
                return m_imp != rhs.m_imp;
            }

            template <int K>
            bool operator >(const SqlStringFaceted<K> & rhs) const
            {
                return m_imp > rhs.m_imp;
            }

            template <int K>
            bool operator <= (const SqlStringFaceted<K> & rhs) const
            {
                return m_imp <= rhs.m_imp;
            }

            template <int K>
            bool operator >= (const SqlStringFaceted<K> & rhs) const
            {
                return m_imp >= rhs.m_imp;
            }

            int GetScopeHashCode() const
            {
                return m_imp.GetScopeHashCode();
            }

            unsigned __int64 GetCRC32Hash(unsigned __int64 crc) const
            {
                return m_imp.GetCRC32Hash(crc);
            }

            ConvertResult ConvertFrom(FStringWithNull & str)
            {
                return m_imp.ConvertFrom(str);
            }

            ConvertResult ConvertFrom(FString & str)
            {
                return m_imp.ConvertFrom(str);
            }

            void SetEmpty()
            {
                m_imp.SetEmpty();
            }

            const char * Buffer() const
            {
                return m_imp.Buffer();
            }

            UINT Size() const
            {
                return m_imp.Size();
            }

            // Allocate memory for the data
            SAFE_BUFFERS char * Reserve(size_t size, IncrementalAllocator * alloc)
            {
                return m_imp.Reserve(size, alloc);
            }
        };
#pragma endregion SqlStringFaceted

#pragma region SqlStringUtils

#define CONCAT_LEN(K, S) ((K == -1 || S == -1) ? -1 : (K + S > 4000 ? 4000 : K + S))

        class SqlStringUtils
        {
        public:
            template<int K, int S>
            static SqlType_MaxString<CONCAT_LEN(K, S)> Concat(const SqlType_MaxString<K> &s1, const SqlType_MaxString<S> & s2, IncrementalAllocator * alloc)
            {
                // Return type depends on facets of both types. We use the type with facet set 
                // to 4000 if their sum exceeds that value, or the value of the sum otherwise.
                // Special case is when one of the arguments has value max (denoted as -1). Then resulting length is also max.
                return SqlType_MaxString<CONCAT_LEN(K, S)>(SqlString::Concat(s1.get().m_imp, s2.get().m_imp, alloc));
            }

            template <int K>
            static SqlType_MaxString<K> Upper(const SqlType_MaxString<K> & s, IncrementalAllocator * alloc)
            {
                return SqlType_MaxString<K>(s.get().m_imp.Upper(alloc));
            }

            template<int K, int S>
            static bool Like(const SqlType_MaxString<K> & s, const SqlType_MaxString<S> & pattern)
            {
                return s.get().m_imp.Like(pattern.get().m_imp);
            }
        };
#pragma endregion SqlStringUtils

    }
}

#ifdef PLUGIN_TYPE_SYSTEM_NAMESPACE
namespace PluginType
{
    using namespace ScopeEngine;
    using namespace ScopeEngine::ScopeSqlType;
#else
namespace ScopeEngine
{
#endif
    // Print Decimal to string in G fomat
    INLINE int SqlDecimalToString(const ScopeEngine::ScopeSqlType::SqlDecimal & s, char * finalOut, int size)
    {
        s.ToString(finalOut, &size);

        return size;
    }

    INLINE ostream &operator<<(ostream &o, const ScopeEngine::ScopeSqlType::SqlDecimal & t)
    {
        // This is the max size for one sql decimal, precision is 38, scale is 38, is negative.
        const int maxSize = 41;
        char finalOut[maxSize];

        SqlDecimalToString(t, finalOut, maxSize);

        o << finalOut;
        return o;
    }

    template<int N>
    ScopeEngine::ScopeSqlType::SqlType_MaxString<N> scope_cast(const FString & u, IncrementalAllocator* alloc)
    {
        return ScopeCast<ScopeEngine::ScopeSqlType::SqlType_MaxString<N>, FString>::get(u, alloc);
    }

    template<int N>
    ScopeEngine::ScopeSqlType::SqlType_MaxString<N> scope_cast(FString && u, IncrementalAllocator* alloc)
    {
        return ScopeCast<ScopeEngine::ScopeSqlType::SqlType_MaxString<N>, FString>::get(std::move(u), alloc);
    }

    template <int N>
    struct ScopeCast<ScopeEngine::ScopeSqlType::SqlType_MaxString<N>, FString>
    {
        static ScopeEngine::ScopeSqlType::SqlType_MaxString<N> get(const FString & value, IncrementalAllocator * alloc)
        {
            ScopeEngine::ScopeSqlType::SqlStringFaceted<N> sqlStr(value, alloc);
            ScopeEngine::ScopeSqlType::SqlType_MaxString<N> s(sqlStr);
            
            return s;
        }

        static ScopeEngine::ScopeSqlType::SqlType_MaxString<N> get(FString && value, IncrementalAllocator * alloc)
        {
            ScopeEngine::ScopeSqlType::SqlStringFaceted<N> sqlStr(std::move(value), alloc);
            ScopeEngine::ScopeSqlType::SqlType_MaxString<N> s(std::move(sqlStr));

            return s;
        }
    };

    template<typename T>
    class ::std::hash<class ScopeEngine::ScopeSqlType::SqlNativeNullable<T> >
    {
    public:
        size_t operator()(const ScopeEngine::ScopeSqlType::SqlNativeNullable<T> & value) const
        {
            return value.GetStdHashCode();
        }
    };

    template<typename ToType, typename FromType>
    struct ScopeCast<ToType, ScopeEngine::ScopeSqlType::SqlNativeNullable<FromType>>
    {
        static ToType get(const ScopeEngine::ScopeSqlType::SqlNativeNullable<FromType>& value)
        {
            return static_cast<ToType>(value.safe_get());
        }
    };

    template<typename ToType, typename FromType>
    struct ScopeCast<NativeNullable<ToType>, ScopeEngine::ScopeSqlType::SqlNativeNullable<FromType>>
    {
        static NativeNullable<ToType> get(const ScopeEngine::ScopeSqlType::SqlNativeNullable<FromType>& value)
        {
            return NativeNullable<ToType>(value);
        }
    };

    template<typename ToType, typename FromType>
    struct ScopeCast<ScopeEngine::ScopeSqlType::SqlNativeNullable<ToType>, NativeNullable<FromType>>
    {
        static ScopeEngine::ScopeSqlType::SqlNativeNullable<ToType> get(const NativeNullable<FromType>& value)
        {
            return ScopeEngine::ScopeSqlType::SqlNativeNullable<ToType>(value);
        }
    };

    template<typename ToType, typename FromType>
    struct ScopeCast<ScopeEngine::ScopeSqlType::SqlNativeNullable<ToType>, ScopeEngine::ScopeSqlType::SqlNativeNullable<FromType>>
    {
        static ScopeEngine::ScopeSqlType::SqlNativeNullable<ToType> get(const ScopeEngine::ScopeSqlType::SqlNativeNullable<FromType>& value)
        {
            return ScopeEngine::ScopeSqlType::SqlNativeNullable<ToType>(value);
        }
    };

    template<typename ToTypeI, typename ToTypeT, typename FromTypeI, typename FromTypeT>
    struct ScopeCast<ScopeEngine::ScopeSqlType::SqlNativeNullable<ScopeEngine::ScopeSqlType::ArithmeticSqlType<ToTypeI, ToTypeT>>, ScopeEngine::ScopeSqlType::SqlNativeNullable<ScopeEngine::ScopeSqlType::ArithmeticSqlType<FromTypeI, FromTypeT >> >
    {
        static ScopeEngine::ScopeSqlType::SqlNativeNullable<ScopeEngine::ScopeSqlType::ArithmeticSqlType<ToTypeI, ToTypeT>> get(const ScopeEngine::ScopeSqlType::SqlNativeNullable<ScopeEngine::ScopeSqlType::ArithmeticSqlType<FromTypeI, FromTypeT >> &value)
        {
            return ScopeEngine::ScopeSqlType::SqlNativeNullable<ScopeEngine::ScopeSqlType::ArithmeticSqlType<ToTypeI, ToTypeT>>((ToTypeT)value.get().getValue());
        }
    };

    template<class I, typename T, typename C>
    struct ScopeCast<ScopeEngine::ScopeSqlType::SqlNativeNullable<ScopeEngine::ScopeSqlType::ArithmeticSqlType<I, T>>, C >
    {
        static ScopeEngine::ScopeSqlType::SqlNativeNullable<ScopeEngine::ScopeSqlType::ArithmeticSqlType<I, T>> get(const typename enable_if<std::is_integral<C>::value, C>::type &value)
        {
            return ScopeEngine::ScopeSqlType::SqlNativeNullable<ScopeEngine::ScopeSqlType::ArithmeticSqlType<I, T>>((T)value);
        }
    };

    template<class I, typename T>
    struct ScopeCast<ScopeEngine::ScopeSqlType::SqlNativeNullable<ScopeEngine::ScopeSqlType::ArithmeticSqlType<I, T>>, nullptr_t >
    {
        static ScopeEngine::ScopeSqlType::SqlNativeNullable<ScopeEngine::ScopeSqlType::ArithmeticSqlType<I, T>> get(const nullptr_t &value)
        {
            return ScopeEngine::ScopeSqlType::SqlNativeNullable<ScopeEngine::ScopeSqlType::ArithmeticSqlType<I, T>>(nullptr);
        }
    };

    // Global operator for comparison.
    //
    template<typename T>
    INLINE int ScopeTypeCompare(const ScopeEngine::ScopeSqlType::SqlNativeNullable<T>& x, const ScopeEngine::ScopeSqlType::SqlNativeNullable<T>& y)
    {
        return x.CompareTo<T>(y);
    }

    template<typename T>
    INLINE bool ScopeTypeCompare_LessEqual(const ScopeEngine::ScopeSqlType::SqlNativeNullable<T>& x, const ScopeEngine::ScopeSqlType::SqlNativeNullable<T>& y)
    {
        if (IsAnyNull(x,y))
        {
             return false;
        }

        return ScopeTypeCompare(x,y) <= 0;
    }

    template<typename T>
    INLINE bool ScopeTypeCompare_GreaterThan(const ScopeEngine::ScopeSqlType::SqlNativeNullable<T>& x, const ScopeEngine::ScopeSqlType::SqlNativeNullable<T>& y)
    {
        if (IsAnyNull(x,y))
        {
            return false;
        }

        return ScopeTypeCompare(x,y) > 0;
    }

    template<typename T>
    INLINE bool ScopeTypeCompare_GreaterEqual(const ScopeEngine::ScopeSqlType::SqlNativeNullable<T>& x, const ScopeEngine::ScopeSqlType::SqlNativeNullable<T>& y)
    {
        if (IsAnyNull(x,y))
        {
            return false;
        }

        return ScopeTypeCompare(x,y) >= 0;
    }

    template<typename T>
    INLINE bool ScopeTypeCompare_EqualEqual(const ScopeEngine::ScopeSqlType::SqlNativeNullable<T>& x, const ScopeEngine::ScopeSqlType::SqlNativeNullable<T>& y)
    {
        if (IsAnyNull(x,y))
        {
            return false;
        }

        return ScopeTypeCompare(x,y) == 0;
    }

    template<typename T>
    INLINE bool ScopeTypeCompare_LessThan(const ScopeEngine::ScopeSqlType::SqlNativeNullable<T>& x, const ScopeEngine::ScopeSqlType::SqlNativeNullable<T>& y)
    {
        if (IsAnyNull(x, y))
        {
            return false;
        }

        return ScopeTypeCompare(x, y) < 0;
    }
}
