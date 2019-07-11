#pragma once

// definition for import scopeengine.dll
#ifndef SCOPE_ENGINE_API
#define SCOPE_ENGINE_API __declspec(dllimport)
#endif

// definition for import scopeenginemanaged.dll
#ifndef SCOPE_ENGINE_MANAGED_API
#define SCOPE_ENGINE_MANAGED_API __declspec(dllimport)
#endif

// SCOPE_RUNTIME_API is used to select compilation mode for methods of certain classes.
// With SCOPE_RUNTIME_API defined to empty, the methods are inlined. This is used in batch mode to improve runtime performance.
// With SCOPE_RUNTIME_API defined to __declspec(dllimport), the actual function calls are generated to the methods defined
// in ScopeRuntimeLib static library. The library is generated using explicit template class instantiation.
// The generated code is linked against the library. This mode is used in iScope to reduce compile time.
// IMPORTANT! Those and only those classes that are intended to be pre-instantiated in ScopeRuntimeLib static library
// should be annotated with SCOPE_RUNTIME_API. All other classes should be annotated with SCOPE_ENGINE_API or nothing.
// TFS #7427789 is created to clean up use of SCOPE_RUNTIME_API.
#if defined(SCOPE_RUNTIME_EXPORT_DLL)
#define SCOPE_RUNTIME_API __declspec(dllexport)
#define SCOPE_NO_UDT
#elif defined(SCOPE_RUNTIME_IMPORT_DLL)
#define SCOPE_RUNTIME_API __declspec(dllimport)
#define SCOPE_NO_UDT
#else
#define SCOPE_RUNTIME_API
#endif

#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <ostream>
#include <limits>
#include <memory>
#include <map>
#include <algorithm>
#include <GuidDef.h>    // for GUID
#include <regex>
#include <sstream>
#include <nmmintrin.h>
#include <codecvt>

#include "scopeengine.h"
#include "PluginTypeSystem.h"
#include "ScopeError.h"
#include "ScopeDateTime.h"

using namespace std;
using namespace std::tr1;

struct ICLRRuntimeHost;

#define INLINE inline
#define FORCE_INLINE __forceinline
#define NO_INLINE __declspec(noinline)
#define SAFE_BUFFERS __declspec(safebuffers)

namespace ScopeEngine
{
    namespace ScopeSqlType
    {
        class SqlStringImp;

        template <typename T>
        class SqlNativeNullable;
    }
}

namespace ScopeEngine
{
    // ScopeManagedHandle class avoids showing managed types to native compilation

    class ScopeManagedHandle;
    SCOPE_ENGINE_MANAGED_API void DestroyScopeHandle(ScopeManagedHandle * handle);

    class ScopeManagedHandle
    {
    public:
        SCOPE_ENGINE_MANAGED_API ScopeManagedHandle();
        template <typename T> ScopeManagedHandle(T t);
        ~ScopeManagedHandle()
        {
            DestroyScopeHandle(this);
        }

        SCOPE_ENGINE_MANAGED_API void reset();
        template<typename T> ScopeManagedHandle& operator=(T t);
        template<typename T> operator T() const;

        friend ostream & operator<<(ostream &o, const ScopeManagedHandle & h);

    private:
        SCOPE_ENGINE_MANAGED_API ScopeManagedHandle(const ScopeManagedHandle & src);
        void * m_handle;
    };

    INLINE ostream & operator<<(ostream & o, const ScopeManagedHandle & h)
    {
        o << hex << "UDT = 0x" << h.m_handle;
        return o;
    }

    extern int BitNthMsf(ULONG mask);
    extern int BitNthMsf(UINT64 mask);
    extern USHORT ByteSwap(USHORT mask);
    extern ULONG ByteSwap(ULONG num);
    extern std::wstring MakeBigString(const wchar_t *first, const wchar_t *second = NULL, ...);
    extern std::string MakeBigString(const char *first, const char *second = NULL, ...);
}

// TODO xiaoyuc, this namespace should be removed when we have switched to plugin type system. This is for testing purpose only
#ifdef PLUGIN_TYPE_SYSTEM_NAMESPACE
namespace PluginType
{
using namespace ScopeEngine;
#else
namespace ScopeEngine
{
#endif
#pragma region ScopeCEP
    enum ScopeCEPEventType
    {
        SCOPECEP_NORMAL = 0,
        /// CTI: current time increment. http://technet.microsoft.com/en-us/library/ff518502.aspx
        SCOPECEP_START_EDGE = 1,
        SCOPECEP_END_EDGE = 2,
        SCOPECEP_CTI_ADJUST = 0x20,
        SCOPECEP_CTI = 0x40,
        SCOPECEP_CHECKPOINT = 0x80,
        SCOPECEP_FINAL_ROW=0xff,
    };

#define SCOPECEP_CTI_CHECKPOINT ((UINT8)SCOPECEP_CTI | (UINT8)SCOPECEP_CHECKPOINT)

#pragma endregion ScopeCEP
    // Classes
    class ScopeDecimal;
    class ScopeGuid;
    template <typename T> class NativeNullable;
    template<typename InputStream> class SCOPE_RUNTIME_API BinaryInputStreamBase;
    template<typename OutputType> class SCOPE_RUNTIME_API BinaryOutputStreamBase;
    class SCOPE_RUNTIME_API CosmosInput;
    class SCOPE_RUNTIME_API MemoryInput;
    class SCOPE_RUNTIME_API CosmosOutput;
    class SCOPE_RUNTIME_API MemoryOutput;
    class SCOPE_RUNTIME_API DummyOutput;
    class SCOPE_RUNTIME_API MemoryInputStream;
    class SCOPE_RUNTIME_API BinaryInputStream;
    class SCOPE_RUNTIME_API BinaryOutputStream;
    class SCOPE_RUNTIME_API MemoryOutputStream;
    class TextOutputStreamBase;
    class SStreamDataOutputStream;

    template<typename Schema> struct ManagedRow;

    // Functions
    SCOPE_ENGINE_MANAGED_API LONGLONG GCTotalMemory();
    SCOPE_ENGINE_MANAGED_API void GCCollect();
    SCOPE_ENGINE_MANAGED_API void GCCollect(int level);

#if defined(SCOPE_RUNTIME_EXPORT_DLL) || defined(SCOPE_RUNTIME_IMPORT_DLL)
    template class SCOPE_RUNTIME_API allocator<char>;
    template class SCOPE_RUNTIME_API allocator<wchar_t>;
    template class SCOPE_RUNTIME_API basic_string<char, char_traits<char>, allocator<char>>;
    template class SCOPE_RUNTIME_API basic_string<wchar_t, char_traits<wchar_t>, allocator<wchar_t>>;
    template class SCOPE_RUNTIME_API allocator<const BufferDescriptor *>;
    template class SCOPE_RUNTIME_API vector<const BufferDescriptor *>;
    template class SCOPE_RUNTIME_API allocator<int>;
    template class SCOPE_RUNTIME_API vector<int>;
#endif
#pragma endregion ForwardDeclarations

#pragma region Utilities

    // key weight for key range in structured stream.
    static const BYTE x_keyWeightNormal = 0;
    static const BYTE x_keyWeightMin = 1;
    static const BYTE x_keyWeightMax = 2;

    // Commit in chunks of 128Kb
    // Commit chunk size is used by IncrementalAllocator which is often used to store row data
    // Commit size is chosen to be big enough to be able to hold at least one row (97% of rows in Cosmos are less than 128Kb)
    // Bigger size may cause commiting to much unused memory in case of vertex with multiple inputs
    #define COMMIT_BLOCK_SIZE (128 * 1024)

    // calculate the minimum size needed by round up to block size
    INLINE SIZE_T RoundUp_Size(SIZE_T size)
    {
        /*COMMIT_BLOCK_SIZE = 1 << 17*/
        SIZE_T numBlocks = (size + COMMIT_BLOCK_SIZE - 1) >> 17;

        return numBlocks * COMMIT_BLOCK_SIZE;
    }

    #define med3(a, b, c) med3func(a, b, c, depth)

    static const int x_NULLHASH = 0x32e56baf;  // some odd int value with random-looking bit settings

    #define DECIMAL_LOG_NEGINF -1000
    #define UINT64_HIGHBIT 0x8000000000000000
    #define UINT32_HIGHBIT 0x80000000
    #define DECIMAL_MAX_SCALE 28
    #define DECIMAL_MAX_INTFACTORS 9

    #define DECIMAL_SUCCESS 0
    #define DECIMAL_FINISHED 1
    #define DECIMAL_INVALID_CHARACTER 2
    #define DECIMAL_INTERNAL_ERROR 3
    #define DECIMAL_DIVIDE_BY_ZERO 4
    #define DECIMAL_BUFFER_OVERFLOW 5
    #define DECIMAL_OVERFLOW 6

    #define DEF_Int128(hi, mid, lo) { (((UINT64)mid)<<32 | lo), hi }

    typedef struct {} None;

    typedef struct {
        UINT64 lo;
        UINT64 hi;
    } Int128;

    static const Int128 dec128decadeFactors[DECIMAL_MAX_SCALE+1] = {
        DEF_Int128( 0, 0, 1u), /* == 1 */
        DEF_Int128( 0, 0, 10u), /* == 10 */
        DEF_Int128( 0, 0, 100u), /* == 100 */
        DEF_Int128( 0, 0, 1000u), /* == 1e3m */
        DEF_Int128( 0, 0, 10000u), /* == 1e4m */
        DEF_Int128( 0, 0, 100000u), /* == 1e5m */
        DEF_Int128( 0, 0, 1000000u), /* == 1e6m */
        DEF_Int128( 0, 0, 10000000u), /* == 1e7m */
        DEF_Int128( 0, 0, 100000000u), /* == 1e8m */
        DEF_Int128( 0, 0, 1000000000u), /* == 1e9m */
        DEF_Int128( 0, 2u, 1410065408u), /* == 1e10m */
        DEF_Int128( 0, 23u, 1215752192u), /* == 1e11m */
        DEF_Int128( 0, 232u, 3567587328u), /* == 1e12m */
        DEF_Int128( 0, 2328u, 1316134912u), /* == 1e13m */
        DEF_Int128( 0, 23283u, 276447232u), /* == 1e14m */
        DEF_Int128( 0, 232830u, 2764472320u), /* == 1e15m */
        DEF_Int128( 0, 2328306u, 1874919424u), /* == 1e16m */
        DEF_Int128( 0, 23283064u, 1569325056u), /* == 1e17m */
        DEF_Int128( 0, 232830643u, 2808348672u), /* == 1e18m */
        DEF_Int128( 0, 2328306436u, 2313682944u), /* == 1e19m */
        DEF_Int128( 5u, 1808227885u, 1661992960u), /* == 1e20m */
        DEF_Int128( 54u, 902409669u, 3735027712u), /* == 1e21m */
        DEF_Int128( 542u, 434162106u, 2990538752u), /* == 1e22m */
        DEF_Int128( 5421u, 46653770u, 4135583744u), /* == 1e23m */
        DEF_Int128( 54210u, 466537709u, 2701131776u), /* == 1e24m */
        DEF_Int128( 542101u, 370409800u, 1241513984u), /* == 1e25m */
        DEF_Int128( 5421010u, 3704098002u, 3825205248u), /* == 1e26m */
        DEF_Int128( 54210108u, 2681241660u, 3892314112u), /* == 1e27m */
        DEF_Int128( 542101086u, 1042612833u, 268435456u), /* == 1e28m */
    };

    static const ULONG constantsDecadeInt32Factors[DECIMAL_MAX_INTFACTORS+1] =
        {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

    static const char x_HexTable [] = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};

    static const char x_digit_pairs[201] = {
                                            "00010203040506070809"
                                            "10111213141516171819"
                                            "20212223242526272829"
                                            "30313233343536373839"
                                            "40414243444546474849"
                                            "50515253545556575859"
                                            "60616263646566676869"
                                            "70717273747576777879"
                                            "80818283848586878889"
                                            "90919293949596979899"
                                        };

    // Text Encoding we support for default text extractor and outputer
    enum TextEncoding
    {
        NONE,
        ASCII,
        Default,
        UTF7,
        UTF8,
        UTF32,
        Unicode,
        BigEndianUnicode,
        BigEndianUTF32,
    };

    INLINE std::string GetEncodingName(TextEncoding encoding)
    {
        switch (encoding)
        {
        case NONE:
            return "None";
        case ASCII:
            return "ASCII";
        case Default:
            return "Default";
        case UTF7:
            return "UTF-7";
        case UTF8:
            return "UTF-8";
        case UTF32:
            return "UTF-32";
        case Unicode:
            return "Unicode";
        case BigEndianUnicode:
            return "Big Endian Unicode";
        case BigEndianUTF32:
            return "Big Endian UTF-32";
        default:
            SCOPE_ASSERT(false);
            return "UNKNOWN";
        }
    }

    INLINE int Log2_32(ULONG a)
    {
        if (a == 0)
            return DECIMAL_LOG_NEGINF;

        return BitNthMsf (a) + 1;
    }

    /* returns log2(a) or DECIMAL_LOG_NEGINF for a = 0 */
    INLINE int Log2_64(UINT64 a)
    {
        if (a == 0)
            return DECIMAL_LOG_NEGINF;

        if ((a >> 32) == 0)
            return BitNthMsf ((ULONG)a) + 1;
        else
            return BitNthMsf ((ULONG)(a >> 32)) + 1 + 32;
    }

    template <class T>
    INLINE void swap2 ( T* a, T* b )
    {
        T t(*a);

        *a = *b;
        *b = t;
    }

    // default implementation based on > and < operator
    template<typename T>
    INLINE int ScopeTypeCompare(const T & x, const T & y)
    {
        return (x < y ? -1 : x == y ? 0 : 1);
    }

    // a set of intrinsic comparison functions that have SCOPE semantics (i.e. null is the highest value)
    template<typename T>
    INLINE bool ScopeTypeCompare_LessThan(const T & x, const T & y)
    {
        return ScopeTypeCompare(x,y) < 0;
    }

    template<typename T>
    INLINE bool ScopeTypeCompare_LessEqual(const T & x, const T & y)
    {
        return ScopeTypeCompare(x,y) <= 0;
    }

    template<typename T>
    INLINE bool ScopeTypeCompare_GreaterThan(const T & x, const T & y)
    {
        return ScopeTypeCompare(x,y) > 0;
    }

    template<typename T>
    INLINE bool ScopeTypeCompare_GreaterEqual(const T & x, const T & y)
    {
        return ScopeTypeCompare(x,y) >= 0;
    }

    template<typename T>
    INLINE bool ScopeTypeCompare_EqualEqual(const T & x, const T & y)
    {
        return ScopeTypeCompare(x,y) == 0;
    }

    template<>
    INLINE int ScopeTypeCompare<float>(const float & x, const float & y)
    {
        if (_isnan(x))
        {
            return _isnan(y) ? ScopeTypeCompare(*(int*)&x, *(int*)&y) : -1;
        }

        return (x < y ? -1 : x == y ? 0 : 1);
    }

    template<>
    INLINE int ScopeTypeCompare<double>(const double & x, const double & y)
    {
        if (_isnan(x))
        {
            return _isnan(y) ? ScopeTypeCompare(*(__int64*)&x, *(__int64*)&y) : -1;
        }

        return (x < y ? -1 : x == y ? 0 : 1);
    }

    template<typename T>
    INLINE int ScopeHash(const T& v)
    {
        return v.GetScopeHashCode();
    }

    template<>
    INLINE int ScopeHash<float>(const float& v)
    {
        if (v == 0)
        {
            return 0;
        }
        return *(int*)(&v);
    }

    template<>
    INLINE int ScopeHash<bool>(const bool& v)
    {
        return v ? 0x172ba9c7 : -0x3a59cb12;
    }

    template<>
    INLINE int ScopeHash<int>(const int& v)
    {
        // Use Thomas Wang's public domain 32-bit integer hash
        unsigned int a = v;
        a = (a ^ 61) ^ (a >> 16);
        a += (a << 3);
        a ^= (a >> 4);
        a *= 0x27d4eb2d;
        a ^= (a >> 15);
        return (int)a;
    }

    template<>
    INLINE int ScopeHash<double>(const double& v)
    {
        if (v == 0)
        {
            return 0;
        }

        __int64 value = *(__int64*)(&v);
        return (int)(value) ^ (int)(value >> 32);
    }

    template<>
    INLINE int ScopeHash<__int64>(const __int64& v)
    {
        // Use Thomas Wang's public domain 64-bit to 32-bit hash
        unsigned __int64 a = v;
        a = (~a) + (a << 18);
        a ^= (a >> 31);
        a *= 21;
        a ^= (a >> 11);
        a += (a << 6);
        a ^= (a >> 22);
        return (int)a;
    }

    template<>
    INLINE int ScopeHash<char>(const char& v)
    {
        return ScopeHash((int) v);
    }

    template<>
    INLINE int ScopeHash<unsigned char>(const unsigned char& v)
    {
        return ScopeHash((int) v);
    }

    template<>
    INLINE int ScopeHash<short>(const short& v)
    {
        return ScopeHash((int) v);
    }

    template<>
    INLINE int ScopeHash<unsigned short>(const unsigned short& v)
    {
        return ScopeHash((int) v);
    }

    template<>
    INLINE int ScopeHash<wchar_t>(const wchar_t& v)
    {
        return ScopeHash((int) v);
    }

    template<>
    INLINE int ScopeHash<unsigned int>(const unsigned int& v)
    {
        return ScopeHash((int) v);
    }

    template<>
    INLINE int ScopeHash<unsigned __int64>(const unsigned __int64& v)
    {
        return ScopeHash((__int64) v);
    }

    /*
     * CRC32Hash function uses SSE4 instruction crc32 to compute the result. The instruction
     * accumulates 32-bit CRC, unsigned __int64 return type is used for convenience.
     *
     * All 8-byte primitive types are processed with _mm_crc32_u64 intrinsic; all 4-or-less-byte
     * primitive types are processed with _mm_crc32_u32 intinsic [_mm_crc32_u64(0, 1) != _mm_crc32_u32(0, 1)].
     *
     * NB: CRC32 hash codes do not match the CLR hash codes hence cannot be used for hash partitioning.
     */
    template <typename T>
    INLINE unsigned __int64 CRC32Hash(unsigned __int64 crc, const T& value)
    {
        return value.GetCRC32Hash(crc);
    }

    template <>
    INLINE unsigned __int64 CRC32Hash<unsigned __int64>(unsigned __int64 crc, const unsigned __int64& value)
    {
        return _mm_crc32_u64(crc, value);
    }

    template <>
    INLINE unsigned __int64 CRC32Hash<unsigned int>(unsigned __int64 crc, const unsigned int& value)
    {
        return _mm_crc32_u32(static_cast<unsigned int>(crc), value);
    }

    // float -> unsigned int
    template<>
    INLINE unsigned __int64 CRC32Hash<float>(unsigned __int64 crc, const float& value)
    {
        return CRC32Hash(crc, *reinterpret_cast<unsigned int*>(const_cast<float*>((&value))));
    }

    // double -> unsigned __int64
    template<>
    INLINE unsigned __int64 CRC32Hash<double>(unsigned __int64 crc, const double& value)
    {
        return CRC32Hash(crc, *reinterpret_cast<unsigned __int64*>(const_cast<double*>((&value))));
    }

    // int -> unsigned int
    template<>
    INLINE unsigned __int64 CRC32Hash<int>(unsigned __int64 crc, const int& value)
    {
        return CRC32Hash(crc, static_cast<unsigned int>(value));
    }

    // char -> unsigned int
    template<>
    INLINE unsigned __int64 CRC32Hash<char>(unsigned __int64 crc, const char& value)
    {
        return CRC32Hash(crc, static_cast<unsigned int>(value));
    }

    // unsigned char -> unsigned int
    template<>
    INLINE unsigned __int64 CRC32Hash<unsigned char>(unsigned __int64 crc, const unsigned char& value)
    {
        return CRC32Hash(crc, static_cast<unsigned int>(value));
    }

    // short -> unsigned int
    template<>
    INLINE unsigned __int64 CRC32Hash<short>(unsigned __int64 crc, const short& value)
    {
        return CRC32Hash(crc, static_cast<unsigned int>(value));
    }

    // unsigned short -> unsigned int
    template<>
    INLINE unsigned __int64 CRC32Hash<unsigned short>(unsigned __int64 crc, const unsigned short& value)
    {
        return CRC32Hash(crc, static_cast<unsigned int>(value));
    }

    // wchar_t -> unsigned int
    template<>
    INLINE unsigned __int64 CRC32Hash<wchar_t>(unsigned __int64 crc, const wchar_t& value)
    {
        return CRC32Hash(crc, static_cast<unsigned int>(value));
    }

    // bool -> unsigned int
    template<>
    INLINE unsigned __int64 CRC32Hash<bool>(unsigned __int64 crc, const bool& value)
    {
        return CRC32Hash(crc, static_cast<unsigned int>(value));
    }

    // INT64 -> unsigned __int64
    template<>
    INLINE unsigned __int64 CRC32Hash<__int64>(unsigned __int64 crc, const __int64& value)
    {
        return CRC32Hash(crc, static_cast<unsigned __int64>(value));
    }

    class SCOPE_RUNTIME_API IncrementalAllocator
    {
    public:
        struct SCOPE_RUNTIME_API StatCounters
        {
            SIZE_T m_peakSize; // peak committed size
            UINT m_commitCount;
            UINT m_resetCount;
            SIZE_T m_stringPeakSize;
            SIZE_T m_binaryPeakSize;
            SIZE_T m_rowDataPeakSize;

            StatCounters()
            {
                Reset();
            }

            void Reset()
            {
                m_peakSize = 0;
                m_commitCount = 0;
                m_resetCount = 0;
                m_stringPeakSize = 0;
                m_binaryPeakSize = 0;
                m_rowDataPeakSize = 0;
            }
        };

        struct SCOPE_RUNTIME_API Statistics
        {
            string m_ownerName;
            SIZE_T m_maxSize; // reservation size
            StatCounters m_counters;

            // intermediate data that support calculation of average values of size statistics
            UINT m_count; // number of individual statistics aggregated in this object
            double m_sumMaxSize;
            double m_sumPeakSize;
            double m_sumStringPeakSize;
            double m_sumBinaryPeakSize;
            double m_sumRowDataPeakSize;

            Statistics()
                : m_ownerName(), m_maxSize(0), m_counters(),
                  m_count(0), m_sumMaxSize(0), m_sumPeakSize(0),
                  m_sumStringPeakSize(0), m_sumBinaryPeakSize(0), m_sumRowDataPeakSize(0)
            {
            }

            Statistics(const string& ownerName, SIZE_T maxSize, const StatCounters& counters)
                : m_ownerName(ownerName), m_maxSize(maxSize), m_counters(counters),
                  m_count(1), m_sumMaxSize((double)maxSize), m_sumPeakSize((double)counters.m_peakSize),
                  m_sumStringPeakSize((double)counters.m_stringPeakSize),
                  m_sumBinaryPeakSize((double)counters.m_binaryPeakSize),
                  m_sumRowDataPeakSize((double)counters.m_rowDataPeakSize)
            {
            }

            bool IsEmpty() const
            {
                return m_count == 0;
            }

            void Aggregate(SIZE_T maxSize, const StatCounters& counters)
            {
                m_maxSize = std::max<SIZE_T>(maxSize, m_maxSize);
                m_counters.m_peakSize = std::max<SIZE_T>(counters.m_peakSize, m_counters.m_peakSize);
                m_counters.m_commitCount = std::max<UINT>(counters.m_commitCount, m_counters.m_commitCount);
                m_counters.m_resetCount = std::max<UINT>(counters.m_resetCount, m_counters.m_resetCount);
                m_counters.m_stringPeakSize = std::max<SIZE_T>(counters.m_stringPeakSize, m_counters.m_stringPeakSize);
                m_counters.m_binaryPeakSize = std::max<SIZE_T>(counters.m_binaryPeakSize, m_counters.m_binaryPeakSize);
                m_counters.m_rowDataPeakSize = std::max<SIZE_T>(counters.m_rowDataPeakSize, m_counters.m_rowDataPeakSize);

                m_count++;
                m_sumMaxSize += maxSize;
                m_sumPeakSize += counters.m_peakSize;
                m_sumStringPeakSize += counters.m_stringPeakSize;
                m_sumBinaryPeakSize += counters.m_binaryPeakSize;
                m_sumRowDataPeakSize += counters.m_rowDataPeakSize;
            }

            // allocator contains only data of variable size (e.g string columns etc)
            // in order to get full row size we need size of fixed part too (e.g int columns etc)
            // default value (-1) of fixedRowDataSize means allocator contains something different than row data
            void WriteRuntimeStats(TreeNode & root, long fixedRowDataSize = -1) const
            {
                auto & node = root.AddElement("Allocator_" + m_ownerName);
                node.AddAttribute(RuntimeStats::MaxReservedSize(), m_maxSize);
                node.AddAttribute(RuntimeStats::MaxCommittedSize(), m_counters.m_peakSize);
                node.AddAttribute(RuntimeStats::MaxCommitCount(), m_counters.m_commitCount);
                node.AddAttribute(RuntimeStats::MaxResetCount(), m_counters.m_resetCount);
                node.AddAttribute(RuntimeStats::MaxStringSize(), m_counters.m_stringPeakSize);
                node.AddAttribute(RuntimeStats::MaxBinarySize(), m_counters.m_binaryPeakSize);
                if (fixedRowDataSize >= 0)
                {
                    node.AddAttribute(RuntimeStats::MaxRowDataSize(), m_counters.m_rowDataPeakSize + fixedRowDataSize);
                    node.AddAttribute(RuntimeStats::MaxFixedRowDataSize(), fixedRowDataSize);
                }

                node.AddAttribute(RuntimeStats::AvgReservedSize(), m_count == 0 ? 0 : (LONGLONG)(m_sumMaxSize / m_count));
                node.AddAttribute(RuntimeStats::AvgCommittedSize(), m_count == 0 ? 0 : (LONGLONG)(m_sumPeakSize / m_count));
                node.AddAttribute(RuntimeStats::AvgStringSize(), m_count == 0 ? 0 : (LONGLONG)(m_sumStringPeakSize / m_count));
                node.AddAttribute(RuntimeStats::AvgBinarySize(), m_count == 0 ? 0 : (LONGLONG)(m_sumBinaryPeakSize / m_count));
                if (fixedRowDataSize >= 0)
                {
                    node.AddAttribute(RuntimeStats::AvgRowDataSize(), m_count == 0 ? 0 : (LONGLONG)(m_sumRowDataPeakSize / m_count) + fixedRowDataSize);
                }
            }
        };

    private:
        static const int x_idLimit = 0xFFFF; // 64K allocator at most
        static const ULONG x_versionLimit = 0xFFFFFFFF;

        MemoryManager * m_memMgr;
        const ReservationDescriptor * m_reservation;
        vector<const BufferDescriptor *> m_commits;

        char  * m_buffer;
        SIZE_T m_maxSize; // reservation size
        SIZE_T m_size; // commit size
        SIZE_T m_available;
        SIZE_T m_rowDataSize; // contains size of data without space for alignment or helper data

        // Pointers to saved marker
        char * m_marker;

        // Pointers to the next available space
        char * m_current;

        LONG   m_id;  // globe unique id of the allocator
        ULONG  m_version; // current version of the allocator. Each time we call reset, rollback, version will be bump up.

        string m_ownerName;
        StatCounters m_statCounters;

        bool   m_initialized;

        bool   m_useCommitMaxSize;
        SIZE_T m_commitMaxSize;

        void Expire()
        {
            if (m_version < x_versionLimit)
            {
                ++m_version;
            }
        }

    protected:
        virtual void ThrowOutOfMemory(SIZE_T /*maxSize*/)
        {
            throw RuntimeMemoryException(m_ownerName.c_str());
        }

    public:
        // Policies controlling how much memory is decommited when Reset is called
        struct ReclaimAllMemoryPolicy
        {
            static const SIZE_T x_reservedPageCnt = 0;
        };

        struct DontReclaimMemoryPolicy
        {
            static const SIZE_T x_reservedPageCnt = 0xFFFFFFFF; // some very large number but not INT64MAX to avoid overflow
        };

        struct AmortizeMemoryAllocationPolicy
        {
            static const SIZE_T x_reservedPageCnt = 64;
        };

        enum AllocationType
        {
            OtherAllocation = 0,
            StringAllocation = 1,
            BinaryAllocation = 2
        };

        //
        // maxSize = max reservation size
        //
        IncrementalAllocator(SIZE_T maxSize, const char* ownerName) : m_version(0), m_initialized(false)
        {
            // Get a unique id for allocator
            m_id = CreateAllocatorId();
            Init(maxSize, ownerName);
        }

        IncrementalAllocator(SIZE_T maxSize, const string& ownerName) : m_version(0), m_initialized(false)
        {
            // Get a unique id for allocator
            m_id = CreateAllocatorId();
            Init(maxSize, ownerName);
        }

        IncrementalAllocator() : m_memMgr(NULL),
                      m_reservation(NULL),
                      m_buffer(NULL),
                      m_maxSize(0),
                      m_size(0),
                      m_marker(NULL),
                      m_current(NULL),
                      m_available(0),
                      m_version(0),
                      m_initialized(false),
                      m_useCommitMaxSize(false),
                      m_commitMaxSize(0),
                      m_ownerName()
        {
            m_id = CreateAllocatorId();
        }

        // Get a unique id for allocator
        LONG CreateAllocatorId()
        {
            LONG id = MemoryManager::GetGlobal()->GetNextAllocatorId();
            if (id > x_idLimit)
            {
                throw RuntimeException(E_SYSTEM_ERROR, "IncrementalAllocator has run out of Id.");
            }
            return id;
        }

        void Init(SIZE_T maxSize, const char* ownerName)
        {
            SCOPE_ASSERT(ownerName != NULL);
            Init(maxSize, string(ownerName));
        }

        void Init(SIZE_T maxSize, const string& ownerName)
        {
            if (m_initialized)
            {
                throw RuntimeException(E_SYSTEM_ERROR, "IncrementalAllocator can only be initialized once.");
            }

            SIZE_T limit = RoundUp_Size(maxSize);
            m_memMgr = MemoryManager::GetGlobal();
            m_reservation = m_memMgr->Reserve(limit, BT_Execution);
            m_buffer = (char*)m_reservation->m_buffer;
            m_maxSize = limit;
            m_current = m_buffer;

            m_size = 0;
            m_marker = NULL;
            m_available = 0;
            m_initialized = true;
            m_useCommitMaxSize = false;
            m_commitMaxSize = 0;

            m_statCounters.Reset();
            m_rowDataSize = 0;

            SCOPE_ASSERT(ownerName != "");
            m_ownerName = ownerName;
        }

        //
        // Destruct (free all memory)
        //
        ~IncrementalAllocator(void)
        {
            if (m_memMgr)
            {
                for (vector<const BufferDescriptor *>::const_reverse_iterator it = m_commits.rbegin(); it != m_commits.rend(); it++)
                {
                    m_memMgr->Decommit(*it);
                }
                m_memMgr->Release(m_reservation);
            }
        }

        //
        // Sets an upper bound on commited memory;
        // used in HashCombinerV2
        //
        void ResetSoftMaxSize(SIZE_T softMaxSize)
        {
            m_useCommitMaxSize = true;
            m_commitMaxSize = softMaxSize;
        }

        //
        // Returns amount of reserved memory
        //
        SIZE_T GetMaxSize() const
        {
            return m_maxSize;
        }

        //
        // Returns amount of committed memory
        //
        SIZE_T GetSize() const
        {
            return m_commits.size() * COMMIT_BLOCK_SIZE;
        }

        //
        // Pointer to the beginning of the buffer
        //
        void* Buffer()
        {
            return m_buffer;
        }

        //
        // allocator unique id
        //
        USHORT Id() const
        {
            return m_id & x_idLimit;
        }

        //
        // current version of allocator. Each reset/rollback will bump up version.
        //
        ULONG Version() const
        {
            return m_version;
        }

        //
        // Checks if allocator matches Id and Version
        //
        bool Match(USHORT id, ULONG version) const
        {
            if (Version() < x_versionLimit)
            {
                return Id() == id && version == Version();
            }

            return false;
        }

        //
        // Returns amount of available space
        //
        SIZE_T Available()
        {
            return m_available;
        }

        //
        // Start new memory transaction
        //
        void SaveMarker()
        {
            char * marker = Allocate(sizeof(char*));
            *(char **)marker = m_marker;
            m_marker = m_current-sizeof(char*);
        }

        void CommitSlow(SIZE_T size)
        {
            // calculate how much more memory we need to commit exclude available memory.
            SIZE_T commitSize = RoundUp_Size(size - m_available);

            if (m_useCommitMaxSize && commitSize > m_commitMaxSize)
            {
                ThrowOutOfMemory(m_commitMaxSize);
            }

            if (m_size + commitSize > m_maxSize)
            {
                ThrowOutOfMemory(m_maxSize);
            }

            for (ULONG i = 0; i<commitSize / COMMIT_BLOCK_SIZE; ++i)
            {
                m_commits.push_back(m_memMgr->Commit(m_reservation, COMMIT_BLOCK_SIZE));
            }

            m_size += commitSize;
            m_available += commitSize;

            m_statCounters.m_commitCount++;
            m_statCounters.m_peakSize = std::max<SIZE_T>(m_statCounters.m_peakSize, m_size);
        }

        //
        // Allocates memory
        //
        SAFE_BUFFERS char * Allocate(SIZE_T size)
        {
            // Check if we need to commit reservation
            if (m_available < size)
            {
                CommitSlow(size);
            }

            char *result = m_current;
            m_current += size;
            m_available -= size;
            return result;
        }

        void UpdateDataSizeStats(SIZE_T dataSize, AllocationType allocType)
        {
            if (allocType == StringAllocation)
            {
                m_statCounters.m_stringPeakSize = std::max<SIZE_T>(m_statCounters.m_stringPeakSize, dataSize);
            }
            else if (allocType == BinaryAllocation)
            {
                m_statCounters.m_binaryPeakSize = std::max<SIZE_T>(m_statCounters.m_binaryPeakSize, dataSize);
            }
            m_rowDataSize += dataSize;
        }

        //
        // Allocates memory and the start position is aligned.
        //
        template <int alignment>
        char * AllocateAligned(SIZE_T size)
        {
            static_assert(alignment > 0 && ((alignment & (alignment - 1)) == 0), "alignment must be power of 2 and > 0");

            const SIZE_T mask = (SIZE_T)(alignment - 1);

            const SIZE_T padSize = ((SIZE_T)alignment - (SIZE_T)m_current & mask) & mask;

            const SIZE_T newSize = size + padSize;

            char * p = Allocate(newSize);

            return p + padSize;
        }

        //
        // set top point
        //
        void SetTop(char * top)
        {
            // Check if we can SetTop
            if (m_current > top && (top > m_marker+sizeof(char*)))
            {
                m_available += m_current-top;
                m_current = top;
            }
        }

        //
        // Rollbacks transaction
        //
        void Rollback()
        {
            m_current = m_marker;
            m_marker = *(char **)m_current;
            m_available = m_buffer + m_size - m_current;

            Expire();
        }

        //
        // Empties storage
        //
        template <class ReclaimMemoryPolicy>
        void Reset()
        {
            // It is expensive to do memory manager allocate and decommit because all the requests will be
            // synchronized in memory manager. We need to cache certain pages during reset to avoid memory manager call.
            if (m_size > COMMIT_BLOCK_SIZE * ReclaimMemoryPolicy::x_reservedPageCnt)
            {
                SIZE_T releaseCnt = m_commits.size() - ReclaimMemoryPolicy::x_reservedPageCnt;

                // Decommit all commits but the first x_reservedPageCnt
                while (releaseCnt-- > 0)
                {
                    m_memMgr->Decommit(m_commits.back());
                    m_commits.pop_back();
                }

                m_size = COMMIT_BLOCK_SIZE * ReclaimMemoryPolicy::x_reservedPageCnt;
            }

            m_marker = NULL;
            m_current = m_buffer;
            m_available = m_size;
            m_useCommitMaxSize = false;
            m_commitMaxSize = 0;

            m_statCounters.m_resetCount++;
            EndOfRowUpdateStats();

            Expire();
        }

        // Reset with default reclaim policy
        void Reset()
        {
           Reset<AmortizeMemoryAllocationPolicy>();
        }

        void EndOfRowUpdateStats()
        {
            m_statCounters.m_rowDataPeakSize = std::max<SIZE_T>(m_statCounters.m_rowDataPeakSize, m_rowDataSize);
            m_rowDataSize = 0;
        }

        bool HasStatistics() const
        {
            return m_statCounters.m_commitCount > 0;
        }

        Statistics GetStatistics() const
        {
            return Statistics(m_ownerName, GetMaxSize(), m_statCounters);
        }

        void AggregateToOuterStatistics(Statistics& stats) const
        {
            if (stats.IsEmpty())
            {
                stats.m_ownerName = m_ownerName;
            }
            else
            {
                SCOPE_ASSERT(stats.m_ownerName == m_ownerName);
            }
            stats.Aggregate(GetMaxSize(), m_statCounters);
        }

        virtual void WriteRuntimeStats(TreeNode & root, long fixedRowDataSize = -1) const
        {
            GetStatistics().WriteRuntimeStats(root, fixedRowDataSize);
        }
    };

    // throws user friendly error in case of out of memory condition
    class SCOPE_RUNTIME_API RowEntityAllocator : public IncrementalAllocator
    {
    public:
        enum ContentType
        {
            ColumnContent = 1,
            KeyContent = 2,
            RowContent = 3
        };

    private:
        ContentType m_contentType;

        void ThrowOutOfMemory(SIZE_T maxSize)
        {
            const char* contentName;
            ErrorNumber errorNumber;

            switch (m_contentType)
            {
            case ColumnContent:
                contentName = "Column";
                errorNumber = E_USER_COLUMN_TOO_BIG;
                break;
            case KeyContent:
                contentName = "Key";
                errorNumber = E_USER_KEY_TOO_BIG;
                break;
            case RowContent:
                contentName = "Row";
                errorNumber = E_USER_ROW_TOO_BIG;
                break;
            default:
                throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Unknown content type of RowEntityAllocator");
            }

            std::stringstream ss;
            ss << contentName << " size exceeds the maximum allowed size of " << maxSize << " bytes";
            throw RuntimeException(errorNumber, ss.str().c_str());
        }

    public:
        RowEntityAllocator(SIZE_T maxSize, const char* ownerName, ContentType contentType) : IncrementalAllocator(maxSize, ownerName), m_contentType(contentType)
        {
        }

        RowEntityAllocator(SIZE_T maxSize, const string& ownerName, ContentType contentType) : IncrementalAllocator(maxSize, ownerName), m_contentType(contentType)
        {
        }

        explicit RowEntityAllocator(ContentType contentType) : IncrementalAllocator(), m_contentType(contentType)
        {
        }
    };

    // An in-order allocator, for data that is destroyed in creation order.
    // CreateTime(x) < CreateTime(y) <=> DestroyTime(x) < DestroyTime(y)
    //
    // Destruction is controlled by the client, by signaling a new epoch whenever
    // it is safe to dispose of the stalest allocations.
    //
    // Not thread-safe.
    // This is not a complete allocator, as it does not implement Allocate.
    // Instead, to allocate, you request the current suballocator to use.
    //
    // Implementation:
    // Say the maximum simultaneous allocation is K.  Use two IncrementalAllocators of size >=K.
    // Allocate from one of them until it fills, then clear the other and switch to it.
    //
    class SCOPE_RUNTIME_API InOrderAllocator
    {
    private:
        IncrementalAllocator suballocators[2];
        SIZE_T m_epoch;                            // Increases every time we switch allocators.

        SIZE_T Current() const { return m_epoch % 2; }

    public:
        void Init(SIZE_T maxSize, const string& ownerName)
        {
            suballocators[0].Init(maxSize/2, ownerName + "_InOrder1");
            suballocators[1].Init(maxSize/2, ownerName + "_InOrder2");
            m_epoch = 0;
        }

        IncrementalAllocator &CurrentAllocator() { return suballocators[Current()]; }

        void AdvanceEpoch()
        {
            ++m_epoch;
            CurrentAllocator().Reset();
        }

        void WriteRuntimeStats(TreeNode & root) const
        {
            suballocators[0].WriteRuntimeStats(root);
            suballocators[1].WriteRuntimeStats(root);
        }
    };

    /*************************************************/
    /** STL compartible IncrementalAllocator wrapper */

    template <typename T>
    class STLIncrementalAllocator
    {
    public:
        typedef T value_type;
        typedef T* pointer;
        typedef T& reference;
        typedef const T* const_pointer;
        typedef const T& const_reference;

        template <typename U>
        struct rebind
        {
            typedef STLIncrementalAllocator<U> other;
        };

    private:
        IncrementalAllocator* m_alloc;

    public:
        STLIncrementalAllocator()
            : m_alloc(nullptr)
        {
        }

        STLIncrementalAllocator(IncrementalAllocator* alloc)
            : m_alloc(alloc)
        {
        }

        STLIncrementalAllocator(const STLIncrementalAllocator& other)
            : m_alloc(other.m_alloc)
        {
        }

        template <typename U>
        STLIncrementalAllocator(const STLIncrementalAllocator<U>& other)
            : m_alloc(other.m_alloc)
        {
        }

        IncrementalAllocator* allocator() const
        {
            return m_alloc;
        }

        pointer allocate(SIZE_T n)
        {
            SIZE_T size = n * sizeof(value_type);
            char* memory = nullptr;

            memory = m_alloc->Allocate(size);

            SCOPE_ASSERT(memory);

            return reinterpret_cast<pointer>(memory);
        }

        void deallocate(pointer p, SIZE_T /*n*/)
        {
            // Deallocate should never be called for STLIncrementalAllocator.
            // This method is called from std::_Tree_alloc destructor, so:
            // 1) SCOPE_ASSERT can't be used as we don't want to throw from the destructor.
            // 2) If statement is used to avoid C4722 warning while compiling xtree header.
            if (p)
            {
                SCOPE_TERMINATE("Unexpected STLIncrementalAllocator::deallocate call.");
            }
        }

        void construct(pointer p, const T& t)
        {
            new ((void*)p) T(t);
        }

        template<class U, class... Args>
        void construct(U* p, Args&&... args)
        {
            new ((void*)p) U(std::forward<Args>(args)...);
        }

        void destroy(pointer p)
        {
            p->T::~T();
        }

        bool operator == (const STLIncrementalAllocator& other) const
        {
            return m_alloc == other.m_alloc;
        }

        bool operator != (const STLIncrementalAllocator& other) const
        {
            return !operator == (other);
        }

        template <class U> friend class STLIncrementalAllocator;
    };

    // Void specialization of STLIncrementalAllocator used for convenience.
    template <>
    class STLIncrementalAllocator<void>
    {
    public:
        typedef void value_type;
        typedef void* pointer;
        typedef const void* const_pointer;

        template <typename U>
        struct rebind
        {
            typedef STLIncrementalAllocator<U> other;
        };

    private:
        IncrementalAllocator* m_alloc;

    public:
        STLIncrementalAllocator()
            : m_alloc(nullptr)
        {
        }

        STLIncrementalAllocator(IncrementalAllocator* alloc)
            : m_alloc(alloc)
        {
        }

        STLIncrementalAllocator(const STLIncrementalAllocator& other)
            : m_alloc(other.m_alloc)
        {
        }

        template <typename U>
        STLIncrementalAllocator(const STLIncrementalAllocator<U>& other)
            : m_alloc(other.m_alloc)
        {
        }

        bool operator == (const STLIncrementalAllocator& other) const
        {
            return m_alloc == other.m_alloc;
        }

        bool operator != (const STLIncrementalAllocator& other) const
        {
            return !operator == (other);
        }

        template <class U> friend class STLIncrementalAllocator;
    };

    typedef STLIncrementalAllocator<void> DefaultSTLIncrementalAllocator;

    template <typename T> class STLAllocator;

    // [ankorsun] TODO remove along with DistinctHashTable
    template <> class STLAllocator<void>
    {
    public:
        typedef void* pointer;
        typedef const void* const_pointer;
        // reference to void members are impossible.
        typedef void value_type;
        template <class U>
            struct rebind { typedef STLAllocator<U> other; };
    };

    INLINE void destruct(char *){}

    INLINE void destruct(wchar_t*){}

// unreferenced parameter warning
#pragma warning( push )
#pragma warning( disable : 4100)
    template <typename T>
    INLINE void destruct(T *t)
    {
        t->~T();
    }
#pragma warning( pop )

    template <typename T>
    class STLAllocator
    {
        // Default constructor is private.
        STLAllocator();

        // Any allocation over 100000000 will go through new/delete.
        // Our assumption is that such large memory allocation is not frequent and
        // it is important to be able to release such large memory allocation in STL container.
        static const int x_largeMemoryLimit = 100000000;

        // Avoid "assignment operator cannot be generated"
        STLAllocator& operator=(const STLAllocator &);

    public:
        typedef size_t size_type;
        typedef ptrdiff_t difference_type;
        typedef T* pointer;
        typedef const T* const_pointer;
        typedef T& reference;
        typedef const T& const_reference;
        typedef T value_type;

        template <class U>
        struct rebind
        {
            typedef STLAllocator<U> other;
        };

        STLAllocator(IncrementalAllocator & allocator):m_alloc(allocator)
        {
        }

        pointer address(reference x) const
        {
            return &x;
        }

        const_pointer address(const_reference x) const
        {
            return &x;
        }

        pointer allocate(size_type size, STLAllocator<void>::const_pointer /*hint*/ = 0)
        {
            // For large memory allocation we go through windows heap. It is important to
            // release those memory freely. This can be replaced with new blob memory allocator
            // added by Martin.
            if (sizeof(T)*size > x_largeMemoryLimit)
            {
                return (pointer)(new char[sizeof(T)*size]);
            }
            else
            {
                return (pointer)(m_alloc.Allocate(size*sizeof(T)));
            }
        }

        template <class U>
        friend class STLAllocator;

        template <class U>
        STLAllocator(const STLAllocator<U>& c) : m_alloc(c.m_alloc)
        {
        }

        void deallocate(pointer p, size_type n)
        {
            if (sizeof(T)*n > x_largeMemoryLimit)
            {
                delete [] ((char*)p);
            }
        }

        void deallocate(void *p, size_type n)
        {
            if (n > x_largeMemoryLimit)
            {
                delete [] ((char*)p);
            }
        }

        size_type max_size() const throw()
        {
            return size_t(-1) / sizeof(value_type);
        }

        void construct(pointer p, const T& val)
        {
            new(static_cast<void*>(p)) T(val);
        }

        void construct(pointer p)
        {
            new(static_cast<void*>(p)) T();
        }

        void destroy(pointer p)
        {
            destruct(p);
        }

        static void dump()
        {
        }

    private:
        IncrementalAllocator & m_alloc;
    };

    template<>
    void STLAllocator<_Container_proxy>::construct(pointer p, const _Container_proxy & t)
    {
        new(p) _Container_proxy(t);
    }

    template <typename T, typename U>
    INLINE bool operator==(const STLAllocator<T>& a, const STLAllocator<U> & b)
    {
        return &(a.m_alloc) == &(b.m_alloc);
    }

    template <typename T, typename U>
    INLINE bool operator!=(const STLAllocator<T>& a, const STLAllocator<U> & b)
    {
        return &(a.m_alloc) != &(b.m_alloc);
    }

    template<typename T, typename OtherAllocator>
    INLINE bool operator==(const STLAllocator<T> &, const OtherAllocator &)
    {
        return false;
    }

    template<typename T>
    INLINE ConvertResult NumericConvert(const char *, int, T&)
    {
        static_assert(is_arithmetic<T>::value, "Numeric conversions are valid for numeric types only");
        SCOPE_ASSERT(false); // default case is not implemented
        return ConvertErrorUndefined;
    }

    INLINE bool IsWhiteSpace(const char * begin, const char * end)
    {
        if (begin < end)
        {
            SCOPE_ASSERT(*end == '\0');
            for (const char * p = begin; p != end; p++)
            {
                if (!isspace(*p))
                {
                    return false;
                }
            }
        }

        return true;
    }

    template<>
    INLINE ConvertResult NumericConvert<int>(const char * str, int size, int& out)
    {
        char * endConversionPtr = NULL;
        const char * lastCharPtr = str + size - 1;

        errno = 0;
        out = strtol(str, &endConversionPtr, 10);

        // Error case 1: value out of range for the destination
        if (errno != 0)
        {
            SCOPE_ASSERT(errno == ERANGE);
            return ConvertErrorOutOfRange;
        }

        // Error case 2: part of (or full) input has invalid characters
        if (!IsWhiteSpace(endConversionPtr, lastCharPtr))
        {
            return ConvertErrorInvalidCharacter;
        }

        // Error case 3: no characters converted, input is empty or white space
        if (endConversionPtr == str)
        {
            return ConvertErrorEmpty;
        }

        return ConvertSuccess;
    }

    template<>
    INLINE ConvertResult NumericConvert<__int64>(const char * str, int size, __int64& out)
    {
        char * endConversionPtr = NULL;
        const char * lastCharPtr = str + size - 1;

        errno = 0;
        out = _strtoi64(str, &endConversionPtr, 10);

        // Error case 1: value out of range for the destination
        if (errno != 0)
        {
            SCOPE_ASSERT(errno == ERANGE);
            return ConvertErrorOutOfRange;
        }

        // Error case 2: part of (or full) input has invalid characters
        if (!IsWhiteSpace(endConversionPtr, lastCharPtr))
        {
            return ConvertErrorInvalidCharacter;
        }

        // Error case 3: no characters converted, input is empty or white space
        if (endConversionPtr == str)
        {
            return ConvertErrorEmpty;
        }

        return ConvertSuccess;
    }

    template<>
    INLINE ConvertResult NumericConvert<unsigned __int64>(const char * str, int size, unsigned __int64& out)
    {
        char * endConversionPtr = NULL;
        const char * lastCharPtr = str + size - 1;

        errno = 0;
        out = _strtoui64(str, &endConversionPtr, 10);

        // Error case 1: value out of range for the destination
        if (errno != 0)
        {
            SCOPE_ASSERT(errno == ERANGE);
            return ConvertErrorOutOfRange;
        }

        // Error case 2: part of (or full) input has invalid characters
        if (!IsWhiteSpace(endConversionPtr, lastCharPtr))
        {
            return ConvertErrorInvalidCharacter;
        }

        // Error case 3: no characters converted, input is empty or white space
        if (endConversionPtr == str)
        {
            return ConvertErrorEmpty;
        }

        // Error case 4: value is negative, but modulo-2 behavior of _strtoui64 converted the value silently
        // Since the value is properly converted to an int, there are no other invalid characters present,
        // and if there is a '-' it is placed in a valid position, so we can simply look for a '-'.
        for (const char * p = str; p != lastCharPtr; p++)
        {
            if (*p == '-')
            {
                return ConvertErrorOutOfRange;
            }
        }

        return ConvertSuccess;
    }

    template<typename TargetType>
    INLINE ConvertResult NumericConvertFloatType(const char * str, int size, TargetType & out)
    {
        char * endConversionPtr = nullptr;
        const char * nullTerminatorCharPtr = str + size - 1;
        
        // Error case: hexadecimal string could successfully be converted to float, due to 
        // VC14 upgrade, but still illegal in C#
        // Note: assumed that string is trimmed
        if (size > 2 && (str[0] == '0' && (str[1] == 'x' || str[1] == 'X')))
        {
            return ConvertErrorInvalidCharacter;
        }

        errno = 0;

        if (std::is_same<TargetType, double>::value)
        {
            out = static_cast<TargetType>(strtod(str, &endConversionPtr));
        }
        else
        {
            out = static_cast<TargetType>(strtof(str, &endConversionPtr));
        }

        // Error case: value out of range for the destination.
        // Note that small values automatically flush to zero in C#, so even though
        // error is reported by strtod/strtof we choose to ignore it if value is 0.
        if (errno != 0 && out != 0)
        {
            SCOPE_ASSERT(errno == ERANGE);

            return ConvertErrorOutOfRange;
        }

        // Error case: part of (or full) input has invalid characters
        if (!IsWhiteSpace(endConversionPtr, nullTerminatorCharPtr))
        {
            return ConvertErrorInvalidCharacter;
        }

        // Error case: no characters converted, input is empty or white space
        if (endConversionPtr == str)
        {
            return ConvertErrorEmpty;
        }

        // Error case: Case insensitive NaN or +/- Infinity accepted, due to
        // VC14 upgrade, but still illegal in C#

        // infinity() is powered by a compiler intrinsic constexpr and can be inlined from VC14, 
        // while isinf() does not support inlineable-ability. With C++11 isnan is included as standard.
        if (isnan(out) || numeric_limits<TargetType>::infinity() == out || -numeric_limits<TargetType>::infinity() == out)
        {
            return ConvertErrorInvalidCharacter;
        }

        // This weird code ensures that we get rid of -0.0 that does not exist in managed
        if (out == 0)
        {
            out = 0;
        }

        return ConvertSuccess;
    }

    template<>
    INLINE ConvertResult NumericConvert<double>(const char * str, int size, double& out)
    {
        // size is the FixedArrayType's size() function, which will have 1 more byte (for '\0' character)
        // in addition to string length here, hence subtract 1. In other words, size = length + 1 (very weird)
        return NumericConvertFloatType(str, size, out);
    }

    template<>
    INLINE ConvertResult NumericConvert<float>(const char * str, int size, float& out)
    {
        // size is the FixedArrayType's size() function, which will have 1 more byte (for '\0' character)
        // in addition to string length here, hence subtract 1. In other words, size = length + 1 (very weird)
        return NumericConvertFloatType(str, size, out);
    }

    template<typename FromType, typename ToType>
    INLINE ConvertResult ConvertToIntegerHelper(const char * str, int size, ToType& out)
    {
        FromType i;
        ConvertResult res = NumericConvert(str, size, i);

        if (res == ConvertSuccess)
        {
            if (i > numeric_limits<ToType>::max() || i < numeric_limits<ToType>::min())
            {
                return ConvertErrorOutOfRange;
            }

            out = static_cast<ToType>(i);

            return ConvertSuccess;
        }

        return res;
    }

    template<>
    INLINE ConvertResult NumericConvert<unsigned int>(const char * str, int size, unsigned int& out)
    {
        return ConvertToIntegerHelper<__int64, unsigned int>(str, size, out);
    }

    template<>
    INLINE ConvertResult NumericConvert<char>(const char * str, int size, char& out)
    {
        return ConvertToIntegerHelper<int, char>(str, size, out);
    }

    template<>
    INLINE ConvertResult NumericConvert<unsigned char>(const char * str, int size, unsigned char& out)
    {
        return ConvertToIntegerHelper<int, unsigned char>(str, size, out);
    }

    template<>
    INLINE ConvertResult NumericConvert<short>(const char * str, int size, short& out)
    {
        return ConvertToIntegerHelper<int, short>(str, size, out);
    }

    template<>
    INLINE ConvertResult NumericConvert<unsigned short>(const char * str, int size, unsigned short& out)
    {
        return ConvertToIntegerHelper<int, unsigned short>(str, size, out);
    }

    template<>
    INLINE ConvertResult NumericConvert<wchar_t>(const char * str, int size, wchar_t& out)
    {
        return ConvertToIntegerHelper<int, wchar_t>(str, size, out);
    }

    class ConverterCharacterDump
    {

        const char* m_start;
        const char* m_end;
        const char* m_next;

    public:

        ConverterCharacterDump(const char* start, const char* end, const char* next)
            : m_start(start), m_end(end), m_next(next)
        {
        }

        string Detail() const
        {
            std::stringstream out;
            out << std::uppercase << std::hex ;
            const char* p = max(m_next, m_start);
            while (p < min(m_end, m_next + 8))
            {
                out << "0x" << ((*p) & 0xFF) << " ";
                p++;
            }

            out << "\n" << "^";

            return out.str();
        }
    };

#pragma endregion Utilities

#pragma region TypeHelpers
    template<typename T, typename U>
    T scope_cast(const U& u)
    {
        return ScopeCast<T, remove_cv<remove_reference<U>::type>::type>::get(u);
    }

    template<typename T, typename U, bool areArithmetic = is_arithmetic<T>::value && is_arithmetic<U>::value>
    struct ScopeCommonType
    {
        typedef typename common_type<T, U>::type type;
    };

    template<typename T, typename U>
    struct ScopeCommonType<T, U, false>
    {
        typedef typename conditional<is_arithmetic<T>::value, U, T>::type type;
    };

    template<typename T, bool isSmallInteger = is_integral<T>::value && (sizeof(T) == 1 || sizeof(T) == 2)>
    struct ScopeArithmeticType
    {
        typedef typename conditional<is_signed<T>::value, int, unsigned int>::type type;
    };

    template<typename T>
    struct ScopeArithmeticType<T, false>
    {
        typedef T type;
    };

    template<typename ToType, typename FromType>
    struct ScopeCast
    {
        static ToType get(const FromType& value)
        {
            return static_cast<ToType>(value);
        }
    };
#pragma endregion TypeHelpers

#pragma region ScopeSStreamSchema
    class ScopeSStreamSchema
    {
    protected:
        ScopeManagedHandle m_managed;

    public:
        ScopeManagedHandle& ManagedSchema()
        {
            return m_managed;
        }
    };

    template<int SchemaId>
    class ScopeSStreamSchemaStatic : public ScopeSStreamSchema
    {
    public:
        ScopeSStreamSchemaStatic();
    };

#pragma endregion ScopeSStreamSchema

#if !defined(SCOPE_NO_UDT)
#pragma region ScopeUDT

    template<typename Schema>
    struct ManagedSerializationContextFactory
    {
        SCOPE_ENGINE_MANAGED_API static ScopeManagedHandle get(int columnIndex);
    };

    //
    // Class representing SQLIP UDTs
    //
    template<int UserDefinedTypeId>
#ifdef PLUGIN_TYPE_SYSTEM
    class SqlUserDefinedType final : PluginTypeBase<SqlUserDefinedType<UserDefinedTypeId> >
#else
    class SqlUserDefinedType final
#endif
    {
    private:
        ScopeManagedHandle m_managed;
        template<typename Container> INLINE Container GetUdtContainer() const;

    public:
        SqlUserDefinedType();
        SqlUserDefinedType(const SqlUserDefinedType<UserDefinedTypeId> & c);

        // Do a shallow copy of UDT object in ColumnData
        SqlUserDefinedType<UserDefinedTypeId> & operator=(const SqlUserDefinedType<UserDefinedTypeId> & rhs)
        {
            if ((void*)this != (void*)&rhs)
            {
                this->Set(rhs.Get());
            }

            return *this;
        }

        template<int OtherUserDefinedTypeId>
        SqlUserDefinedType<UserDefinedTypeId> & operator=(const SqlUserDefinedType<OtherUserDefinedTypeId> & rhs)
        {
            this->Set(rhs.Get());
            return *this;
        }

        void Set(const ScopeManagedHandle & value);
        ScopeManagedHandle Get() const;
        void Reset();
        bool IsNull() const;
        void SetNull();
        void BinarySerialize(BinaryOutputStreamBase<CosmosOutput> * stream, const ScopeManagedHandle & serializationContext) const;
        void BinarySerialize(BinaryOutputStreamBase<MemoryOutput> * stream, const ScopeManagedHandle & serializationContext) const;
        void BinarySerialize(BinaryOutputStreamBase<DummyOutput> * stream, const ScopeManagedHandle & serializationContext) const;
        void BinarySerialize(SStreamDataOutputStream * stream, const ScopeManagedHandle & serializationContext) const;
        void BinaryDeserialize(BinaryInputStreamBase<CosmosInput> * stream, const ScopeManagedHandle & serializationContext);
        void BinaryDeserialize(BinaryInputStreamBase<MemoryInput> * stream, const ScopeManagedHandle & serializationContext);
        void TextSerialize(TextOutputStreamBase * stream) const;
        void TextDeserialize(const char * str);
        void SSLibSerialize(SStreamDataOutputStream * baseStream) const;
        void SSLibDeserialize(BYTE* buffer, int offset, int length, ScopeSStreamSchema& schema);

        friend ostream & operator<<(ostream &o, const SqlUserDefinedType<UserDefinedTypeId> &t);

    // Pending implementation, TODO xiaoyuc
#pragma region PluginTypeSystem
#ifdef PLUGIN_TYPE_SYSTEM
        int Compare(const SqlUserDefinedType & ) const
        {
            return 0;
        }

        // Serialize in ScopeMapOutputMemoryStream::Write
        template<typename BufferType>
        void ScopeMapOutputMemoryStreamSerialize(void*) const
        {
        }

        // Serialize in BinaryOutputStreamBase::Write
        template<typename OutputType>
        void Serialize(BinaryOutputStreamBase<OutputType>* ) const
        {
        }

        // Serialize in, ScopeIO.h TextOutputStream::Write
        template<typename TextOutputStreamTraits>
        void Serialize(TextOutputStream<TextOutputStreamTraits>* ) const
        {
        }

        // Serialize in SStreamDataOutputStream::Write
        template<typename BufferType>
        void SStreamDataOutputSerialize(void*) const
        {
        }

        // DeSerialize in ScopeMapInputMemoryStream::Read
        template<typename AllocatorType>
        void ScopeMapInputMemoryStreamDeserialize(const char* & cur, void* ia)
        {
        }

        // DeSerialize in BinaryInputStreamBase::Read
        template<typename InputType>
        void Deserialize(BinaryInputStreamBase<InputType>* , IncrementalAllocator* )
        {
        }

        // DeSerialize in TextInputStream::Read
        template<typename TextInputStreamTraits, typename InputStreamType>
        void Deserialize(TextInputStream<TextInputStreamTraits, InputStreamType>* , FStringWithNull &, bool)
        {
        }

        // Swap the values
        void Swap(SqlUserDefinedType & )
        {
        }

        // The hash function
        size_t GetHashCode() const
        {
            return 0;
        }

        // TrySqlUserDefinedTypeToT, see scopeio.h
        template<typename StringType>
        ConvertResult FromFString(void *)
        {
            return ConvertSuccess;
        }

        // already defined in this class
        // bool IsNull() const
        // {
        //    return false;
        // }

        // For Aggregate_MIN and Aggregate_MAX
        // void SetNull()
        // {
        // }

        // Deep copier
        void DeepCopyFrom(SqlUserDefinedType const & , IncrementalAllocator * )
        {
        }

        // Shallow copier
        void ShallowCopyFrom(SqlUserDefinedType const & )
        {
        }

        // Used by Aggregate_UNCHECKED_SUMx2
        SqlUserDefinedType Multiply(SqlUserDefinedType const & ) const
        {
            return SqlUserDefinedType();
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        SqlUserDefinedType & AddByUnchecked(SqlUserDefinedType const & )
        {
            return *this;
        }

        // For Aggregate_SUM: Should check overflow; Should skip if the value is null
        SqlUserDefinedType & AddByChecked(SqlUserDefinedType const & )
        {
            return *this;
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        SqlUserDefinedType & SetZero()
        {
            return *this;
        }

        static SqlUserDefinedType Min()
        {
            return SqlUserDefinedType();
        }

        static SqlUserDefinedType Max()
        {
            return SqlUserDefinedType();
        }

        enum {isNullablePrimaryType=false};
        enum {is_floating_point=false};
        enum {is_signed=false};
        enum {is_unsigned=false};
        enum {is_integral=false};
        enum {need_deep_copy=false};
#endif
#pragma endregion PluginTypeSystem

    };

    template<int UserDefinedTypeId>
    INLINE ostream & operator<<(ostream & o, const SqlUserDefinedType<UserDefinedTypeId> & t)
    {
        o << t.m_managed;
        return o;
    }

    //
    // Class representing the legacy Scope UDT type (stores reference to ScopeRuntime::ColumnData type)
    // For SQL-IP we will stop using the ScopeRuntime::ColumnData type.
    //
    template<int UserDefinedTypeId>
#ifdef PLUGIN_TYPE_SYSTEM
    class ScopeUserDefinedType final : PluginTypeBase<ScopeUserDefinedType<UserDefinedTypeId> >
#else
    class ScopeUserDefinedType final
#endif
    {
    private:
        ScopeManagedHandle m_managed;
        template<typename ColumnData> INLINE ColumnData GetColumnData() const;
        template<typename WriterType, typename StreamType> INLINE void Serialize(StreamType * stream) const;
        template<typename ReaderType, typename StreamType> INLINE void Deserialize(StreamType * stream);

    public:
        ScopeUserDefinedType();
        ScopeUserDefinedType(const ScopeUserDefinedType<UserDefinedTypeId> & c);

        // Do a shallow copy of UDT object in ColumnData
        ScopeUserDefinedType<UserDefinedTypeId> & operator=(const ScopeUserDefinedType<UserDefinedTypeId> & rhs)
        {
            if ((void*)this != (void*)&rhs)
            {
                this->Set(rhs.Get());
            }

            return *this;
        }

        template<int OtherUserDefinedTypeId>
        ScopeUserDefinedType<UserDefinedTypeId> & operator=(const ScopeUserDefinedType<OtherUserDefinedTypeId> & rhs)
        {
            this->Set(rhs.Get());
            return *this;
        }

        void Set(const ScopeManagedHandle & value);
        ScopeManagedHandle Get() const;
        void Reset();
        bool IsNull() const;
        void SetNull();
        void BinarySerialize(BinaryOutputStreamBase<CosmosOutput> * stream, const ScopeManagedHandle & serializationContext) const;
        void BinarySerialize(BinaryOutputStreamBase<MemoryOutput> * stream, const ScopeManagedHandle & serializationContext) const;
        void BinarySerialize(BinaryOutputStreamBase<DummyOutput> * stream, const ScopeManagedHandle & serializationContext) const;
        void BinarySerialize(SStreamDataOutputStream * stream, const ScopeManagedHandle & serializationContext) const;
        void BinaryDeserialize(BinaryInputStreamBase<CosmosInput> * stream, const ScopeManagedHandle & serializationContext);
        void BinaryDeserialize(BinaryInputStreamBase<MemoryInput> * stream, const ScopeManagedHandle & serializationContext);
        void TextSerialize(TextOutputStreamBase * stream) const;
        void TextDeserialize(const char * str);
        void SSLibSerialize(SStreamDataOutputStream * baseStream) const;
        void SSLibDeserialize(BYTE* buffer, int offset, int length, ScopeSStreamSchema& schema);

    // Pending implementation, TODO xiaoyuc
#pragma region PluginTypeSystem
#ifdef PLUGIN_TYPE_SYSTEM
        int Compare(const ScopeUserDefinedType & ) const
        {
            return 0;
        }

        // Serialize in ScopeMapOutputMemoryStream::Write
        template<typename BufferType>
        void ScopeMapOutputMemoryStreamSerialize(void*) const
        {
        }

        // Serialize in BinaryOutputStreamBase::Write
        template<typename OutputType>
        void Serialize(BinaryOutputStreamBase<OutputType>* ) const
        {
        }

        // Serialize in, ScopeIO.h TextOutputStream::Write
        template<typename TextOutputStreamTraits>
        void Serialize(TextOutputStream<TextOutputStreamTraits>* ) const
        {
        }

        // Serialize in SStreamDataOutputStream::Write
        template<typename BufferType>
        void SStreamDataOutputSerialize(void*) const
        {
        }

        // DeSerialize in ScopeMapInputMemoryStream::Read
        template<typename AllocatorType>
        void ScopeMapInputMemoryStreamDeserialize(const char* & cur, void* ia)
        {
        }

        // DeSerialize in BinaryInputStreamBase::Read
        template<typename InputType>
        void Deserialize(BinaryInputStreamBase<InputType>* , IncrementalAllocator* )
        {
        }

        // DeSerialize in TextInputStream::Read
        template<typename TextInputStreamTraits, typename InputStreamType>
        void Deserialize(TextInputStream<TextInputStreamTraits, InputStreamType>* , FStringWithNull &, bool)
        {
        }

        // Swap the values
        void Swap(ScopeUserDefinedType & )
        {
        }

        // The hash function
        size_t GetHashCode() const
        {
            return 0;
        }

        // TryScopeUserDefinedTypeToT, see scopeio.h
        template<typename StringType>
        ConvertResult FromFString(void*)
        {
            return ConvertSuccess;
        }

        // already defined in this class
        // bool IsNull() const
        // {
        //    return false;
        // }

        // For Aggregate_MIN and Aggregate_MAX
        // void SetNull()
        // {
        // }

        // Deep copier
        void DeepCopyFrom(ScopeUserDefinedType const & , IncrementalAllocator * )
        {
        }

        // Shallow copier
        void ShallowCopyFrom(ScopeUserDefinedType const & )
        {
        }

        // Used by Aggregate_UNCHECKED_SUMx2
        ScopeUserDefinedType Multiply(ScopeUserDefinedType const & ) const
        {
            return ScopeUserDefinedType();
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        ScopeUserDefinedType & AddByUnchecked(ScopeUserDefinedType const & )
        {
            return *this;
        }

        // For Aggregate_SUM: Should check overflow; Should skip if the value is null
        ScopeUserDefinedType & AddByChecked(ScopeUserDefinedType const & )
        {
            return *this;
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        ScopeUserDefinedType & SetZero()
        {
            return *this;
        }

        static ScopeUserDefinedType Min()
        {
            return ScopeUserDefinedType();
        }

        static ScopeUserDefinedType Max()
        {
            return ScopeUserDefinedType();
        }

        enum {isNullablePrimaryType=false};
        enum {is_floating_point=false};
        enum {is_signed=false};
        enum {is_unsigned=false};
        enum {is_integral=false};
        enum {need_deep_copy=false};
#endif
#pragma endregion PluginTypeSystem

        friend ostream & operator<<(ostream &o, const ScopeUserDefinedType<UserDefinedTypeId> &t);
    };

    template<int UserDefinedTypeId>
    INLINE ostream & operator<<(ostream & o, const ScopeUserDefinedType<UserDefinedTypeId> & t)
    {
        o << t.m_managed;
        return o;
    }

#pragma endregion ScopeUDT
#endif // SCOPE_NO_UDT
} // namespace ScopeEngine or PluginType

namespace ScopeEngine
{
#pragma region ExceptionRegion
    // Scope input/output stream read write exception
    // this is used for internal exceptions not intended to go to the user
    class ScopeStreamException : public ExceptionWithStack
    {
    public:
        enum ErrorState
        {
            EndOfFile,
            NewLine,
            NewLineExpected,
            ErrorNeedsStackFrame, // this is a fake error number, any error beyond this error will capture a stack frame
            InvalidRow,
            BadDevice,
            BadConvert,
            InvalidCharacter,
            PassSplitEndPoint,
            SkipFirstRowsUnexpectedEOF,
            // In splits scenario, for non-last split if reading reach stream EOF that means JM does not split stream correctly or Store error.
            // JM should make sure for non-last split (crosssplit=true), between "split end point" and EOF there are 4mb+ bytes, if there is no
            // many bytes, should no split.
            StreamSplitUnexpectedEOF,
            InvalidCharacterAfterTextQualifier,
        };

        SCOPE_ENGINE_API ScopeStreamException(ErrorState error) throw();
        SCOPE_ENGINE_API ScopeStreamException(ErrorState error, const std::string details) throw();
        SCOPE_ENGINE_API ScopeStreamException(const ScopeStreamException& c) throw();
        SCOPE_ENGINE_API virtual ~ScopeStreamException() throw() override;
        SCOPE_ENGINE_API virtual ExceptionWithStack* Clone() const override;

        SCOPE_ENGINE_API virtual const char* what() const throw() override;

        SCOPE_ENGINE_API ErrorState Error() const;

    private:
        ErrorState    m_error;
    };

    // Scope input/output read/write exception intended for user
    class ScopeStreamExceptionWithEvidence: public ExceptionWithStack
    {
    public:
        SCOPE_ENGINE_API ScopeStreamExceptionWithEvidence(
            ErrorNumber error,
            const std::initializer_list<ScopeCommon::ScopeErrorArg> &args,
            const std::function<void (stringstream &out)>& getEvidence) throw();

        SCOPE_ENGINE_API ScopeStreamExceptionWithEvidence(
            ErrorNumber error,
            const std::initializer_list<ScopeCommon::ScopeErrorArg> &args,
            const std::string & evidence) throw();

        // Create a new exception using stack and details stored in a StreamException as evidence
        SCOPE_ENGINE_API ScopeStreamExceptionWithEvidence(
            ErrorNumber error,
            const std::initializer_list<ScopeCommon::ScopeErrorArg> &args,
            const ScopeStreamException & ex) throw();

        SCOPE_ENGINE_API ScopeStreamExceptionWithEvidence(const ScopeStreamExceptionWithEvidence& c) throw();
        SCOPE_ENGINE_API virtual ~ScopeStreamExceptionWithEvidence() override;
        SCOPE_ENGINE_API virtual ExceptionWithStack* Clone() const override;
    };

    // Scope decimal computation exception
    class ScopeDecimalException : public ScopeEngineException
    {
    public:
        // We should fix this to be multiple different errors and give more context, but for now match current behaviour
        SCOPE_ENGINE_API ScopeDecimalException(int error);
        SCOPE_ENGINE_API ScopeDecimalException(const ScopeDecimalException& c);
        SCOPE_ENGINE_API ScopeDecimalException& operator=(const ScopeDecimalException& c) throw();
        SCOPE_ENGINE_API virtual ~ScopeDecimalException() throw() override;
        SCOPE_ENGINE_API virtual ExceptionWithStack* Clone() const override;

        SCOPE_ENGINE_API virtual const char* what() const throw() override;

        SCOPE_ENGINE_API int Error() const;

        SCOPE_ENGINE_API ConvertResult ToConvertResult() const;

    private:
        static const char * Reason(int error);

        int m_error;
    };

    enum ScopeHttpExceptionErrorType
    {
        ScopeHttp_SystemError,
        ScopeHttp_HttpError,
        ScopeHttp_ExecutionProxyError
    };

    class ScopeHttpException : public ScopeEngineException
    {
        std::string m_description;

    public:
        SCOPE_ENGINE_API ScopeHttpException(ScopeHttpExceptionErrorType errType, const char * description, DWORD errorCode);
        SCOPE_ENGINE_API ScopeHttpException(const ScopeHttpException& c);
        SCOPE_ENGINE_API virtual ~ScopeHttpException() throw() override;
        SCOPE_ENGINE_API virtual ExceptionWithStack* Clone() const override;

        SCOPE_ENGINE_API virtual const char* what() const override;

        int m_errCode;
    };

#pragma endregion ExceptionRegion
} // namespace ScopeEngine

// TODO xiaoyuc, this namespace should be removed when we have switched to plugin type system. This is for testing purpose only
#ifdef PLUGIN_TYPE_SYSTEM_NAMESPACE
namespace PluginType
{
using namespace ScopeEngine;
#else
namespace ScopeEngine
{
#endif

#pragma region ContainerRegion
    // Template class to implement a tagged share pointer.
    // The pointer highest bit is always set to 1.
    // The pointer points to a SharedPtrHeader structure, actual data is placed afther the header.
    template<typename T>
    class SCOPE_RUNTIME_API SharedPtr
    {
        template<typename T> friend class FixedArrayType;

        static const char x_taggedPtrMask = 0x4;

        // This is the header structure gets stored at the beginning of the m_ptr pointed location.
        struct SharedPtrHeader
        {
            ULONG     m_version;                    // Destination allocator version when first copy was made
            USHORT    m_allocatorID;                // Destination allocator ID
            SharedPtrHeader*    m_destCopy;         // First copy address - lower 48 bits

            SharedPtrHeader() : m_allocatorID(0), m_version(0)
            {
                memset(m_destCopy, 0, x_usablePtrSize);
            }

            void Init()
            {
                memset(this, 0, sizeof(SharedPtrHeader));
            }

            void SetDestination(SharedPtrHeader* dest)
            {
                m_destCopy = dest;
            }

            SharedPtrHeader* GetDestination() const
            {
                return m_destCopy;
            }
        };

        SharedPtrHeader*    m_ptr;         // 8
        char                m_padding[3];  // 3
        char                m_flags;       // 1
        UINT                m_size;        // 4

        SharedPtrHeader * GetHeaderPtr() const
        {
            return m_ptr;
        }

        void Assign(SharedPtrHeader * ptr, size_t size, char flags)
        {
            Reset();
            m_ptr = ptr;
            m_size = static_cast<UINT>(size);
            m_flags = flags | x_taggedPtrMask;
        }

        //
        // Set destination allocator ID and Version. Set destination address as well.
        //
        void SetPtrInfo(USHORT allocId, ULONG allocVersion, ULONGLONG destId) const
        {
            SharedPtrHeader * header = GetHeaderPtr();
            header->m_allocatorID = allocId;
            header->m_version = allocVersion;
            header->SetDestination((SharedPtrHeader*)destId);
        }

        //
        // Returns SharedPtr to the destination object (NULL if none or in case of Id/Version mismatch)
        //
        void GetPtrInfo(USHORT & allocId, ULONG & allocVersion, ULONGLONG & destId) const
        {
            SharedPtrHeader * header = GetHeaderPtr();
            allocId = header->m_allocatorID;
            allocVersion = header->m_version;
            destId = (ULONGLONG)header->GetDestination();
        }

    public:
        //
        // Allocate a new shared memory ptr
        //
        SAFE_BUFFERS static SharedPtr<T> AllocateSharedPtr(IncrementalAllocator * alloc, size_t size, char flags)
        {
            SharedPtrHeader * headerWithBuf = reinterpret_cast<SharedPtrHeader*>(alloc->AllocateAligned<alignment_of<SharedPtrHeader>::value>(size + sizeof(SharedPtrHeader)));
            // custom "constructor" to avoid a placement new operator as the operator unnecessarily checks the "this" pointer and we know it is never null
            headerWithBuf->Init();

            SharedPtr<T> tmp;
            tmp.Assign(headerWithBuf, size, flags);
            return tmp;
        }

        //
        // Copy Shared memory ptr, if it is already copied and the destination is valid,
        // We will copy the SharedPtr.
        //
        static SharedPtr<T> CopySharedPtr(const SharedPtr<T> & ptr, IncrementalAllocator * alloc)
        {
            SharedPtrHeader * headerPtr = ptr.GetHeaderPtr();
            if (alloc->Match(headerPtr->m_allocatorID, headerPtr->m_version))
            {
                // We copy shared pointer only when we match the alloc id and version.
                SharedPtr<T> tmp;
                tmp.Assign(headerPtr->GetDestination(), ptr.GetSize(), ptr.m_flags);
                return tmp;
            }

            // We have not copied before or the allocator does not match the cached one.
            SharedPtr<T> tmp = AllocateSharedPtr(alloc, ptr.GetSize(), ptr.m_flags);

            // Deep copy the blob
            memcpy(tmp.GetBuffer(), ptr.GetBuffer(), ptr.GetSize()*sizeof(T));

            // Cache address of the first deep copy at source.
            // Remember destination allocator ID and Version.
            ptr.SetPtrInfo(alloc->Id(), alloc->Version(), (ULONGLONG)tmp.GetHeaderPtr());

            return tmp;
        }

        void Reset()
        {
            memset(this, 0, sizeof(SharedPtr<T>));
        }

        T * GetBuffer() const
        {
            return (T*)(GetHeaderPtr()+1);
        }

        unsigned int GetSize() const
        {
            return m_size;
        }

        void ReduceSize(unsigned int size)
        {
            // this method doesn't change allocated memory
            SCOPE_ASSERT(size <= m_size);
            m_size = size;
        }

        void Expand(UINT allocSize, IncrementalAllocator * alloc)
        {
            char * tmp = alloc->Allocate(allocSize);
            // assert the allocation is continuous
            SCOPE_ASSERT(tmp == GetBuffer() + m_size);
            m_size += allocSize;
        }

        //
        // NULL is represented as a pointer having all bits set to 0 except highest bit that is set to 1.
        // FString() handles NULL values itself and does not invoke this method.
        //
        bool IsNull() const
        {
            return GetHeaderPtr() == nullptr;
        }
    };

    // Bigtuple implements an up-to-10-way tuple on top of a 5-way tuple.
    // This is a VS2012 workaround that is not needed once VS2013 is here.
    template<typename T0>
    class bigtuple1
    {
    public:
        tuple<T0> first;

        bigtuple1(T0 a0)
            : first(a0)
            {}

        T0 get0() const { return get<x>(first); }
        typedef T0 type0;
    };

    template<typename T0, typename T1>
    class bigtuple2
    {
    public:
        tuple<T0, T1> first;

        bigtuple2(T0 a0, T1 a1)
            : first(a0, a1)
            {}

        T0 get0() const { return get<0>(first); }
        T1 get1() const { return get<1>(first); }
        typedef T0 type0;
        typedef T1 type1;
    };

    template<typename T0, typename T1, typename T2>
    class bigtuple3
    {
    public:
        tuple<T0, T1, T2> first;

        bigtuple3(T0 a0, T1 a1, T2 a2)
            : first(a0, a1, a2)
            {}

        T0 get0() const { return get<0>(first); }
        T1 get1() const { return get<1>(first); }
        T2 get2() const { return get<2>(first); }
        typedef T0 type0;
        typedef T1 type1;
        typedef T2 type2;
    };

    template<typename T0, typename T1, typename T2, typename T3>
    class bigtuple4
    {
    public:
        tuple<T0, T1, T2, T3> first;

        bigtuple4(T0 a0, T1 a1, T2 a2, T3 a3)
            : first(a0, a1, a2, a3)
            {}

        T0 get0() const { return get<0>(first); }
        T1 get1() const { return get<1>(first); }
        T2 get2() const { return get<2>(first); }
        T3 get3() const { return get<3>(first); }
        typedef T0 type0;
        typedef T1 type1;
        typedef T2 type2;
        typedef T3 type3;
    };

    template<typename T0, typename T1, typename T2, typename T3, typename T4>
    class bigtuple5
    {
    public:
        tuple<T0, T1, T2, T3, T4> first;

        bigtuple5(T0 a0, T1 a1, T2 a2, T3 a3, T4 a4)
            : first(a0, a1, a2, a3, a4)
            {}

        T0 get0() const { return get<0>(first); }
        T1 get1() const { return get<1>(first); }
        T2 get2() const { return get<2>(first); }
        T3 get3() const { return get<3>(first); }
        T4 get4() const { return get<4>(first); }
        typedef T0 type0;
        typedef T1 type1;
        typedef T2 type2;
        typedef T3 type3;
        typedef T4 type4;
    };

    template<typename T0, typename T1, typename T2, typename T3, typename T4, typename T5>
    class bigtuple6
    {
    public:
        tuple<T0, T1, T2, T3, T4> first;
        tuple<T5> second;

        bigtuple6(T0 a0, T1 a1, T2 a2, T3 a3, T4 a4, T5 a5)
            : first(a0, a1, a2, a3, a4),
            second(a5)
            {}

        T0 get0() const { return get<0>(first); }
        T1 get1() const { return get<1>(first); }
        T2 get2() const { return get<2>(first); }
        T3 get3() const { return get<3>(first); }
        T4 get4() const { return get<4>(first); }
        T5 get5() const { return get<0>(second); }
        typedef T0 type0;
        typedef T1 type1;
        typedef T2 type2;
        typedef T3 type3;
        typedef T4 type4;
        typedef T5 type5;
    };

    template<typename T0, typename T1, typename T2, typename T3, typename T4, typename T5, typename T6>
    class bigtuple7
    {
    public:
        tuple<T0, T1, T2, T3, T4> first;
        tuple<T5, T6> second;

        bigtuple7(T0 a0, T1 a1, T2 a2, T3 a3, T4 a4, T5 a5, T6 a6)
            : first(a0, a1, a2, a3, a4),
            second(a5, a6)
            {}

        T0 get0() const { return get<0>(first); }
        T1 get1() const { return get<1>(first); }
        T2 get2() const { return get<2>(first); }
        T3 get3() const { return get<3>(first); }
        T4 get4() const { return get<4>(first); }
        T5 get5() const { return get<0>(second); }
        T6 get6() const { return get<1>(second); }
        typedef T0 type0;
        typedef T1 type1;
        typedef T2 type2;
        typedef T3 type3;
        typedef T4 type4;
        typedef T5 type5;
        typedef T6 type6;
    };

    template<typename T0, typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7>
    class bigtuple8
    {
    public:
        tuple<T0, T1, T2, T3, T4> first;
        tuple<T5, T6, T7> second;

        bigtuple8(T0 a0, T1 a1, T2 a2, T3 a3, T4 a4, T5 a5, T6 a6, T7 a7)
            : first(a0, a1, a2, a3, a4),
            second(a5, a6, a7)
            {}

        T0 get0() const { return get<0>(first); }
        T1 get1() const { return get<1>(first); }
        T2 get2() const { return get<2>(first); }
        T3 get3() const { return get<3>(first); }
        T4 get4() const { return get<4>(first); }
        T5 get5() const { return get<0>(second); }
        T6 get6() const { return get<1>(second); }
        T7 get7() const { return get<2>(second); }
        typedef T0 type0;
        typedef T1 type1;
        typedef T2 type2;
        typedef T3 type3;
        typedef T4 type4;
        typedef T5 type5;
        typedef T6 type6;
        typedef T7 type7;
    };

    template<typename T0, typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7, typename T8>
    class bigtuple9
    {
    public:
        tuple<T0, T1, T2, T3, T4> first;
        tuple<T5, T6, T7, T8> second;

        bigtuple9(T0 a0, T1 a1, T2 a2, T3 a3, T4 a4, T5 a5, T6 a6, T7 a7, T8 a8)
            : first(a0, a1, a2, a3, a4),
            second(a5, a6, a7, a8)
            {}

        T0 get0() const { return get<0>(first); }
        T1 get1() const { return get<1>(first); }
        T2 get2() const { return get<2>(first); }
        T3 get3() const { return get<3>(first); }
        T4 get4() const { return get<4>(first); }
        T5 get5() const { return get<0>(second); }
        T6 get6() const { return get<1>(second); }
        T7 get7() const { return get<2>(second); }
        T8 get8() const { return get<3>(second); }
        typedef T0 type0;
        typedef T1 type1;
        typedef T2 type2;
        typedef T3 type3;
        typedef T4 type4;
        typedef T5 type5;
        typedef T6 type6;
        typedef T7 type7;
        typedef T8 type8;
    };

    template<typename T0, typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7, typename T8, typename T9>
    class bigtuple10
    {
    public:
        tuple<T0, T1, T2, T3, T4> first;
        tuple<T5, T6, T7, T8, T9> second;

        bigtuple10(T0 a0, T1 a1, T2 a2, T3 a3, T4 a4, T5 a5, T6 a6, T7 a7, T8 a8, T9 a9)
            : first(a0, a1, a2, a3, a4),
            second(a5, a6, a7, a8, a9)
            {}

        T0 get0() const { return get<0>(first); }
        T1 get1() const { return get<1>(first); }
        T2 get2() const { return get<2>(first); }
        T3 get3() const { return get<3>(first); }
        T4 get4() const { return get<4>(first); }
        T5 get5() const { return get<0>(second); }
        T6 get6() const { return get<1>(second); }
        T7 get7() const { return get<2>(second); }
        T8 get8() const { return get<3>(second); }
        T9 get9() const { return get<4>(second); }
        typedef T0 type0;
        typedef T1 type1;
        typedef T2 type2;
        typedef T3 type3;
        typedef T4 type4;
        typedef T5 type5;
        typedef T6 type6;
        typedef T7 type7;
        typedef T8 type8;
        typedef T9 type9;
    };

    // A wrapper class to implement auto expandable buffer.
    // It relies on exclusive usage of the IncrementalAllocator and the allocator will
    // allocate memory continuously.
    class SCOPE_RUNTIME_API AutoExpandBuffer
    {
        IncrementalAllocator *     m_allocator;// allocator
        SharedPtr<char> m_sharedPtr; // sharePtr header start position
        char      *     m_buffer;   // buffer start point
        SIZE_T          m_index;    // next write position

        // needs to be big enough to fit a UTF-8 character
        static const UINT x_allocationUnit = 1 << 16; // 64k

    public:
        AutoExpandBuffer(IncrementalAllocator * alloc):
          m_allocator(alloc),
              m_buffer(NULL),
              m_index(0)
          {
              static_assert(x_allocationUnit >= 4, "x_allocationUnit needs to be big enough for append method to work");
              m_allocator->SaveMarker();
              m_sharedPtr = SharedPtr<char>::AllocateSharedPtr(alloc, x_allocationUnit, 0);
              m_buffer = m_sharedPtr.GetBuffer();
          }

          ~AutoExpandBuffer()
          {
              // if the buffer is not taken, release all the memory allocated
              if (m_buffer)
              {
                  m_allocator->Rollback();
                  m_buffer = NULL;
                  m_sharedPtr.Reset();
              }
          }

          // Append a byte at the end
          FORCE_INLINE void Append(char c)
          {
              if (m_index >= m_sharedPtr.GetSize())
              {
                  m_sharedPtr.Expand(x_allocationUnit, m_allocator);
              }

              m_buffer[m_index++] = c;
          }

          // Append a series of bytes encoded in a UINT64 in little endian order into the current buffer
          // The UINT64 may represent a UTF8 encoded character or just one 8 byte ascii characters
          FORCE_INLINE void Append(UINT64 c)
          {
              // This code relies on UTF8 having no 0 bytes in valid byte sequences other than the first byte
              // - this is true as all continuation bytes have the top bit set (see https://en.wikipedia.org/wiki/UTF-8)
              int byteCount = 1;
              UINT64 mask = 0xFF00;

              while ((c & mask) != 0)
              {
                  mask <<= 8;
                  ++byteCount;
              }

              if (m_index + byteCount >= m_sharedPtr.GetSize())
              {
                  m_sharedPtr.Expand(x_allocationUnit, m_allocator);
              }

              switch (byteCount)
              {
                  default:
                      SCOPE_ASSERT(false);
                  case 4:
                      m_buffer[m_index++] = (unsigned char)((c >> 24) & 0xFF);
                      // fall through
                  case 3:
                      m_buffer[m_index++] = (unsigned char)((c >> 16) & 0xFF);
                      // fall through
                  case 2:
                      m_buffer[m_index++] = (unsigned char)((c >> 8) & 0xFF);
                      // fall through
                  case 1:
                      m_buffer[m_index++] = (unsigned char)(c & 0xFF);
              }
          }

          // Remove i character from the end
          FORCE_INLINE void RemoveEnd(SIZE_T i)
          {
              if (m_index >= i)
              {
                  m_index -= i;
                  return;
              }

              SCOPE_ASSERT(!"AutoExpandBuffer does not have enough character in it.");

          }

          // Current size
          SIZE_T Size()
          {
              return m_index;
          }

          // array accessor to get to the buffer element
          char & operator[] (SIZE_T i)
          {
              return m_buffer[i];
          }

          // Take control of the buffer and
          // shrink buffer to its size by release additional memory back to
          // allocator.
          SharedPtr<char> TakeBuffer(IncrementalAllocator::AllocationType allocType)
          {
              m_allocator->UpdateDataSizeStats(Size(), allocType);
              SharedPtr<char> tmp = m_sharedPtr;
              tmp.ReduceSize((int)Size());

              // shrink the allocator
              m_allocator->SetTop(m_buffer+m_index);

              // buffer is returned.
              m_buffer = NULL;
              m_sharedPtr.Reset();

              // make sure no more allocation can happen in the auto expand buffer
              m_allocator = NULL;

              return tmp;
          }
    };

    struct SCOPE_RUNTIME_API ConstCharPtr
    {
        void AssignPtr(const char * ptr, UINT size)
        {
            SCOPE_ASSERT(ptr != NULL);

            m_ptr = ptr;
            memset(m_padding, 0, array_size(m_padding));
            m_flags = (0 | x_constPtrMask);
            m_size = size;
        }

        UINT GetSize() const
        {
            return m_size;
        }

        const char * GetBuffer() const
        {
            return m_ptr;
        }

    private:
        static const char x_constPtrMask = 0x2;

        const char*   m_ptr;         // 8
        char          m_padding[3];  // 3
        char          m_flags;       // 1
        UINT          m_size;        // 4
    };

    template <typename T>
    struct ElementTraits
    {
        static void VerifyLimit(SIZE_T /*size*/)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Unknown element type in ElementTraits::VerifyLimit");
        }

        static IncrementalAllocator::AllocationType GetAllocationType()
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Unknown element type in ElementTraits::GetAllocationType");
        }
    };

    template<>
    struct ElementTraits<char>
    {
        static void VerifyLimit(SIZE_T size)
        {
            SIZE_T maxSize = Configuration::GetGlobal().GetMaxStringSize();
            if (size > maxSize + 1) // possible null termination
            {
                std::stringstream ss;
                ss << "String size " << size << " exceeds the maximum allowed size of " << maxSize;
                throw RuntimeException(E_USER_STRING_TOO_BIG, ss.str().c_str());
            }
        }

        static IncrementalAllocator::AllocationType GetAllocationType()
        {
            return IncrementalAllocator::StringAllocation;
        }
    };

    template<>
    struct ElementTraits<wchar_t>
    {
        static void VerifyLimit(SIZE_T size)
        {
            SIZE_T maxSize = Configuration::GetGlobal().GetMaxStringSize();
            if (size > maxSize + 1) // possible null termination
            {
                std::stringstream ss;
                ss << "String size " << size << " exceeds the maximum allowed size of " << maxSize;
                throw RuntimeException(E_USER_STRING_TOO_BIG, ss.str().c_str());
            }
        }

        static IncrementalAllocator::AllocationType GetAllocationType()
        {
            return IncrementalAllocator::StringAllocation;
        }
    };

    template<>
    struct ElementTraits<unsigned char>
    {
        static void VerifyLimit(SIZE_T size)
        {
            SIZE_T maxSize = Configuration::GetGlobal().GetMaxBinarySize();
            if (size > maxSize)
            {
                std::stringstream ss;
                ss << "Byte array size " << size << " exceeds the maximum allowed size of " << maxSize;
                throw RuntimeException(E_USER_BINARY_TOO_BIG, ss.str().c_str());
            }
        }

        static IncrementalAllocator::AllocationType GetAllocationType()
        {
            return IncrementalAllocator::BinaryAllocation;
        }
    };

    // Fixed length array class.
    // The class is used to represent array data type in native runtime.
    // It is the underlying common functionality for FString and FBinary
    //
    // There is currently a lot of string specific functionality in here that should be moved to FString
    template<typename T>
    class SCOPE_RUNTIME_API FixedArrayType
    {
        friend class InteropAllocator;
        typedef ElementTraits<T> Traits;

    protected:
        static_assert(sizeof(T *) == 8, "Only 64-bit code generation is supported.");
        static_assert(sizeof(T) == 1, "Only char/unsigned char are supported by FixedArrayType.");

        // Maximum size we can inline into the structure
        static_assert(sizeof(SharedPtr<T>) == 16, "Unexpected SharedPtr size.");
        static_assert(sizeof(ConstCharPtr) == 16, "Unexpected ConstCharPtr size.");
        static const int x_inlineSizeMax = sizeof(SharedPtr<T>) - 5; // sizeof((SharedPtr<T>)) - sizeof(flags) - sizeof(size)

        // FixedArrayType has four forms:
        // 1. inline : where m_isBlob == 0, m_isConst = 0, m_size = string length, m_buf = <string>
        // 2. const: where m_isBlob == 0, m_isConst == 1, m_constPtr contains a pointer to ConstPtr data structure. Refer to ConstPtr for more details.
        // 3. NULL : m_blob == NULL or think as m_inline has every bits is 0
        // 4. Blob:  m_isBlob == 1, m_blob. Refer to SharedPtr for more details.
        union
        {
            // raw access
            LONGLONG m_binary[2];

            // inline field representation
            struct
            {
                T             m_buf[x_inlineSizeMax]; // 11
                unsigned char m_isNull : 1;
                unsigned char m_isConst : 1;
                unsigned char m_isBlob : 1;
                unsigned char m_pad0 : 5;             //  1
                UINT          m_size;                 //  4
            } m_inline;

            // blob field representation.
            SharedPtr<T> m_blob;

            // pointer to the const array.
            ConstCharPtr m_constPtr;
        }; // Union to in place data or pointer

        // compute 32 bit hash for array
        int Get32BitHashCode() const
        {
            ULONG a, b, c;

            unsigned char * buf = (unsigned char *)buffer();
            ULONG len = size() * sizeof(T);

            // Set up the internal state
            a = b = c = (ULONG)(0xdeadbeef + len);
            c += 0;

            int index = 0;
            while (len > 12)
            {
                a += (ULONG)buf[index] + (((ULONG)buf[index + 1]) << 8) + (((ULONG)buf[index + 2]) << 16) + (((ULONG)buf[index + 3]) << 24);
                b += (ULONG)buf[index + 4] + (((ULONG)buf[index + 5]) << 8) + (((ULONG)buf[index + 6]) << 16) + (((ULONG)buf[index + 7]) << 24);
                c += (ULONG)buf[index + 8] + (((ULONG)buf[index + 9]) << 8) + (((ULONG)buf[index + 10]) << 16) + (((ULONG)buf[index + 11]) << 24);

                a -= c;
                a ^= (c << 4) | (c >> 28);
                c += b;
                b -= a;
                b ^= (a << 6) | (a >> 26);
                a += c;
                c -= b;
                c ^= (b << 8) | (b >> 24);
                b += a;
                a -= c;
                a ^= (c << 16) | (c >> 16);
                c += b;
                b -= a;
                b ^= (a << 19) | (a >> 13);
                a += c;
                c -= b;
                c ^= (b << 4) | (b >> 28);
                b += a;
                index += 12;
                len -= 12;
            }

            switch (len)
            {
                case 12:
                    c += ((ULONG)buf[index + 11]) << 24;
                case 11:
                    c += ((ULONG)buf[index + 10]) << 16;
                case 10:
                    c += ((ULONG)buf[index + 9]) << 8;
                case 9:
                    c += ((ULONG)buf[index + 8]);
                case 8:
                    b += ((ULONG)buf[index + 7]) << 24;
                case 7:
                    b += ((ULONG)buf[index + 6]) << 16;
                case 6:
                    b += ((ULONG)buf[index + 5]) << 8;
                case 5:
                    b += ((ULONG)buf[index + 4]);
                case 4:
                    a += ((ULONG)buf[index + 3]) << 24;
                case 3:
                    a += ((ULONG)buf[index + 2]) << 16;
                case 2:
                    a += ((ULONG)buf[index + 1]) << 8;
                case 1:
                    a += ((ULONG)buf[index]);
                    break;
                case 0:
                    return c;
            }

            c ^= b;
            c -= (b << 14) | (b >> 18);
            a ^= c;
            a -= (c << 11) | (c >> 21);
            b ^= a;
            b -= (a << 25) | (a >> 7);
            c ^= b;
            c -= (b << 16) | (b >> 16);
            a ^= c;
            a -= (c << 4) | (c >> 28);
            b ^= a;
            b -= (a << 14) | (a >> 18);
            c ^= b;
            c -= (b << 24) | (b >> 8);
            return (int)c;
        }


        bool IsBlob() const
        {
            return (m_inline.m_isBlob == TRUE);
        }

        void Reset()
        {
            m_binary[0] = 0;
            m_binary[1] = 0;
        }

        //
        // Lookups information about destination object (if any)
        //
        bool GetSharedPtrInfo(USHORT & allocId, ULONG & allocVersion, ULONGLONG & destId) const
        {
            if (!IsBlob())
            {
                return false;
            }

            m_blob.GetPtrInfo(allocId, allocVersion, destId);

            return true;
        }

        //
        // Updates information about destination object (if any)
        //
        bool SetSharedPtrInfo(USHORT allocId, ULONG allocVersion, ULONGLONG destId) const
        {
            if (!IsBlob())
            {
                return false;
            }

            m_blob.SetPtrInfo(allocId, allocVersion, destId);

            return true;
        }

        SAFE_BUFFERS T * GetBuffer() const
        {
            if (IsNull())
            {
                return nullptr;
            }
            else if (IsInline())
            {
                return const_cast<T *>(m_inline.m_buf);
            }
            else if (IsConst())
            {
                throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "We should never request to modify a const buffer!");
            }
            else
            {
                return m_blob.GetBuffer();
            }
        }

        SAFE_BUFFERS const T * GetConstBuffer() const
        {
            if (IsConst())
            {
                return (const T *) m_constPtr.GetBuffer();
            }
            else
            {
                return GetBuffer();
            }
        }

        // verifies that FString/FBinary size is under supported limit
        static void ValidateSize(SIZE_T size)
        {
            Traits::VerifyLimit(size);
        }

        // Update array size. This function cannot deal with change of representation or memory allocation, i.e. going from const to inline to blob.
        // All of these transformations must be done before calling this function.
        void ReduceSize(UINT newSize)
        {
            if (IsConst())
            {
                throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Cannot set size on null or const FixedArrayType");
            }

            SCOPE_ASSERT(newSize <= size());
            if (IsInline())
            {
                m_inline.m_size = newSize;
                m_inline.m_isNull = FALSE;
            }
            else
            {
                m_blob.ReduceSize(newSize);
            }
        }

        void SetConstPtr(const char* data, size_t size)
        {
            Reset();

            if (data == NULL)
            {
                SetNull();
            }
            else if (size == 0)
            {
                SetEmpty();
            }
            else if (size <= x_inlineSizeMax)
            {
                // Note the hash code relies on this
                // (i.e we effectively hash inlined strings differently than long ones)
                memcpy(m_inline.m_buf, data, size);
                m_inline.m_size = static_cast<UINT>(size);
            }
            else
            {
                ValidateSize(size);
                m_constPtr.AssignPtr(data, static_cast<UINT>(size));
                SCOPE_ASSERT(m_inline.m_isConst == TRUE);
            }
        }

        void SetInternalState(const LONGLONG binary[2])
        {
            m_binary[0] = binary[0];
            m_binary[1] = binary[1];
        }

        // Split out from compare to help inlining
        int SlowCompare(const FixedArrayType<T> & b) const
        {
            if (IsNull())
            {
                if (b.IsNull())
                {
                    return 0;
                }

                return 1;
            }
            else if (b.IsNull())
            {
                return -1;
            }

            int delta = size() - b.size();
            unsigned int len = delta  < 0 ? size() : b.size();

            // memcmp returns 0 if length in bytes passed is 0, so r
            // will be 0 for the empty string/binary
            int r = memcmp(buffer(), b.buffer(), len * sizeof(T));

            if (r != 0)
            {
                return r;
            }
            else
            {
                return delta;
            }
        }

        // DeSerialize in BinaryInputStreamBase::Read
        template<typename InputType>
        void DoDeserialize(BinaryInputStreamBase<InputType>* stream, IncrementalAllocator* m_allocator)
        {
            unsigned int len = stream->ReadLen();
            if (len == 0)
            {
                SetEmpty();
                return;
            }

            char * dest = (char *)Reserve(len, m_allocator);
            unsigned int bytesRead = stream->Read(dest, len);

            if (bytesRead != len)
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        // Serialize in BinaryOutputStreamBase::Write
        template<typename OutputType>
        void DoSerialize(BinaryOutputStreamBase<OutputType>* stream) const
        {
            unsigned int s = size();
            char b;

            do
            {
                b = s & 0x7f;
                s = s >> 7;
                if (s > 0)
                    b |= 0x80;
                stream->WriteChar(b);
            } while (s);

            stream->Write((const char*)buffer(), size());
        }

        // Serialize in ScopeMapOutputMemoryStream::Write
        void DoScopeMapOutputSerialize(AutoBuffer* buf) const
        {
            UINT size = m_inline.m_size;
            UINT value = size;
            while (value >= 0x80)
            {
                buf->Put((BYTE)(value | 0x80));
                value = value >> 7;
            }

            buf->Put((BYTE)value);
            buf->Write((const char*)buffer(), size);
        }

        // DeSerialize in ScopeMapInputMemoryStream::Read
        void DoScopeMapInputMemoryStreamDeserialize(const char* & cur, IncrementalAllocator* ia)
        {
            UINT size = 0;
            char b = 0;
            UINT shift = 0;
            do
            {
                memcpy(&b, cur, 1);
                cur += 1;
                size |= (b & 0x7f) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);

            CopyFrom((const T*)cur, size, ia);
            cur += size;
        }

        // DeSerialize in TextInputStream::Read
        template<typename TextInputStreamTraits, typename InputStreamType>
        void DoDeserialize(TextInputStream<TextInputStreamTraits, InputStreamType>* stream, FStringWithNull & str, bool lastEmptyColumn)
        {
            if (lastEmptyColumn)
            {
                // create an empty string
                SetEmpty();
                stream->EndRow(false);
                return;
            }

            auto result = TextConversions::TryFStringToT(str, *this);
            if (result != ConvertSuccess)
            {
                if (stream->silent)
                {
                    SetNull();
                }
                else
                {
                    stream->StoreBadConvertException(result, str);
                }
            }

            if (stream->CurrentField() == stream->ColumnExpected())
            {
                stream->EndRow(true);
            }
        }

    public:
        typedef T   ValueType;
        typedef T * PointerType;

        // Copy constructor (does shallow copy)
        FixedArrayType(const FixedArrayType<T> & f)
        {
            SetInternalState(f.m_binary);
        }

        // Copy constructor with allocator.
        // It is a deep copy. The string will be copied as well.
        FixedArrayType(const FixedArrayType<T> & f, IncrementalAllocator * alloc)
        {
            SetInternalState(f.m_binary);

            if (f.IsBlob())
            {
                SIZE_T byteSize = f.m_blob.GetSize() * sizeof(T);
                ValidateSize(byteSize);
                // Deep copy with copy-once optimization.
                m_blob = SharedPtr<T>::CopySharedPtr(f.m_blob, alloc);
                alloc->UpdateDataSizeStats(byteSize, Traits::GetAllocationType());
            }

            if (f.IsConst())
            {
                CopyFrom(f.buffer(), f.size(), alloc);
            }
        }

        // Default constructor
        FixedArrayType()
        {
            SetNull();
        }

        // Null constructor
        FixedArrayType(nullptr_t)
        {
            SetNull();
        }

        // [ankorsun] made it public to add asserts to the hashtable
        bool IsConst() const
        {
            return (m_inline.m_isBlob != TRUE) && (m_inline.m_isConst == TRUE);
        }

        // Inline representation includes NULL pointers and empty strings
        // [ankorsun] made it public to optimize memory usage in the hashtable
        bool IsInline() const
        {
            return (m_inline.m_isBlob != TRUE) && (m_inline.m_isConst != TRUE);
        }

        // Allocate memory for the data
        SAFE_BUFFERS T * Reserve(size_t size, IncrementalAllocator * alloc)
        {
            Reset();

            if (size <= x_inlineSizeMax)
            {
                m_inline.m_size = static_cast<int>(size);
                return m_inline.m_buf;
            }
            else
            {
                SIZE_T byteSize = size * sizeof(T);
                ValidateSize(byteSize);
                m_blob = SharedPtr<T>::AllocateSharedPtr(alloc, size, 0);
                alloc->UpdateDataSizeStats(byteSize, Traits::GetAllocationType());
                return m_blob.GetBuffer();
            }
        }

        // Copies data into the buffer
        SAFE_BUFFERS void CopyFrom(const T * ptr, size_t size, IncrementalAllocator * alloc)
        {
            if (ptr == NULL)
            {
                SetNull();
                return;
            }

            if (size == 0)
            {
                SetEmpty();
                return;
            }

            T * buf = Reserve(size, alloc);
            memcpy(buf, ptr, sizeof(T) * size);
        }

        // Copies data into the buffer
        SAFE_BUFFERS void CopyFromNotNull(const T * ptr, UINT size, IncrementalAllocator * alloc)
        {
            T * buf = Reserve(size, alloc);
            memcpy(buf, ptr, sizeof(T)* size);
        }

        // Copies src buffer to a new buffer allocated in the alloc
        // and sets const pointer to that buffer
        template <typename Allocator>
        SAFE_BUFFERS void CopyFrom(const FixedArrayType & src, Allocator * alloc)
        {
            static_assert(sizeof(Allocator::value_type) == 1, "Only char allocators are supported");

            SCOPE_ASSERT(!src.IsInline());

            size_t size = src.size();

            void* buffer = alloc->allocate(size);
            memcpy(buffer, src.buffer(), size);

            SetConstPtr(reinterpret_cast<const char*>(buffer), size);
        }

        // array size
        UINT size() const
        {
            // size field of both m_constPtr and m_blob is aligned
            // with inline size, so it does not matter what to return
            return m_inline.m_size;
        }

        // array buffer pointer
        // - this should be removed as it exposes the internal state
        const T * buffer() const
        {
            return GetConstBuffer();
        }

        // Is null array
        bool IsNull() const
        {
            return (m_inline.m_isBlob != TRUE) && (m_inline.m_isNull == TRUE);
        }

        // set array to null
        // NULL array is always inline.
        void SetNull()
        {
            Reset();
            m_inline.m_isNull = TRUE;
        }

        bool IsEmpty() const
        {
            // size field of both m_constPtr and m_blob is aligned
            // with inline size
            return !IsNull() && m_inline.m_size == 0;
        }

        // set array to zero length
        // Zero length array is always inline.
        void SetEmpty()
        {
            // empty string means that:
            // 1) the length is zero
            // 2) it is not null
            // 3) nor const ptr or blob
            Reset();
        }

        // Is null or empty fixed array
        //
        bool IsNullOrEmpty() const
        {
            return IsNull() || IsEmpty();
        }

        // compare function for FixedArrayType
        int Compare(const FixedArrayType<T> & b) const
        {
            // if the inline string or the pointers are equal
            // there is no reason to do any more comparisons
            if (m_binary[0] == b.m_binary[0] && m_binary[1] == b.m_binary[1])
                return 0;

            return SlowCompare(b);
        }

        /*
         * GetCRC32Hash function uses SSE4 instruction crc32 to compute the result.
         *
         * For inlined data the result is the hash of the 16-byte binary representation
         * of the object;
         * for data that is referenced as a const T* or is stored in a SharedPtr
         * the result is the sum of the hash codes of the 8-byte long chunks
         * of data.
         *
         */
        unsigned __int64 GetCRC32Hash(unsigned __int64 crc) const
        {
            unsigned __int64 result = crc;

            if (IsNull())
            {
                result = CRC32Hash(result, x_NULLHASH);
            }
            else if (IsInline())
            {
                result = _mm_crc32_u64(result, m_binary[0]);
                result = _mm_crc32_u64(result, m_binary[1]);
            }
            else
            {
                const T* ptr = buffer();
                const UINT len = size();

                UINT q = len / sizeof(unsigned __int64);
                while (q--)
                {
                    unsigned __int64 chunk = *reinterpret_cast<const unsigned __int64*>(ptr);
                    result = _mm_crc32_u64(result, chunk);
                    ptr += sizeof(unsigned __int64);
                }

                UINT r = len % sizeof(unsigned __int64);
                if (r)
                {
                    ptr -= (sizeof(unsigned __int64) - r);
                    UINT64 chunk = *reinterpret_cast<const unsigned __int64*>(ptr);
                    result = _mm_crc32_u64(result, chunk);
                }
            }

            return result;
        };

        // compute 32 bit hash for string
        int GetScopeHashCode() const
        {
            if (IsNull())
            {
                return x_NULLHASH;
            }

            return Get32BitHashCode();
        }

        //equality compare operator
        bool operator==(const FixedArrayType<T> & other) const
        {
            return (Compare(other) == 0);
        }

        bool operator!=(const FixedArrayType<T> & other) const
        {
            return !(*this == other);
        }

        int operator-(const FixedArrayType<T> & other) const
        {
            return Compare(other);
        }

        bool operator < ( const FixedArrayType<T> & t) const
        {
            return Compare(t) < 0;
        }

        bool operator > ( const FixedArrayType<T> & t) const
        {
            return Compare(t) > 0;
        }

        bool operator <= ( const FixedArrayType<T> & t) const
        {
            return Compare(t) <= 0;
        }

        bool operator >= ( const FixedArrayType<T> & t) const
        {
            return Compare(t) >= 0;
        }
    };

    /*
     * MemoryTracker is a counter for buckets & container memory usage
     * for the Hashtable.
     *
     */
    class MemoryTracker
    {
    private:
        const int      m_allocTax;
        const SIZE_T   m_max;
        SIZE_T         m_payload;
        SIZE_T         m_housekeeping;

    public:
        /*
         * Constucts MemoryTracker object and initializes
         * its contents.
         *
         * max -- maximum number of bytes allowed for container memory usage.
         */
        MemoryTracker(SIZE_T max, SIZE_T allocTax)
            : m_max(max)
            , m_allocTax((int)allocTax)
            , m_payload(0)
            , m_housekeeping(0)
        {
        }

        MemoryTracker& operator=(const MemoryTracker& other) = delete;

        void UpdateHousekeeping(int diff)
        {
            // a sanity check for negative diff values:
            // the absolute value of diff must be less than
            // the current memory value
            SCOPE_ASSERT(diff >= 0 || (-diff) <= m_housekeeping);
            m_housekeeping += diff;
        }

        void UpdateTaxablePayload(int diff)
        {
            UpdatePayload(diff + (diff > 0 ? m_allocTax : -m_allocTax));
        }

        /*
         * Updates current memory usage value by a diff,
         * positive values indicate increase in memory
         * usage, negative indicate decrease.
         */
        void UpdatePayload(int diff)
        {
            // a sanity check for negative diff values:
            // the absolute value of diff must be less than
            // the current memory value
            SCOPE_ASSERT(diff >= 0 || (-diff) <= m_payload);
            m_payload += diff;
        }

        bool AvailableTaxable(SIZE_T bytes) const
        {
            return Available((int)(bytes + m_allocTax));
        }

        bool Available(SIZE_T bytes) const
        {
            return Total() + bytes <= m_max;
        }

        SIZE_T Payload() const
        {
            return m_payload;
        }

        SIZE_T Total() const
        {
            return m_payload + m_housekeeping;
        }
    };


    /*
     * FixedArrayTypeMemoryManager is a FixedArrayType objects copy
     * manager for the Hashtable.
     *
     * Hashtable uses the object of FixedArrayTypeMemoryManager class
     * to control memory usage and prevent memory leaks
     * in Insert and Update operations.
     *
     * FixedArrayTypeMemoryManager usage pattern for one Insert/Updates operation
     * is the following:
     * 1) Call Reset();
     * 2) Call Copy() as many times as needed. A Copy() operation:
     *    2.1) Checks with MemoryTracker whether there is enough memory
     *         to allocate the src buffer;
     *    2.2) If not, sets object to invalid and returns; else
     *    2.3) allocates copy of the src buffer in the internal allocator;
     *    2.4) overwrites data pointer in dest;
     *    2.5) records both old and new buffer pointers to be used
     *         in Commit() and Rollback() operations.
     * 3) Call Valid() -- returns true if all the Copy() operations were successfull;
     * 4) If the object is Valid() call Commit() -- release all the overwritten buffers
     *    and update memory usage in the memory tracker; else call Rollback() --
     *    release all the allocated buffers.
     */
    template <typename Allocator>
    class FixedArrayTypeMemoryManager
    {
    private:
        typedef typename Allocator::template rebind<char>::other  CharAllocator;

    private:
        CharAllocator       m_alloc;
        MemoryTracker*      m_memoryTracker;

        std::vector<char*>  m_releasedBuffers;
        std::vector<char*>  m_allocatedBuffers;
        SIZE_T              m_memoryAllocated;
        SIZE_T              m_memoryReleased;
        bool                m_valid;

    private:
        template <typename T>
        static char * DataPointer(const T & t)
        {
            // reinterpret_cast is needed to unify FixedArrayType<char>/FString
            // and FixedArrayType<unsigned char>/FBinary
            static_assert(sizeof(T::ValueType) == 1, "Unexpected FixedArrayType::ValueType size");
            return const_cast<char*>(reinterpret_cast<const char*>(t.buffer()));
        }

        template <typename T>
        void CopyBufferAndOverwriteDest(T & dest, const T & src)
        {
            // no need to account for m_memoryReleased because
            // the update requires the amount of memory
            // enough to hold both copies
            if (!m_memoryTracker->Available(m_memoryAllocated + src.size()))
            {
                m_valid = false;
                return;
            }

            dest.CopyFrom(src, &m_alloc);

            m_allocatedBuffers.push_back(DataPointer(dest));
            m_memoryAllocated += dest.size();
        }


    public:
        FixedArrayTypeMemoryManager(Allocator& alloc, MemoryTracker* memoryTracker)
            : m_alloc(alloc)
            , m_memoryTracker(memoryTracker)
        {
            Reset();
        }

        /*
         * Returns true if all the Copy() operations executed since the last
         * Reset() call were successfull.
         */
        bool Valid() const
        {
            return m_valid;
        }

        /*
         * Releases memory for all buffers that were overwritten by the Copy() operations
         * and updates memory usage in the MemoryTracker.
         */
        void Commit()
        {
            SCOPE_ASSERT(m_valid);

            m_memoryTracker->UpdatePayload((INT)m_memoryAllocated - (INT)m_memoryReleased);
            std::for_each(m_releasedBuffers.begin(), m_releasedBuffers.end(), [this] (char* ptr) { m_alloc.deallocate(ptr, 0 /*unused*/); });
        }

        /*
         * Releases memory for all the buffers that were allocated by the Copy() operations.
         */
        void Rollback()
        {
            std::for_each(m_allocatedBuffers.begin(), m_allocatedBuffers.end(), [this] (char* ptr) { m_alloc.deallocate(ptr, 0 /*unused*/); });
        }

        /*
         * Copies t buffer to the internal allocator
         * and overwrites t data pointer.
         *
         * NOTE: Used in Hashtable::Insert.
         */
        template <typename T>
        void Copy(T & t)
        {
            // if a previous copy failed there is no need
            // to copy anything else
            if (!m_valid)
                return;

            if (!t.IsInline())
                CopyBufferAndOverwriteDest(t, t);
        }

        /*
         * Copies src buffer to the internal allocator
         * and overwrites dest data pointer.
         *
         * NOTE: used in Hashtable::Update.
         */
        template <typename T>
        void Copy(T & dest, const T & src)
        {
            // if a previous copy failed there is no need
            // to attempt to copy other data
            if (!m_valid)
                return;

            if (!dest.IsInline())
            {
                m_releasedBuffers.push_back(DataPointer(dest));
                m_memoryReleased += dest.size();
            }

            if (src.IsInline())
                dest = src;
            else
                CopyBufferAndOverwriteDest(dest, src);
        }

        template <typename T>
        void Delete(T & t)
        {
            if (!t.IsInline())
            {
                m_releasedBuffers.push_back(DataPointer(t));
                m_memoryReleased += t.size();
            }
        }

        void Reset()
        {
            m_releasedBuffers.clear();
            m_allocatedBuffers.clear();
            m_memoryReleased = 0;
            m_memoryAllocated = 0;
            m_valid = true;
        }
    };

    class FStringWithNull;

#ifdef PLUGIN_TYPE_SYSTEM
    class FString : public FixedArrayType<char>, public PluginTypeBase<FString>
#else
    class FString : public FixedArrayType<char>
#endif
    {
    public:
        // Copy constructor (does shallow copy)
        FString(const FString & s) : FixedArrayType<char>(s)
        {
        }

        // Copy constructor with allocator.
        // It is a deep copy. The string will be copied as well.
        FString(const FString & s, IncrementalAllocator * alloc) : FixedArrayType(s, alloc)
        {
        }

        // Default constructor
        FString() : FixedArrayType<char>()
        {
        }

        // Null constructor
        FString(nullptr_t) : FixedArrayType<char>(__nullptr)
        {
        }

        // Constructor for string literals
        FString(const char * str, size_t size) : FixedArrayType<char>()
        {
            SetConstPtr(str, size);
        }

        // type conversion, to DTStringInput for ScopeDateTime.Parse to consume
        operator DTStringInput () const
        {
            return DTStringInput(buffer(), size());
        }

        // Takes ownership of AutoExpandBuffer buffer
        void MoveFrom(AutoExpandBuffer & autoBuffer)
        {
            // @TODO: verify that null termination is there
            SharedPtr<char> str = autoBuffer.TakeBuffer(IncrementalAllocator::StringAllocation);
            unsigned int size = str.GetSize();
            ValidateSize(size);

            Reset();

            if (size == 0)
            {
                SetNull();
                return;
            }
            else if (size == 1)
            {
                SetEmpty();
                return;
            }

            // Remove the null termination
            size--;

            if (size <= x_inlineSizeMax)
            {
                memcpy(m_inline.m_buf, str.GetBuffer(), size);
                m_inline.m_size = size;
            }
            else
            {
                memcpy(&m_blob, &str, sizeof(SharedPtr<char>));
                m_blob.ReduceSize(size);
            }
        }

        ConvertResult ConvertFrom(FStringWithNull & str);

#pragma region StringIntrinsics
        // This method is not to be confused with size of the string, or the number of UTF8 individual
        // characters. It will return the true string.Length, just like in C#.
        int Length() const
        {
            if (IsNullOrEmpty())
            {
                return 0;
            }

            const char * buffPtr = buffer();
            int len = 0;
            unsigned int pos = 0;

            while (pos < size())
            {
                // Count only bytes that start a new UTF8 letter, which are all that
                // do not start with bits 10.
                // @TODO: based on the start char we can immediately skip to next one based
                // on how many top bits are set. We don't do this in the initial implementation.
                if (IsUtf8StartChar(buffPtr[pos]))
                {
                    len++;
                }

                pos++;
            }

            return len;
        }

        bool StartsWith(const FString & s) const
        {
            if (IsNull() || s.IsNull())
            {
                return false;
            }

            unsigned int strSize = s.size();

            if (strSize > size())
            {
                return false;
            }

            return memcmp(buffer(), s.buffer(), strSize * sizeof(char)) == 0;
        }

        bool EndsWith(const FString & s) const
        {
            if (IsNull() || s.IsNull())
            {
                return false;
            }

            unsigned int thisSize = size();
            unsigned int strSize = s.size();

            if (strSize > thisSize)
            {
                return false;
            }

            return memcmp(buffer() + (thisSize-strSize), s.buffer(), strSize * sizeof(char)) == 0;
        }

        FString Substring(int startIndex, int length, IncrementalAllocator * alloc) const
        {
            if (startIndex < 0 || length < 0)
            {
                throw RuntimeException(E_USER_SUBSTRING_OUT_RANGE, startIndex, length);
            }

            const char * buffPtr = buffer();
            UINT buffSize = size();

            UINT byteOffset;
            if (!GetByteSize(buffPtr, buffSize, (UINT)startIndex, &byteOffset))
            {
                throw RuntimeException(E_USER_SUBSTRING_OUT_RANGE, startIndex, length);
            }

            UINT byteSize;
            if (!GetByteSize(buffPtr + byteOffset, buffSize - byteOffset, (UINT)length, &byteSize))
            {
                throw RuntimeException(E_USER_SUBSTRING_OUT_RANGE, startIndex, length);
            }

            FString result;
            result.CopyFrom(buffPtr + byteOffset, byteSize, alloc);
            return result;
        }

        bool Contains(const FString & s) const
        {
            if (IsNull() || s.IsNull())
            {
                return false;
            }

            unsigned int containsStrSize = s.size();
            unsigned int origStrSize = size();
            const char * containsBuff = s.buffer();
            const char * origBuff = buffer();

            while (containsStrSize <= origStrSize)
            {
                // @TODO: We could skip more characters based on what the UTF8 start char is.
                if (IsUtf8StartChar(*origBuff))
                {
                    if (memcmp(origBuff, containsBuff, containsStrSize * sizeof(char)) == 0)
                    {
                        return true;
                    }
                }

                origBuff++;
                origStrSize--;
            }

            return false;
        }

        FString ToLowerInvariant(IncrementalAllocator * alloc) const
        {
            return ChangeCase(true, alloc);
        }

        FString ToUpperInvariant(IncrementalAllocator * alloc) const
        {
            return ChangeCase(false, alloc);
        }

#pragma endregion StringIntrinsics

    // Pending implementation, TODO xiaoyuc
#pragma region PluginTypeSystem
#ifdef PLUGIN_TYPE_SYSTEM
        // Already defined in base class
        //int Compare(const FString & ) const
        //{
        //    return 0;
        //}
        using FixedArrayType<char>::Compare;

        // Serialize in ScopeMapOutputMemoryStream::Write
        template<typename BufferType>
        void ScopeMapOutputMemoryStreamSerialize(void* buffer) const
        {
            DoScopeMapOutputSerialize((BufferType*)buffer);
        }

        // Serialize in BinaryOutputStreamBase::Write
        template<typename OutputType>
        void Serialize(BinaryOutputStreamBase<OutputType>* stream) const
        {
            DoSerialize(stream);
        }

        // Serialize in, ScopeIO.h TextOutputStream::Write
        template<typename TextOutputStreamTraits>
        void Serialize(TextOutputStream<TextOutputStreamTraits>* stream) const
        {
            if (IsNull())
            {
                // handle null value
                if (stream->escape || stream->nullEscape != nullptr)
                {
                    stream->WriteNull();
                }

                return;
            }

            stream->WriteString<stream->textQualifier>(buffer(), size());
        }

        // Serialize in SStreamDataOutputStream::Write
        template<typename BufferType>
        void SStreamDataOutputSerialize(void* buf) const
        {
            ((BufferType*)buf)->Write(buffer(), size());
        }

        // DeSerialize in ScopeMapInputMemoryStream::Read
        template<typename AllocatorType>
        void ScopeMapInputMemoryStreamDeserialize(const char* & cur, void* ia)
        {
            DoScopeMapInputMemoryStreamDeserialize(cur, (AllocatorType*)ia);
        }

        // DeSerialize in BinaryInputStreamBase::Read
        template<typename InputType>
        void Deserialize(BinaryInputStreamBase<InputType>* stream, IncrementalAllocator* m_allocator)
        {
            DoDeserialize(stream, m_allocator);
        }

        // DeSerialize in TextInputStream::Read
        template<typename TextInputStreamTraits, typename InputStreamType>
        void Deserialize(TextInputStream<TextInputStreamTraits, InputStreamType>* stream, FStringWithNull & str, bool lastEmptyColumn)
        {
            if (lastEmptyColumn)
            {
                // create an empty string
                SetEmpty();
                stream->EndRow(false);
                return;
            }

            auto result = TextConversions::TryFStringToT(str, *this);
            if (result != ConvertSuccess)
            {
                if (stream->silent)
                {
                    SetNull();
                }
                else
                {
                    stream->StoreBadConvertException(result, str);
                }
            }

            if (stream->CurrentField() == stream->ColumnExpected())
            {
                stream->EndRow(true);
            }
        }

        // Swap the values
        void Swap(FString & )
        {
        }

        // The hash function
        size_t GetHashCode() const
        {
            return 0;
        }

        // TryFStringToT, see scopeio.h
        template<typename StringType>
        ConvertResult FromFString(void* str)
        {
            return ConvertFrom(*(StringType*)str);
        }

        // Already defined in base class
        // bool IsNull() const
        // {
        //    return false;
        // }
        using FixedArrayType<char>::IsNull;

        // For Aggregate_MIN and Aggregate_MAX
        // void SetNull()
        // {
        // }
        using FixedArrayType<char>::SetNull;

        // Deep copier
        void DeepCopyFrom(FString const & , IncrementalAllocator * )
        {
        }

        // Shallow copier
        void ShallowCopyFrom(FString const & )
        {
        }

        // Used by Aggregate_UNCHECKED_SUMx2
        FString Multiply(FString const & ) const
        {
            return FString();
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        FString & AddByUnchecked(FString const & )
        {
            return *this;
        }

        // For Aggregate_SUM: Should check overflow; Should skip if the value is null
        FString & AddByChecked(FString const & )
        {
            return *this;
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        FString & SetZero()
        {
            return *this;
        }

        static FString Min()
        {
            return FString();
        }

        static FString Max()
        {
            return FString();
        }

        enum {isNullablePrimaryType=false};
        enum {is_floating_point=false};
        enum {is_signed=false};
        enum {is_unsigned=false};
        enum {is_integral=false};
        enum {need_deep_copy=false};
#endif
#pragma endregion PluginTypeSystem

        friend ostream & operator<<(ostream & os, const FString & s);
        friend class FBinary;
        friend class ScopeEngine::ScopeSqlType::SqlStringImp;

    private:

        // Takes ownership of other's object buffer
        void MoveFrom(FString & s)
        {
            // Shallow copy
            SetInternalState(s.m_binary);
            s.SetNull();
        }

        static bool IsUtf8StartChar(char c)
        {
            return (c & 0xc0) != 0x80;
        }

        // Calculates amount of UTF-8 bytes for the charSize characters (returns 'false' in case of "out-of-range")
        static bool GetByteSize(const char * buffer, UINT bufferSize, UINT charSize, UINT * byteSize)
        {
            *byteSize = 0;

            while (*byteSize < bufferSize && charSize > 0)
            {
                char utf8Byte = buffer[*byteSize];

                SCOPE_ASSERT(IsUtf8StartChar(utf8Byte));

                if (utf8Byte & 0x80)
                {
                    // In a sequence of n octets, n>1, the initial octet has the n higher-order bits set to 1,
                    // followed by a bit set to 0 (see RFC 3629 http://tools.ietf.org/html/rfc3629)
                    while (utf8Byte & 0x80)
                    {
                        utf8Byte <<= 1;
                        ++(*byteSize);
                    }
                }
                else
                {
                    // ASCII symbol
                    ++(*byteSize);
                }

                --charSize;
            }

            return charSize == 0;
        }

        // Cuts tail of the string
        void Truncate(unsigned int length)
        {
            SCOPE_ASSERT(length <= size() && !IsConst() && !IsNull());

            if (length == 0)
            {
                SetEmpty();
            }
            else if (length <= x_inlineSizeMax)
            {
                char * buf = m_blob.GetBuffer();

                if (!IsInline())
                {
                    Reset();

                    memcpy(m_inline.m_buf, buf, length);
                }

                m_inline.m_size = length;
            }
            else
            {
                // Set new size
                m_blob.ReduceSize(length);
            }
        }

        FString ChangeCase(bool toLowerCase, IncrementalAllocator * alloc) const
        {
            std::wstring_convert<std::codecvt_utf8_utf16<wchar_t, 0x10ffff, std::little_endian>, wchar_t> utf16conv;
            const char * buffPtr = buffer();
            UINT buffSize = size();
            std::wstring utf16string = utf16conv.from_bytes(buffPtr, buffPtr + buffSize);
            wchar_t * utf16ptr = (wchar_t *)utf16string.data();
            int utf16size = (int)utf16string.size();
            int res = LCMapStringEx(LOCALE_NAME_INVARIANT, toLowerCase ? LCMAP_LOWERCASE : LCMAP_UPPERCASE, utf16ptr, utf16size, utf16ptr, utf16size, NULL, NULL, 0);
            SCOPE_ASSERT(res == utf16size);
            std::string utf8string = utf16conv.to_bytes(utf16ptr, utf16ptr + utf16size);

            FString result;
            result.CopyFrom(utf8string.c_str(), utf8string.size(), alloc);
            return result;
        }
    };

    class ScopeLikeUtil
    {
    public:
        static bool Like(const FString & input, const FString & match, wchar_t escape)
        {
            // null LIKE match == false
            // input LIKE null == false
            if (input.IsNull() || match.IsNull())
            {
                return false;
            }

            std::wstring_convert<codecvt_utf8<wchar_t>> converter;
            string utf8Escape = converter.to_bytes(escape);
            return Like(input.buffer(), input.size(), 0, match.buffer(), match.size(), 0, utf8Escape.c_str(), (int)utf8Escape.length());
        }

        static bool NotLike(const FString & input, const FString & match, wchar_t escape)
        {
            // null LIKE match == false
            // input LIKE null == false
            if (input.IsNull() || match.IsNull())
            {
                return false;
            }

            std::wstring_convert<codecvt_utf8<wchar_t>> converter;
            string utf8Escape = converter.to_bytes(escape);
            return !Like(input.buffer(), input.size(), 0, match.buffer(), match.size(), 0, utf8Escape.c_str(), (int)utf8Escape.length());
        }

        static bool Like(const char * input, const char * match, const char * escape)
        {
            // null LIKE match == false
            // input LIKE null == false
            if (input == NULL || match == NULL)
            {
                return false;
            }

            return Like(input, (int)strlen(input), 0, match, (int)strlen(match), 0, escape, (int)strlen(escape));
        }

        static bool NotLike(const char * input, const char * match, const char * escape)
        {
            // null LIKE match == false
            // input LIKE null == false
            if (input == NULL || match == NULL)
            {
                return false;
            }

            return !Like(input, (int)strlen(input), 0, match, (int)strlen(match), 0, escape, (int)strlen(escape));
        }

    private:
        static bool Like(const char * input, int inputSize, int inputPosition, const char * match, int matchSize, int matchPosition, const char * escape, int escapeSize)
        {
            while (matchPosition < matchSize)
            {
                char m = match[matchPosition];
                if ('%' == m)
                {
                    // Collapse multiple % to single %
                    while (++matchPosition < matchSize && '%' == match[matchPosition])
                    {
                    }

                    // If % is at the end of match, i.e. "pattern%",  and we have successfully matched "pattern" so far, we are done.
                    if (matchPosition == matchSize)
                    {
                        return true;
                    }

                    // If the remaining match is "%pattern" and pattern doesn't contain any metacharacter or the escape character,
                    // the remaining match can be translated to EndsWith()
                    if (EndsWithRemainingMatch(input, inputSize, inputPosition, match, matchSize, matchPosition, escape, escapeSize))
                    {
                        return true;
                    }

                    // To evaluate "%pattern", we try to match "pattern" starting from the current input position.
                    for (; inputPosition < inputSize; inputPosition++)
                    {
                        if (!IsUtf8StartChar(input[inputPosition]))
                        {
                            continue;
                        }

                        if (Like(input, inputSize, inputPosition, match, matchSize, matchPosition, escape, escapeSize))
                        {
                            return true;
                        }
                    }

                    return false;
                }

                // End of input while there's still characters to match.
                if (inputPosition == inputSize)
                {
                    return false;
                }

                if ('_' == m)
                {
                    // Skip over one multibyte char in the input.
                    while (++inputPosition < inputSize && !IsUtf8StartChar(input[inputPosition]))
                    {
                    }

                    matchPosition++;
                    continue;
                }

                if ('[' == m)
                {
                    int closeSquareBracketPosition = FindCloseSquareBracket(match, matchSize, matchPosition + 1, escape, escapeSize);
                    if (closeSquareBracketPosition < 0)
                    {
                        return false;
                    }

                    UINT inputChar;
                    int charSize;
                    if (!GetChar(input, inputSize, inputPosition, inputChar, charSize))
                    {
                        return false;
                    }

                    if (!MatchCharClass(inputChar, match, matchPosition, closeSquareBracketPosition, escape, escapeSize))
                    {
                        return false;
                    }

                    inputPosition += charSize;
                    matchPosition = closeSquareBracketPosition + 1;
                    continue;
                }

                // Handle escape char
                if (IsEscapeChar(match, matchSize, matchPosition, escape, escapeSize))
                {
                    matchPosition += escapeSize;
                    if (matchPosition >= matchSize)
                    {
                        return false;
                    }

                    if (!MatchOneChar(input, inputSize, inputPosition, match, matchSize, matchPosition))
                    {
                            return false;
                    }

                    continue;
                }

                if (!MatchUntilNextMetaChar(input, inputSize, inputPosition, match, matchSize, matchPosition, escape, escapeSize))
                {
                    return false;
                }
            }

            // Reached the end of input and match at the same time, success.
            if (inputPosition == inputSize)
            {
                return true;
            }

            // Reached the end of match but we still have unmatched input characters remaining.
            // Trailing spaces in the input are insignificant for TSQL LIKE.
            return EndsWithSpaces(input, inputSize, inputPosition);
        }

        static int FindCloseSquareBracket(const char * match, int matchSize, int startPosition, const char * escape, int escapeSize)
        {
            for (int position = startPosition; position < matchSize; position++)
            {
                if (IsEscapeChar(match, matchSize, position, escape, escapeSize))
                {
                    position += escapeSize;

                    // Skip over the character after the escape character.
                    continue;
                }

                if (match[position] == ']')
                {
                    return position;
                }
            }

            return -1;
        }

        static bool IsUtf8StartChar(char c)
        {
            return (c & 0xc0) != 0x80;
        }

        /// <summary />
        static bool MatchCharClass(UINT inputChar, const char * match, int openSquareBracketPosition, int closeSquareBracketPosition, const char * escape, int escapeSize)
        {
            int startPosition = openSquareBracketPosition + 1;
            bool invert = false;
            if (match[startPosition] == '^')
            {
                // match [^]
                if (startPosition == closeSquareBracketPosition - 1)
                {
                    return inputChar == '^';
                }

                invert = true;
                startPosition++;
            }

            // Unescaped '-' can only be matched if it is the first or last character inside [...]
            if (inputChar == '-' && (match[startPosition] == '-' || match[closeSquareBracketPosition - 1] == '-'))
            {
                return !invert;
            }

            // Try match character range, e.g. [A-Z]
            bool inRange;
            if (TryMatchCharRange(inputChar, match, startPosition, closeSquareBracketPosition, escape, escapeSize, inRange))
            {
                return invert ^ inRange;
            }

            // Match character set.
            int charSize;
            for (int matchPosition = startPosition; matchPosition < closeSquareBracketPosition; matchPosition += charSize)
            {
                bool foundEscape = false;
                if (IsEscapeChar(match, closeSquareBracketPosition, matchPosition, escape, escapeSize))
                {
                    matchPosition += escapeSize;
                    foundEscape = true;
                }

                UINT m;
                if (!GetChar(match, closeSquareBracketPosition, matchPosition, m, charSize))
                {
                    return false;
                }

                // '-' is ignored if it is not the first and last character in the set.
                if ('-' == m && !foundEscape)
                {
                    continue;
                }

                if (inputChar == m)
                {
                    return !invert;
                }
            }

            return invert;
        }

        static bool TryMatchCharRange(UINT inputChar, const char * match, int position, int endPosition, const char * escape, int escapeSize, bool & inRange)
        {
            UINT startChar;
            UINT endChar;
            int bytesRead;
            bool foundEscape;
            if (!GetUnescapedChar(match, endPosition, position, escape, escapeSize, startChar, foundEscape, bytesRead)
                || (position += bytesRead) >= endPosition
                || match[position] != '-'
                || ++position >= endPosition
                || !GetUnescapedChar(match, endPosition, position, escape, escapeSize, endChar, foundEscape, bytesRead)
                || (position + bytesRead) != endPosition)
            {
                inRange = false;
                return false;
            }

            inRange = inputChar >= startChar && inputChar <= endChar;
            return true;
        }

        static bool MatchOneChar(const char * input, int inputSize, int & inputPosition, const char * match, int matchSize, int & matchPosition)
        {
            do
            {
                if (input[inputPosition++] != match[matchPosition++])
                {
                    return false;
                }

            } while (inputPosition < inputSize && matchPosition < matchSize && !IsUtf8StartChar(match[matchPosition]));

            return true;
        }

        static bool MatchUntilNextMetaChar(const char * input, int inputSize, int & inputPosition, const char * match, int matchSize, int & matchPosition, const char * escape, int escapeSize)
        {
            for (; inputPosition < inputSize && matchPosition < matchSize; inputPosition++, matchPosition++)
            {
                if (IsMetaChar(match, matchSize, matchPosition, escape, escapeSize))
                {
                    break;
                }

                if (input[inputPosition] != match[matchPosition])
                {
                    return false;
                }
            }

            return true;
        }

        static bool IsMetaChar(const char * match, int matchSize, int matchPosition, const char * escape, int escapeSize)
        {
            char c = match[matchPosition];
            if ('%' == c || '_' == c || '[' == c)
            {
                return true;
            }

            if (IsEscapeChar(match, matchSize, matchPosition, escape, escapeSize))
            {
                return true;
            }

            return false;
        }

        static bool EndsWithRemainingMatch(const char * input, int inputSize, int inputPosition, const char * match, int matchSize, int matchPosition, const char * escape, int escapeSize)
        {
            int ii = inputSize;
            while (ii - 1 >= inputPosition && input[--ii] == ' ') { }

            int mi = matchSize;
            while (mi - 1 >= matchPosition && match[--mi] == ' ') { }

            // Trailing spaces in the match is significant while they are insignificant in the input.
            if (matchSize - mi > inputSize - ii)
            {
                return false;
            }

            // Fail early if we have more remaining match than remaining input.
            if (mi - matchPosition > ii - inputPosition)
            {
                return false;
            }

            for (; mi >= matchPosition; ii--, mi--)
            {
                char m = match[mi];
                if (input[ii] != m)
                {
                    return false;
                }

                if (!IsUtf8StartChar(m))
                {
                    // Only need to check for IsMetaCharacter if m is the Utf8StartChar
                    continue;
                }

                if (IsMetaChar(match, matchSize, mi, escape, escapeSize))
                {
                    return false;
                }
            }

            return true;
        }

        static bool EndsWithSpaces(const char * buffer, int bufferSize, int position)
        {
            for (; position < bufferSize; position++)
            {
                if (buffer[position] != ' ')
                {
                    return false;
                }
            }

            return true;
        }

        static bool IsEscapeChar(const char * match, int matchSize, int matchPosition, const char * escape, int escapeSize)
        {
            switch (escapeSize)
            {
                case 1:
                    return escape[0] == match[matchPosition];
                case 2:
                    return matchPosition + escapeSize <= matchSize
                        && escape[0] == match[matchPosition]
                        && escape[1] == match[matchPosition + 1];
                case 3:
                    return matchPosition + escapeSize <= matchSize
                        && escape[0] == match[matchPosition]
                        && escape[1] == match[matchPosition + 1]
                        && escape[2] == match[matchPosition + 2];
                case 4:
                    return matchPosition + escapeSize <= matchSize
                        && escape[0] == match[matchPosition]
                        && escape[1] == match[matchPosition + 1]
                        && escape[2] == match[matchPosition + 2]
                        && escape[3] == match[matchPosition + 3];
            }

            return false;
        }

        static bool GetChar(const char * buffer, int bufferSize, int position, UINT & charValue, int & charSize)
        {
            charValue = buffer[position];

            // ASCII
            // 0x80 == 10000000
            if (charValue < 0x80)
            {
                charSize = 1;
                return true;
            }

            // 2 bytes encoding: 110XXXXX 10XXXXXX
            // 0xE0 == 11100000
            // 0xC0 == 11000000
            if ((charValue & 0xE0) == 0xC0 && position + 1 < bufferSize)
            {
                // 0x1F == 00011111
                charValue &= 0x1F;
                charSize = 2;

                // 0x3F = 00111111
                charValue <<= 6;
                charValue += (UINT) (buffer[position + 1] & 0x3F);

                return true;
            }

            // 3 bytes encoding: 1110XXXX 10XXXXXX 10XXXXXX
            // 0xF0 == 11110000
            // 0xE0 == 11100000
            if ((charValue & 0xF0) == 0xE0 && position + 2 < bufferSize)
            {
                charValue &= 0xF;
                charSize = 3;

                // 0x3F = 00111111
                charValue <<= 6;
                charValue += (UINT) (buffer[position + 1] & 0x3F);
                charValue <<= 6;
                charValue += (UINT) (buffer[position + 2] & 0x3F);

                return true;
            }

            // 4 bytes encoding: 11110XXX 10XXXXXX 10XXXXXX 10XXXXXX
            // 0xF8 == 11111000
            // 0xF0 == 11110000
            if ((charValue & 0xF8) == 0xF0 && position + 3 < bufferSize)
            {
                charValue &= 0x7;
                charSize = 4;

                // 0x3F = 00111111
                charValue <<= 6;
                charValue += (UINT) (buffer[position + 1] & 0x3F);
                charValue <<= 6;
                charValue += (UINT) (buffer[position + 2] & 0x3F);
                charValue <<= 6;
                charValue += (UINT) (buffer[position + 3] & 0x3F);

                return true;
            }

            charSize = -1;
            return false;
        }

        static bool GetUnescapedChar(const char * match, int matchSize, int matchPosition, const char * escape, int escapeSize, UINT & charValue, bool & foundEscape, int & bytesRead)
        {
            foundEscape = false;
            bytesRead = 0;
            if (IsEscapeChar(match, matchSize, matchPosition, escape, escapeSize))
            {
                bytesRead = escapeSize;
            }

            int charSize;
            if (!GetChar(match, matchSize, matchPosition + bytesRead, charValue, charSize))
            {
                return false;
            }

            bytesRead += charSize;
            return true;
        }
    };

    INLINE ostream & operator<<(ostream & os, const FString & fs)
    {
        if (fs.IsNull())
        {
            os << "NULL";
        }
        else
        {
            os << '\"';
            os.write(fs.buffer(), fs.size());
            os << '\"';
        }

        return os;
    }

    class FStringWithNull : public FString
    {
    public:
        // Default constructor
        FStringWithNull() : FString()
        {
        }

        unsigned int DataSize() const
        {
            return size() - 1;
        }

        // Trim white space from begin and end of a string
        // The FString always has a null terminator.
        void TrimWhiteSpace()
        {
            char * buf = GetBuffer();
            unsigned int len = DataSize();
            unsigned int start, end;

            for(start = 0; start < len; start++)
            {
                //skip leading white space.
                if (!isspace(buf[start]))
                {
                    break;
                }
            }

            for(end = len-1; end > start; end--)
            {
                //skip trailing white space.
                if (!isspace(buf[end]))
                {
                    break;
                }
            }

            // Reset the FString buffer to new string.
            if (end >= start)
            {
                len = end - start + 1;

                // Modify the string
                memmove(buf, buf + start, len);
            }
            else
            {
                len = 0;
            }

            ReduceSize(len + 1);

            buf[len] = '\0';
        }

        // Takes ownership of AutoExpandBuffer buffer
        void MoveFrom(AutoExpandBuffer & autoBuffer)
        {
            SharedPtr<char> str = autoBuffer.TakeBuffer(IncrementalAllocator::StringAllocation);
            unsigned int size = str.GetSize();
            ValidateSize(size);

            Reset();

            if (size == 0)
            {
                SetNull();
            }
            else if (size == 1)
            {
                // This automatically has the first byte as '\0'
                SetEmpty();
            }
            else if (size <= x_inlineSizeMax)
            {
                memcpy(m_inline.m_buf, str.GetBuffer(), size);
                m_inline.m_size = size;
            }
            else
            {
                memcpy(&m_blob, &str, sizeof(SharedPtr<char>));
            }
        }
    };

    INLINE ostream & operator<<(ostream & os, const FStringWithNull & fs)
    {
        if (fs.IsNull())
        {
            os << "NULL";
        }
        else if(fs.IsEmpty())
        {
            os << "";
        }
        else
        {
            os << '\"';
            os.write(fs.buffer(), fs.DataSize());
            os << '\"';
        }

        return os;
    }

    // Defined outside FString class because it must be defined after FStringWithNull
    INLINE ConvertResult FString::ConvertFrom(FStringWithNull & str)
    {
        if (str.IsNull())
        {
            SetNull();
            return ConvertSuccess;
        }

        if (str.IsEmpty())
        {
            SetEmpty();
            return ConvertSuccess;
        }

        str.Truncate(str.DataSize());

        // move buffer to this FString
        MoveFrom(str);

        return ConvertSuccess;
    }

#ifdef PLUGIN_TYPE_SYSTEM
    class FBinary : public FixedArrayType<unsigned char>, public PluginTypeBase<FBinary>
#else
    class FBinary : public FixedArrayType<unsigned char>
#endif
    {
    public:
        // Copy constructor (does shallow copy)
        FBinary(const FBinary & s) : FixedArrayType<unsigned char>(s)
        {
        }

        // Copy constructor with allocator.
        // It is a deep copy. The string will be copied as well.
        FBinary(const FBinary & s, IncrementalAllocator * alloc) : FixedArrayType(s, alloc)
        {
        }

        // Default constructor
        FBinary() : FixedArrayType<unsigned char>()
        {
        }

        // Null constructor
        FBinary(nullptr_t) : FixedArrayType<unsigned char>(__nullptr)
        {
        }

        // Constructor for external data
        FBinary(const char * data, size_t size) : FixedArrayType<unsigned char>()
        {
            SetConstPtr(data, size);
        }

        ConvertResult ConvertFrom(FStringWithNull & str)
        {
            // If string is escaped null then it is a null value for Binary
            if (str.IsNull())
            {
                SetNull();
                return ConvertSuccess;
            }

            if (str.IsEmpty())
            {
                SetEmpty();
                return ConvertSuccess;
            }

            try
            {
                unsigned int size = str.DataSize();

                // String length must be an even number
                if (size % 2 != 0)
                {
                    return ConvertErrorInvalidLength;
                }

                unsigned int j = 0;
                char * buf = str.GetBuffer();

                for(unsigned int i = 0; i < size; i += 2)
                {
                    unsigned char hi, low;

                    hi = buf[i];
                    low = buf[i+1];
                    buf[j++] = static_cast<unsigned char>(((CharToHex(hi) << 4) & 0xF0) | CharToHex(low));
                }

                str.Truncate(j);
                ValidateSize(str.size());

                // move buffer to FBinary out
                MoveFrom(str);
            }
            catch (RuntimeException ex)
            {
                // Unfortunatelty we lose the exception details here - but until we rework the error handling
                // this is the best we can do
                // - invalid characters and strings too long will both get caught here
                if (ex.GetErrorNumber() == E_USER_BINARY_CONVERSION)
                {
                    return ConvertErrorInvalidCharacter;
                }
                else if (ex.GetErrorNumber() == E_USER_BINARY_TOO_BIG)
                {
                    return ConvertErrorTooLong;
                }
                else
                {
                    return ConvertErrorUndefined;
                }
            }

            return ConvertSuccess;
        }

        friend ostream & operator<<(ostream & os, const FBinary & b);

    // Pending implementation, TODO xiaoyuc
#pragma region PluginTypeSystem
#ifdef PLUGIN_TYPE_SYSTEM
        // Implemented by base class
        // int Compare(const FBinary & ) const
        // {
        //     return 0;
        // }
        using FixedArrayType<unsigned char>::Compare;

        // Serialize in ScopeMapOutputMemoryStream::Write
        template<typename BufferType>
        void ScopeMapOutputMemoryStreamSerialize(void* buffer) const
        {
            DoScopeMapOutputSerialize((BufferType*)buffer);
        }

        // Serialize in BinaryOutputStreamBase::Write
        template<typename OutputType>
        void Serialize(BinaryOutputStreamBase<OutputType>* stream) const
        {
            DoSerialize(stream);
        }

        // Serialize in, ScopeIO.h TextOutputStream::Write
        template<typename TextOutputStreamTraits>
        void Serialize(TextOutputStream<TextOutputStreamTraits>* stream) const
        {
            if (IsNull())
            {
                // handle null value
                if (stream->escape || stream->nullEscape != nullptr)
                {
                    stream->WriteNull();
                }

                return;
            }

            // If it is an empty binary, nothing left to write
            if (IsEmpty())
            {
                return;
            }

            // convert to "X2" format string
            unique_ptr<char> buf(new char[size()*2]);

            unsigned int j = 0;
            const unsigned char * data = buffer();

            for(unsigned int i=0; i < size(); i++)
            {
                unsigned char t = data[i];
                unsigned char hi = (t>>4)&0xF;
                unsigned char low = t&0xF;

                buf.get()[j++] = x_HexTable[hi];
                buf.get()[j++] = x_HexTable[low];
            }

            stream->WriteString<false>(buf.get(), j);
        }

        // Serialize in SStreamDataOutputStream::Write
        template<typename BufferType>
        void SStreamDataOutputSerialize(void* buf) const
        {
            ((BufferType*)buf)->Write((const char *)buffer(), size());
        }

        // DeSerialize in ScopeMapInputMemoryStream::Read
        template<typename AllocatorType>
        void ScopeMapInputMemoryStreamDeserialize(const char* & cur, void* ia)
        {
            DoScopeMapInputMemoryStreamDeserialize(cur, (AllocatorType*)ia);
        }

        // DeSerialize in BinaryInputStreamBase::Read
        template<typename InputType>
        void Deserialize(BinaryInputStreamBase<InputType>* stream, IncrementalAllocator* m_allocator)
        {
            DoDeserialize(stream, m_allocator);
        }

        // DeSerialize in TextInputStream::Read
        template<typename TextInputStreamTraits, typename InputStreamType>
        void Deserialize(TextInputStream<TextInputStreamTraits, InputStreamType>* stream, FStringWithNull & str, bool lastEmptyColumn)
        {
            if (lastEmptyColumn)
            {
                // create an empty string
                SetEmpty();
                stream->EndRow(false);
                return;
            }

            auto result = TextConversions::TryFStringToT(str, *this);
            if (result != ConvertSuccess)
            {
                if (stream->silent)
                {
                    SetNull();
                }
                else
                {
                    stream->StoreBadConvertException(result, str);
                }
            }

            if (stream->CurrentField() == stream->ColumnExpected())
            {
                stream->EndRow(true);
            }
        }

        // Swap the values
        void Swap(FBinary & )
        {
        }

        // The hash function
        size_t GetHashCode() const
        {
            return 0;
        }

        // FromFString, see scopeio.h
        template<typename StringType>
        ConvertResult FromFString(void* str)
        {
            return ConvertFrom(*(StringType*)str);
        }

        // implemented by base class
        // bool IsNull() const
        // {
        //    return false;
        // }
    using FixedArrayType<unsigned char>::IsNull;

        // implemented by base class
        // For Aggregate_MIN and Aggregate_MAX
        // void SetNull()
        // {
        // }
    using FixedArrayType<unsigned char>::SetNull;

        // Deep copier
        void DeepCopyFrom(FBinary const & , IncrementalAllocator * )
        {
        }

        // Shallow copier
        void ShallowCopyFrom(FBinary const & )
        {
        }

        // Used by Aggregate_UNCHECKED_SUMx2
        FBinary Multiply(FBinary const & ) const
        {
            return FBinary();
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        FBinary & AddByUnchecked(FBinary const & )
        {
            return *this;
        }

        // For Aggregate_SUM: Should check overflow; Should skip if the value is null
        FBinary & AddByChecked(FBinary const & )
        {
            return *this;
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        FBinary & SetZero()
        {
            return *this;
        }

        static FBinary Min()
    {
            return FBinary();
    }

        static FBinary Max()
    {
            return FBinary();
    }

        enum {isNullablePrimaryType=false};
        enum {is_floating_point=false};
        enum {is_signed=false};
        enum {is_unsigned=false};
        enum {is_integral=false};
        enum {need_deep_copy=false};
#endif
#pragma endregion PluginTypeSystem

    private:
        // Takes ownership of other's object buffer
        void MoveFrom(FString & s)
        {
            // Shallow copy
            SetInternalState(s.m_binary);
            s.SetNull();
        }

        static unsigned char CharToHex(unsigned char c)
        {
            if (c >= '0' && c <= '9')
            {
                return c - '0';
            }
            else if (c >= 'A' && c <= 'F')
            {
                return c - 'A' + 10;
            }
            else if (c >= 'a' && c <= 'f')
            {
                return c - 'a' + 10;
            }

            // Invalid hex format for binary
            throw RuntimeException(E_USER_BINARY_CONVERSION, c, 0);
        }
    };

    INLINE ostream & operator<<(ostream & os, const FBinary & fs)
    {
        if (fs.IsNull())
        {
            os << "NULL";
        }
        else
        {
            for(unsigned int i=0; i < fs.size(); i++)
            {
                unsigned char t = fs.buffer()[i];
                unsigned char hi = (t>>4)&0xF;
                unsigned char low = t&0xF;

                os << (hi > 9 ? hi - 10 + 'A' : hi + '0');
                os << (hi > 9 ? low - 10 + 'A' : low + '0');
            }
        }

        return os;
    }

    template<>
    INLINE int ScopeTypeCompare<FString>(const FString & x, const FString & y)
    {
        return x.Compare(y);
    }

    template<>
    INLINE int ScopeTypeCompare<FBinary>(const FBinary & x, const FBinary & y)
    {
        return x.Compare(y);
    }

#pragma region IntermediatePayloadMetadata
    //
    // Intermediate payload metadata representation
    // MetadataID is equal to the the OperatorUID of the operator that "creates" this metadata
    //
    template<typename Schema, int UID>
    class PartitionPayloadMetadata : public PartitionMetadata
    {
        typedef PartitionPayloadMetadata<Schema, UID> MetadataType;

        static const char* const sm_className;
        static const int HAS_SCHEMA_MASK = 0x2;
        static const int HAS_LB_MASK = 0x4;
        static const int HAS_UB_MASK = 0x8;

        __int64 m_partitionId;

        FString m_schemaDef;

        unique_ptr<Schema> m_lb;
        unique_ptr<Schema> m_ub;

        bool m_includeLB;
        bool m_includeUB;

        IncrementalAllocator * m_allocator;

    public:
        PartitionPayloadMetadata(int partitionIdx) : m_partitionId(partitionIdx), m_includeLB(true), m_includeUB(false), m_schemaDef(), m_lb(), m_ub(), m_allocator(NULL)
        {
        }

        PartitionPayloadMetadata(int partitionIdx, Schema * lb, Schema * ub) : m_partitionId(partitionIdx), m_includeLB(true), m_includeUB(false)
        {
            m_allocator = new IncrementalAllocator(Configuration::GetGlobal().GetPartitionPayloadMetadataSize(), sm_className);
            InitForKeyRangePartition(lb, ub, m_allocator);
        }

        PartitionPayloadMetadata(int partitionIdx, Schema * lb, Schema * ub, IncrementalAllocator& alloc) : m_partitionId(partitionIdx), m_includeLB(true), m_includeUB(false), m_allocator(NULL)
        {
            InitForKeyRangePartition(lb, ub, &alloc);
        }

        PartitionPayloadMetadata(BinaryInputStream * stream) : m_includeLB(true), m_includeUB(false), m_schemaDef(), m_lb(), m_ub(), m_allocator(NULL)
        {
            Deserialize(stream);
        }

        PartitionPayloadMetadata(BinaryInputStream * stream, IncrementalAllocator* alloc) : m_includeLB(true), m_includeUB(false), m_schemaDef(), m_lb(), m_ub(), m_allocator(NULL)
        {
            Deserialize(stream, alloc);
        }

        virtual ~PartitionPayloadMetadata()
        {
            if (m_allocator)
            {
                delete m_allocator;
            }
        }

        static void Discard(BinaryInputStream * stream)
        {
            __int64 partitionId;
            stream->Read(partitionId);

            unsigned char flags;
            stream->Read(flags);

            if (flags & HAS_SCHEMA_MASK)
            {
                FString schema;
                stream->Read(schema);

                bool includeLB, includeUB;
                stream->Read(includeLB);
                stream->Read(includeUB);
            }

            Schema partitionRow;

            if (flags & HAS_LB_MASK)
            {
                BinaryExtractPolicy<Schema>::Deserialize(stream, partitionRow);
            }

            if (flags & HAS_UB_MASK)
            {
                BinaryExtractPolicy<Schema>::Deserialize(stream, partitionRow);
            }
        }

        virtual void Serialize(BinaryOutputStream * stream)
        {
            stream->Write(m_partitionId);

            unsigned char flags = 0;

            if (!m_schemaDef.IsNull())
            {
                flags |= HAS_SCHEMA_MASK;
            }

            if (m_lb)
            {
                flags |= HAS_LB_MASK;
            }

            if (m_ub)
            {
                flags |= HAS_UB_MASK;
            }

            stream->Write(flags);

            if (!m_schemaDef.IsNull())
            {
                stream->Write(m_schemaDef);
                stream->Write(m_includeLB);
                stream->Write(m_includeUB);
            }

            if (m_lb)
            {
                BinaryOutputPolicy<Schema>::Serialize(stream, *m_lb.get());
            }

            if (m_ub)
            {
                BinaryOutputPolicy<Schema>::Serialize(stream, *m_ub.get());
            }
        }

        void Deserialize(BinaryInputStream* stream)
        {
            if (!m_allocator)
            {
                m_allocator = new IncrementalAllocator(Configuration::GetGlobal().GetPartitionPayloadMetadataSize(), sm_className);
            }

            Deserialize(stream, m_allocator);
        }

        virtual void Deserialize(BinaryInputStream* stream, IncrementalAllocator* alloc)
        {
            stream->Read(m_partitionId);

            unsigned char flags;
            stream->Read(flags);

            if (flags & HAS_SCHEMA_MASK)
            {
                FString schema;

                stream->Read(schema);
                stream->Read(m_includeLB);
                stream->Read(m_includeUB);

                // make deep copy
                m_schemaDef = FString(schema, alloc);
            }

            Schema partitionRow;

            if (flags & HAS_LB_MASK)
            {
                BinaryExtractPolicy<Schema>::Deserialize(stream, partitionRow);
                m_lb.reset(new Schema(partitionRow, alloc));
            }

            if (flags & HAS_UB_MASK)
            {
                BinaryExtractPolicy<Schema>::Deserialize(stream, partitionRow);
                m_ub.reset(new Schema(partitionRow, alloc));
            }
        }

        virtual __int64 GetPartitionId() const
        {
            return m_partitionId;
        }

        virtual int GetMetadataId() const
        {
            return UID;
        }

        FString& SchemaDef() const
        {
            return m_schemaDef;
        }

        Schema* LowerBound() const
        {
            return m_lb.get();
        }

        Schema* UpperBound() const
        {
            return m_ub.get();
        }

        bool IncludeLower() const
        {
            return m_includeLB;
        }

        bool IncludeUpper() const
        {
            return m_includeUB;
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            if (m_allocator)
            {
                m_allocator->WriteRuntimeStats(node);
            }
        }

        virtual OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements(Configuration::GetGlobal().GetPartitionPayloadMetadataSize(), Configuration::GetGlobal().GetPartitionPayloadMetadataSize());
        }

    private:

        void InitForKeyRangePartition(Schema * lb, Schema * ub, IncrementalAllocator * alloc)
        {
            string schemaDef = Schema::GetDefinition();
            int size = (int)schemaDef.size();

            m_schemaDef.CopyFrom(schemaDef.c_str(), size, alloc);

            m_lb.reset(lb ? new Schema(*lb, alloc) : nullptr);
            m_ub.reset(ub ? new Schema(*ub, alloc) : nullptr);
        }
    };

    template <typename Schema, int UID>
    const char* const PartitionPayloadMetadata<Schema, UID>::sm_className = "PartitionPayloadMetadata";

    //
    // Intermediate payload metadata container
    //
    template<int UID>
    class PartitionMetadataContainer : public PartitionMetadata
    {
        std::vector<unique_ptr<PartitionMetadata>> m_metadata;

    public:

        PartitionMetadataContainer(int partitionCount)
        {
            m_metadata.reserve(partitionCount);
        }

        virtual __int64 GetPartitionId() const
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "GetPartitionId not work for PartitionMetadataContainer!");
        }

        virtual int GetMetadataId() const
        {
            return UID;
        }

        virtual void Serialize(BinaryOutputStream * stream)
        {
            size_t count = m_metadata.size();
            SCOPE_ASSERT(count > 0);

            // Serialize count
            stream->Write(static_cast<__int64>(count));

            // Serialize payload one by one
            for(size_t idx = 0; idx < count; ++idx)
            {
                m_metadata[idx]->Serialize(stream);
            }
        }

        void AddOnePartitionMetadata(unique_ptr<PartitionMetadata> payload)
        {
            m_metadata.push_back(std::move(payload));
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            for (size_t i = 0; i < m_metadata.size(); ++i)
            {
                m_metadata[i]->WriteRuntimeStats(root);
            }
        }

        virtual OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements(2 * Configuration::GetGlobal().GetPartitionPayloadMetadataSize(), 2* Configuration::GetGlobal().GetPartitionPayloadMetadataSize());
        }
    };

    // Partition Key range metadata representation
    // MetadataID is equal to the the OperatorUID of the operator that "creates" this metadata
    // Must be codeGened.
    template<typename Schema, int UID>
    class PartitionKeyRange
    {
    public:
        static void SerializeForSS(MemoryOutputStream* stream, PartitionMetadata* md)
        {
            typedef PartitionPayloadMetadata<Schema, UID> PayloadType;
            typedef BinaryOutputPolicy<Schema> OutputPolicyType;

            auto payload = reinterpret_cast<PayloadType*>(md);

            if (payload->LowerBound())
            {
                stream->Write(x_keyWeightNormal);
                OutputPolicyType::SerializeKeyForSS(stream, *payload->LowerBound());
            }
            else
            {
                stream->Write(x_keyWeightMin);
            }
            stream->Write(payload->IncludeLower());

            if (payload->UpperBound())
            {
                stream->Write(x_keyWeightNormal);
                OutputPolicyType::SerializeKeyForSS(stream, *payload->UpperBound());
            }
            else
            {
                stream->Write(x_keyWeightMax);
            }
            stream->Write(payload->IncludeUpper());
        }
    };

    // dummy template specification for non-exist partition key range.
    // too make compiler happy.
    template<>
    class PartitionKeyRange<void, -1>
    {
    public:
        static void SerializeForSS(MemoryOutputStream* stream, PartitionMetadata* md)
        {
            SCOPE_ASSERT(stream != nullptr);
            SCOPE_ASSERT(md != nullptr);
            SCOPE_ASSERT(false);
        }
    };

#pragma endregion IntermediatePayloadMetadata

    // Define NativeNullable template for NativeNullable data type in native runtime.
    template <typename T>
#ifdef PLUGIN_TYPE_SYSTEM
    class NativeNullable : public PluginTypeBase<NativeNullable<T> >
#else
    class NativeNullable
#endif
    {
    private:
        T value;
        bool hasValue;

    public:
        // types
        typedef NativeNullable<T>       ThisType;
        typedef T                       ValueType;
        typedef T &                     ReferenceType;
        typedef T const &               ReferenceConstType;
        typedef T *                     PointerType;
        typedef T const *               PointerConstType;
        typedef T const &               ArgumentType;

        // constructors
        NativeNullable () : hasValue(false), value(T())
        {
        }

        NativeNullable (const ArgumentType& val) : hasValue(true), value(val)
        {
        }

        NativeNullable(T && val) : hasValue(true), value(std::move(val))
        {
        }

        NativeNullable (const NativeNullable<T>& val) : hasValue(val.hasValue), value(val.value)
        {
        }


        NativeNullable(const ArgumentType& val, IncrementalAllocator * alloc) : hasValue(true), value(val, alloc)
        {
        }

        NativeNullable(const NativeNullable<T>& val, IncrementalAllocator * alloc) : hasValue(val.hasValue), value(val.value, alloc)
        {
        }

#if !defined(SCOPE_NO_UDT)
        template<int UserDefinedTypeId>
        explicit NativeNullable(const ScopeUserDefinedType<UserDefinedTypeId> & val) : hasValue(!val.IsNull()), value(T())
        {
            if (!IsNull())
            {
                this->value = static_cast<T>(val);
            }
        }
#endif // SCOPE_NO_UDT

        template<typename O>
        NativeNullable (const ScopeSqlType::SqlNativeNullable<O>& val) : hasValue(!val.IsNull())
        {
            if (!IsNull())
            {
                this->value = scope_cast<T>(val.get());
            }
        }

        template<typename O>
        NativeNullable (const NativeNullable<O>& val) : hasValue(!val.IsNull())
        {
            if (!IsNull())
            {
                this->value = scope_cast<T>(val.get());
            }
        }

        template<typename O>
        NativeNullable (const O& val) : hasValue(true), value(scope_cast<T>(val))
        {
        }

        NativeNullable (nullptr_t) : hasValue(false), value(T())
        {
        }

        // assignments
        NativeNullable<T> & operator=( ArgumentType val )
        {
            this->hasValue = true;
            this->value = val;
            return *this;
        }

        NativeNullable<T> & operator=(const NativeNullable<T>& rhs )
        {
            this->hasValue = !rhs.IsNull();
            if (!IsNull())
            {
                this->value = rhs.get();
            }
            return *this;
        }

        template<typename U>
        NativeNullable<typename ScopeCommonType<T, U>::type> operator+(const NativeNullable<U>& other) const
        {
            if (IsNull() || other.IsNull())
            {
                return NativeNullable<typename ScopeCommonType<T, U>::type>();
            }

            return NativeNullable<typename ScopeCommonType<T, U>::type>(get() + other.get());
        }

        template<typename U>
        NativeNullable<typename ScopeCommonType<T, U>::type> operator-(const NativeNullable<U>& other) const
        {
            if (IsNull() || other.IsNull())
            {
                return NativeNullable<typename ScopeCommonType<T, U>::type>();
            }

            return NativeNullable<typename ScopeCommonType<T, U>::type>(get() - other.get());
        }

        template<typename U>
        NativeNullable<typename ScopeCommonType<T, U>::type> operator*(const NativeNullable<U>& other) const
        {
            if (IsNull() || other.IsNull())
            {
                return NativeNullable<typename ScopeCommonType<T, U>::type>();
            }

            return NativeNullable<typename ScopeCommonType<T, U>::type>(get() * other.get());
        }

        template<typename U>
        NativeNullable<typename ScopeCommonType<T, U>::type> operator/(const NativeNullable<U>& other) const
        {
            if (IsNull() || other.IsNull())
            {
                return NativeNullable<typename ScopeCommonType<T, U>::type>();
            }

            return NativeNullable<typename ScopeCommonType<T, U>::type>(get() / other.get());
        }

        template<typename U>
        NativeNullable<typename ScopeCommonType<T, U>::type> operator%(const NativeNullable<U>& other) const
        {
            if (IsNull() || other.IsNull())
            {
                return NativeNullable<typename ScopeCommonType<T, U>::type>();
            }

            return NativeNullable<typename ScopeCommonType<T, U>::type>(get() % other.get());
        }

        template<typename U>
        NativeNullable<typename ScopeCommonType<T, U>::type> operator&(const NativeNullable<U>& other) const
        {
            if (IsNull() || other.IsNull())
            {
                return NativeNullable<typename ScopeCommonType<T, U>::type>();
            }

            return NativeNullable<typename ScopeCommonType<T, U>::type>(get() & other.get());
        }

        template<typename U>
        NativeNullable<typename ScopeCommonType<T, U>::type> operator|(const NativeNullable<U>& other) const
        {
            if (IsNull() || other.IsNull())
            {
                return NativeNullable<typename ScopeCommonType<T, U>::type>();
            }

            return NativeNullable<typename ScopeCommonType<T, U>::type>(get() | other.get());
        }

        template<typename U>
        NativeNullable<typename ScopeCommonType<T, U>::type> operator^(const NativeNullable<U>& other) const
        {
            if (IsNull() || other.IsNull())
            {
                return NativeNullable<typename ScopeCommonType<T, U>::type>();
            }

            return NativeNullable<typename ScopeCommonType<T, U>::type>(get() ^ other.get());
        }

        // If one value is null and the other is not, the result is false (unordered compare);
        // if they are both null, return true, otherwise compare the elements.
        template<typename U>
        bool operator==(NativeNullable<U> const& y) const
        {
            return (IsNull() != y.IsNull()) ? false : (IsNull() ? true : ScopeTypeCompare<typename ScopeCommonType<T, U>::type>(get(), y.get()) == 0);
        }

        // If either value is null, the result is false; otherwise compare the elements.
        template<typename U>
        bool operator<(NativeNullable<U> const& y) const
        {
            return ((IsNull() || y.IsNull()) ? false : ScopeTypeCompare<typename ScopeCommonType<T, U>::type>(get(), y.get()) < 0);
        }

        template<typename U>
        bool operator!=(NativeNullable<U> const& y) const
        {
            return !(*this == y);
        }

        template<typename U>
        bool operator>(NativeNullable<U> const& y) const
        {
            return y < *this;
        }

        template<typename U>
        bool operator<=(NativeNullable<U> const& y) const
        {
            return ((IsNull() || y.IsNull()) ? false : ScopeTypeCompare(get(), y.get()) <= 0);
        }

        template<typename U>
        bool operator>=(NativeNullable<U> const& y) const
        {
            return y <= *this;
        }

        NativeNullable<typename ScopeArithmeticType<T>::type> operator+() const
        {
            if(IsNull())
            {
                return NativeNullable<typename ScopeArithmeticType<T>::type>();
            }

            return NativeNullable<typename ScopeArithmeticType<T>::type>(+get());
        }

        NativeNullable<typename ScopeArithmeticType<T>::type> operator-() const
        {
            if(IsNull())
            {
                return NativeNullable<typename ScopeArithmeticType<T>::type>();
            }

            return NativeNullable<typename ScopeArithmeticType<T>::type>(-get());
        }

        NativeNullable<typename ScopeArithmeticType<T>::type> operator~() const
        {
            if(IsNull())
            {
                return NativeNullable<typename ScopeArithmeticType<T>::type>();
            }

            return NativeNullable<typename ScopeArithmeticType<T>::type>(~get());
        }

        NativeNullable<bool> operator!() const
        {
            if(IsNull())
            {
                return NativeNullable<bool>();
            }

            return NativeNullable<bool>(!get());
        }

        NativeNullable<typename ScopeArithmeticType<T>::type> operator<<(int y) const
        {
            if(IsNull())
            {
                return NativeNullable<typename ScopeArithmeticType<T>::type>();
            }

            return NativeNullable<typename ScopeArithmeticType<T>::type>(get() << y);
        }

        NativeNullable<typename ScopeArithmeticType<T>::type> operator>>(int y) const
        {
            if(IsNull())
            {
                return NativeNullable<typename ScopeArithmeticType<T>::type>();
            }

            return NativeNullable<typename ScopeArithmeticType<T>::type>(get() >> y);
        }

        NativeNullable<typename ScopeArithmeticType<T>::type> operator<<(NativeNullable<int> const& y) const
        {
            if(IsNull() || y.IsNull())
            {
                return NativeNullable<typename ScopeArithmeticType<T>::type>();
            }

            return NativeNullable<typename ScopeArithmeticType<T>::type>(get() << y.get());
        }

        NativeNullable<typename ScopeArithmeticType<T>::type> operator>>(NativeNullable<int> const& y) const
        {
            if(IsNull() || y.IsNull())
            {
                return NativeNullable<typename ScopeArithmeticType<T>::type>();
            }

            return NativeNullable<typename ScopeArithmeticType<T>::type>(get() >> y.get());
        }

        NativeNullable<T>& operator+=(NativeNullable<T> const& y)
        {
            if (!IsNull() && !y.IsNull())
            {
                value += y.get();
            }

            return *this;
        }

        NativeNullable<T>& operator++()
        {
            if (!IsNull())
            {
                value += 1;
            }

            return *this;
        }

        NativeNullable<T>& operator--()
        {
            if (!IsNull())
            {
                value -= 1;
            }

            return *this;
        }

        // accessors
        ReferenceConstType get() const
        {
            return this->value;
        }

        ReferenceType get()
        {
            return this->value;
        }

        ArgumentType safe_get() const
        {
            if (IsNull())
            {
                std::stringstream ss;
                ss << "Nullable<" << typeid(T).name() << "> columns don't have a value when they are NULL";
                throw RuntimeException(E_USER_NULL_VALUE, ss.str().c_str());
            }

            return this->value;
        }

        ReferenceConstType operator*()  const
        {
            return this->value;
        }

        ReferenceType operator*()
        {
            return this->value;
        }

        bool empty() const
        {
            return !this->hasValue;
        }

        void reset()
        {
            this->hasValue = false;
        }

        void reset( ArgumentType val )
        {
            *this = val;
        }

        // test & set NULL
        bool IsNull() const
        {
            return empty();
        }

        void SetNull()
        {
            reset();
        }

        void ClearNull()
        {
            this->hasValue = true;
        }

        int GetScopeHashCode() const
        {
            if (empty())
            {
                return x_NULLHASH;
            }

            return ScopeHash<T>(get());
        }

        unsigned __int64 GetCRC32Hash(unsigned __int64 crc) const
        {
            return empty() ? CRC32Hash(crc, x_NULLHASH) : CRC32Hash(crc, get());
        }

        size_t GetStdHashCode() const
        {
            if (empty())
            {
                return x_NULLHASH;
            }

            return std::tr1::hash<T>()(this->value);
        }

#pragma region PluginTypeSystem
#ifdef PLUGIN_TYPE_SYSTEM
        int Compare(const NativeNullable & ) const
        {
            return 0;
        }

        // Serialize in ScopeMapOutputMemoryStream::Write
        template<typename BufferType>
        void ScopeMapOutputMemoryStreamSerialize(void* buffer) const
        {
            ((BufferType*)buffer)->Write((const char*)&(get()), sizeof(T));
        }

        // Serialize in BinaryOutputStreamBase::Write
        template<typename OutputType>
        void Serialize(BinaryOutputStreamBase<OutputType>* stream) const
        {
            // we will never call write a null value in binary outputer
            SCOPE_ASSERT(!IsNull());
            stream->Write(get());
        }

        // Serialize in, ScopeIO.h TextOutputStream::Write
        template<typename TextOutputStreamTraits>
        void Serialize(TextOutputStream<TextOutputStreamTraits>* stream) const
        {
            if(IsNull())
            {
                // handle null value
                if (stream->escape || stream->nullEscape != nullptr)
                {
                    stream->WriteNull();
                }

                return;
            }

            stream->Write(get());
        }

        // Serialize in SStreamDataOutputStream::Write
        template<typename BufferType>
        void SStreamDataOutputSerialize(void* buffer) const
        {
            // we will never call write a null value in sstream outputer
            SCOPE_ASSERT(!IsNull());

            ((BufferType*)buffer)->Write((const char*)&(get()), sizeof(T));
        }

        // DeSerialize in ScopeMapInputMemoryStream::Read
        template<typename AllocatorType>
        void ScopeMapInputMemoryStreamDeserialize(const char* & cur, void* ia)
        {
            memcpy((char*)(&value), cur, sizeof(T));
            cur += sizeof(T);
            ClearNull();
        }

        // DeSerialize in BinaryInputStreamBase::Read
        template<typename InputType>
        void Deserialize(BinaryInputStreamBase<InputType>* stream, IncrementalAllocator* )
        {
            stream->Read(value);
            ClearNull();
        }

        // DeSerialize in TextInputStream::Read
        template<typename TextInputStreamTraits, typename InputStreamType>
        void Deserialize(TextInputStream<TextInputStreamTraits, InputStreamType>* stream, FStringWithNull & str, bool lastEmptyColumn)
        {
            if (lastEmptyColumn)
            {
                SetNull();
                stream->EndRow(false);
                return;
            }

            auto result = TextConversions::TryFStringToT(str, *this);
            if (result != ConvertSuccess)
            {
                if (stream->silent)
                {
                    SetNull();
                }
                else
                {
                    stream->StoreBadConvertException(result, str);
                }
            }

            if (stream->CurrentField() == stream->ColumnExpected())
            {
                stream->EndRow(true);
            }
        }

        // Swap the values
        void Swap(NativeNullable & )
        {
        }

        // The hash function
        size_t GetHashCode() const
        {
            return 0;
        }

        //These two methods are needed by plugin type system but are already implemented
        //bool IsNull() const
        //{
        //    return empty();
        //}

        //void SetNull()
        //{
        //    reset();
        //}

        // FromFString, see scopeio.h
        template<typename StringType>
        ConvertResult FromFString(void* str)
        {
            StringType* s = (StringType*)str;
            // Null or Empty string is treated as null for NativeNullable<T> type.
            if (s->IsNullOrEmpty())
            {
                SetNull();
                return ConvertSuccess;
            }

            ConvertResult result = TextConversions::TryFStringToT(*s, get());

            if(result == ConvertSuccess)
            {
                ClearNull();
            }

            return result;
        }

        // Deep copier
        void DeepCopyFrom(NativeNullable const & , IncrementalAllocator * )
        {
        }

        // Shallow copier
        void ShallowCopyFrom(NativeNullable const & )
        {
        }

        // Used by Aggregate_UNCHECKED_SUMx2
        NativeNullable Multiply(NativeNullable const & ) const
        {
            return NativeNullable();
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        NativeNullable & AddByUnchecked(NativeNullable const & )
        {
            return *this;
        }

        // For Aggregate_SUM: Should check overflow; Should skip if the value is null
        NativeNullable & AddByChecked(NativeNullable const & )
        {
            return *this;
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        NativeNullable & SetZero()
        {
            return *this;
        }

        static NativeNullable Min()
    {
            return NativeNullable();
    }

        static NativeNullable Max()
    {
            return NativeNullable();
    }

        enum {isNullablePrimaryType=true};
        enum {is_floating_point=true};
        enum {is_signed=true};
        enum {is_unsigned=true};
        enum {is_integral=true};
        enum {need_deep_copy=false};
#endif
#pragma endregion PluginTypeSystem

    };

    INLINE ostream &operator<<(ostream &o, const ScopeDateTime &t)
    {
        char buffer [80];
        t.ToString(buffer, 80);
        o << buffer;
        return o;
    }

    // System.Guid
#ifdef PLUGIN_TYPE_SYSTEM
    class ScopeGuid : public PluginTypeBase<ScopeGuid>
#else
    class ScopeGuid
#endif
    {
    private:
        GUID m_guid;

    public:
        const static int x_lengthOfStringFormat = 36;

        ScopeGuid()
        {
            memset(&m_guid, 0, sizeof(GUID));
        }

        void Reset()
        {
            memset(&m_guid, 0, sizeof(GUID));
        }

        ScopeGuid(const ScopeGuid & o)
        {
            m_guid = o.m_guid;
        }

        ScopeGuid & operator= (ScopeGuid const& o)
        {
            m_guid = o.m_guid;
            return *this;
        }

        bool operator == (const ScopeGuid & o) const
        {
            return InlineIsEqualGUID(m_guid, o.m_guid) != 0;
        }

        bool operator != (const ScopeGuid & o) const
        {
            return !(*this == o);
        }

        bool operator < (const ScopeGuid & o) const
        {
            if (m_guid.Data1 != o.m_guid.Data1)
            {
                return m_guid.Data1 < o.m_guid.Data1;
            }
            else if (m_guid.Data2 != o.m_guid.Data2)
            {
                return m_guid.Data2 < o.m_guid.Data2;
            }
            else if (m_guid.Data3 != o.m_guid.Data3)
            {
                return m_guid.Data3 < o.m_guid.Data3;
            }
            else
                return memcmp(&(m_guid.Data4), &(o.m_guid.Data4), sizeof(m_guid.Data4)) < 0;
        }

        bool operator > (const ScopeGuid & o) const
        {
            if (m_guid.Data1 != o.m_guid.Data1)
            {
                return m_guid.Data1 > o.m_guid.Data1;
            }
            else if (m_guid.Data2 != o.m_guid.Data2)
            {
                return m_guid.Data2 > o.m_guid.Data2;
            }
            else if (m_guid.Data3 != o.m_guid.Data3)
            {
                return m_guid.Data3 > o.m_guid.Data3;
            }
            else
                return memcmp(&(m_guid.Data4), &(o.m_guid.Data4), sizeof(m_guid.Data4)) > 0;
        }

        bool operator <= (const ScopeGuid & o) const
        {
            return !(*this > o);
        }

        bool operator >= (const ScopeGuid & o) const
        {
            return !(*this < o);
        }

        const GUID & get() const
        {
            return m_guid;
        }

        // compute 32 bit hash for ScopeGuid
        int GetScopeHashCode() const
        {
            return ((m_guid.Data1 ^ ((m_guid.Data2 << 0x10) | m_guid.Data3)) ^ ((m_guid.Data4[2] << 0x18) | m_guid.Data4[7]));
        }

        unsigned __int64 GetCRC32Hash(unsigned __int64 crc) const
        {
            unsigned __int64 result = crc;
            result = CRC32Hash<int>(result, m_guid.Data1);
            result = CRC32Hash<short>(result, m_guid.Data2);
            result = CRC32Hash<short>(result, m_guid.Data3);
            result = CRC32Hash<unsigned __int64>(result, *((unsigned __int64*)(m_guid.Data4)));

            return result;
        }

        void CopyFrom(const unsigned char * ptr)
        {
            memcpy(&m_guid, ptr, sizeof(GUID));
        }

        static bool TryParse(const char * str, unsigned int size, ScopeGuid & guid)
        {
            if (!GuidFromString(str, size, guid.m_guid))
            {
                guid.Reset();
                return false;
            }

            return true;
        }

        static ScopeGuid Parse(const FString & s)
        {
            ScopeGuid sg;

            if (!TryParse(s.buffer(), s.size(), sg))
            {
                throw RuntimeException(E_USER_INVALID_GUID, std::string(s.buffer(), s.size()));
            }

            return sg;
        }

#pragma region PluginTypeSystem
#ifdef PLUGIN_TYPE_SYSTEM
        int Compare(const ScopeGuid & ) const
        {
            return 0;
        }

        // Serialize in ScopeMapOutputMemoryStream::Write
        template<typename BufferType>
        void ScopeMapOutputMemoryStreamSerialize(void* buffer) const
        {
            ((BufferType*)buffer)->Write((const char*)this, sizeof(*this));
        }

        // Serialize in BinaryOutputStreamBase::Write
        template<typename OutputType>
        void Serialize(BinaryOutputStreamBase<OutputType>* stream) const
        {
            stream->Write((const char *)this, sizeof(ScopeGuid));
        }

        // Serialize in, ScopeIO.h TextOutputStream::Write
        template<typename TextOutputStreamTraits>
        void Serialize(TextOutputStream<TextOutputStreamTraits>* stream) const
        {
            string str = ScopeEngine::GuidToString(get());

            if (!str.empty())
            {
                stream->WriteString<false>(str.c_str(), str.length());
            }
        }

        // Serialize in SStreamDataOutputStream::Write
        template<typename BufferType>
        void SStreamDataOutputSerialize(void* buffer) const
        {
            ((BufferType*)buffer)->Write(reinterpret_cast<char *>(const_cast<ScopeGuid *>(this)), sizeof(ScopeGuid));
        }

        // DeSerialize in ScopeMapInputMemoryStream::Read
        template<typename AllocatorType>
        void ScopeMapInputMemoryStreamDeserialize(const char* & cur, void* ia)
        {
            memcpy((char*)this, cur, sizeof(ScopeGuid));
            cur += sizeof(ScopeGuid);
        }

        // DeSerialize in BinaryInputStreamBase::Read
        template<typename InputType>
        void Deserialize(BinaryInputStreamBase<InputType>* stream, IncrementalAllocator* )
        {
            if (stream->Read(reinterpret_cast<char *>(this), sizeof(*this)) != sizeof(*this))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        // DeSerialize in TextInputStream::Read
        template<typename TextInputStreamTraits, typename InputStreamType>
        void Deserialize(TextInputStream<TextInputStreamTraits, InputStreamType>* stream, FStringWithNull & str, bool lastEmptyColumn)
        {
            stream->DoFStringToT(str, *this, lastEmptyColumn);
        }

        // Swap the values
        void Swap(ScopeGuid & )
        {
        }

        // The hash function
        size_t GetHashCode() const
        {
            return 0;
        }

        // FromFString, see scopeio.h
        template<typename StringType>
        ConvertResult FromFString(void* str)
        {
            StringType* s = (StringType*)str;
            s->TrimWhiteSpace();
            return TryParse(s->buffer(), s->DataSize(), *this) ? ConvertSuccess : ConvertErrorUndefined;
        }

        // IsNull is conflict with ScopeGuid::IsNull, so we must use a new name IsNull. We should switch back after runtime switching to Plugin Type System
        bool IsNull() const
        {
            return false;
        }

        // For Aggregate_MIN and Aggregate_MAX
        void SetNull()
        {
        }

        // Deep copier
        void DeepCopyFrom(ScopeGuid const & , IncrementalAllocator * )
        {
        }

        // Shallow copier
        void ShallowCopyFrom(ScopeGuid const & )
        {
        }

        // Used by Aggregate_UNCHECKED_SUMx2
        ScopeGuid Multiply(ScopeGuid const & ) const
        {
            return ScopeGuid();
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        ScopeGuid & AddByUnchecked(ScopeGuid const & )
        {
            return *this;
        }

        // For Aggregate_SUM: Should check overflow; Should skip if the value is null
        ScopeGuid & AddByChecked(ScopeGuid const & )
        {
            return *this;
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        ScopeGuid & SetZero()
        {
            return *this;
        }

        static ScopeGuid Min()
    {
            return ScopeGuid();
    }

        static ScopeGuid Max()
    {
            return ScopeGuid();
    }

        enum {isNullablePrimaryType=false};
        enum {is_floating_point=false};
        enum {is_signed=false};
        enum {is_unsigned=false};
        enum {is_integral=false};
        enum {need_deep_copy=false};
#endif
#pragma endregion PluginTypeSystem

    };

    INLINE ostream &operator<<(ostream &o, const ScopeGuid &t)
    {
        o << ScopeEngine::GuidToString(t.get());

        return o;
    }

    // 128 bits integer
    class ScopeInt128
    {
        UINT64 m_hi;
        UINT64 m_lo;

    public:
        ScopeInt128() : m_hi(0), m_lo(0)
        {
        }

        ScopeInt128(const ScopeInt128 & c) : m_hi(c.m_hi), m_lo(c.m_lo)
        {
        }

        ScopeInt128(const ULONG hi, const ULONG mid, const ULONG lo)
        {
            m_lo = ((UINT64)(mid) << 32) | lo;
            m_hi = hi;
        }

        ScopeInt128(const UINT64 hi, const UINT64 lo)
        {
            m_lo = lo;
            m_hi = hi;
        }

        void Reset(const ULONG hi, const ULONG mid, const ULONG lo)
        {
            m_lo = ((UINT64)(mid) << 32) | lo;
            m_hi = hi;
        }

        UINT64 Hi64Bit() const
        {
            return m_hi;
        }

        UINT64 Lo64Bit() const
        {
            return m_lo;
        }

        ULONG Hi32Bit() const
        {
            return (ULONG)(m_hi & 0xFFFFFFFF);
        }

        ULONG Mi32Bit() const
        {
            return (ULONG)((m_lo >> 32) & 0xFFFFFFFF);
        }

        ULONG Lo32Bit() const
        {
            return (ULONG)(m_lo & 0xFFFFFFFF);
        }

        bool IsZero()
        {
            return (m_hi == 0 && m_lo == 0);
        }

        ScopeInt128 & operator= ( ScopeInt128 const& rhs )
        {
            this->m_hi = rhs.m_hi;
            this->m_lo = rhs.m_lo;
            return *this;
        }

        FORCE_INLINE ScopeInt128 & operator+=(const ScopeInt128 & rhs)
        {
            m_lo += rhs.m_lo;

            if (m_lo < rhs.m_lo)
                m_hi++; /* carry */

            m_hi += rhs.m_hi;

            return (*this);
        }

        FORCE_INLINE ScopeInt128 & operator -=(const ScopeInt128 & rhs)
        {
            bool borrow = m_lo < rhs.m_lo;

            m_lo -= rhs.m_lo;
            m_hi -= rhs.m_hi;

            /* borrow */
            if (borrow)
                m_hi--;

            return (*this);
        }

        const ScopeInt128 operator+(const ScopeInt128 & rhs) const
        {
            ScopeInt128 result = *this;

            result += rhs;            // Use += to add other to the copy.
            return result;                // All done!
        }

        const ScopeInt128 operator-(const ScopeInt128 & rhs) const
        {
            ScopeInt128 result = *this;

            result -= rhs;
            return result;
        }

        FORCE_INLINE bool NegComplement()
        {
            if (m_hi & UINT64_HIGHBIT)
            {
                m_lo--;
                m_lo = ~m_lo;

                if (m_lo == 0)
                    m_hi--;

                m_hi = ~m_hi;

                return true;
            }

            return false;
        }

        void RoundUp128()
        {
            if (++m_lo == 0)
                ++m_hi;
        }

        /* division: x(128bit) /= factor(32bit)
           returns roundBit */
        FORCE_INLINE int Div128by32(ULONG factor, ULONG * pRest)
        {
            UINT64 a, b, c, h;

            h = m_hi;
            a = (ULONG)(h >> 32);
            b = a / factor;
            a -= b * factor;
            a <<= 32;
            a |= (ULONG) h;
            c = a / factor;
            a -= c * factor;
            a <<= 32;
            m_hi = b << 32 | (ULONG)c;

            h = m_lo;
            a |= (ULONG)(h >> 32);
            b = a / factor;
            a -= b * factor;
            a <<= 32;
            a |= (ULONG) h;
            c = a / factor;
            a -= c * factor;
            m_lo = b << 32 | (ULONG)c;

            if (pRest)
                *pRest = (ULONG) a;

            a <<= 1;
            return (a >= factor && (a > factor || (m_lo & 1) == 1)) ? 1 : 0;
     }

        /* multiplication c(128bit) *= b(32bit) */
        FORCE_INLINE int Mult128by32(ULONG factor, int roundBit)
        {
            UINT64 a;
            ULONG h0, h1;

            a = ((UINT64)(ULONG)(m_lo)) * factor;
            if (roundBit)
                a += factor / 2;
            h0 = (ULONG) a;

            a >>= 32;
            a += (m_lo >> 32) * factor;
            h1 = (ULONG) a;

            m_lo = ((UINT64)h1) << 32 | h0;

            a >>= 32;
            a += ((UINT64)(ULONG)(m_hi)) * factor;
            h0 = (ULONG) a;

            a >>= 32;
            a += (m_hi >> 32) * factor;
            h1 = (ULONG) a;

            m_hi = ((UINT64)h1) << 32 | h0;

            return ((a >> 32) == 0) ? DECIMAL_SUCCESS : DECIMAL_OVERFLOW;
        }

        FORCE_INLINE int Normalize128(int& scale, int roundFlag, int roundBit)
        {
            ULONG overhang = (ULONG)(m_hi >> 32);
            int deltaScale;

            while (overhang != 0)
            {
                for (deltaScale = 1; deltaScale < DECIMAL_MAX_INTFACTORS; deltaScale++)
                {
                    if (overhang < constantsDecadeInt32Factors[deltaScale])
                        break;
                }

                scale -= deltaScale;
                if (scale < 0)
                    return DECIMAL_OVERFLOW;

                roundBit = Div128by32(constantsDecadeInt32Factors[deltaScale], NULL);

                overhang = (ULONG)(m_hi >> 32);
                if (roundFlag && roundBit && m_lo == (UINT64)-1 && (int)m_hi == (int)-1)
                {
                    overhang = 1;
                }
            }

            if (roundFlag && roundBit) {
                RoundUp128();
            }

            return DECIMAL_SUCCESS;
        }


        /* returns log2(a) or DECIMAL_LOG_NEGINF for a = 0 */
        int Log2_128()
        {
            if (m_hi == 0)
                return Log2_64(m_lo);
            else
                return Log2_64(m_hi) + 64;
        }

        /* returns a upper limit for log2(a) considering scale */
        FORCE_INLINE int Log2WithScale_128(int scale)
        {
            int tlog2 = Log2_128();
            if (tlog2 < 0)
                tlog2 = 0;
            return tlog2 - (scale * 33219) / 10000;
        }

        FORCE_INLINE int AdjustScale128(int deltaScale)
        {
            int idx, rc;

            if (deltaScale < 0)
            {
                deltaScale *= -1;
                if (deltaScale > DECIMAL_MAX_SCALE)
                    return DECIMAL_INTERNAL_ERROR;

                while (deltaScale > 0)
                {
                    idx = (deltaScale > DECIMAL_MAX_INTFACTORS) ? DECIMAL_MAX_INTFACTORS : deltaScale;
                    deltaScale -= idx;
                    Div128by32(constantsDecadeInt32Factors[idx], 0);
                }
            }
            else if (deltaScale > 0)
            {
                if (deltaScale > DECIMAL_MAX_SCALE)
                    return DECIMAL_INTERNAL_ERROR;

                while (deltaScale > 0)
                {
                    idx = (deltaScale > DECIMAL_MAX_INTFACTORS) ? DECIMAL_MAX_INTFACTORS : deltaScale;
                    deltaScale -= idx;

                    rc = Mult128by32(constantsDecadeInt32Factors[idx], 0);
                    if (rc != DECIMAL_SUCCESS)
                        return rc;
                }
            }

            return DECIMAL_SUCCESS;
        }

        void Rshift128()
        {
            m_lo >>= 1;
            m_lo |= (m_hi & 1) << 63;
            m_hi >>= 1;
        }

        void Lshift128()
        {
            Lshift128(&m_hi, &m_lo);
        }

        static void Lshift128(UINT64 * pchi, UINT64* pclo)
        {
            *pchi <<= 1;
            *pchi |= (*pclo & UINT64_HIGHBIT) >> 63;
            *pclo <<= 1;
        }


        //   input: c * 10^-(*pScale) * 2^-exp
        //   output: c * 10^-(*pScale) with
        //   minScale <= *pScale <= maxScale and (chi >> 32) == 0
        FORCE_INLINE int Rescale128(int & scale, int texp, int minScale, int maxScale, int roundFlag)
        {
            ULONG factor, overhang;
            int i, rc, roundBit = 0;

            SCOPE_ASSERT(texp >= 0);

            if (texp > 0)
            {
                /* reduce exp */
                while (texp > 0 && scale <= maxScale)
                {
                    overhang = (ULONG)(m_hi >> 32);

                    if (overhang > 0)
                    {
                        int msf = BitNthMsf(overhang);
                        int shift = msf - (DECIMAL_MAX_INTFACTORS + 2);

                        if (shift >= texp)
                        {
                            shift = texp - 1;
                        }

                        if (shift > 0)
                        {
                            texp -= shift;
                            m_lo = (m_lo >> shift) | ((m_hi & ((1 << shift) - 1)) << (64 - shift));
                            m_hi >>= shift;
                            overhang >>= shift;

                            SCOPE_ASSERT(texp > 0);
                            SCOPE_ASSERT(overhang > (2 << DECIMAL_MAX_INTFACTORS));
                        }
                    }

                    while (texp > 0 && (overhang > (2<<DECIMAL_MAX_INTFACTORS) || (m_lo & 1) == 0))
                    {
                        if (--texp == 0)
                        {
                            roundBit = (int)(m_lo & 1);
                        }

                        Rshift128();

                        overhang >>= 1;
                    }

                    if (texp > DECIMAL_MAX_INTFACTORS)
                    {
                        i = DECIMAL_MAX_INTFACTORS;
                    }
                    else
                    {
                        i = texp;
                    }

                    if (scale + i > maxScale)
                    {
                        i = maxScale - scale;
                    }

                    if (i == 0)
                    {
                        break;
                    }

                    texp -= i;
                    scale += i;
                    factor = constantsDecadeInt32Factors[i] >> i; /* 10^i/2^i=5^i */
                    Mult128by32(factor, 0);
                }

                while (texp > 0)
                {
                    if (--texp == 0)
                    {
                        roundBit = (int)(m_lo & 1);
                    }

                    Rshift128();
                }
            }

            SCOPE_ASSERT(texp == 0);

            while (scale > maxScale)
            {
                i = scale - maxScale;
                if (i > DECIMAL_MAX_INTFACTORS)
                    i = DECIMAL_MAX_INTFACTORS;

                scale -= i;
                roundBit = Div128by32(constantsDecadeInt32Factors[i], 0);
            }

            while (scale < minScale)
            {
                if (!roundFlag)
                    roundBit = 0;

                i = minScale - scale;

                if (i > DECIMAL_MAX_INTFACTORS)
                    i = DECIMAL_MAX_INTFACTORS;

                scale += i;

                rc = Mult128by32(constantsDecadeInt32Factors[i], roundBit);

                if (rc != DECIMAL_SUCCESS)
                    return rc;

                roundBit = 0;
            }

            SCOPE_ASSERT(scale >= 0 && scale <= DECIMAL_MAX_SCALE);

            return Normalize128(scale, roundFlag, roundBit);
        }

        /* performs a += factor * constants[idx] */
        FORCE_INLINE int IncMultConstant128(int idx, int factor)
        {
            ScopeInt128 b;
            UINT64 h;

            SCOPE_ASSERT(idx >= 0 && idx <= DECIMAL_MAX_SCALE);
            SCOPE_ASSERT(factor > 0 && factor <= 9);

            b.m_lo = dec128decadeFactors[idx].lo;
            h = b.m_hi = dec128decadeFactors[idx].hi;
            if (factor != 1)
            {
                b.Mult128by32(factor, 0);
                if (h > b.m_hi)
                    return DECIMAL_OVERFLOW;
            }

            h = m_hi;
            *this += b;
            if (h > m_hi)
                return DECIMAL_OVERFLOW;

            return DECIMAL_SUCCESS;
        }

        /* calc significant digits of mantisse */
        FORCE_INLINE int CalcDigits()
        {
            int tlog2 = 0;
            int tlog10;

            if (m_hi == 0)
            {
                if (m_lo == 0)
                {
                    return 0; /* zero has no signficant digits */
                }
                else
                {
                    tlog2 = Log2_64(m_lo);
                }
            }
            else
            {
                tlog2 = 64 + Log2_64(m_hi);
            }

            tlog10 = (tlog2 * 1000) / 3322;

            /* we need an exact floor value of log10(a) */
            if (dec128decadeFactors[tlog10].hi > m_hi
                || (dec128decadeFactors[tlog10].hi == m_hi &&
                    dec128decadeFactors[tlog10].lo > m_lo)
                )
            {
                --tlog10;
            }

            return tlog10+1;
        }

        FORCE_INLINE void Div128DecadeFactor(int powerOfTen)
        {
            int idx, roundBit = 0;

            while (powerOfTen > 0)
            {
                idx = (powerOfTen > DECIMAL_MAX_INTFACTORS) ? DECIMAL_MAX_INTFACTORS : powerOfTen;
                powerOfTen -= idx;
                roundBit = Div128by32(constantsDecadeInt32Factors[idx], 0);
            }

            if (roundBit)
                RoundUp128();
        }

        int Mult128DecadeFactor(int powerOfTen)
        {
            int idx, rc;

            while (powerOfTen > 0)
            {
                idx = (powerOfTen >= DECIMAL_MAX_INTFACTORS) ? DECIMAL_MAX_INTFACTORS : powerOfTen;
                powerOfTen -= idx;
                rc = Mult128by32(constantsDecadeInt32Factors[idx], 0);
                if (rc != DECIMAL_SUCCESS)
                    return rc;
            }

            return DECIMAL_SUCCESS;
        }

        /* multiplication c(128bit) = a(96bit) * b(32bit) */
        void Mult96by32to128(ULONG factor)
        {
            UINT64 a;
            ULONG h0, h1;

            a = ((UINT64)Lo32Bit()) * factor;
            h0 = (ULONG) a;

            a >>= 32;
            a += ((UINT64)Mi32Bit()) * factor;
            h1 = (ULONG) a;

            a >>= 32;
            a += ((UINT64)Hi32Bit()) * factor;

            m_lo = ((UINT64)h1) << 32 | h0;
            m_hi = a;
        }

        void TrimExcessScale(int* pScale)
        {
            UINT64 lastlo;
            UINT64 lasthi;
            int scale = *pScale;
            int i = 0, roundBit;
            ULONG rest = 0;

            while (scale > 0)
            {
                scale--;
                i++;
                lastlo = m_lo;
                lasthi = m_hi;

                roundBit = Div128by32(10, &rest);
                if (rest != 0)
                {
                    i--;

                    // restore original value
                    m_lo = lastlo;
                    m_hi = lasthi;

                    if (i == 0)
                    {
                        return;
                    }

                    *pScale = scale+1;
                    return;
                }
            }

            // All of the scale digits were zeros
            *pScale = 0;
        }
    };

    // 192 bits integer
    class ScopeInt192
    {
        UINT64 m_hi;
        UINT64 m_mi;
        UINT64 m_lo;

    public:
        ScopeInt192() :    m_hi(0), m_mi(0), m_lo(0)
        {
        }

        ScopeInt192(const ScopeInt192 & c) : m_hi(c.m_hi), m_mi(c.m_mi), m_lo(c.m_lo)
        {
        }

        ScopeInt192(const UINT64 hi, const UINT64 mid, const UINT64 lo)
        {
            m_lo = lo;
            m_mi = mid;
            m_hi = hi;
        }

        UINT64 Hi64Bit() const
        {
            return m_hi;
        }

        UINT64 Mi64Bit() const
        {
            return m_mi;
        }

        UINT64 Lo64Bit() const
        {
            return m_lo;
        }

        bool IsZero()
        {
            return (m_hi == 0 && m_mi == 0 && m_lo == 0);
        }

        // division: x(192bit) /= factor(32bit)
        void Divby32(ULONG factor)
        {
            UINT64 a, b, c, h;

            h = m_hi;
            a = (ULONG)(h >> 32);
            b = a / factor;
            a -= b * factor;
            a <<= 32;
            a |= (ULONG) h;
            c = a / factor;
            a -= c * factor;
            a <<= 32;
            m_hi = b << 32 | (ULONG)c;

            h = m_mi;
            a |= (ULONG)(h >> 32);
            b = a / factor;
            a -= b * factor;
            a <<= 32;
            a |= (ULONG) h;
            c = a / factor;
            a -= c * factor;
            a <<= 32;
            m_mi = b << 32 | (ULONG)c;

            h = m_lo;
            a |= (ULONG)(h >> 32);
            b = a / factor;
            a -= b * factor;
            a <<= 32;
            a |= (ULONG) h;
            c = a / factor;
            a -= c * factor;
            a <<= 32;
            m_lo = b << 32 | (ULONG)c;
        }

        /* 192 bit subtraction: c = a - b
           subtraction is modulo 2**192, any carry is lost */
        void Sub192(const ScopeInt192 & other)
        {
            UINT64 clo, cmi, chi;

            clo = Lo64Bit() - other.Lo64Bit();
            cmi = Mi64Bit() - other.Mi64Bit();
            chi = Hi64Bit() - other.Hi64Bit();
            if (Lo64Bit() < other.Lo64Bit())
            {
                if (cmi == 0)
                {
                    chi--; /* borrow mid */
                }

                cmi--; /* borrow low */
            }

            if (Mi64Bit() < other.Mi64Bit())
            {
                chi--; /* borrow mid */
            }

            m_lo = clo;
            m_mi = cmi;
            m_hi = chi;
        }

        // 192 bit addition: c = a + b
        //   addition is modulo 2**192, any carry is lost
        void Add192(const ScopeInt192 & other)
        {
            m_lo += other.Lo64Bit();
            if (m_lo < other.Lo64Bit())
            {
                /* carry low */
                m_mi++;
                if (m_mi == 0)
                {
                    m_hi++; /* carry mid */
                }
            }

            m_mi += other.Mi64Bit();
            if (m_mi < other.Mi64Bit())
            {
                m_hi++; /* carry mid */
            }

            m_hi += other.Hi64Bit();
        }

        // returns upper 32bit for a(192bit) /= b(32bit)
        //   a will contain remainder
        ULONG Div192by96to32withRest(const ScopeInt128 & other)
        {
            ULONG c;

            if (Hi64Bit() >= (((UINT64)(other.Hi32Bit())) << 32))
            {
                c = 0xFFFFFFFF;
            }
            else
            {
                c = (ULONG) (Hi64Bit() / other.Hi32Bit());
            }

            ScopeInt128 b(other);
            b.Mult96by32to128(c);

            ScopeInt192 t(b.Hi64Bit(), b.Lo64Bit(), 0);

            Sub192(t);

            while (((__int64)Hi64Bit()) < 0)
            {
                c--;
                ScopeInt192 n(other.Hi64Bit(), (((UINT64)other.Mi32Bit())<<32) | other.Lo64Bit(), 0);
                Add192(n);
            }

            return c;
        }

        // c(128bit) = a(192bit) / b(96bit)
        // b must be >= 2^95
        ScopeInt128 Div192by96to128(const ScopeInt128 & other)
        {
            ScopeInt192 r; // remainder
            ULONG h, c;
            UINT64 hiBit, loBit;

            /* high 32 bit*/
            r = *this;
            h = r.Div192by96to32withRest(other);

            /* mid 32 bit*/
            ScopeInt192 m((r.Hi64Bit()<< 32) | (r.Mi64Bit()>> 32), (r.Mi64Bit() << 32) | (r.Lo64Bit() >> 32), r.Lo64Bit()<<32);


            hiBit = (((UINT64)h) << 32) | m.Div192by96to32withRest(other);

            /* low 32 bit */
            ScopeInt192 l((m.Hi64Bit()<< 32) | (m.Mi64Bit()>> 32), (m.Mi64Bit() << 32) | (m.Lo64Bit() >> 32), m.Lo64Bit()<<32);
            h = l.Div192by96to32withRest(other);

            /* estimate lowest 32 bit (two last bits may be wrong) */
            if (l.Hi64Bit() >= other.Hi32Bit())
            {
                c = 0xFFFFFFFF;
            }
            else
            {
                c = (ULONG) ((l.Hi64Bit()<<32) / other.Hi32Bit());
            }

            loBit = (((UINT64)h) << 32) | c;

            return ScopeInt128(hiBit, loBit);

        }


        // Divide 192 interger with 128 interger. Result is stored in out.
        int DecimalDivSub(ScopeInt128 & other, ScopeInt128 & out, int * pExp)
        {
            int ashift, bshift, extraBit, texp;

            if (other.IsZero())
            {
                return DECIMAL_DIVIDE_BY_ZERO;
            }

            if (IsZero())
            {
                out.Reset(0, 0, 0);
                return DECIMAL_FINISHED;
            }

            /* enlarge dividend to get maximal precision */
            if (m_hi == 0)
            {
                m_hi = m_mi;
                m_mi = 0;
                for (ashift = 64; (m_hi & UINT64_HIGHBIT) == 0; ++ashift)
                {
                    m_hi <<= 1;
                }
            }
            else
            {
                for (ashift = 0; (m_hi & UINT64_HIGHBIT) == 0; ++ashift)
                {
                    ScopeInt128::Lshift128(&m_hi, &m_mi);
                }
            }

            ULONG blo, bmi, bhi;

            bhi = other.Hi32Bit();
            bmi = other.Mi32Bit();
            blo = other.Lo32Bit();

            /* ensure that divisor is at least 2^95 */
            if (bhi == 0)
            {
                if (bmi == 0)
                {
                    ULONG hi_shift;
                    bhi = blo;
                    bmi = 0;
                    blo = 0;

                    hi_shift = 31 - BitNthMsf(bhi);
                    bhi <<= hi_shift;
                    bshift = 64 + hi_shift;
                }
                else
                {
                    bhi = bmi;
                    bmi = blo;
                    blo = 0;

                    for (bshift = 32; (bhi & UINT32_HIGHBIT) == 0; ++bshift)
                    {
                        bhi <<= 1;
                        bhi |= (bmi & UINT32_HIGHBIT) >> 31;
                        bmi <<= 1;
                    }
                }
            }
            else
            {
                for (bshift = 0; (bhi & UINT32_HIGHBIT) == 0; ++bshift)
                {
                    bhi <<= 1;
                    bhi |= (bmi & UINT32_HIGHBIT) >> 31;
                    bmi <<= 1;
                    bmi |= (blo & UINT32_HIGHBIT) >> 31;
                    blo <<= 1;
                }
            }

            ScopeInt192 t(((UINT64)bhi)<<32 | bmi, ((UINT64)blo)<<32, 0);

            if (Hi64Bit() > t.Hi64Bit() || (Hi64Bit() == t.Hi64Bit() && Mi64Bit() >= t.Mi64Bit()))
            {
                Sub192(t);
                extraBit = 1;
            }
            else
            {
                extraBit = 0;
            }

            ScopeInt128 b(bhi, bmi, blo);

            out = Div192by96to128(b);

            texp = 128 + ashift - bshift;

            if (extraBit)
            {
                out.Rshift128();
                out = ScopeInt128(out.Hi64Bit() + UINT64_HIGHBIT, out.Lo64Bit());
                texp--;
            }

            /* try loss free right shift */
            while (texp > 0 && (out.Lo64Bit() & 1) == 0)
            {
                /* right shift */
                out.Rshift128();
                texp--;
            }

            *pExp = texp;

            return DECIMAL_SUCCESS;
        }
    };

    /* multiplication c(192bit) = a(96bit) * b(96bit) */
    INLINE ScopeInt192 operator *(const ScopeInt128 & v1, const ScopeInt128 & v2)
    {
        UINT64 a, b, c, d;
        unsigned int h0, h1, h2, h3, h4, h5;
        int carry0, carry1;

        a = ((UINT64)v1.Lo32Bit()) * v2.Lo32Bit();
        h0 = (unsigned int) a;

        a >>= 32;
        carry0 = 0;
        b = ((UINT64)(v1.Lo32Bit())) * v2.Mi32Bit();
        c = ((UINT64)(v1.Mi32Bit())) * v2.Lo32Bit();
        a += b;
        if (a < b)
        {
            carry0++;
        }

        a += c;
        if (a < c)
        {
            carry0++;
        }
        h1 = (unsigned int) a;

        a >>= 32;
        carry1 = 0;
        b = ((UINT64)(v1.Lo32Bit())) * v2.Hi32Bit();
        c = ((UINT64)(v1.Mi32Bit())) * v2.Mi32Bit();
        d = ((UINT64)(v1.Hi32Bit())) * v2.Lo32Bit();
        a += b;
        if (a < b)
        {
            carry1++;
        }

        a += c;
        if (a < c)
        {
            carry1++;
        }

        a += d;
        if (a < d)
        {
            carry1++;
        }

        h2 = (unsigned int) a;

        a >>= 32;
        a += carry0;
        carry0 = 0;
        b = ((UINT64)v1.Mi32Bit()) * v2.Hi32Bit();
        c = ((UINT64)v1.Hi32Bit()) * v2.Mi32Bit();
        a += b;
        if (a < b)
        {
            carry0++;
        }

        a += c;
        if (a < c)
        {
            carry0++;
        }
        h3 = (unsigned int) a;

        a >>= 32;
        a += carry1;
        b = ((UINT64)v1.Hi32Bit()) * v2.Hi32Bit();
        a += b;
        h4 = (unsigned int) a;

        a >>= 32;
        a += carry0;
        h5 = (unsigned int) a;

        return ScopeInt192(((UINT64)h5) << 32 | h4,
                            ((UINT64)h3) << 32 | h2,
                            ((UINT64)h1) << 32 | h0);
    }

    // Print Decimal to string in G fomat
    int ScopeDecimalToString(const ScopeDecimal & s, char * finalOut, int size); // forward

    // Decimal is represented as 128bits
    // 96 bits of Mantissa
    // 1 bits for sign
    // 8 bits for scale/exponent
#ifdef PLUGIN_TYPE_SYSTEM
    class ScopeDecimal : public PluginTypeBase<ScopeDecimal>
#else
    class ScopeDecimal
#endif
    {
    protected:
        union {
            ULONG m_ss;
            struct {
                unsigned int reserved1 : 16;
                unsigned int m_scale   : 8;
                unsigned int reserved2 : 7;
                unsigned int m_sign       : 1;
            } m_signscale;
        } m_u;            // Union to hold sign and exponent

        ULONG m_hi;        //high 32 bits of Mantissa
        ULONG m_mid;    //middle 32 bits of Mantissa
        ULONG m_lo;        //low 32 bits of Mantissa

        // Is this decimal valid
        bool IsValid() const
        {
            if (Scale() > DECIMAL_MAX_SCALE)
                return false;

            return true;
        }

        //approximation for log2 of a
        //If q is the exact value for log2(a), then q <= Log2(a) <= q+1
        FORCE_INLINE int Log2() const
        {
            int tlog2;

            if (m_hi != 0)
                tlog2 = 64 + Log2_32(m_hi);
            else if (m_mid != 0)
                tlog2 = 32 + Log2_32(m_mid);
            else
                tlog2 = Log2_32(m_lo);

            if (tlog2 != DECIMAL_LOG_NEGINF)
            {
                tlog2 -= (Scale() * 33219) / 10000;
            }
            return tlog2;
        }

        void ConvertFromDouble(const double & c, int digits)
        {
            UINT64 * p = (UINT64*)(&c);
            int sigDigits, sign, texp, rc, scale;
            USHORT k;

            sign = ((*p & UINT64_HIGHBIT) != 0) ? 1 : 0;

            // Exponent
            k = ((USHORT)((*p) >> 52)) & 0x7FF;

            // Special cases for zero, Nan, Infinity and Subnormals
            // (See http://en.wikipedia.org/wiki/Binary64)
            // - behaviour confirmed via C# test code
            // - exp=0 result=zero (for both zero and subnormal case)
            // - exp=0x7ff result=overflow (for both Infinity and Nan cases)

            if (k == 0x7FF)
            {
                /* NaNs, SNaNs, Infinities */
                throw ScopeDecimalException(DECIMAL_OVERFLOW);
            }

            if (k == 0)
            {
                /* Subnormals, Zeros  */
                *this = ScopeDecimal(0);
                return;
            }

            // 1-bit followed by the fraction component from the float (52 bits)
            // (double has an implied leading 1 bit except for zero which we handled above)
            ScopeInt128 a(0, (*p & 0xFFFFFFFFFFFFF) | (0x10000000000000));

            // Remove exponent bias (0x3ff) to get real exponent
            // adjust for differences in decimal point position (right side in decimal, left in double)
            // 52 is size in bits of the value part of a double
            texp = (k & 0x7FF) - 0x3FF;
            texp -= 52;
            if (texp > 0)
            {
                for (; texp > 0; texp--)
                {
                    a.Lshift128();
                }
            }

            scale = 0;
            rc = a.Rescale128(scale, -texp, 0, DECIMAL_MAX_SCALE, 1);
            if (rc != DECIMAL_SUCCESS)
                throw ScopeDecimalException(rc);

            sigDigits = a.CalcDigits();

            /* too much digits, then round */
            if (sigDigits > digits)
            {
                a.Div128DecadeFactor(sigDigits - digits);
                scale -= sigDigits - digits;

                /* check value, may be 10^(digits+1) caused by rounding */
                if (a.Hi64Bit() == dec128decadeFactors[digits].hi
                    && a.Lo64Bit() == dec128decadeFactors[digits].lo)
                {
                    a.Div128by32(10, 0);
                    scale--;
                }

                if (scale < 0)
                {
                    rc = a.Mult128DecadeFactor(-scale);
                    if (rc != DECIMAL_SUCCESS)
                        throw ScopeDecimalException(rc);

                    scale = 0;
                }
            }

            //
            // Turn the double 0.6 which at this point is:
            // 0.6000000000000000
            // into:
            // 0.6
            //
            a.TrimExcessScale (&scale);

            m_lo = (ULONG) a.Lo64Bit();
            m_mid = (ULONG) (a.Lo64Bit()>> 32);
            m_hi = (ULONG) a.Hi64Bit();

            // clear reserve field, sign, and scale first
            m_u.m_ss = 0;

            m_u.m_signscale.m_sign = sign;
            m_u.m_signscale.m_scale = scale;

            SCOPE_ASSERT(IsValid());
        }

#pragma region PluginTypeSystem
    public:
#ifdef PLUGIN_TYPE_SYSTEM
        //int Compare(const ScopeDecimal & rhs) const
        //{
        //    return 0;
        //}
        //already defined

        // Serialize in ScopeMapOutputMemoryStream::Write
        template<typename BufferType>
        void ScopeMapOutputMemoryStreamSerialize(void* buffer) const
        {
            ((BufferType*)buffer)->Write((const char*)this, sizeof(*this));
        }

        // Serialize in BinaryOutputStreamBase::Write
        template<typename OutputType>
        void Serialize(BinaryOutputStreamBase<OutputType>* stream) const
        {
            stream->Write(Lo32Bit());
            stream->Write(Mid32Bit());
            stream->Write(Hi32Bit());
            stream->Write(SignScale32Bit());
        }

        // Serialize in, ScopeIO.h TextOutputStream::Write
        template<typename TextOutputStreamTraits>
        void Serialize(TextOutputStream<TextOutputStreamTraits>* stream) const
        {
            char finalOut[80];
            int n = ScopeDecimalToString(*this, finalOut, 80);

            if (n > 0)
            {
                stream->WriteString<false>(finalOut, n);
            }
        }

        // Serialize in SStreamDataOutputStream::Write
        template<typename BufferType>
        void SStreamDataOutputSerialize(void* buffer) const
        {
            ((BufferType*)buffer)->Write((const char*)&m_lo, sizeof(ULONG));
            ((BufferType*)buffer)->Write((const char*)&m_mid, sizeof(ULONG));
            ((BufferType*)buffer)->Write((const char*)&m_hi, sizeof(ULONG));
            ((BufferType*)buffer)->Write((const char*)&m_u.m_ss, sizeof(ULONG));
        }

        // DeSerialize in ScopeMapInputMemoryStream::Read
        template<typename AllocatorType>
        void ScopeMapInputMemoryStreamDeserialize(const char* & cur, void* ia)
        {
            memcpy((char*)this, cur, sizeof(ScopeDecimal));
            cur += sizeof(ScopeDecimal);
        }

        // DeSerialize in BinaryInputStreamBase::Read
        template<typename InputType>
        void Deserialize(BinaryInputStreamBase<InputType>* stream, IncrementalAllocator* )
        {
            stream->Read(m_lo);
            stream->Read(m_mid);
            stream->Read(m_hi);
            stream->Read(m_u.m_ss);
            SCOPE_ASSERT(IsValid());
        }

        // DeSerialize in TextInputStream::Read
        template<typename TextInputStreamTraits, typename InputStreamType>
        void Deserialize(TextInputStream<TextInputStreamTraits, InputStreamType>* stream, FStringWithNull & str, bool lastEmptyColumn)
        {
            stream->DoFStringToT(str, *this, lastEmptyColumn);
        }

        // Swap the values
        void Swap(ScopeDecimal & )
        {
        }

        // The hash function
        size_t GetHashCode() const
        {
            return 0;
        }

        // FromFString, see scopeio.h
        template<typename StringType>
        ConvertResult FromFString(void* str)
        {
            StringType* s = (StringType*)str;
            try
            {
                s->TrimWhiteSpace();

                // Convert string to decimal
                *this = ScopeDecimal::Parse(s->buffer(), s->DataSize());
            }
            catch(ScopeDecimalException & e)
            {
                return e.ToConvertResult();
            }

            return ConvertSuccess;
        }

        // IsNull is conflict with ScopeDecimal::IsNull, so we must use a new name IsNull. We should switch back after runtime switching to Plugin Type System
        bool IsNull() const
        {
            return false;
        }

        // For Aggregate_MIN and Aggregate_MAX
        void SetNull()
        {
        }

        // Deep copier
        void DeepCopyFrom(ScopeDecimal const & , IncrementalAllocator * )
        {
        }

        // Shallow copier
        void ShallowCopyFrom(ScopeDecimal const & )
        {
        }

        // Used by Aggregate_UNCHECKED_SUMx2
        ScopeDecimal Multiply(ScopeDecimal const & ) const
        {
            return ScopeDecimal();
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        ScopeDecimal & AddByUnchecked(ScopeDecimal const & )
        {
            return *this;
        }

        // For Aggregate_SUM: Should check overflow; Should skip if the value is null
        ScopeDecimal & AddByChecked(ScopeDecimal const & )
        {
            return *this;
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        ScopeDecimal & SetZero()
        {
            return *this;
        }

        static ScopeDecimal Min()
        {
            return ScopeDecimal();
        }

        static ScopeDecimal Max()
        {
            return ScopeDecimal();
        }

        enum {isNullablePrimaryType=false};
        enum {is_floating_point=true};
        enum {is_signed=true};
        enum {is_unsigned=true};
        enum {is_integral=false};
        enum {need_deep_copy=false};
#endif
        // TODO:xiaoyuc just for testing, they should be removed when plugin type system is done
        const ULONG & Hi32Bit_PTS() const
        {
            return m_hi;
        }

        const ULONG & Mid32Bit_PTS() const
        {
            return m_mid;
        }

        const ULONG & Lo32Bit_PTS() const
        {
            return m_lo;
        }

        const ULONG & SignScale32Bit_PTS() const
        {
            return m_u.m_ss;
        }
#pragma endregion PluginTypeSystem

    private:
            // Private APIs for binary reader/writer for extra performance
            template <typename T> friend class BinaryInputStreamBase;
            template <typename T> friend class BinaryOutputStreamBase;
            friend class SStreamDataOutputStream;
            friend class ScopeManagedInterop;
            friend class InteropAllocator;

            ULONG & Hi32Bit()
            {
                return m_hi;
            }

            ULONG & Mid32Bit()
            {
                return m_mid;
            }

            ULONG & Lo32Bit()
            {
                return m_lo;
            }

            ULONG & SignScale32Bit()
            {
                return m_u.m_ss;
            }

            const ULONG & Hi32Bit() const
            {
                return m_hi;
            }

            const ULONG & Mid32Bit() const
            {
                return m_mid;
            }

            const ULONG & Lo32Bit() const
            {
                return m_lo;
            }

            const ULONG & SignScale32Bit() const
            {
                return m_u.m_ss;
            }

        // str: significant digits of the decimal
        // decrDecimal: location of the decimal position from beginning
        // sign: sign of the decimal
        //
        // converts a digit string to decimal
        // The significant digits must be passed as an integer in buf !
        //
        // 1. Example:
        //    if you want to convert the number 123.456789012345678901234 to decimal
        //       buf := "123456789012345678901234"
        //       decrDecimal := 3
        //       sign := 0
        //
        // 2. Example:
        //    you want to convert -79228162514264337593543950335 to decimal
        //      buf := "79228162514264337593543950335"
        //      decrDecimal := 29
        //      sign := 1
        static ScopeDecimal Parse(const char * buf, size_t len, int decrDecimal, int sign)
        {
            const char *p;
            ScopeInt128 a;
            int n, rc, i, sigLen = -1, firstNonZero;
            int scale, roundBit = 0;

            for (p = buf, i = 0; *p != 0; i++, p++)
            {
                n = *p - '0';
                if (n < 0 || n > 9)
                {
                    throw ScopeDecimalException(DECIMAL_INVALID_CHARACTER);
                }

                if (n)
                {
                    if (sigLen < 0)
                    {
                        firstNonZero = i;
                        sigLen = (static_cast<int>(len) - firstNonZero > DECIMAL_MAX_SCALE+1)? DECIMAL_MAX_SCALE+1+firstNonZero : static_cast<int>(len);

                        if (decrDecimal > sigLen+1)
                            throw ScopeDecimalException(DECIMAL_OVERFLOW);
                    }

                    if (i >= sigLen)
                        break;

                    rc = a.IncMultConstant128(sigLen - 1 - i, n);
                    if (rc != DECIMAL_SUCCESS)
                    {
                        throw ScopeDecimalException(rc);
                    }
                }
            }

            scale = sigLen - decrDecimal;

            if (i < len)
            {
                // too much digits, we must round
                n = buf[i] - '0';
                if (n < 0 || n > 9)
                {
                    throw ScopeDecimalException(DECIMAL_INVALID_CHARACTER);
                }

                if (n > 5)
                {
                    roundBit = 1;
                }
                else if (n == 5)
                {
                    // need addtitional check to decide round bit
                    n = buf[i-1] - '0';
                    for (++i; i < len; ++i)
                    {
                        // we are greater than .5
                        if (buf[i] != '0')
                            break;
                    }

                    if (i < len        // greater than exactly .5
                        || n % 2 == 1) // exactly .5, use banker's rule for rounding
                    {
                        roundBit = 1;
                    }
                }
            }

            if (a.Hi64Bit()!= 0)
            {
                rc = a.Normalize128(scale, 1, roundBit);

                if (rc != DECIMAL_SUCCESS)
                    throw ScopeDecimalException(rc);
            }

            if (a.Hi64Bit()==0 && a.Lo64Bit()==0)
            {
                sigLen = static_cast<int>(len); //it's zero value, all coming chars are counted as significant digits
            }

            return ScopeDecimal(a, sigLen - decrDecimal, sign);
        }

#pragma region Helpers For ScopeDecimal GetHashcode
        // These helpers are part of .NET GetHashCode implementation.
        // For more details, https://github.com/dotnet/coreclr/blob/master/src/palrt/decconv.cpp VarR8FromDec method.
        // (original source in OLEAut32 source in Windows source.

        static double fnDblPower10(ULONG ix)
        {
            static const double dblPower10[] = {
                1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11,
                1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21,
                1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29, 1e30, 1e31,
                1e32, 1e33, 1e34, 1e35, 1e36, 1e37, 1e38, 1e39, 1e40, 1e41,
                1e42, 1e43, 1e44, 1e45, 1e46, 1e47, 1e48, 1e49, 1e50, 1e51,
                1e52, 1e53, 1e54, 1e55, 1e56, 1e57, 1e58, 1e59, 1e60, 1e61,
                1e62, 1e63, 1e64, 1e65, 1e66, 1e67, 1e68, 1e69, 1e70, 1e71,
                1e72, 1e73, 1e74, 1e75, 1e76, 1e77, 1e78, 1e79, 1e80
            };

            if (ix < _countof(dblPower10))
            {
                return dblPower10[ix];
            }

            return pow(10.0, ix);
        }

        typedef union {
            struct {
                    ULONG mantLo;
                    ULONG mantHi : 20;
                    ULONG exp : 11;
                    ULONG sign : 1;
            } u;
            double dbl;
        } DBLSTRUCT;

        #define DBLBIAS 1022
        const DBLSTRUCT ds2to64 = { {0, 0, DBLBIAS + 65, 0} };

#pragma endregion Helpers For ScopeDecimal GetHashcode

    public:
        ScopeDecimal()
        {
            memset(this,0, sizeof(ScopeDecimal));
        }

        ScopeDecimal(const ScopeDecimal & c)
        {
            m_hi = c.m_hi;
            m_mid = c.m_mid;
            m_lo = c.m_lo;
            m_u = c.m_u;
            SCOPE_ASSERT(IsValid());
        }

        // enable_if is a common trick used to allow binding of this ctor to only specified types, in this
        // case integers. Since enable_if is a type trait, it can only be used in that form, so we are forced
        // to invent a second default parameter of type void** to solve the binding issue, yet not to allow
        // any other type to bind to this 2 argument ctor. Name of default argument is omitted.
        template<class T>
        ScopeDecimal(const T & c, typename enable_if<std::is_integral<T>::value, void>::type ** = 0)
        {
            if (c >= 0)
            {
                m_hi = 0;
                m_mid = (ULONG)(static_cast<ULONGLONG>(c) >> 32);
                m_lo = (ULONG)c;
                m_u.m_ss = 0;
            }
            else
            {
                m_hi = 0;
                m_mid = (ULONG)((-static_cast<LONGLONG>(c)) >> 32);
                m_lo = (ULONG)(-static_cast<LONGLONG>(c));
                m_u.m_ss = 0;
                m_u.m_signscale.m_sign = 1;
            }

            SCOPE_ASSERT(IsValid());
        }

        ScopeDecimal(const double & c)
        {
            ConvertFromDouble(c, numeric_limits<double>::digits10);
        }

        ScopeDecimal(const float & c)
        {
            double d = c;
            ConvertFromDouble(d, numeric_limits<float>::digits10);
        }

        ScopeDecimal(const ScopeInt128 & c, int scale, int sign)
        {
            SCOPE_ASSERT((c.Hi64Bit() >> 32) == 0);
            SCOPE_ASSERT(sign == 0 || sign == 1);
            SCOPE_ASSERT(scale >= 0 && scale <= DECIMAL_MAX_SCALE);

            m_lo = (ULONG) c.Lo64Bit();
            m_mid = (ULONG) (c.Lo64Bit()>> 32);
            m_hi = (ULONG) c.Hi64Bit();

            // clear reserve field, sign, and scale first
            m_u.m_ss = 0;

            m_u.m_signscale.m_sign = sign;
            m_u.m_signscale.m_scale = scale;

            SCOPE_ASSERT(IsValid());
        }

        ScopeDecimal(int hi, int mid, int lo, int signscale)
        {
            m_lo = (ULONG)lo;
            m_mid = (ULONG)mid;
            m_hi = (ULONG)hi;
            m_u.m_ss = (ULONG)signscale;
            SCOPE_ASSERT(IsValid());
        }

        void Reset(int hi, int mid, int lo, int signscale)
        {
            m_lo = (ULONG)lo;
            m_mid = (ULONG)mid;
            m_hi = (ULONG)hi;
            m_u.m_ss = (ULONG)signscale;
            SCOPE_ASSERT(IsValid());
        }

        static ScopeDecimal Parse(const char * buf, size_t len)
        {
            char * outBuffer = (char *) _alloca(len);
            int outIndex = 0;
            int decPos = -1;
            int sign = 0;
            int i = 0;

            if(buf[i] == '-')
            {
                sign = 1;
                i++;
            }
            else if (buf[i] == '+')
            {
                sign = 0;
                i++;
            }

            int start = i;
            for(; i<len ; i++)
            {
                if (isdigit(buf[i]))
                {
                    // copy to normalized buffer which is on the local stack
                    outBuffer[outIndex++] = buf[i];
                }
                else if (buf[i] == '.')
                {
                    // if we see multiple ".", it is invalid.
                    if (decPos != -1)
                    {
                        // Invalid character.
                        throw ScopeDecimalException(DECIMAL_INVALID_CHARACTER);
                    }

                    decPos = i-start;
                }
                else
                {
                    // Invalid character.
                    throw ScopeDecimalException(DECIMAL_INVALID_CHARACTER);
                }
            }

            // null terminate the output.
            outBuffer[outIndex++] = 0;

            // if there is no decimal point found, then the decPos is at the end of the normalized string
            if (decPos == -1)
            {
                decPos = outIndex-1;
            }

            // Convert string to decimal
            return ScopeDecimal::Parse(outBuffer, outIndex-1, decPos, sign);
        }

        //
        // returns minimal number of digit string to represent decimal
        // No leading or trailing zeros !
        // Examples:
        // *this == 0            =>    outBuf = "", decPos = 1, sign = 0
        // *this == 12.34        =>    outBuf = "1234", decPos = 2, sign = 0
        // *this == -1000.0000   =>    outBuf = "1", decPos= 4, sign = 1
        // *this == -0.00000076  =>    outBuf = "76", decPos = -6, sign = 0
        //
        // Parameters:
        //      digits     < 0: use decimals instead
        //                 = 0: gets mantisse as integer
        //                 > 0: gets at most <digits> digits, rounded according to banker's rule if necessary
        //      decimals     only used if digits < 0
        //                 >= 0: number of decimal places
        //      outBuf         pointer to result buffer
        //      bufSize     size of buffer
        //      decPos     receives insert position of decimal point relative to start of buffer
        //      sign      receives sign
        //
        void ToString(int digits, int decimals, char * outBuf, int bufSize, int & decPos, int & sign) const
        {
            char tmp[41];
            char *buf = outBuf;
            char *q, *p = tmp;
            ULONG rest;
            int sigDigits, d;
            int i, scale, len;

            ScopeInt128 a(m_hi, m_mid, m_lo);

            // significant digits
            sigDigits = a.CalcDigits();
            scale = Scale();

            /* calc needed digits (without leading or trailing zeros) */
            d = (digits == 0) ? sigDigits : digits;
            if (d < 0)
            {
                /* use decimals ? */
                if (0 <= decimals && decimals < scale)
                {
                    d = sigDigits - scale + decimals;
                }
                else
                {
                    d = sigDigits; /* use all you can get */
                }
            }

            if (sigDigits > d)
            {
                ScopeDecimal aa(a, DECIMAL_MAX_SCALE, Sign());

                aa.Round(DECIMAL_MAX_SCALE - sigDigits + d);
                a.Reset(aa.Hi32Bit(), aa.Mid32Bit(), aa.Lo32Bit());
                sigDigits += a.CalcDigits() - d;
            }

            len = 0;
            if (d > 0)
            {
                /* get digits starting from the tail */
                for (; (a.Lo64Bit()!= 0 || a.Hi64Bit()!= 0) && len < 40; len++)
                {
                    a.Div128by32(10, &rest);
                    *p++ = '0' + (char) rest;
                }
            }
            *p = 0;

            if (len >= bufSize)
                throw ScopeDecimalException(DECIMAL_BUFFER_OVERFLOW);

            q = buf;

            // now we have the minimal count of digits,
            // extend to wished count of digits or decimals
            if (digits >= 0)
            {
                /* count digits */
                if (digits >= bufSize)
                    throw ScopeDecimalException(DECIMAL_BUFFER_OVERFLOW);

                if (len == 0)
                {
                    /* zero or rounded to zero */
                    decPos = 1;
                }
                else
                {
                    /* copy significant digits */
                    for (i = 0; i < len; i++) {
                        *q++ = *(--p);
                    }
                    decPos = sigDigits - scale;
                }

                /* add trailing zeros */
                for (i = len; i < digits; i++)
                {
                    *q++ = '0';
                }
            }
            else
            {
                /* count decimals */
                if (scale >= sigDigits)
                {
                    /* add leading zeros */
                    if (decimals+2 >= bufSize)
                        throw ScopeDecimalException(DECIMAL_BUFFER_OVERFLOW);

                    decPos = 1;
                    for (i = 0; i <= scale - sigDigits; i++)
                        {
                        *q++ = '0';
                    }
                }
                else
                {
                    if (sigDigits - scale + decimals+1 >= bufSize)
                        throw ScopeDecimalException(DECIMAL_BUFFER_OVERFLOW);

                    decPos = sigDigits - scale;
                }

                /* copy significant digits */
                for (i = 0; i < len; i++) {
                    *q++ = *(--p);
                }

                /* add trailing zeros */
                for (i = scale; i < decimals; i++) {
                    *q++ = '0';
                }
            }
            *q = 0;

            // zero has positive sign
            sign = (sigDigits > 0) ? Sign(): 0;
        }

        ScopeDecimal & operator= ( ScopeDecimal const& rhs )
        {
            this->m_hi = rhs.m_hi;
            this->m_mid = rhs.m_mid;
            this->m_lo = rhs.m_lo;
            this->m_u = rhs.m_u;

            SCOPE_ASSERT(IsValid());
            return *this;
        }

        ULONG Sign() const
        {
            return m_u.m_signscale.m_sign;
        }

        ULONG Scale() const
        {
            return m_u.m_signscale.m_scale;
        }

        bool IsZero() const
        {
            return (m_lo == 0 && m_mid == 0 && m_hi == 0);
        }

        void Negate()
        {
            m_u.m_signscale.m_sign = 1 - m_u.m_signscale.m_sign;
        }

        FORCE_INLINE int Compare(const ScopeDecimal & other) const
        {
            int log2a, log2b, delta, sign;

            sign = Sign() ? -1 : 1;
            if (Sign() ^ (other.Sign()))
            {
                return (IsZero() && other.IsZero()) ? 0 : sign;
            }

            /* try fast comparison via log2 */
            log2a = Log2();
            log2b = other.Log2();
            delta = log2a - log2b;

            /* decimalLog2 is not exact, so we can say nothing  if abs(delta) <= 1 */
            if (delta < -1)
                return -sign;

            if (delta > 1)
                return sign;

            ScopeDecimal aa(*this);

            aa.Negate();

            aa = aa+other;

            if (aa.IsZero())
                return 0;

            return (aa.Sign()) ? 1 : -1;
        }

        FORCE_INLINE void Round(int decimals)
        {
            ScopeInt128 a(Hi32Bit(), Mid32Bit(), Lo32Bit());
            int scale, sign;

            scale = Scale();
            sign = Sign();
            if (scale > decimals)
            {
                a.Div128DecadeFactor(scale - decimals);
                scale = decimals;
            }

            m_lo = (ULONG) a.Lo64Bit();
            m_mid = (ULONG) (a.Lo64Bit()>> 32);
            m_hi = (ULONG) a.Hi64Bit();
            m_u.m_signscale.m_sign = sign;
            m_u.m_signscale.m_scale = scale;
            SCOPE_ASSERT(IsValid());
        }

        /* unary negation operator for scopedecimal*/
        ScopeDecimal operator -() const
        {
            ScopeDecimal aa(*this);

            aa.Negate();

            return aa;
        }

        /* unary plus operator for scopedecimal*/
        ScopeDecimal operator +() const
        {
            return *this;
        }

        /* addition operator for scopedecimal*/
        FORCE_INLINE ScopeDecimal operator+ (const ScopeDecimal & other) const
        {
            ScopeInt128 a(Hi32Bit(), Mid32Bit(), Lo32Bit());
            ScopeInt128 b(other.Hi32Bit(), other.Mid32Bit(), other.Lo32Bit());

            int log2A, log2B, log2Result, log10Result, rc;
            int subFlag, sign, scaleA, scaleB;

            sign = Sign();
            subFlag = sign - (int)other.Sign();
            scaleA = Scale();
            scaleB = other.Scale();
            if (scaleA == scaleB)
            {
                if (subFlag)
                {
                    a = a-b;
                    if (a.NegComplement())
                    {
                        sign = !sign;
                    }
                }
                else
                {
                    a = a+b;
                }

                rc = a.Normalize128(scaleA, 1, 0);
            }
            else
            {
                /* scales must be adjusted */
                /* Estimate log10 and scale of result for adjusting scales */
                log2A = a.Log2WithScale_128(scaleA);
                log2B = b.Log2WithScale_128(scaleB);
                log2Result = std::max (log2A, log2B);

                if (!subFlag)
                {
                    log2Result++; /* result can have one bit more */
                }

                log10Result = (log2Result * 1000) / 3322 + 1;

                /* we will calculate in 128bit, so we may need to adjust scale */
                if (scaleB > scaleA)
                    scaleA = scaleB;

                if (scaleA + log10Result > DECIMAL_MAX_SCALE + 7)
                {
                    /* this may not fit in 128bit, so limit it */
                    scaleA = DECIMAL_MAX_SCALE + 7 - log10Result;
                }

                rc = a.AdjustScale128(scaleA - (int)Scale());
                if (rc != DECIMAL_SUCCESS)
                    throw ScopeDecimalException(rc);

                rc = b.AdjustScale128(scaleA - scaleB);
                if (rc != DECIMAL_SUCCESS)
                    throw ScopeDecimalException(rc);

                if (subFlag) {
                    a = a-b;
                    if (a.NegComplement())
                    {
                        sign = !sign;
                    }
                }
                else
                {
                    a = a + b;
                }

                rc = a.Rescale128(scaleA, 0, 0, DECIMAL_MAX_SCALE, 1);
            }

            if (rc != DECIMAL_SUCCESS)
                throw ScopeDecimalException(rc);

            return ScopeDecimal(a, scaleA, sign);
        }

        ScopeDecimal operator- (const ScopeDecimal & other) const
        {
            return -other + *this;
        }

        /* multiply operator for scopedecimal*/
        ScopeDecimal operator* (const ScopeDecimal & other) const
        {
            ScopeInt128 a(Hi32Bit(), Mid32Bit(), Lo32Bit());
            ScopeInt128 b(other.Hi32Bit(), other.Mid32Bit(), other.Lo32Bit());
            ULONG factor;
            int scale, sign, rc;

            ScopeInt192 t = a*b;

            /* adjust scale and sign */
            scale = (int)Scale()+ (int)other.Scale();
            sign = Sign()^ other.Sign();

            /* first scaling step */
            factor = constantsDecadeInt32Factors[DECIMAL_MAX_INTFACTORS];
            while (t.Hi64Bit() != 0 || (t.Mi64Bit()>>32) >= factor)
            {
                if (t.Hi64Bit() < 100)
                {
                    factor /= 1000; /* we need some digits for final rounding */
                    scale -= DECIMAL_MAX_INTFACTORS - 3;
                }
                else
                {
                    scale -= DECIMAL_MAX_INTFACTORS;
                }

                t.Divby32(factor);
            }

            ScopeInt128 r(t.Mi64Bit(),t.Lo64Bit());

            /* second and final scaling */
            rc = r.Rescale128(scale, 0, 0, DECIMAL_MAX_SCALE, 1);
            if (rc != DECIMAL_SUCCESS)
                throw ScopeDecimalException(rc);

            return ScopeDecimal(r, scale, sign);
        }

        /* divide operator for scopedecimal*/
        ScopeDecimal operator/ (const ScopeDecimal & other) const
        {
            ScopeInt128 c;
            int scale, texp, rc;

            // Check for common cases
            if (Compare(other) == 0)
            {
                // return 1
                return ScopeDecimal(1);
            }

            if (Compare(-other) == 0)
            {
                // Minus one
                return ScopeDecimal(-1);
            }

            ScopeInt192 a((((UINT64)Hi32Bit()) << 32) | Mid32Bit(), ((UINT64)Lo32Bit()) << 32, 0);
            ScopeInt128 b(other.Hi32Bit(), other.Mid32Bit(), other.Lo32Bit());

            rc = a.DecimalDivSub(b, c, &texp);
            if (rc != DECIMAL_SUCCESS)
            {
                if (rc == DECIMAL_FINISHED)
                {
                    // return zero, c is zero.
                    return ScopeDecimal(c, 0, 0);
                }

                throw ScopeDecimalException(rc);
            }

            /* adjust scale and sign */
            scale = (int)Scale()- (int)other.Scale();

            rc = c.Rescale128(scale, texp, 0, DECIMAL_MAX_SCALE, 1);
            if (rc != DECIMAL_SUCCESS)
                throw ScopeDecimalException(rc);

            c.TrimExcessScale (&scale);

            return ScopeDecimal(c, scale, Sign()^other.Sign());
        }

        /* modulus operator for scopedecimal*/
        ScopeDecimal operator% (const ScopeDecimal & other) const
        {
            ScopeDecimal d = *this / other;
            ScopeDecimal d1 = d;
            d1.Round(0);

            // Whole number as a result, exit early to avoid +/- 0.5 which
            // will Round using an even number rule.
            if (d1 == d)
            {
                return ScopeDecimal(0);
            }

            if (d.Sign())
            {
                d = d + 0.5;
            }
            else
            {
                d = d - 0.5;
            }

            d.Round(0);

            return *this - (d * other);
        }

        bool operator < ( const ScopeDecimal & t ) const
        {
            return Compare(t)<0;
        }

        bool operator == ( const ScopeDecimal & t ) const
        {
            return Compare(t)==0;
        }

        bool operator != ( const ScopeDecimal & t ) const
        {
            return Compare(t)!=0;
        }

        bool operator > ( const ScopeDecimal & t ) const
        {
            return Compare(t)>0;
        }

        bool operator <= ( const ScopeDecimal & t ) const
        {
            return Compare(t)<=0;
        }

        bool operator >= ( const ScopeDecimal & t ) const
        {
            return Compare(t)>=0;
        }

        template <typename T>
        typename enable_if<is_arithmetic<T>::value, T>::type explicit_cast() const
        {
            char buf[80];
            T out;

            int endLen = ScopeDecimalToString(*this, buf, 80);
            SCOPE_ASSERT(endLen != 0);

            // @TODO: This is a VERY slow way of converting decimals, we ought to implement direct conversion routines.
            ConvertResult res = NumericConvert(buf, endLen+1, out);

            if (res == ConvertErrorOutOfRange)
            {
                throw std::out_of_range("Decimal casted to a numeric type that cannot hold the extra precision");
            }
            SCOPE_ASSERT(res == ConvertSuccess || res == ConvertErrorInvalidCharacter);

            return out;
        }

        // Compute 32 bit hash for ScopeDecimal
        // This hash computation is the same as the one used in .NET
        // For more details, look at https://github.com/dotnet/coreclr/blob/master/src/classlibnative/bcltype/decimal.cpp
        // and https://github.com/dotnet/coreclr/blob/master/src/palrt/decconv.cpp VarR8FromDec method. (original source 
        // in OLEAut32 source in Windows source.
        int GetScopeHashCode() const
        {
            double dbl;
            ScopeInt128 a(Hi32Bit(), Mid32Bit(), Lo32Bit());

            if ((LONG)Mid32Bit() < 0)
            {
                dbl = (ds2to64.dbl + (double)(LONGLONG)a.Lo64Bit() +
                    (double)Hi32Bit() * ds2to64.dbl) / fnDblPower10(Scale());
            }
            else
            {
                dbl = ((double)(LONGLONG)a.Lo64Bit() +
                    (double)Hi32Bit() * ds2to64.dbl) / fnDblPower10(Scale());
            }

            if (Sign())
            {
                dbl = -dbl;
            }

            if (dbl == 0.0) 
            {
                // Ensure 0 and -0 have the same hash code
                return 0;
            }

            // conversion to double is lossy and produces rounding errors so we mask off the lowest 4 bits
            // 
            // For example these two numerically equal decimals with different internal representations produce
            // slightly different results when converted to double:
            //
            // decimal a = new decimal(new int[] { 0x76969696, 0x2fdd49fa, 0x409783ff, 0x00160000 });
            //                     => (decimal)1999021.176470588235294117647000000000 => (double)1999021.176470588
            // decimal b = new decimal(new int[] { 0x3f0f0f0f, 0x1e62edcc, 0x06758d33, 0x00150000 }); 
            //                     => (decimal)1999021.176470588235294117647000000000 => (double)1999021.1764705882
            //
            return ((((int *)&dbl)[0]) & 0xFFFFFFF0) ^ ((int *)&dbl)[1];
        }

        // Use GetScopeHashCode to normalize the decimal.
        // Unnonrmalized decimals may lead to different hash codes for different representation
        // of same decimal. For instance 123.400 and 123.400000
        unsigned __int64 GetCRC32Hash(unsigned __int64 crc) const
        {
            return CRC32Hash<int>(crc, GetScopeHashCode());
        }
    };

    // forward declare
    template<typename K, typename V> class ScopeMapNative;
    template<typename T> class ScopeArrayNative;

    template<typename T>
    struct IsNullablePrimaryTypeTraits
    {
        enum { val = false };
    };

    template<>
    struct IsNullablePrimaryTypeTraits<FBinary>
    {
        enum { val = true };
    };

    template<>
    struct IsNullablePrimaryTypeTraits<FString>
    {
        enum { val = true };
    };

    template<typename T>
    struct IsNullablePrimaryTypeTraits< NativeNullable<T> >
    {
        enum { val = true };
    };

    template<typename T>
    struct need_deep_copy_traits
    {
        enum { val = false };
    };

    template<>
    struct need_deep_copy_traits<FBinary>
    {
        enum { val = true };
    };

    template<>
    struct need_deep_copy_traits<FString>
    {
        enum { val = true };
    };

    template<typename K, typename V>
    struct need_deep_copy_traits<ScopeMapNative<K,V>>
    {
        enum { val = true };
    };

    template<typename T>
    struct need_deep_copy_traits<ScopeArrayNative<T>>
    {
        enum { val = true };
    };

    // in memory stream for scopemap deserialize
    class ScopeMapInputMemoryStream
    {
        IncrementalAllocator *m_alloc;
        const char* &m_cur;

    public:

        ScopeMapInputMemoryStream(IncrementalAllocator * allocator, const char* &buffer)
            : m_alloc(allocator), m_cur(buffer)
        {
        }

        ScopeMapInputMemoryStream& operator=(const ScopeMapInputMemoryStream& other) = delete;

        INLINE void Read(char* dest, UINT size)
        {
            memcpy(dest, m_cur, size);
            m_cur += size;
        }
#ifdef PLUGIN_TYPE_SYSTEM
        template<typename T>
        INLINE void DoRead(T& t)
        {
            Read((char*)&t, static_cast<UINT>(sizeof(t)));
        }

        template<typename T>
        INLINE void Read(T & t)
        {
            t.ScopeMapInputMemoryStreamDeserialize<IncrementalAllocator>(m_cur, m_alloc);
        }

        INLINE void Read(bool & s)
        {
            DoRead(s);
        }

        INLINE void Read(unsigned char & s)
        {
            DoRead(s);
        }

        INLINE void Read(char & s)
        {
            DoRead(s);
        }

        INLINE void Read(short & s)
        {
            DoRead(s);
        }

        INLINE void Read(unsigned short & s)
        {
            DoRead(s);
        }

        INLINE void Read(wchar_t & s)
        {
            DoRead(s);
        }

        INLINE void Read(int & s)
        {
            DoRead(s);
        }

        INLINE void Read(unsigned int & s)
        {
            DoRead(s);
        }

        INLINE void Read(__int64 & s)
        {
            DoRead(s);
        }

        INLINE void Read(unsigned __int64 & s)
        {
            DoRead(s);
        }

        INLINE void Read(float & s)
        {
            DoRead(s);
        }

        INLINE void Read(double & s)
        {
            DoRead(s);
        }
#else
        template<typename T>
        INLINE void Read(T& t)
        {
            Read((char*)&t, static_cast<UINT>(sizeof(t)));
        }

        template<typename T>
        INLINE void Read(NativeNullable<T>& t)
        {
            Read(t.get());
            t.ClearNull();
        }

        void Read(FString & str)
        {
            ReadFixedArray(str);
        }

        void Read(FBinary & bin)
        {
            ReadFixedArray(bin);
        }

        template<typename K, typename V>
        INLINE void Read(ScopeMapNative<K,V>& t)
        {
            t.Deserialize(m_cur, m_alloc);
        }

        template<typename T>
        INLINE void Read(ScopeArrayNative<T>& t)
        {
            t.Deserialize(m_cur, m_alloc);
        }

#endif
    private:

        template<typename T>
        INLINE void ReadFixedArray(FixedArrayType<T> & s)
        {
            UINT size = ReadLen();
            s.CopyFrom((const T*)m_cur, size, m_alloc);
            m_cur += size;
        }

        INLINE UINT ReadLen()
        {
            UINT value = 0;
            char b = 0;
            UINT shift = 0;
            do
            {
                Read(b);
                value |= (b & 0x7f) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);

            return value;
        }
    };

    // in memory stream for scope map serialize
    class ScopeMapOutputMemoryStream
    {
        ScopeEngine::AutoBuffer * m_inner;

    public:

        ScopeMapOutputMemoryStream(ScopeEngine::AutoBuffer * buffer)
            : m_inner(buffer)
        {
        }

        ScopeEngine::AutoBuffer* GetOutputer()
        {
            return m_inner;
        }

        INLINE void Write(const char* buffer, SIZE_T length)
        {
            m_inner->Write(buffer, length);
        }
#ifdef PLUGIN_TYPE_SYSTEM
        template<typename T>
        INLINE void DoWrite(const T& t)
        {
            Write((const char*)&t, sizeof(t));
        }

        template<typename T>
        INLINE void Write(const T & t)
        {
            t.ScopeMapOutputMemoryStreamSerialize<AutoBuffer>((void*)m_inner);
        }

        INLINE void Write(const bool & s)
        {
            DoWrite(s);
        }

        INLINE void Write(const unsigned char & s)
        {
            DoWrite(s);
        }

        INLINE void Write(const char & s)
        {
            DoWrite(s);
        }

        INLINE void Write(const short & s)
        {
            DoWrite(s);
        }

        INLINE void Write(const unsigned short & s)
        {
            DoWrite(s);
        }

        INLINE void Write(const wchar_t & s)
        {
            DoWrite(s);
        }

        INLINE void Write(const int & s)
        {
            DoWrite(s);
        }

        INLINE void Write(const unsigned int & s)
        {
            DoWrite(s);
        }

        INLINE void Write(const __int64 & s)
        {
            DoWrite(s);
        }

        INLINE void Write(const unsigned __int64 & s)
        {
            DoWrite(s);
        }

        INLINE void Write(const float & s)
        {
            DoWrite(s);
        }

        INLINE void Write(const double & s)
        {
            DoWrite(s);
        }
#else
        template<typename T>
        INLINE void Write(const T& t)
        {
            Write((const char*)&t, sizeof(t));
        }

        template<typename T>
        INLINE void Write(const NativeNullable<T>& t)
        {
            Write(t.get());
        }

        void Write(const FString & str)
        {
            WriteFixedArray(str);
        }

        void Write(const FBinary & bin)
        {
            WriteFixedArray(bin);
        }

        template<typename K, typename V>
        INLINE void Write(const ScopeMapNative<K,V>& t)
        {
            t.Serialize(this);
        }

        template<typename T>
        INLINE void Write(const ScopeArrayNative<T>& t)
        {
            t.Serialize(this);
        }

        void Write(char c)
        {
            m_inner->Put((BYTE)c);
        }
#endif

    private:

        template<typename T>
        INLINE void WriteFixedArray(const FixedArrayType<T> & fixedArray)
        {
            UINT size = fixedArray.size();
            EncodeLength(size);
            Write((const char*)fixedArray.buffer(), size);
        }

        INLINE void EncodeLength(UINT value)
        {
            while (value >= 0x80)
            {
                Write((char)(value | 0x80));
                value = value >> 7;
            }
            Write((char)value);
        }
    };

    // Scope map for map-like semi structure
    template<typename K, typename V>
#ifdef PLUGIN_TYPE_SYSTEM
    class ScopeMapNative : public PluginTypeBase<ScopeMapNative<K, V> >
#else
    class ScopeMapNative
#endif
    {
        typedef std::map<K, V, less<K>, STLIncrementalAllocator<std::pair<const K,V>>> InnerType;
        typedef typename InnerType::const_iterator T_CONST_ITERATOR;
        IncrementalAllocator* m_alloc;   // ScopeMapNative does not own allocator
        InnerType* m_map; // ScopeMapNative does not own memory, all come from m_alloc.

    public:

         typedef typename InnerType::value_type T_VALUE_TYPE;

        // const_iterator class provide a sequential access interface for ScopeMapNative
        class const_iterator
        {

            T_CONST_ITERATOR m_constIter;

        public:

            const_iterator()
            {
            }

            const_iterator(const T_CONST_ITERATOR& it)
            {
                this->m_constIter = it;
            }

            const_iterator(const const_iterator &it)
            {
                this->m_constIter = it.m_constIter;
            }

            ~const_iterator()
            {
            }

            const_iterator& operator++()
            {
                ++m_constIter;
                return *this;
            }

            const_iterator& operator--()
            {
                --m_constIter;
                return *this;
            }

            const_iterator& operator=(const const_iterator& right)
            {
                this->m_constIter = right.m_constIter;
                return *this;
            }

            bool operator==(const const_iterator& it) const
            {
                return this->m_constIter == it.m_constIter;
            }

            bool operator!=(const const_iterator& it) const
            {
                return !(this->operator==(it));
            }

            const K& Key() const
            {
                return this->m_constIter->first;
            }

            const V& Value() const
            {
                return this->m_constIter->second;
            }
        };

        const_iterator begin() const
        {
            return const_iterator(m_map->begin());
        }

        const_iterator end() const
        {
            return const_iterator(m_map->end());
        }

        // default constructor
        ScopeMapNative() : m_defaultValue(V())
        {
            SetNull();
        }

        // Null constructor
        ScopeMapNative(nullptr_t) : m_defaultValue(V())
        {
            SetNull();
        }

        // constructor with alloc
        explicit
        ScopeMapNative(IncrementalAllocator* alloc) : m_defaultValue(V())
        {
            Reset(alloc);
        }

        // shallow copy to construct scope map
        ScopeMapNative(const ScopeMapNative<K, V>& s)
            : m_alloc(s.m_alloc),
              m_map(s.m_map),
              m_defaultValue(V())
        {
        }

        // deep copy to construct scope map
        ScopeMapNative(const ScopeMapNative<K, V>& s, IncrementalAllocator* alloc) : m_defaultValue(V())
        {
            if (s.IsNull())
            {
                SetNull();
                return;
            }

            Reset(alloc);
            for(const_iterator iter = s.begin(); iter != s.end(); ++iter)
            {
                Add(iter.Key(), iter.Value());
            }
        }

        // deep copy to merge scope map
        ScopeMapNative(const ScopeMapNative<K,V>& another, const ScopeMapNative<K,V>& update, IncrementalAllocator* alloc) : m_defaultValue(V())
        {
            if (another.IsNull() && update.IsNull())
            {
                SetNull();
                return;
            }

            Reset(alloc);
            if (!another.IsNull())
            {
                for(const_iterator iter = another.begin(); iter != another.end(); ++iter)
                {
                    Add(iter.Key(), iter.Value());
                }
            }

            if (!update.IsNull())
            {
                for(const_iterator iter = update.begin(); iter != update.end(); ++iter)
                {
                    Add(iter.Key(), iter.Value());
                }
            }
        }

        // helper constructor with deep copy
        ScopeMapNative(const T_VALUE_TYPE input[], int size, IncrementalAllocator* alloc) : m_defaultValue(V())
        {
            Reset(alloc);
            for (int idx = 0; idx < size; ++idx)
            {
                Add(input[idx].first, input[idx].second);
            }
        }

        ScopeMapNative& operator=(const ScopeMapNative& right)
        {
            m_alloc = right.m_alloc;
            m_map = right.m_map;
            return *this;
        }

        void Reset(IncrementalAllocator* alloc)
        {
            m_alloc = alloc;

            STLIncrementalAllocator<InnerType> allocator(alloc);
            InnerType* p = allocator.allocate(1);
            m_map = new(p) InnerType(less<K>(), STLIncrementalAllocator<std::pair<const K,V>>(m_alloc));
        }

        int count() const
        {
            return (int)(m_map->size());
        }

        // read only indexer.
        const V& operator[](const K& key) const
        {
            const V* v = TryGetValue(key);
            if (v == nullptr)
            {
                return m_defaultValue;
            }

            return *v;
        }

        const V* TryGetValue(const K& key) const
        {
            auto v = m_map->find(key);
            if (v == m_map->end())
            {
                return nullptr;
            }
            else
            {
                return &(v->second);
            }
        }

        // Insert key/value pair, will overwrite exist key
        // Always do deep copy for key and value if they are complex type
        void Add(const K& key, const V& value)
        {
            K newKey = GetKeyObj<need_deep_copy_traits<K>::val>(key);
            V newValue = GetValueObj<need_deep_copy_traits<V>::val>(value);

            (*m_map)[newKey] = newValue;
        }

        // Always do deep copy for complex type value
        void Append(int key, const V& value)
        {
            V newValue = GetValueObj<need_deep_copy_traits<V>::val>(value);
            m_map->emplace_hint(m_map->end(), key, newValue);
        }

        // Move key/value pair, will overwrite exist key, no deep copy
        void AddShallow(const K& key, const V& value)
        {
            (*m_map)[key] = value;
        }

        void SetNull()
        {
            m_map = nullptr;
            m_alloc = nullptr;
        }

        bool IsNull() const
        {
            return m_map == nullptr && m_alloc == nullptr;
        }

        bool ContainsKey(const K& key) const
        {
            return m_map->find(key) != m_map->end();
        }

        bool ContainsValue(const V& value) const
        {
            for(auto iter = m_map->begin(); iter != m_map->end(); ++iter)
            {
                if (iter->second == value)
                {
                    return true;
                }
            }

            return false;
        }

        const ScopeArrayNative<K> Keys() const
        {
            ScopeArrayNative<K> arr(m_alloc);
            for(auto iter = m_map->begin(); iter != m_map->end(); ++iter)
            {
                arr.Append(iter->first);
            }

            return arr;
        }

        const ScopeArrayNative<V> Values() const
        {
            ScopeArrayNative<V> arr(m_alloc);
            for(auto iter = m_map->begin(); iter != m_map->end(); ++iter)
            {
                arr.Append(iter->second);
            }

            return arr;
        }

        bool operator==(const ScopeMapNative<K,V>& right) const
        {
            if (count() != right.count())
            {
                return false;
            }

            for (const_iterator iter1 = begin(), iter2 = right.begin();
                 iter1 != end();
                 ++iter1, ++iter2)
            {
                if (iter1.Key() != iter2.Key() || iter1.Value() != iter2.Value())
                {
                    return false;
                }
            }

            return true;
        }

        bool operator!=(const ScopeMapNative<K,V>& right) const
        {
            return !operator==(right);
        }

        void Serialize(AutoBuffer *buffer) const
        {
            unique_ptr<ScopeMapOutputMemoryStream> writer(new ScopeMapOutputMemoryStream(buffer));
            Serialize(writer.get());
        }

        template<typename WT>
        void Serialize(WT * writer) const
        {
            char nullFlag = IsNull()? (char)1 : (char)0;
            writer->Write(nullFlag);
            if (nullFlag == 1)
            {
                return;
            }

            // header
            MapHeader header(CurrentVersion, count());
            writer->Write((char*)&header, sizeof(header));

            // all children
            for(auto iter = m_map->begin(); iter != m_map->end(); ++iter)
            {
                // Key
                if (IsNullablePrimaryTypeTraits<K>::val)
                {
                    if (IsNullKey<IsNullablePrimaryTypeTraits<K>::val>(iter->first))
                    {
                        throw RuntimeException(E_USER_NULL_KEY_IN_MAP);
                    }

                    writer->Write((char)0);
                }

                writer->Write(iter->first);

                // Value
                char nullValue = 0;
                if (IsNullablePrimaryTypeTraits<V>::val)
                {
                    nullValue = IsNullValue<IsNullablePrimaryTypeTraits<V>::val>(iter->second) ? (char)1 : (char)0;
                    writer->Write(nullValue);
                }

                if(nullValue == 0)
                {
                    writer->Write(iter->second);
                }
            }
        }

        void Deserialize(const char * &buffer, IncrementalAllocator* allocator)
        {
            unique_ptr<ScopeMapInputMemoryStream> reader(new ScopeMapInputMemoryStream(allocator, buffer));
            Deserialize(reader.get(), allocator);
        }

        template<typename RT>
        void Deserialize(RT* reader, IncrementalAllocator* allocator)
        {
            char nullFlag = 0;
            reader->Read(nullFlag);
            if(nullFlag == 1)
            {
                SetNull();
                return;
            }

            Reset(allocator);

            // Header
            MapHeader header;
            reader->Read((char*)&header, static_cast<UINT>(sizeof(header)));
            SCOPE_ASSERT(header.m_version <= CurrentVersion);
            SIZE_T count = header.m_count;

            // All children
            for (SIZE_T idx = 0; idx < count; ++idx)
            {
                // Key
                K key;
                char nullKey = 0;
                if (IsNullablePrimaryTypeTraits<K>::val)
                {
                    reader->Read(nullKey);
                    if (nullKey != 0)
                    {
                        throw RuntimeException(E_USER_NULL_KEY_IN_MAP);
                    }
                }

                reader->Read(key);

                // Value
                V value;
                char nullValue = 0;
                if (IsNullablePrimaryTypeTraits<V>::val)
                {
                    reader->Read(nullValue);
                }

                if (nullValue == 0)
                {
                    reader->Read(value);
                }
                else
                {
                    value = V();
                }

                // Add key/value.
                // Debugability first, alwasy check key exist for better diagnostic.
                // since insert/empalce method will trigger STLIncrementalAllocator::deallocate.
                T_CONST_ITERATOR low = m_map->lower_bound(key);
                if (low != m_map->end() && low->first == key)
                {
                    // key already exist
                    throw RuntimeException(E_USER_ERROR, "Error in map deserialize, indicates input data is corrupt.");
                }
                else
                {
                    m_map->emplace_hint(low, key, value);
                }
            }
        }

#pragma region PluginTypeSystem
#ifdef PLUGIN_TYPE_SYSTEM
        int Compare(const ScopeMapNative & ) const
        {
            return 0;
        }

        // Serialize in ScopeMapOutputMemoryStream::Write
        template<typename BufferType>
        void ScopeMapOutputMemoryStreamSerialize(void* buffer) const
        {
            Serialize((BufferType*)buffer);
        }

        // Already defined
        // Serialize in BinaryOutputStreamBase::Write
        // template<typename OutputType>
        // void Serialize(BinaryOutputStreamBase<OutputType>* ) const
        // {
        // }

        // Serialize in, ScopeIO.h TextOutputStream::Write
        // template<typename TextOutputStreamTraits>
        // void Serialize(TextOutputStream<TextOutputStreamTraits>* ) const
        // {
        // }

        // Serialize in SStreamDataOutputStream::Write
        template<typename BufferType>
        void SStreamDataOutputSerialize(void* buffer) const
        {
            Serialize((BufferType*)buffer);
        }

        // DeSerialize in ScopeMapInputMemoryStream::Read
        template<typename AllocatorType>
        void ScopeMapInputMemoryStreamDeserialize(const char* & cur, void* ia)
        {
            Deserialize(cur, (AllocatorType*)ia);
        }

        // DeSerialize in BinaryInputStreamBase::Read
        // template<typename InputType>
        // void Deserialize(BinaryInputStreamBase<InputType>* , IncrementalAllocator* )
        // {
        // }

        // DeSerialize in TextInputStream::Read
        // template<typename TextInputStreamTraits, typename InputStreamType>
        // void Deserialize(TextInputStream<TextInputStreamTraits, InputStreamType>* , IncrementalAllocator* )
        // {
        // }

        // Swap the values
        void Swap(ScopeMapNative & )
        {
        }

        // The hash function
        size_t GetHashCode() const
        {
            return 0;
        }

        // TryScopeMapNativeToT, see scopeio.h
        template<typename StringType>
        ConvertResult FromFString(void* str)
        {
            return ConvertSuccess;
        }

        // already defined in this class
        // bool IsNull() const
        // {
        //    return false;
        // }

        // For Aggregate_MIN and Aggregate_MAX
        // void SetNull()
        // {
        // }

        // Deep copier
        void DeepCopyFrom(ScopeMapNative const & , IncrementalAllocator * )
        {
        }

        // Shallow copier
        void ShallowCopyFrom(ScopeMapNative const & )
        {
        }

        // Used by Aggregate_UNCHECKED_SUMx2
        ScopeMapNative Multiply(ScopeMapNative const & ) const
        {
            return ScopeMapNative();
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        ScopeMapNative & AddByUnchecked(ScopeMapNative const & )
        {
        }

        // For Aggregate_SUM: Should check overflow; Should skip if the value is null
        ScopeMapNative & AddByChecked(ScopeMapNative const & )
        {
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        ScopeMapNative & SetZero()
        {
        }

        static ScopeMapNative Min()
    {
    }

        static ScopeMapNative Max()
    {
    }

        enum {isNullablePrimaryType=false};
        enum {is_floating_point=false};
        enum {is_signed=false};
        enum {is_unsigned=false};
        enum {is_integral=false};
        enum {need_deep_copy=false};
#endif
#pragma endregion PluginTypeSystem

    private:

        template<bool IsNullablePrimaryType>
        INLINE bool IsNullKey(const K& /*key*/) const
        {
            return false;
        }

        template<>
        INLINE bool IsNullKey<true>(const K& key) const
        {
            return key.IsNull();
        }

        template<bool IsNullablePrimaryType>
        INLINE bool IsNullValue(const V& /*value*/) const
        {
            return false;
        }

        template<>
        INLINE bool IsNullValue<true>(const V& value) const
        {
            return value.IsNull();
        }

        template<bool deepCopy>
        K GetKeyObj(const K& key)
        {
            return  K(key, m_alloc);
        }

        template<>
        K GetKeyObj<false>(const K& key)
        {
            return  key;
        }

        template<bool deepCopy>
        V GetValueObj(const V& value)
        {
            return  V(value, m_alloc);
        }

        template<>
        V GetValueObj<false>(const V& value)
        {
            return  value;
        }

#pragma pack(push,1)

        // serialized map structure header
        struct MapHeader
        {
            BYTE   m_version;  // serialization version
            SIZE_T m_count;    // element count

            MapHeader()
                : m_version(0), m_count(0)
            {
            }

            MapHeader(BYTE version, SIZE_T count)
                : m_version(version), m_count(count)
            {
            }
        };

#pragma pack(pop)

        static const BYTE MAP_HEADER_VERSION_V1 = 0x1;
        static const BYTE CurrentVersion = MAP_HEADER_VERSION_V1;

        const V m_defaultValue;
    };

    // ScopeArrayNative is layered on ScopeMapNative
    template<typename T>
#ifdef PLUGIN_TYPE_SYSTEM
    class ScopeArrayNative : public PluginTypeBase<ScopeArrayNative<T> >
#else
    class ScopeArrayNative
#endif
    {
        // Implement by ScopeMapNative
        typedef typename ScopeMapNative<int, T> InnerType;
        typedef typename InnerType::const_iterator T_CONST_ITERATOR;
        InnerType* m_array; // ScopeArrayNative does not own memory

    public:

        typedef typename T T_VALUE_TYPE;

        // const_iterator class provide a sequential access interface for ScopeArrayNative
        class const_iterator
        {

            T_CONST_ITERATOR m_constIter;

        public:

            const_iterator(const T_CONST_ITERATOR& it)
            {
                this->m_constIter = it;
            }

            const_iterator(const const_iterator &it)
            {
                this->m_constIter = it.m_constIter;
            }

            ~const_iterator()
            {
            }

            const_iterator& operator++()
            {
                ++m_constIter;
                return *this;
            }

            const_iterator& operator--()
            {
                --m_constIter;
                return *this;
            }

            const_iterator& operator=(const const_iterator& right)
            {
                this->m_constIter = right.m_constIter;
                return *this;
            }

            bool operator==(const const_iterator& it) const
            {
                return this->m_constIter == it.m_constIter;
            }

            bool operator!=(const const_iterator& it) const
            {
                return !(this->operator==(it));
            }

            const T& Value() const
            {
                return this->m_constIter.Value();
            }
        };

        const_iterator begin() const
        {
            return const_iterator(m_array->begin());
        }

        const_iterator end() const
        {
            return const_iterator(m_array->end());
        }

        // default constructor
        ScopeArrayNative()
        {
            SetNull();
        }

        // Null constructor
        ScopeArrayNative(nullptr_t)
        {
            SetNull();
        }

        // constructor with alloc
        explicit
        ScopeArrayNative(IncrementalAllocator* alloc)
        {
            Reset(alloc);
        }

        // shallow copy to construct scope array
        ScopeArrayNative(const ScopeArrayNative<T>& another)
            : m_array(another.m_array)
        {
        }

        // deep copy to construct scope array
        ScopeArrayNative(const ScopeArrayNative<T>& another, IncrementalAllocator* alloc)
        {
            if (another.IsNull())
            {
                SetNull();
                return;
            }

            Reset(alloc);
            for(const_iterator iter = another.begin(); iter != another.end(); ++iter)
            {
                Append(iter.Value());
            }
        }

        // deep copy to merge scope array
        ScopeArrayNative(const ScopeArrayNative<T>& another, const ScopeArrayNative<T>& append, IncrementalAllocator* alloc)
        {
            if (another.IsNull() && append.IsNull())
            {
                SetNull();
                return;
            }

            Reset(alloc);
            if (!another.IsNull())
            {
                for(const_iterator iter = another.begin(); iter != another.end(); ++iter)
                {
                    Append(iter.Value());
                }
            }

            if (!append.IsNull())
            {
                for(const_iterator iter = append.begin(); iter != append.end(); ++iter)
                {
                    Append(iter.Value());
                }
            }
        }

        // helper constructor with deep copy
        ScopeArrayNative(const T_VALUE_TYPE input[], int size, IncrementalAllocator* alloc)
        {
            Reset(alloc);
            for (int idx = 0; idx < size; ++idx)
            {
                Append(input[idx]);
            }
        }

        ScopeArrayNative& operator=(const ScopeArrayNative& right)
        {
            m_array = right.m_array;
            return *this;
        }

        void Reset(IncrementalAllocator* alloc)
        {
            STLIncrementalAllocator<InnerType> allocator(alloc);
            InnerType* p = allocator.allocate(1);

            m_array = new(p) InnerType(alloc);
        }

        int count() const
        {
            return m_array->count();
        }

        void Append(const T& value)
        {
            m_array->Append(count(), value);
        }

        void AddShallow(const T& value)
        {
            m_array->AddShallow(count(), value);
        }

        void SetNull()
        {
            m_array = nullptr;
        }

        bool IsNull() const
        {
            return m_array == nullptr;
        }

        bool Contains(const T& value) const
        {
            return m_array->ContainsValue(value);
        }

        bool operator==(const ScopeArrayNative<T>& right) const
        {
            return m_array->operator==(*(right.m_array));
        }

        bool operator!=(const ScopeArrayNative<T>& right) const
        {
            return !operator==(right);
        }

        const T& operator[](int index) const
        {
            const T* v = m_array->TryGetValue(index);
            if (v == nullptr)
            {
                throw RuntimeException(E_USER_ARRAY_INDEX, index);
            }

            return *v;
        }

        void Serialize(AutoBuffer *buffer) const
        {
            // delegate
            unique_ptr<ScopeMapOutputMemoryStream> writer(new ScopeMapOutputMemoryStream(buffer));
            return Serialize(writer.get());
        }

        template<typename WT>
        void Serialize(WT* writer) const
        {
            char nullFlag = IsNull()? (char)1 : (char)0;
            writer->Write(nullFlag);
            if (nullFlag == 1)
            {
                return;
            }

            m_array->Serialize(writer);
        }

        void Deserialize(const char *&buffer, IncrementalAllocator* alloc)
        {
            // delegate
            unique_ptr<ScopeMapInputMemoryStream> reader(new ScopeMapInputMemoryStream(alloc, buffer));
            return Deserialize(reader.get(), alloc);
        }

        template<typename RT>
        void Deserialize(RT* reader, IncrementalAllocator* alloc)
        {
            char nullFlag = 0;
            reader->Read(nullFlag);

            if(nullFlag == 1)
            {
                SetNull();
                return;
            }

            Reset(alloc);
            m_array->Deserialize(reader, alloc);
        }

        // Pending implementation, TODO xiaoyuc
#pragma region PluginTypeSystem
#ifdef PLUGIN_TYPE_SYSTEM
        int Compare(const ScopeArrayNative & ) const
        {
        }

        // Serialize in ScopeMapOutputMemoryStream::Write
        template<typename BufferType>
        void ScopeMapOutputMemoryStreamSerialize(void* buffer) const
        {
            Serialize((BufferType*)buffer);
        }

        // already defined
        // Serialize in BinaryOutputStreamBase::Write
        // template<typename OutputType>
        // void Serialize(BinaryOutputStreamBase<OutputType>* ) const
        // {
        // }

        // Serialize in, ScopeIO.h TextOutputStream::Write
        // template<typename TextOutputStreamTraits>
        // void Serialize(TextOutputStream<TextOutputStreamTraits>* ) const
        // {
        //}

        // Serialize in SStreamDataOutputStream::Write
        template<typename BufferType>
        void SStreamDataOutputSerialize(void* buffer) const
        {
            Serialize((BufferType*)buffer);
        }

        // DeSerialize in ScopeMapInputMemoryStream::Read
        template<typename AllocatorType>
        void ScopeMapInputMemoryStreamDeserialize(const char* & cur, void* ia)
        {
            Deserialize(cur, (AllocatorType*)ia);
        }

        // already defined
        // DeSerialize in BinaryInputStreamBase::Read
        // template<typename InputType>
        // void Deserialize(BinaryInputStreamBase<InputType>* , IncrementalAllocator* )
        // {
        // }

        // DeSerialize in TextInputStream::Read
        // template<typename TextInputStreamTraits, typename InputStreamType>
        // void Deserialize(TextInputStream<TextInputStreamTraits, InputStreamType>* , IncrementalAllocator* )
        // {
        // }

        // Swap the values
        void Swap(ScopeArrayNative & )
        {
        }

        // The hash function
        size_t GetHashCode() const
        {
        }

        // TryScopeArrayNativeToT, see scopeio.h
        template<typename StringType>
        ConvertResult FromFString(void* str)
        {
        }

        // already defined in this class
        // bool IsNull() const
        // {
        //    return false;
        // }

        // For Aggregate_MIN and Aggregate_MAX
        // void SetNull()
        // {
        // }

        // Deep copier
        void DeepCopyFrom(ScopeArrayNative const & , IncrementalAllocator * )
        {
        }

        // Shallow copier
        void ShallowCopyFrom(ScopeArrayNative const & )
        {
        }

        // Used by Aggregate_UNCHECKED_SUMx2
        ScopeArrayNative Multiply(ScopeArrayNative const & ) const
        {
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        ScopeArrayNative & AddByUnchecked(ScopeArrayNative const & )
        {
        }

        // For Aggregate_SUM: Should check overflow; Should skip if the value is null
        ScopeArrayNative & AddByChecked(ScopeArrayNative const & )
        {
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        ScopeArrayNative & SetZero()
        {
        }

        static ScopeArrayNative Min()
    {
    }

        static ScopeArrayNative Max()
    {
    }

        enum {isNullablePrimaryType=false};
        enum {is_floating_point=false};
        enum {is_signed=false};
        enum {is_unsigned=false};
        enum {is_integral=false};
        enum {need_deep_copy=false};
#endif
#pragma endregion PluginTypeSystem

    };

    // Print Decimal to string in G fomat
    INLINE int ScopeDecimalToString(const ScopeDecimal & s, char * finalOut, int size)
    {
        char buffer [41];
        int depos;
        int sign;

        try
        {
            s.ToString(-1, -1, buffer, 41, depos, sign);
        }
        catch(ScopeDecimalException &)
        {
            throw ScopeStreamException(ScopeStreamException::BadConvert);
        }

        int n=0;

        // In case this is zero
        if (buffer[0]==0 && depos == 1 && sign ==0)
        {
            if (size > 0)
            {
                finalOut[0]='0';
                n = 1;
            }
        }
        else if (depos > 0)
        {
            if (sign && n < size)
            {
                finalOut[n++] = '-';
            }

            for(int i=0; i < depos; i++,n < size)
            {
                finalOut[n++] = buffer[i];
            }

            if (n < size)
                finalOut[n++]='.';

            for(int i=depos; buffer[i]!=0; i++, n < size)
            {
                finalOut[n++] = buffer[i];
            }

            // If decimal is at last position, we can omit it.
            if (finalOut[n-1]=='.')
                n--;
        }
        else
        {
            SCOPE_ASSERT(depos < 0);

            if (sign && n < size)
            {
                finalOut[n++] = '-';
            }

            if (n < size -1)
            {
                finalOut[n++] = '0';
                finalOut[n++] = '.';
            }

            // fill in the leading zero
            for(int i=0; i<-depos; i++, n<size)
            {
                finalOut[n++] = '0';
            }

            for(int i=0; buffer[i]!=0; i++, n<size)
            {
                finalOut[n++] = buffer[i];
            }
        }

        //null terminate the string
        if (n < size)
            finalOut[n] = 0;

        return n<size?n:0;
    }

    INLINE ostream &operator<<(ostream &o, const ScopeDecimal & t)
    {
        char finalOut[80];

        ScopeDecimalToString(t, finalOut, 80);

        o << finalOut;
        return o;
    }

    template <class T>
    INLINE ostream &operator<<(ostream &o, const NativeNullable<T> &t)
    {
        if (t.IsNull())
            o << "NULL";
        else
            o << t.get();
        return o;
    }

    template<class T>
    INLINE void swap( NativeNullable<T>& x, NativeNullable<T>& y )
    {
        if ( x.IsNull() && !y.IsNull())
        {
            x = y;
            y = NativeNullable<T>();
        }
        else if ( !x.IsNull() && y.IsNull())
        {
            y = x;
            x = NativeNullable<T>();
        }
        else if ( !x.IsNull() && !y.IsNull())
        {
            using std::swap;
            swap(*x,*y);
        }
    }
#pragma endregion ContainerRegion
#pragma region GlobalOperators
    // Scope has different rules for comparing nullable types. By Scope's definition, null value is the largest.
    template<typename T>
    INLINE int ScopeTypeCompare(const NativeNullable<T>& x, const NativeNullable<T>& y)
    {
        if (x.IsNull())
        {
            return y.IsNull() ? 0 : 1;
        }

        return y.IsNull() ? -1 : ScopeTypeCompare(x.get(), y.get());
    }

    template<typename ToType, typename FromType>
    struct ScopeCast<ToType, NativeNullable<FromType>>
    {
        static ToType get(const NativeNullable<FromType>& value)
        {
            return static_cast<ToType>(value.safe_get());
        }
    };

    template<typename ToType, typename FromType>
    struct ScopeCast<NativeNullable<ToType>, NativeNullable<FromType>>
    {
        static NativeNullable<ToType> get(const NativeNullable<FromType>& value)
        {
            return NativeNullable<ToType>(value);
        }
    };

    template<typename ToType>
    struct ScopeCast<ToType, ScopeDecimal>
    {
        static ToType get(const ScopeDecimal& value)
        {
            return value.explicit_cast<ToType>();
        }
    };

    template<typename ToType>
    struct ScopeCast<NativeNullable<ToType>, ScopeDecimal>
    {
        static NativeNullable<ToType> get(const ScopeDecimal& value)
        {
            return NativeNullable<ToType>(value.explicit_cast<ToType>());
        }
    };

    template<>
    struct ScopeCast<NativeNullable<ScopeDecimal>, ScopeDecimal>
    {
        static NativeNullable<ScopeDecimal> get(const ScopeDecimal& value)
        {
            return NativeNullable<ScopeDecimal>(value);
        }
    };

    template<>
    struct ScopeCast<ScopeDecimal, ScopeDecimal>
    {
        static ScopeDecimal get(const ScopeDecimal& value)
        {
            return value;
        }
    };

    template<typename ToType>
    struct ScopeCast<ToType, NativeNullable<ScopeDecimal>>
    {
        static ToType get(const NativeNullable<ScopeDecimal>& value)
        {
            return value.safe_get().explicit_cast<ToType>();
        }
    };

    template<typename ToType>
    struct ScopeCast<NativeNullable<ToType>, NativeNullable<ScopeDecimal>>
    {
        static NativeNullable<ToType> get(const NativeNullable<ScopeDecimal>& value)
        {
            return NativeNullable<ToType>(value);
        }
    };

    template<>
    struct ScopeCast<NativeNullable<ScopeDecimal>, NativeNullable<ScopeDecimal>>
    {
        static NativeNullable<ScopeDecimal> get(const NativeNullable<ScopeDecimal>& value)
        {
            return value;
        }
    };

    template<>
    struct ScopeCast<ScopeDecimal, NativeNullable<ScopeDecimal>>
    {
        static ScopeDecimal get(const NativeNullable<ScopeDecimal>& value)
        {
            return value.safe_get();
        }
    };

#pragma endregion GlobalOperators

    // default implementation for non-atomic types
    template<typename T>
    INLINE int ScopeTypeCompare_PTS(const T & x, const T & y)
    {
        return x.Compare(y);
    }

    // specializations for atomic types
    template<>
    INLINE int ScopeTypeCompare_PTS<int>(const int & x, const int & y)
    {
        return (x < y ? -1 : x == y ? 0 : 1);
    }

    // specializations for atomic types
    template<>
    INLINE int ScopeTypeCompare_PTS<__int64>(const __int64 & x, const __int64 & y)
    {
        return (x < y ? -1 : x == y ? 0 : 1);
    }

    template<>
    INLINE int ScopeTypeCompare_PTS<bool>(const bool & x, const bool & y)
    {
        return (x < y ? -1 : x == y ? 0 : 1);
    }

    template<>
    INLINE int ScopeTypeCompare_PTS<float>(const float & x, const float & y)
    {
        if (_isnan(x))
        {
            return _isnan(y) ? ScopeTypeCompare_PTS<int>(*(int*)&x, *(int*)&y) : -1;
        }

        return (x < y ? -1 : x == y ? 0 : 1);
    }

    template<>
    INLINE int ScopeTypeCompare_PTS<double>(const double & x, const double & y)
    {
        if (_isnan(x))
        {
            return _isnan(y) ? ScopeTypeCompare_PTS<__int64>(*(__int64*)&x, *(__int64*)&y) : -1;
        }

        return (x < y ? -1 : x == y ? 0 : 1);
    }

    // Check whether a type is atomic
    template<typename T>
    class is_atomic_pts
    {
    public:
        enum { value = false };
    };

    template<>
    class is_atomic_pts<bool>
    {
    public:
        enum { value = true };
    };

    template<>
    class is_atomic_pts<unsigned char>
    {
    public:
        enum { value = true };
    };

    template<>
    class is_atomic_pts<char>
    {
    public:
        enum { value = true };
    };

    template<>
    class is_atomic_pts<short>
    {
    public:
        enum { value = true };
    };

    template<>
    class is_atomic_pts<unsigned short>
    {
    public:
        enum { value = true };
    };

    template<>
    class is_atomic_pts<wchar_t>
    {
    public:
        enum { value = true };
    };

    template<>
    class is_atomic_pts<int>
    {
    public:
        enum { value = true };
    };

    template<>
    class is_atomic_pts<unsigned int>
    {
    public:
        enum { value = true };
    };

    template<>
    class is_atomic_pts<__int64>
    {
    public:
        enum { value = true };
    };

    template<>
    class is_atomic_pts<unsigned __int64>
    {
    public:
        enum { value = true };
    };

    template<>
    class is_atomic_pts<float>
    {
    public:
        enum { value = true };
    };

    template<>
    class is_atomic_pts<double>
    {
    public:
        enum { value = true };
    };

    template<typename T, bool ATOMIC=is_atomic_pts<T>::value>
    struct IsNullablePrimaryTypeTraits_PTS
    {
        enum { val = T::isNullablePrimaryType };
    };

    template<typename T>
    struct IsNullablePrimaryTypeTraits_PTS<T, true>
    {
        enum { val = false };
    };

    template<typename T, bool ATOMIC=is_atomic_pts<T>::value>
    class is_floating_point_pts
    {
    public:
        enum { value = T::is_floating_point };
    };

    template<typename T>
    class is_floating_point_pts<T, true> : public std::is_floating_point<T>
    {
    };

    template<typename T, bool ATOMIC=is_atomic_pts<T>::value>
    class is_signed_pts
    {
    public:
        enum { value = T::is_signed };
    };

    template<typename T>
    class is_signed_pts<T, true> : public std::is_signed<T>
    {
    };

    template<typename T, bool ATOMIC=is_atomic_pts<T>::value>
    class is_unsigned_pts
    {
    public:
        enum { value = T::is_unsigned };
    };

    template<typename T>
    class is_unsigned_pts<T, true> : public std::is_unsigned<T>
    {
    };

    template<typename T, bool ATOMIC=is_atomic_pts<T>::value>
    class is_integral_pts
    {
    public:
        enum { value = T::is_integral };
    };

    template<typename T>
    class is_integral_pts<T, true> : public std::is_integral<T>
    {
    };


    template<typename T, bool ATOMIC=is_atomic_pts<T>::value>
    class equal_to_pts
    {
    public:
        bool operator()(const T & left, const T & right) const
        {
            return left.Compare(right) == 0;
        }
    };

    template<typename T>
    class equal_to_pts<T, true> : public std::equal_to<T>
    {
    };

    template<typename T, bool ATOMIC=is_atomic_pts<T>::value>
    class hash_pts
    {
    public:
        size_t operator()(const T & value) const
        {
            return value.GetHashCode();
        }
    };

    template<typename T>
    class hash_pts<T, true> : public std::hash<T>
    {
    };

    template<typename T, bool ATOMIC=is_atomic_pts<T>::value>
    class numeric_limits_pts
    {
    public:
        static T min()
        {
            return T::Min();
        }

        static T max()
        {
            // return max value
            return T::Max();
        }
    };

    template<typename T>
    class numeric_limits_pts<T, true> : public std::numeric_limits<T>
    {
    };

    template<typename T, bool ATOMIC=is_atomic_pts<T>::value>
    struct need_deep_copy_traits_pts
    {
        enum { val = T::need_deep_copy };
    };

    template<typename T>
    struct need_deep_copy_traits_pts<T, true>
    {
        enum { val = false };
    };

    template<typename T, typename COPIER_TYPE, bool NEED_DEEP_COPY=need_deep_copy_traits_pts<T>::val>
    class DeepCopier_PTS
    {
    public:
        static void Copy(T const & from, T & to, COPIER_TYPE & copier)
        {
            copier.Copy(from, to);
        }

        static void Copy(T const & from, T & to)
        {
            to.DeepCopyFrom(from, NULL);
        }
    };

    template<typename T, typename COPIER_TYPE>
    class DeepCopier_PTS<T, COPIER_TYPE, false>
    {
    public:
        static INLINE void Copy(T const & from, T & to, COPIER_TYPE & copier)
        {
            to = from;
        }

        static void Copy(T const & from, T & to)
        {
            to = from;
        }
    };

    template<typename T, bool ATOMIC=is_atomic_pts<T>::value>
    class ShallowCopier_PTS
    {
    public:
        static void Copy(T const & from, T & to)
        {
            to = from;
        }
    };

    template<typename T>
    class ShallowCopier_PTS<T, false>
    {
    public:
        static INLINE void Copy(T const & from, T & to)
        {
            to.ShallowCopyFrom(from);
        }
    };

    template<typename T, bool NULLABLE=IsNullablePrimaryTypeTraits_PTS<T>::val>
    class NullHelper_PTS
    {
    public:
        static void SetNull(T & value)
        {
            value.SetNull();
        }

        static bool IsNull(T const & value)
        {
            return value.IsNull();
        }

        static bool UpdateToLargerValue(T const & lhs, T const & rhs)
        {
            if (!rhs.IsNull() && lhs.IsNull() || ScopeTypeCompare_PTS(lhs, rhs) < 0)
            {
                return true;
            }

            return false;
        }

        static bool UpdateToSmallerValue(T const & lhs, T const & rhs)
        {
            if (!rhs.IsNull() && lhs.IsNull() || ScopeTypeCompare_PTS(lhs, rhs) > 0)
            {
                return true;
            }

            return false;
        }
    };

    template<typename T>
    class NullHelper_PTS<T, false>
    {
    public:
        static INLINE void SetNull(T & value)
        {
            value = T();
        }

        static INLINE bool IsNull(T const & value)
        {
            return false;
        }

        static bool UpdateToLargerValue(T const & lhs, T const & rhs)
        {
            if (ScopeTypeCompare_PTS(lhs, rhs) < 0)
            {
                return true;
            }

            return false;
        }

        static bool UpdateToSmallerValue(T const & lhs, T const & rhs)
        {
            if (ScopeTypeCompare_PTS(lhs, rhs) > 0)
            {
                return true;
            }

            return false;
        }
    };

    template<typename T, bool ATOMIC=is_atomic_pts<T>::value>
    class NumericOpHelper_PTS
    {
    public:
        static T Multiply(T const & lhs, T const & rhs)
        {
            return lhs.Multiply(rhs);
        }

        static T & AddBy(T & lhs, T const & rhs)
        {
            return lhs.AddByUnchecked(rhs);
        }

        static T & SetZero(T & lhs)
        {
            return lhs.SetZero();
        }
    };

    template<typename T>
    class NumericOpHelper_PTS<T, true>
    {
    public:
        static T Multiply(T const & lhs, T const & rhs)
        {
            return lhs + rhs;
        }

        static T & AddBy(T & lhs, T const & rhs)
        {
            return lhs += rhs;
        }

        static T & SetZero(T & lhs)
        {
            return lhs = 0;
        }
    };
}// end namespace ScopeEngine

namespace ScopeEngine
{
#ifdef PLUGIN_TYPE_SYSTEM_NAMESPACE
using namespace PluginType;
#endif
    namespace SSLibV3
    {
        template <>
        INLINE
        ScopeDateTime ColumnIterator::Data() const
        {
            __int64 *bits = reinterpret_cast<__int64 *>(DataRaw());

            return ScopeDateTime::FromBinary(*bits);
        }

        template <>
        INLINE
        ScopeDecimal ColumnIterator::Data() const
        {
            ScopeDecimal d;
            int* bits = reinterpret_cast<int*>(DataRaw());

            d.Reset(bits[2], bits[1], bits[0], bits[3]);

            return d;
        }
    }

#pragma region I/O Declarations

    template<typename Schema>
    struct SStreamDeserializer
    {
        typedef bool (*V3Fn)(ScopeEngine::SSLibV3::ColumnIterator * iters, Schema & row, IncrementalAllocator * alloc);
    };

    template<typename Schema, int UID>
    class SStreamPartitionWriter
    {
    public:
        SStreamPartitionWriter(std::string* filenames, int fileCnt, SIZE_T bufSize, int bufCnt, const string& outputMetadataFileName, bool preferSSD, bool enableBloomFilter);

        void Init();
        void GetPartitionInfo(PartitionMetadata* payload);
        bool ValidPartition();
        void AppendRow(Schema & output);
        void Close();
        void WriteRuntimeStats(TreeNode & root);
        // the following methods are for SplitOutputter
        void WriteMetadata(PartitionMetadata* metadata);
        void Flush();
        void Finish();
    };

    //
    // Placeholder for the input stream parameters to pass them from the generated code through the Extractor class down to the InputStream class
    //
    struct InputStreamParameters
    {
        UINT64 delimiter;
        UINT64 rowDelimiter;
        UINT64 escapeChar;
        bool escape;
        bool textQualifier;
        TextEncoding encoding;
        bool validateEncoding;
        bool silent;
        const char * nullEscape;
        int skipFirstRowsCount;

        InputStreamParameters()
            : delimiter(),
              rowDelimiter(),
              escapeChar(),
              nullEscape(nullptr),
              escape(),
              textQualifier(),
              encoding(),
              validateEncoding(),
              silent(),
              skipFirstRowsCount(0)
        {
        }

        InputStreamParameters(UINT64        _delimiter,
                              UINT64        _rowDelimiter,
                              UINT64        _escapeChar,
                              const char *  _nullEscape,
                              bool          _escape,
                              bool          _textQualifier,
                              TextEncoding  _encoding,
                              bool          _validateEncoding,
                              bool          _silent,
                              int           _skipFirstRowsCount)
            : delimiter(_delimiter),
              rowDelimiter(_rowDelimiter),
              escapeChar(_escapeChar),
              nullEscape(_nullEscape),
              escape(_escape),
              textQualifier(_textQualifier),
              encoding(_encoding),
              validateEncoding(_validateEncoding),
              silent(_silent),
              skipFirstRowsCount(_skipFirstRowsCount)
        {
        }
    };

    //
    // Placeholder for the output stream parameters to pass them from the generated code through the Extractor class down to the OutputStream class
    //
    struct OutputStreamParameters
    {
        ULONG delimiter;
        ULONG escapeCharacter;
        bool escape;
        bool escapeDelimiter;
        bool textQualifier;
        bool doubleToFloat;
        TextEncoding encoding;
        bool validateEncoding;

        // (usingDateTimeFormat == true) IFF (dateTimeFormat != NULL)
        bool usingDateTimeFormat;
        const char * dateTimeFormat;

        const char * rowDelimiter;
        const char * nullEscape;

        OutputStreamParameters()
            : delimiter(),
              rowDelimiter(nullptr),
              escapeCharacter(),
              nullEscape(nullptr),
              escape(),
              escapeDelimiter(),
              textQualifier(),
              doubleToFloat(),
              usingDateTimeFormat(),
              dateTimeFormat(NULL),
              encoding(),
              validateEncoding()
        {
        }

        OutputStreamParameters(ULONG         _delimiter,
                               const char *  _rowDelimiter,
                               ULONG         _escapeCharacter,
                               const char *  _nullEscape,
                               bool          _escape,
                               bool          _escapeDelimiter,
                               bool          _textQualifier,
                               bool          _doubleToFloat,
                               const char *  _dateTimeFormat,
                               TextEncoding  _encoding,
                               bool          _validateEncoding)
            : delimiter(_delimiter),
              rowDelimiter(_rowDelimiter),
              escapeCharacter(_escapeCharacter),
              nullEscape(_nullEscape),
              escape(_escape),
              escapeDelimiter(_escapeDelimiter),
              textQualifier(_textQualifier),
              doubleToFloat(_doubleToFloat),
              usingDateTimeFormat(_dateTimeFormat != NULL), dateTimeFormat(_dateTimeFormat),
              encoding(_encoding),
              validateEncoding(_validateEncoding)
        {
        }

        OutputStreamParameters(const OutputStreamParameters & other)
            : delimiter(other.delimiter),
              rowDelimiter(other.rowDelimiter),
              escapeCharacter(other.escapeCharacter),
              nullEscape(other.nullEscape),
              escape(other.escape),
              escapeDelimiter(other.escapeDelimiter),
              textQualifier(other.textQualifier),
              doubleToFloat(other.doubleToFloat),
              usingDateTimeFormat(other.usingDateTimeFormat),
              dateTimeFormat(other.dateTimeFormat),
              encoding(other.encoding),
              validateEncoding(other.validateEncoding)
        {
        }
    };

    struct OutputSStreamParameters
    {
        int columngroupCnt;
        bool preferSSD;
        string metataStream;

        OutputSStreamParameters(int _columngroupCnt, bool _preferSSD, const string& _metadataStream)
            : columngroupCnt(_columngroupCnt), preferSSD(_preferSSD), metataStream(_metadataStream)
        {
        }
    };

#pragma endregion I/O Declarations

#pragma region PolicyRegion

    // Template to define schema binary deserialization
    template<typename Schema>
    class BinaryExtractPolicy
    {
    public:
        // Binary deserialization routine (from intermediate format)
        static bool Deserialize(BinaryInputStream * istream, Schema & row);
    };

    // Template to define schema binary serialization
    template<typename Schema>
    class BinaryOutputPolicy
    {
    public:
        // Binary serialization routine (into intermediate format)
        static void Serialize(BinaryOutputStream * ostream, Schema & row);
    };

    // Template to define schema binary serialization for the iSCOPE (ConsoleOutputter)
    template<typename Schema>
    class ConsoleOutputPolicy
    {
    public:
        // Binary serialization routine (into iSCOPE format)
        static void Serialize(BinaryOutputStream * ostream, Schema & row);
    };

    // Template to define schema text deserialization
    // Last template parameter (of "int" type) is an operator UID
    template<typename Schema, int = -1>
    class TextExtractPolicy
    {
    public:
        // Text deserialization routine (from "DefaultTextExtractor" format)
        static bool Deserialize(void * textInputStream, Schema & row);
    };

    // Template to define schema http deserialization
    // Last template parameter (of "int" type) is an operator UID
    template<typename Schema, int = -1>
    class HttpExtractPolicy
    {
    public:
        // Text deserialization routine (from "DefaultTextExtractor" format)
        static bool Deserialize(void * httpInputStream, Schema & row);
    };

    // Template to define schema parameters deserialization
    template<typename Schema>
    class ParameterExtractPolicy
    {
    public:
        // Text deserialization routine
        static void Deserialize(const vector<ParameterValue> & parameterValues, Schema & parameterStructure, IncrementalAllocator * alloc);
    };

    // Template to define schema serialization
    // Last template parameter (of "int" type) is an operator UID
    template<typename Schema, int = -1>
    class TextOutputPolicy
    {
    public:
        // Text serialization routine (into "DefaultTextOutputer" format)
        static void Serialize(void * textOutputStream, Schema & row);
    };

    template<typename Schema, int UID>
    class SplitterSStreamV3OutputPolicy
    {
    public:
        static void Serialize(SStreamPartitionWriter<Schema, UID>* output, Schema& row)
        {
            output->AppendRow(row);
        }
    };

    // Template to define schema sstream deserialization
    // Last template parameter (of "int" type) is an operator UID
    template<typename Schema, int UID>
    class SStreamV2ExtractPolicy
    {
    };

    // this is used only by the metadata processing and it matches the managed StructuredStream.PartitioningType enum
    enum PartitioningType
    {
        Invalid, // iscope uses this as the metadata processing is not available
        RandomPartition,
        HashPartition,
        RangePartition,
        DirectHashPartition,
    };

    class NullSchema
    {
    public:
        NullSchema() {}
        NullSchema(const NullSchema&, IncrementalAllocator*) {}
    };

    // Correlated parameters assign policy
    template<int>
    class CorrelatedParametersPolicy
    {
        static void CopyValues(const void* /*full schema*/ from, void* /*parameters schema*/ to);
    };

    // Template to define schema sstream deserialization
    // <Schema, Operator_UID, CorrelatedParametersSchema>
    template<typename Schema, int = -1, typename ParametersSchema = NullSchema>
    class SStreamV3ExtractPolicy
    {
    public:

        static BYTE m_dataColumnSizes[];
        static BYTE m_indexColumnSizes[];
        // sstream deserialization routine (from sstream)
        static bool Deserialize(ScopeEngine::SSLibV3::ColumnIterator* iters, Schema & row, IncrementalAllocator* alloc);
    };

    // Template to define schema sstream deserialization
    // Last template parameter (of "int" type) is an operator UID
    template<typename Schema, int = -1>
    class SStreamV3OutputPolicy
    {
    public:

        // sstream serialization routine
        static void SerializeRow(HANDLE rowHandle, AutoBuffer* buffer, vector<int>& offsets);
    };

    //template to define key compare algorithm for a pair<schema type, operator UID>
    template<class Schema,int>
    class KeyComparePolicy
    {
    public:
        // default key type
        // specialize according to schema
        typedef int KeyType;

        // compare key value from key and schema objects
        static int Compare(Schema & row, KeyType & key);

        // compare key value from two schema objects
        static int Compare(Schema * n1, Schema * n2);

        // Key function for MKQsort algorithm
        static __int64 Key(Schema * p, int depth);

        // End of Key function for MKQSort algorithm
        static bool EofKey(Schema * p, int depth);
    };

    //template to define row hash algorithm for a pair<schema type, operator UID>
    template<class Schema,int>
    class RowHashPolicy
    {
    public:
        static int Hash(Schema* row);
    };

    template<class Schema, int>
    class IndexedPartitionRowPolicy
    {
    public:
        static void AttachPartitionID(Schema& input, Schema& output, int partitionID);
    };

    //template to define key compare algorithm for combiner.
    template<typename LeftSchema, typename RightSchema, int>
    class RowComparePolicy
    {
    public:
        // compare key from left and right schema object
        static int Compare(LeftSchema * left, RightSchema * right);
    };

    //template to define schema transformation
    template<typename InputSchema, typename OutputSchema, int>
    class RowTransformPolicy
    {
    public:
        // Initialize the policy using this managed row
        void Init(ManagedRow<InputSchema> * managedRow);

        // transform input -> output
        // returns false if row is filtered out (in this case "output" value undefined)
        bool FilterTransformRow(InputSchema & input, OutputSchema & output, IncrementalAllocator * alloc);
    };

    // template to define row generation policy for RowGenerator
    template<typename OutputSchema, int>
    class RowGeneratePolicy
    {
    };

    // Template for the split-output operator
    template<typename InputSchema, int>
    class SplitPolicy
    {
    public:
        // Init SplitOutput chain
        void Init();

        // Close SpitOutput chain
        void Close();

        // Write metadata
        void ProcessMetadata(PartitionMetadata * metadata);

        // Process row
        void ProcessRow(InputSchema & inputRow);

        // return total wait time on all operations
        LONGLONG GetTotalIoWaitTime();

        // Write statistics
        void WriteRuntimeStats(TreeNode & root);

        // flush the output buffer
        void FlushOutput(bool forcePersistent = false);

        // checkpoint
        void DoScopeCEPCheckpoint(BinaryOutputStream & output);

        // load checkpoint
        void LoadScopeCEPCheckpoint(BinaryInputStream & input);
    };

    // Factory class for the SplitPolicy
    template<typename InputSchema, int UID>
    class SplitPolicyFactory
    {
    public:
        // create a new SplitPolicy, but in managed code to honor ODR rule and avoid problems.
        static SplitPolicy<InputSchema, UID> * Create(std::string * outputFileNames, SIZE_T outputBufSize, int outputBufCnt);
    };

    template<typename InputSchema, int UID>
    INLINE SplitPolicy<InputSchema, UID> * SplitPolicyFactory<InputSchema, UID>::Create(std::string * outputFileNames, SIZE_T outputBufSize, int outputBufCnt)
    {
        return new SplitPolicy<InputSchema, UID>(outputFileNames, outputBufSize, outputBufCnt);
    }

    template<class OutputStream, class InputSchema>
    class StreamingOutputChecking
    {
    private:
        BYTE* m_firstRow;
        int   m_firstRowSize;
        bool  m_isFirstRow;
        BinaryOutputStream* m_checkpoint;

        static const char* s_magicString;
        static const int s_magicStringLength = 4;

    public:
        StreamingOutputChecking() : m_firstRow(nullptr), m_firstRowSize(0), m_isFirstRow(true), m_checkpoint(nullptr)
        {
        }

        ~StreamingOutputChecking()
        {
            if (m_firstRow != nullptr)
            {
                delete[] m_firstRow;
                m_firstRow = nullptr;
            }
        }

        INLINE void SetCheckpoint(BinaryOutputStream* checkpoint)
        {
            m_checkpoint = checkpoint;
        }

        INLINE void CheckFirstRowData(const BYTE* firstRow, int rowSize)
        {
            if (!m_isFirstRow)
            {
                return;
            }

            m_isFirstRow = false;
            if (m_firstRow != nullptr)
            {
                std::stringstream ss;
                if (m_firstRowSize != rowSize || memcmp(m_firstRow, firstRow, rowSize) != 0)
                {
                    ss << endl;
                    if (m_firstRowSize != rowSize)
                    {
                        ss << "row size mismatch. atcual = " << rowSize << ", but expected = " << m_firstRowSize << endl;
                    }

                    int dumpBytes = std::min(rowSize, 256);
                    ss << "actual first " << dumpBytes << " bytes:" << endl;
                    ss << std::hex;
                    for (int i = 0; i < dumpBytes; i++)
                    {
                        ss << (firstRow[i] >> 4) << (firstRow[i] & 0x0F) << " " << std::hex;
                    }
                    ss << endl;

                    dumpBytes = std::min(m_firstRowSize, 256);
                    ss << std::dec << "expected first " << dumpBytes << " bytes:" << endl;
                    ss << std::hex;
                    for (int i = 0; i < dumpBytes; i++)
                    {
                        ss << (m_firstRow[i] >> 4) << (m_firstRow[i] & 0x0F) << " ";
                    }
                }

                delete[] m_firstRow;
                m_firstRow = nullptr;
                m_firstRowSize = 0;

                if (ss.tellp() > 0)
                {
                    throw RuntimeException(E_USER_MISMATCH_ROW, ss.str());
                }
            }
        }

        INLINE void CheckFirstRow(OutputStream& output, int rowSize)
        {
            if (!m_isFirstRow)
            {
                return;
            }

            m_isFirstRow = false;
            if (m_firstRow != nullptr)
            {
                BYTE* firstRow = new BYTE[rowSize];
                int readBytes = output.GetOutputer().ReadBack(firstRow, rowSize);
                SCOPE_ASSERT(readBytes == rowSize);
                CheckFirstRowData(firstRow, rowSize);
                delete[] firstRow;
                firstRow = nullptr;
            }
        }

        INLINE void WriteRowToCheckpoint(OutputStream& outputStream, InputSchema& row, int rowSize)
        {
            WriteRowToCheckpoint(outputStream, row, rowSize, true);
        }

        INLINE void WriteRowToCheckpoint(OutputStream& outputStream, InputSchema& row, int rowSize, bool closeCheckpoint)
        {
            if (m_checkpoint == nullptr)
            {
                return;
            }

            BYTE* pBuf = new BYTE[rowSize];
            int readBytes = outputStream.GetOutputer().ReadBack(pBuf, rowSize);
            SCOPE_ASSERT(readBytes == rowSize);
#ifdef SCOPE_DEBUG
            cout << row << endl;
#endif
            WriteDataToCheckpoint(pBuf, rowSize, closeCheckpoint);

            delete[] pBuf;
            pBuf = nullptr;
        }

        INLINE void WriteDataToCheckpoint(const BYTE* pBuf, int bufSize)
        {
            WriteDataToCheckpoint(pBuf, bufSize, true);
        }

        INLINE void WriteDataToCheckpoint(const BYTE* pBuf, int bufSize, bool closeCheckpoint)
        {
            if (m_checkpoint == nullptr)
            {
                return;
            }

            m_checkpoint->Write(s_magicString, (int)strlen(s_magicString));
            m_checkpoint->Write(bufSize);
            m_checkpoint->Write((const char*)pBuf, bufSize);
            if (closeCheckpoint)
            {
                m_checkpoint->Finish();
                m_checkpoint->Close();
                delete m_checkpoint;
            }

            m_checkpoint = nullptr;
        }

        INLINE void GetFirstRowFromCheckpoint(BinaryInputStream& input)
        {
            SCOPE_ASSERT(m_firstRow == nullptr);
            try
            {
                char magicString[s_magicStringLength];
                int bytesRead = input.Read(magicString, s_magicStringLength);
                // bytesRead == 0 means EndOfStream
                if (bytesRead > 0)
                {
                    SCOPE_ASSERT(bytesRead == s_magicStringLength);
                    SCOPE_ASSERT(0 == memcmp(magicString, s_magicString, s_magicStringLength));
                    input.Read(m_firstRowSize);
                }
            }
            catch (ScopeStreamException ex)
            {
                if (ex.Error() != ScopeStreamException::EndOfFile)
                {
                    throw;
                }

                m_firstRowSize = 0;
            }

            if (m_firstRowSize > 0)
            {
                m_firstRow = new BYTE[m_firstRowSize];
                int bytesRead = input.Read((char*)m_firstRow, m_firstRowSize);
                SCOPE_ASSERT(bytesRead == m_firstRowSize);
            }
        }
    };
    template<class OutputStream, class InputSchema>
    const char* StreamingOutputChecking<OutputStream, InputSchema>::s_magicString = "SOCK";


    template <class Outputer, class InputSchema, class OutputType, class OutputStream, bool generateSN>
    class StreamingOutputCTIProcessing
    {
        ScopeDateTime m_lastUpdateCacheMetaCti;
    public:
        void DispatchCTIToOutput(InputSchema& ctiRecord,
            StreamingOutputChannel* streamingChannel,
            OutputStream* outputStream)
        {
            ScopeDateTime ctiTime = ctiRecord.GetScopeCEPEventStartTime();
            __int64 ctiTimeBinary = ctiTime.Ticks();
            if (ctiTimeBinary > 0 && ctiTimeBinary < ScopeDateTime::FileTimeOffset)
            {
                stringstream ss;
                char buf[256];
                ctiTime.ToString(buf, _countof(buf));
                ss << "An invalid CTI value is provided: " << buf << ". CTI has to be either DateTime.MinValue or a value larger than 1601/1/1";
                throw RuntimeException(E_USER_INVALID_CTI, ss.str().c_str());
            }

            if (!streamingChannel->TryAdvanceCTI(ctiRecord.GetScopeCEPEventStartTime(), ScopeDateTime::MaxValue, true))
            {
                // write the CTI to the output and flush so that we will have the CTI record at both the end of the
                // previous output and the beginning of the new one.
                if (generateSN)
                {
                    outputStream->Write(g_scopeCEPCheckpointManager->GetCurrentSeqNumber());
                }

                SCOPE_LOG_FMT_INFO(
                    "StreamingOutputCTIProcessing",
                    "Appending terminating CTI record to the stream: %s",
                    outputStream->GetOutputer().StreamName().c_str());

                OutputType::Serialize(outputStream, ctiRecord);
                outputStream->Commit();

                ULONGLONG start = GetTickCount64();

                // Flush the terminating CTI to the output with the forcePersist flag set to true. This is done to avoid
                // race condition between the original vertex, and a possible duplicate vertex. Consider a scenario where:
                // 1. Duplicate vertex is scheduled, while original vertex is still running.
                // 2. Duplicate vertex would first seal the output stream, and append a CTI record since last checkpoint
                //    to the output stream.
                // 3. The original vertex, if the CTI has advanced past the hourly window, would then attempt to flush
                //    the last CTI of the hour to the stream. That would fail since the last offset has been changed
                //    by the duplicate vertex.
                // 4. This method, without forcePersist flag set to true, would move ahead and advance the CTI. As part
                //    of advancing the CTI, the underlying streamingChannel would seal the hourly stream, and start a
                //    a new stream for the next hour.
                // Net effect: The stream that was sealed would lose its terminating CTI thereby causing a violation of 
                // contract with our customers.
                //
                // We forcePersist the write in the Flush call below to address this race condition. It avoids the race
                // condition by ensuring that the CTI is advanced *only* when the terminating CTI has been successfully
                // appended to the output stream.
                // This also means we are spending extra time to wait for the return value of the flush operation. 
                // The trade-off makes sense here because:
                // 1. The code block is executed *once* per each time the CTI passes the boundary. In most cases, 
                //    this means once per hour, reducing any meaningful impact to the pipeline's latency.
                // 2. The change is small, and well scoped, and does not impact the rest of the logic of StreamingBlockDevice.
                // Please see the bug http://sqlbuvsts01:8080/Main/Cosmos/_workitems/edit/7570045 if interested in more details.
                outputStream->Flush(true /*forcePersist*/);
                
                SCOPE_LOG_FMT_INFO(
                    "StreamingOutputCTIProcessing", 
                    "Completed appending the terminating CTI to stream: %s in %I64u milliseconds",
                    outputStream->GetOutputer().StreamName().c_str(),
                    GetTickCount64() - start);

                if (!streamingChannel->TryAdvanceCTI(ctiRecord.GetScopeCEPEventStartTime(), ScopeDateTime::MaxValue, false))
                {
                    throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "AdvanceCTI fail with no buffered data.");
                }

                if (generateSN)
                {
                    outputStream->Write(g_scopeCEPCheckpointManager->GetCurrentSeqNumber());
                }

                OutputType::Serialize(outputStream, ctiRecord);
                outputStream->Commit();
            }
            else
            {
                if (generateSN)
                {
                    outputStream->Write(g_scopeCEPCheckpointManager->GetCurrentSeqNumber());
                }

                OutputType::Serialize(outputStream, ctiRecord);
                outputStream->Commit();
                // TODO: flush the output at certain interval to prevent stall
            }

            if (generateSN &&
                (m_lastUpdateCacheMetaCti == ScopeDateTime::MinValue ||
                 m_lastUpdateCacheMetaCti == ctiRecord.GetScopeCEPEventStartTime() ||
                 ctiRecord.GetScopeCEPEventStartTime() >= m_lastUpdateCacheMetaCti.AddMinutes(2)))
            {
                m_lastUpdateCacheMetaCti = ctiRecord.GetScopeCEPEventStartTime();
                char* pBuffer;
                UINT32 length;
                streamingChannel->GetCacheMetadata(pBuffer, length);
                if (length != 0)
                {
                    outputStream->Write(ScopeCEPCheckpointManager::SYSTEM_RESERVED_SN_FOR_CACHE_METADATA);
                    outputStream->Write(length);
                    ((BinaryOutputStream*)outputStream)->Write(pBuffer, length);
                }
            }
        }

        void WriteFinalRow(OutputStream* outputStream)
        {
            if (generateSN)
            {
                InputSchema finalRecord;
                finalRecord.ResetScopeCEPStatus(ScopeDateTime::MaxValue, ScopeDateTime::MaxValue, SCOPECEP_FINAL_ROW);
                g_scopeCEPCheckpointManager->IncrementSeqNumber();
                outputStream->Write(g_scopeCEPCheckpointManager->GetCurrentSeqNumber());
                OutputType::Serialize(outputStream, finalRecord);
                outputStream->Commit();
            }
        }
    };

    //
    // Marks the end of split output chain
    //
    template<typename InputSchema>
    class SplitOutputterSentinel : public ExecutionStats
    {
    public:
        typedef typename InputSchema InputSchema;

        void Init(ManagedRow<InputSchema> *)
        {
        }

        void ProcessMetadata(PartitionMetadata *)
        {
        }

        void ProcessRow(InputSchema &)
        {
        }

        void Close()
        {
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        LONGLONG GetTotalIoWaitTime()
        {
            return 0;
        }

        LONGLONG GetInclusiveTimeMillisecond()
        {
            return 0;
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }

        void FlushOutput(bool forcePersistent = false)
        {
            (forcePersistent);
        }

        void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
        }

        void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
        }

        void SetCheckpoint(BinaryOutputStream*)
        {
        }

        void LoadFirstRowFromCheckpoint(BinaryInputStream&)
        {
        }
    };

    //
    // Element of split output chain
    //
    template <typename NextOperator, typename OutputSchema, typename OutputType, typename OutputStream, int UID = -1>
    class SplitOutputter : public ExecutionStats
    {
    protected:
        static const char* const sm_className;

        NextOperator * m_child;  // next operator in split output chain
        OutputSchema   m_output;
        OutputStream   m_stream; // output stream: text, binary, etc
        int            m_operatorId;

        RowEntityAllocator      m_allocator;

        RowTransformPolicy<typename NextOperator::InputSchema,OutputSchema,UID> m_transformPolicy;

    public:
        typedef typename NextOperator::InputSchema InputSchema;

        SplitOutputter(NextOperator * child, std::string & fileName, SIZE_T bufSize, int bufCnt, int operatorId, bool maintainBoundaries = false) :
            m_child(child),
            m_stream(fileName, bufSize, bufCnt, maintainBoundaries),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
            m_operatorId(operatorId)
        {
        }

        SplitOutputter(NextOperator * child, std::string & fileName, SIZE_T bufSize, int bufCnt, int operatorId, const OutputStreamParameters & outputStreamParams, bool maintainBoundaries = false) :
            m_child(child),
            m_stream(fileName, bufSize, bufCnt, outputStreamParams, maintainBoundaries),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
            m_operatorId(operatorId)
        {
        }

        SplitOutputter(NextOperator * child, std::string & fileName, SIZE_T bufSize, int bufCnt, int operatorId, const OutputSStreamParameters & outputSStreamParams) :
            m_child(child),
            m_stream(&fileName, outputSStreamParams.columngroupCnt, bufSize, bufCnt, outputSStreamParams.metataStream, outputSStreamParams.preferSSD, false),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
            m_operatorId(operatorId)
        {
        }

        void Init(ManagedRow<InputSchema> * managedRow)
        {
            m_transformPolicy.Init(managedRow);
            m_stream.Init();

            // pass the row to the next operator in chain
            m_child->Init(managedRow);
        }

        void ProcessMetadata(PartitionMetadata * metadata)
        {
            m_stream.WriteMetadata(metadata);

            // pass metadata to the next operator in chain
            m_child->ProcessMetadata(metadata);
        }

        void ProcessRow(InputSchema & input)
        {
            if (m_transformPolicy.FilterTransformRow(input, m_output, &m_allocator))
            {
                OutputType::Serialize(&m_stream, m_output);
                IncreaseRowCount(1);
            }

            m_allocator.Reset();

            // pass the row to the next operator in chain
            m_child->ProcessRow(input);
        }

        virtual void Close()
        {
            m_stream.Finish();
            m_stream.Close();
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();

            m_child->Close();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            // TODO: We do not evaluate inclusiveTime for this operator since it's managed code and calling Win32 API is expensive
            RuntimeStats::WriteRowCount(node, GetRowCount());
            node.AddAttribute(RuntimeStats::OperatorId(), m_operatorId);

            m_stream.WriteRuntimeStats(node);
            m_allocator.WriteRuntimeStats(node);

            // Pass "root" to the child to make stats looks like two level tree (while in reality it's a list)
            m_child->WriteRuntimeStats(root);
        }

        // return total wait time on all operations
        LONGLONG GetTotalIoWaitTime()
        {
            return m_stream.GetTotalIoWaitTime() + m_child->GetTotalIoWaitTime();
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements(m_transformPolicy.GetOperatorRequirements()).Add(m_child->GetOperatorRequirements());
        }

        void FlushOutput(bool forcePersistent = false)
        {
            m_stream.Flush(forcePersistent);
            m_child->FlushOutput(forcePersistent);
        }

        void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
        }

        void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
        }
    };

    template <typename NextOperator, typename OutputSchema, typename OutputType, typename OutputStream, int UID>
    const char* const SplitOutputter<NextOperator, OutputSchema, OutputType, OutputStream, UID>::sm_className = "SplitOutputter";

    template <typename NextOperator, typename OutputSchema, typename OutputType, typename OutputStream, int RunScopeCEPMode, bool isFinalOutput, bool checkOutput, int UID = -1>
    class StreamingSplitOutputter: public SplitOutputter<NextOperator, OutputSchema, OutputType, OutputStream, UID>
    {
        StreamingOutputChannel* m_streamingChannel;
        StreamingOutputCTIProcessing<StreamingSplitOutputter, OutputSchema, OutputType, OutputStream, !isFinalOutput> m_ctiProcessing;
        std::string m_fileName;
        StreamingOutputChecking<OutputStream, OutputSchema> m_streamingChecking;
        static const char* s_emptyRowInCheckpoint;
        static const int s_emptyRowInCheckpointLength = 8;

    public:
        StreamingSplitOutputter(NextOperator * child, std::string & fileName, SIZE_T bufSize, int bufCnt, int operatorId) :
            SplitOutputter(child, fileName, bufSize, bufCnt, operatorId, RunScopeCEPMode == SCOPECEP_MODE_REAL),
            m_fileName(fileName)
        {
        }

        StreamingSplitOutputter(NextOperator * child, std::string & fileName, SIZE_T bufSize, int bufCnt, int operatorId, const OutputStreamParameters & outputStreamParams) :
            SplitOutputter(child, fileName, bufSize, bufCnt, operatorId, outputStreamParams, RunScopeCEPMode == SCOPECEP_MODE_REAL),
            m_fileName(fileName)
        {
        }

        void Init(ManagedRow<InputSchema> * managedRow)
        {
            SplitOutputter::Init(managedRow);
            m_streamingChannel = IOManager::GetGlobal()->GetStreamingOutputChannel(m_fileName);
            m_streamingChannel->SetAllowDuplicateRecord(true);
        }

        void ProcessRow(InputSchema & input)
        {
            bool inputIsCTI = input.IsScopeCEPCTI();
            if (m_transformPolicy.FilterTransformRow(input, m_output, &m_allocator))
            {
                if (inputIsCTI)
                {
                    SCOPE_ASSERT(m_output.IsScopeCEPCTI());
                    m_ctiProcessing.DispatchCTIToOutput(m_output, m_streamingChannel, &m_stream);
                    g_scopeCEPCheckpointManager->UpdateLastCTITime(m_output.GetScopeCEPEventStartTime());
                }
                else
                {
                    if (!isFinalOutput)
                    {
                        m_stream.Write(g_scopeCEPCheckpointManager->GetCurrentSeqNumber());
                    }

                    SIZE_T curPos = m_stream.GetOutputer().GetCurrentPosition();
                    OutputType::Serialize(&m_stream, m_output);
                    int rowSize = (int)(m_stream.GetOutputer().GetCurrentPosition() - curPos);
                    if (checkOutput && RunScopeCEPMode == SCOPECEP_MODE_REAL)
                    {
                        m_streamingChecking.CheckFirstRow(m_stream, rowSize);
                        // checkingpoint object will be deleted by splitter.
                        // so, it only needs to release the pointer.
                        m_streamingChecking.WriteRowToCheckpoint(m_stream, m_output, rowSize, false);
                    }
                }

                m_stream.Commit();
                IncreaseRowCount(1);
            }
            else if (!inputIsCTI && checkOutput && RunScopeCEPMode == SCOPECEP_MODE_REAL)
            {
                m_streamingChecking.CheckFirstRowData((const BYTE*)s_emptyRowInCheckpoint, s_emptyRowInCheckpointLength);
                m_streamingChecking.WriteDataToCheckpoint((const BYTE*)s_emptyRowInCheckpoint, s_emptyRowInCheckpointLength, false);
            }

            m_allocator.Reset();

            // pass the row to the next operator in chain
            m_child->ProcessRow(input);
        }

        void FlushOutput(bool forcePersist = false)
        {
            // temp: disable auto flush for global splitoutputer since textstream still have trouble with supporting maintainboundary and async flush
            m_stream.Flush(forcePersist);
            m_child->FlushOutput(forcePersist);
        }

        virtual void Close()
        {
            m_ctiProcessing.WriteFinalRow(&m_stream);
            SplitOutputter<NextOperator, OutputSchema, OutputType, OutputStream, UID>::Close();
        }

        void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            m_stream.GetOutputer().SaveState(output);
            m_child->DoScopeCEPCheckpoint(output);
        }

        void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            m_stream.GetOutputer().LoadState(input);
            m_child->LoadScopeCEPCheckpoint(input);
        }

        void SetCheckpoint(BinaryOutputStream* checkpoint)
        {
            if (checkOutput && RunScopeCEPMode == SCOPECEP_MODE_REAL)
            {
                m_streamingChecking.SetCheckpoint(checkpoint);
            }

            m_child->SetCheckpoint(checkpoint);
        }

        void LoadFirstRowFromCheckpoint(BinaryInputStream & input)
        {
            if (checkOutput && RunScopeCEPMode == SCOPECEP_MODE_REAL)
            {
                m_streamingChecking.GetFirstRowFromCheckpoint(input);
            }

            m_child->LoadFirstRowFromCheckpoint(input);
        }
    };
    template <typename NextOperator, typename OutputSchema, typename OutputType, typename OutputStream, int RunScopeCEPMode, bool isFinalOutput, bool checkOutput, int UID = -1>
    const char* StreamingSplitOutputter<NextOperator, OutputSchema, OutputType, OutputStream, RunScopeCEPMode, isFinalOutput, checkOutput, UID>::s_emptyRowInCheckpoint = "EmptyRow";

    //template to assign RANK to the fields
    template<typename RowSchema, int>
    class RowRankPolicy
    {
    public:
        // copy corresponding column from input to output
        static void SetRank(RowSchema * row, __int64 rank);
    };

    //template to perform construction of interval schema for histogram collector
    template<typename RowSchema, typename IntervalSchema, int UID>
    class BucketRowPolicy
    {
    public:
        // make bucket row using bottom boundary, top boundary and bucket count
        static void MakeBucketRow(typename KeyComparePolicy<RowSchema,UID>::KeyType * bottom, typename KeyComparePolicy<RowSchema,UID>::KeyType * top, __int64 dataSize, __int64 rowCount, IntervalSchema * bucket);
    };

    // policy template for hash aggregators
    template <typename InputSchema, typename OutputSchema, int>
    class HashAggregationPolicy
    {
    public:
        typedef NullSchema KeySchema;
        typedef NullSchema StateSchema;

        struct Hash {};
        struct EqualTo {};

    public:
        static const SIZE_T m_memoryQuota = 0;

    public:
        // Shallow copy input schema fields to key schema
        static void GetKey(const InputSchema & row, KeySchema & key);

        // Init state schema fields with aggregate defaults
        static void GetDefaultState(const InputSchema & row, StateSchema & defaultState);

        // Aggregate state fields with row fields
        template <typename Hashtable>
        static typename Hashtable::EResult InsertOrUpdateState(const KeySchema & key, const StateSchema & defaultState, const InputSchema & row, Hashtable & hashtable);

        // Shallow copy key and aggregated values to the output
        static void GetOutput(const KeySchema & key, const StateSchema & state, OutputSchema & row);
    };

    // policy template for hash aggregators v2
    template <typename InputSchema, typename OutputSchema, int>
    class HashAggregationPolicyV2
    {
    };

    //template to define stream aggregation
    template<typename InputSchema, typename OutputSchema, int>
    class AggregationPolicy
    {
    public:
        // Clear any state
        void Reset();

        // begin key range (copy grouping key to output)
        void BeginKey(typename KeyComparePolicy<InputSchema,-1>::KeyType * key, OutputSchema * output);

        // process input row, called for each row in key range
        void AddRow(InputSchema * input);

        // write aggregated data, called after key is completely scanned
        void Aggregate(OutputSchema * output);

        void WriteRuntimeStats(TreeNode & root);

        OperatorRequirements GetOperatorRequirements();
    };

    //template to define stream rollup
    template<typename InputSchema, typename OutputSchema, int>
    class RollupPolicy
    {
    public:
        void Finalize(int level);
        bool Outputting();
        void GetNextRow(typename KeyComparePolicy<InputSchema,-1>::KeyType * key, OutputSchema * output);
        void AddRow(InputSchema * input);
        void WriteRuntimeStats(TreeNode & root);
        OperatorRequirements GetOperatorRequirements();
    };

    //template to define window aggregation
    template<typename InputSchema, typename OutputSchema, int>
    class WindowPolicy
    {
    public:
        // begin key range (copy grouping key to output)
        void BeginKey(typename KeyComparePolicy<InputSchema,-1>::KeyType * key, OutputSchema * output);

        // process input row, called for each row in key range
        void AddRow(InputSchema * input);

        // write aggregated data, called after key is completely scanned
        void Aggregate(OutputSchema * output);

        void WriteRuntimeStats(TreeNode & root);

        OperatorRequirements GetOperatorRequirements();
    };

    //template to define sequence project
    template<typename InputSchema, typename OutputSchema, int>
    class SequenceProjectPolicy
    {
    public:
        // Digest a row and output a row
        void AdvanceAndOutput(
            bool isNewGroup,
            bool isNewGroupOrder,
            bool isNull,
            OutputSchema &output,
            InputSchema &input);
    };

    //template to define coordinated join
    template<typename InputOperators, typename OutputSchema, typename CorrelatedParametersSchema, int UID>
    class CoordinatedJoinPolicy
    {
    public:
        explicit CoordinatedJoinPolicy(InputOperators *children);

        void Init(const CorrelatedParametersSchema & params);
        PartitionMetadata * GetMetadata();
        bool GetNextRow(OutputSchema & output);
        void Close();
        void WriteRuntimeStats(TreeNode & root, LONGLONG & sumChildInclusiveTimeOut);
    };

    // Template policy for combiner. It defines following interfaces:
    //  1. It defines left and right row compare method.
    //  2. It defines how to copy left and right row columns to output row.
    //  3. It defines how to only copy left side columns to output row.
    //  4. It defines how to only copy right side columns to output row.
    //  5. It defines how to set null value for the columns in output row that coming from left side.
    //  6. It defines how to set null value for the columns in output row that coming fomr right side.
    template<int>
    class CombinerPolicy
    {
    public:
        static int Compare(const void * left, const void * right);
        static void CopyRow(const void * left, const void * right, void * out);
        static void CopyLeftRow(const void * left, const void * out);
        static void CopyRightRow(const void * left, const void * out);
        static void NullifyLeftSide(const void * out);
        static void NullifyRightSide(const void * out);
    };

    template <typename ProbeSchema, typename BuildSchema, typename OutputSchema, int>
    class HashCombinerPolicy
    {
    public:
        typedef NullSchema KeySchema;
        typedef NullSchema ValueSchema;

        struct Hash {};
        struct EqualTo {};

        // Shallow copy probe schema fields to key, probe schemas
        struct ProbeKeyValue {};

        // Shallow copy build schema fields to key, value schemas
        struct BuildKeyValue {};

    public:
        // Shallow copy probe and build schema fields to the output
        static void CopyBoth(const ProbeSchema & probeRow, const ValueSchema & buildValue, OutputSchema & output);

        // Shallow copy probe schema fields to the output and nullify all the rest
        static void CopyProbeAndNullifyBuild(const ProbeSchema & probeRow, OutputSchema & output);
    };

    template <typename ProbeSchema, typename BuildSchema, typename OutputSchema, int>
    class HashCombinerWithFallbackPolicy
    {
    };

    template <typename ProbeSchema, typename BuildSchema, typename OutputSchema, int>
    class HashCombinerPolicyV2
    {
    };

    // Place holder for non exist key compare policy for cross join.
    template<class Schema, int UID = -1>
    class EmptyKeyPolicy
    {
    public:
        EmptyKeyPolicy()
        {
        }

        // default key type
        // specialize according to schema
        typedef int KeyType;

        // compare key value from key and schema objects
        static int Compare(Schema & row, KeyType & key)
        {
            return -1;
        }

        // compare key value from two schema objects
        static int Compare(Schema * n1, Schema * n2)
        {
            return 0;
        }

        // Key function for MKQsort algorithm
        static __int64 Key(Schema * p, int depth)
        {
            return 0;
        }

        // End of Key function for MKQSort algorithm
        static bool EofKey(Schema * p, int depth)
        {
            return true;
        }
    };

#pragma endregion PolicyRegion

#pragma region OperatorHelpers

    template<typename Schema>
    class ManagedRowFactory
    {
    public:
        static void Create(ManagedRow<Schema> * schema);
    };

    // Row Iterator to read and cache a row.
    template<typename Operator, typename InputSchema>
    class RowIterator
    {
    public:
        RowIterator(RowIterator<Operator, InputSchema> & opi) :
            m_operator(opi.m_operator),
            m_row(opi.m_row),
            m_moreRow(opi.m_moreRow),
            m_keyCount(0)
        {
        }

        RowIterator() :
            m_operator(NULL),
            m_moreRow(true),
            m_keyCount(0)
        {
        }

        explicit RowIterator(Operator * op) :
            m_operator(op),
            m_moreRow(true),
            m_keyCount(0)
        {
        }

        // Read first row
        void ReadFirst()
        {
            Increment();
        }

        // Setup operator for the row iterator
        void SetOperator(Operator * op)
        {
            //SetOperator should only be called once
            SCOPE_ASSERT(m_operator == NULL);

            m_operator = op;
            m_moreRow = true;
        }

        Operator* GetOperator()
        {
            return m_operator;
        }

        // Check if we reach the end
        bool End()
        {
            return !m_moreRow;
        }

        void SetEnd()
        {
            m_moreRow = false;
        }

        // Get the current row object
        InputSchema * GetRow()
        {
            return &m_row;
        }

        // Move to next row
        FORCE_INLINE void Increment()
        {
            SCOPE_ASSERT(m_operator != NULL);

            m_moreRow = m_operator->GetNextRow(m_row);    // FIXME:  RowIterator and KeyIterator should not copy the row!

            if (m_moreRow && m_keyCount == 0)
            {
                m_keyCount = 1;
            }
        }

        void AdjustCti(ScopeDateTime& cti)
        {
            m_operator->AdjustCti(cti);
        }

        // return inclusive time in ms of input
        LONGLONG GetInclusiveTimeMillisecond()
        {
            return m_operator->GetInclusiveTimeMillisecond();
        }

    protected:
        InputSchema  m_row; // current row cache
        LONGLONG m_keyCount; // Number of distinct keys in the input rowset (used in derived classes, always "1" for the RowIterator as it does not distinguish keys)

    private:
        Operator * m_operator; // operator to read row
        bool  m_moreRow;       // whether there is more row from operator
    };

    // Scan rows stopping at each key change
    template<typename Operator, typename InputSchema, typename KeyPolicy>
    class KeyIterator : public RowIterator<Operator, InputSchema>
    {
        typedef RowIterator<Operator, InputSchema> inherited;

        // avoid compiler warnings about missing copy constructor
        KeyIterator(const KeyIterator<Operator, InputSchema, KeyPolicy> & opi);

    public:
        KeyIterator() :
            inherited(),
            m_isKeyChanged(false),
            m_allocator(Configuration::GetGlobal().GetMaxKeySize(), "KeyIterator", RowEntityAllocator::KeyContent)
        {
        }

        explicit KeyIterator(Operator * op) :
            inherited(op),
            m_isKeyChanged(false),
            m_allocator(Configuration::GetGlobal().GetMaxKeySize(), "KeyIterator", RowEntityAllocator::KeyContent)
        {
        }

        // Stores current key (deep copy) and resets flag
        FORCE_INLINE void ResetKey()
        {
            // If there is no more rows, resetkey will be a no op.
            if (!inherited::End())
            {
                m_allocator.Reset();
                new ((char*)&m_key) KeyPolicy::KeyType(m_row, &m_allocator);

                if (m_isKeyChanged)
                {
                   m_keyCount++;
                   m_isKeyChanged = false;
                }
            }
        }

        // Get the current row key
        typename KeyPolicy::KeyType * GetKey()
        {
            return & m_key;
        }

        // Move to next row (overrides base class method)
        FORCE_INLINE void Increment()
        {
            inherited::Increment();

            if (!inherited::End())
            {
                int compareResult = Compare(m_row, m_key);

                if (compareResult < 0)
                {
#ifdef SCOPE_DEBUG
                        cout << "Current row: " << endl;
                        cout << m_row << endl;
                        cout << "Previous key: " << endl;
                        cout << m_key << endl;
#endif
                    throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Input data for the KeyIterator is not sorted");
                }

                m_isKeyChanged = compareResult != 0;
            }
        }

        FORCE_INLINE int Compare(InputSchema  &row, typename KeyPolicy::KeyType  &key)
        {
            return KeyPolicy::Compare(m_row, m_key);
        }

        FORCE_INLINE int GetMatchLevel(InputSchema  &row, typename KeyPolicy::KeyType  &key)
        {
            return KeyPolicy::GetMatchLevel(m_row, m_key);
        }

        // Provide next row.  Call this instead of Increment
        FORCE_INLINE void SetRow(InputSchema *row)
        {
            m_row = *row;                        // FIXME:  RowIterator and KeyIterator should not copy the row!

            if (!inherited::End())
            {
                m_isKeyChanged = Compare(m_row, m_key) != 0;
            }
        }

        // Drain the rows with same key
        FORCE_INLINE ULONGLONG Drain()
        {
            ULONGLONG rowCnt = 0;
            while (!End())
            {
                ++rowCnt;
                Increment();
            }

            return rowCnt;
        }

        // Check if we reach the EOF or end of key (overrides base class method)
        bool End()
        {
            return inherited::End() || m_isKeyChanged;
        }

        // Check if we reach EOF
        bool HasMoreRows()
        {
            return !inherited::End();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("KeyIterator");

            RuntimeStats::WriteKeyCount(node, m_keyCount);
            m_allocator.WriteRuntimeStats(node);

            GetOperator()->WriteRuntimeStats(node);
        }

    private:
        typename KeyPolicy::KeyType m_key; // key cache
        bool m_isKeyChanged;  // whether key has changed
        RowEntityAllocator m_allocator; // for deep-copying of the key (if key tracking is requested)
    };

    // Scan rows with dummy key compare policy
    template<typename Operator, typename InputSchema, typename KeyPolicy = EmptyKeyPolicy<NullSchema>>
    class CrossJoinKeyIterator : public RowIterator<Operator, InputSchema>
    {
        typedef RowIterator<Operator, InputSchema> inherited;

        // avoid compiler warnings about missing copy constructor
        CrossJoinKeyIterator(const CrossJoinKeyIterator<Operator, InputSchema, KeyPolicy> & opi);

    public:
        CrossJoinKeyIterator() :
            inherited()
        {
        }

        explicit CrossJoinKeyIterator(Operator * op) :
            inherited(op)
        {
        }

        // Stores current key (deep copy) and resets flag
        void ResetKey()
        {
        }

        // Drain the rows with same key
        FORCE_INLINE void Drain()
        {
            while (!End())
            {
                Increment();
            }
        }

        // Check if we reach EOF
        bool HasMoreRows()
        {
            return !inherited::End();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("CrossJoinKeyIterator");

            RuntimeStats::WriteKeyCount(node, m_keyCount);

            GetOperator()->WriteRuntimeStats(node);
        }
    };

    template <class T>
    class MKQSortInternal
    {
        typedef __int64 (*KeyMethodType)(T *, int);
        typedef bool (*EndOfKeyType)(T *, int);
        typedef void (*SortType)(T *, SIZE_T);

        static const int x_maxMKQSortDepth = 256;

        KeyMethodType  m_getKey;
        EndOfKeyType   m_endOfKey;
        SortType       m_stdSort;

        // work around VC 10 compiler issue
        // change it back to FORCE_INLINE when we get VS2012
        NO_INLINE static void vecswap2( T * a, T * b, __int64 n)
        {
            while (n-- > 0) {
                T t = *a;
                *a++ = *b;
                *b++ = t;
            }
        }

        FORCE_INLINE T * med3func(T *a, T *b, T *c, int depth)
        {
            __int64 va, vb, vc;

            if ((va=(*m_getKey)(a, depth)) == (vb=(*m_getKey)(b, depth)))
            {
                return a;
            }

            if ((vc=(*m_getKey)(c, depth)) == va || vc == vb)
            {
                return c;
            }

            return va < vb ?
                  (vb < vc ? b : (va < vc ? c : a ) )
                : (vb > vc ? b : (va < vc ? a : c ) );
        }

        void inssort(T * a, __int64 n, int d)
        {
            T *pi, *pj;

            for (pi = a + 1; --n > 0; pi++)
            {
                for (pj = pi; pj > a; pj--)
                {
                    int depth = d;
                    // Inline strcmp: break if *(pj-1) <= *pj
                    for (; (*m_getKey)(pj-1, depth)==(*m_getKey)(pj, depth) && !(*m_endOfKey)(pj-1, depth); depth++)
                        ;

                    if ((*m_getKey)(pj-1, depth)<=(*m_getKey)(pj, depth))
                        break;

                    swap2(pj, pj-1);
                }
            }
        }

    public:
        void ssort2(T *a, __int64 n, int depth)
        {
            __int64 d, r, partval;
            T *pa, *pb, *pc, *pd, *pl, *pm, *pn;

            if (n < 10) {
                inssort(a, n, depth);
                return;
            }

            // To avoid stack overflow. If we have hit max recursion depth, go with quick sort.
            if (depth > x_maxMKQSortDepth)
            {
                (*m_stdSort)(a, n);
                return;
            }

            pl = a;
            pm = a + (n/2);
            pn = a + (n-1);

            if (n > 30) { // On big arrays, pseudomedian of 9
                d = (n/8);
                pl = med3(pl, pl+d, pl+2*d);
                pm = med3(pm-d, pm, pm+d);
                pn = med3(pn-2*d, pn-d, pn);
            }

            pm = med3(pl, pm, pn);
            swap2(a, pm);

            partval = (*m_getKey)(a, depth);
            pa = pb = a + 1;
            pc = pd = a + n-1;
            for (;;)
            {
                while (pb <= pc && ((r = (*m_getKey)(pb, depth)) <= partval))
                {
                    if (r == partval)
                    {
                        swap2(pa, pb);
                        pa++;
                    }
                    pb++;
                }

                while (pb <= pc && ((r = (*m_getKey)(pc, depth)) >= partval))
                {
                    if (r == partval)
                    {
                        swap2(pc, pd);
                        pd--;
                    }
                    pc--;
                }

                if (pb > pc)
                    break;

                swap2(pb, pc);

                pb++;
                pc--;
            }

            pn = a + n;
            r = min(pa-a, pb-pa);
            vecswap2(a,  pb-r, r);

            r = min(pd-pc, pn-pd-1);
            vecswap2(pb, pn-r, r);

            if ((r = pb-pa) > 1)
                ssort2(a, r, depth);

            if (!(*m_endOfKey)(a + r, depth))
                ssort2(a + r, pa-a + pn-pd-1, depth+1);

            if ((r = pd-pc) > 1)
                ssort2(a + n-r, r, depth);
        }

        MKQSortInternal(KeyMethodType getKey, EndOfKeyType endOfKey, SortType stdSort): m_getKey(getKey), m_endOfKey(endOfKey), m_stdSort(stdSort)
        {
        }
    };

    template<class Schema, class KeyPolicy, bool inlineRow = false>
    class MKQSortKeyPolicy
    {
    public:
        typedef Schema* RowType;

        static __int64 Key(Schema ** row, int level)
        {
            return KeyPolicy::Key(*row, level);
        }

        static bool EofKey(Schema ** row, int level)
        {
            return KeyPolicy::EofKey(*row, level);
        }
    };

    template<class Schema, class KeyPolicy>
    class MKQSortKeyPolicy<Schema, KeyPolicy, true>
    {
    public:
        typedef Schema RowType;

        static __int64 Key(Schema * row, int level)
        {
            return KeyPolicy::Key(row, level);
        }

        static bool EofKey(Schema * row, int level)
        {
            return KeyPolicy::EofKey(row, level);
        }
    };

    template <class Schema>
    class MKQSort
    {
    public:
        template<class KeyPolicy, bool inlineRow>
        static void Sort(typename MKQSortKeyPolicy<Schema, KeyPolicy, inlineRow>::RowType * begin, SIZE_T N)
        {
            MKQSortInternal<typename MKQSortKeyPolicy<Schema, KeyPolicy, inlineRow>::RowType>  sort(&MKQSortKeyPolicy<Schema, KeyPolicy, inlineRow>::Key, &MKQSortKeyPolicy<Schema, KeyPolicy, inlineRow>::EofKey, &StdSort<Schema>::Sort<KeyPolicy,inlineRow>);

            sort.ssort2(begin, (int)N, 0);
        }
    };

    // Delegate class for operator.
    // The class will delegate all operator interface with one direct function invocation.
    template<typename Schema>
    class OperatorDelegate
    {
        OperatorDelegate(): m_objectPtr(NULL),
                               m_initPtr(NULL),
                               m_closePtr(NULL),
                               m_reWindPtr(NULL),
                               m_getMDPtr(NULL),
                               m_getNextRowPtr(NULL),
                               m_writeStatsPtr(NULL),
                               m_moreRow(false),
                               m_loadScopeCEPCheckpointPtr(NULL),
                               m_doScopeCEPCheckpointPtr(NULL)
        {}

    public:
        typedef typename Schema Schema;

        template <class T>
        OperatorDelegate<Schema>(T* object_ptr)
        {
            *this = FromOperator(object_ptr);
        }

        template <class T>
        static OperatorDelegate<Schema> FromOperator(T* object_ptr)
        {
            OperatorDelegate<Schema> d;

            d.m_objectPtr = object_ptr;
            d.m_initPtr  = &VoidMethodStub<T, reinterpret_cast<void (T::*)()>(&T::Init)>;
            d.m_closePtr = &VoidMethodStub<T, reinterpret_cast<void (T::*)()>(&T::Close)>;
            d.m_getMDPtr = &GetMDStub<T, reinterpret_cast<PartitionMetadata * (T::*)()>(&T::GetMetadata)>;
            d.m_reWindPtr = &VoidMethodStub<T, reinterpret_cast<void (T::*)()>(&T::ReWind)>;
            d.m_getNextRowPtr = &GetMethodStub<T, reinterpret_cast<bool (T::*)(Schema & )>(&T::GetNextRow)>;
            d.m_writeStatsPtr = &WriteStatsStub<T, reinterpret_cast<void (T::*)(TreeNode&)>(&T::WriteRuntimeStats)>;
            d.m_getInclusiveTimePtr = &GetTimeStub<T, reinterpret_cast<LONGLONG (T::*)()>(&T::GetInclusiveTimeMillisecond)>;
            d.m_doScopeCEPCheckpointPtr = &DoCheckpointMethodStub<T, reinterpret_cast<void (T::*)(BinaryOutputStream & )>(&T::DoScopeCEPCheckpoint)>;
            d.m_loadScopeCEPCheckpointPtr = &LoadCheckpointMethodStub<T, reinterpret_cast<void (T::*)(BinaryInputStream & )>(&T::LoadScopeCEPCheckpoint)>;
            d.m_adjustCtiPtr = &AdjustCtiMethodStub<T, reinterpret_cast<void (T::*)(ScopeDateTime &)>(&T::AdjustCti)>;
            return d;
        }

        FORCE_INLINE void Init()
        {
            return (*m_initPtr)(m_objectPtr);
        }

        FORCE_INLINE void Close()
        {
            return (*m_closePtr)(m_objectPtr);
        }

        FORCE_INLINE void ReWind()
        {
            return (*m_reWindPtr)(m_objectPtr);
        }

        FORCE_INLINE bool GetNextRow(Schema & output)
        {
            return (*m_getNextRowPtr)(m_objectPtr, output);
        }

        // Row only shallow copied. The caller needs to be careful about child operator
        // memory reset.
        FORCE_INLINE bool MoveNext()
        {
            m_moreRow = (*m_getNextRowPtr)(m_objectPtr, m_rowCache);
            return m_moreRow;
        }

        FORCE_INLINE void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            return (*m_doScopeCEPCheckpointPtr)(m_objectPtr, output);
        }

        FORCE_INLINE void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            return (*m_loadScopeCEPCheckpointPtr)(m_objectPtr, input);
        }

        FORCE_INLINE void AdjustCti(ScopeDateTime& cti)
        {
            return (*m_adjustCtiPtr)(m_objectPtr, cti);
        }

        Schema & CurrentRow()
        {
            return m_rowCache;
        }

        Schema * CurrentRowPtr()
        {
            return &m_rowCache;
        }

        bool End()
        {
            return !m_moreRow;
        }

        bool IsNull()
        {
            return m_objectPtr == NULL;
        }

        // there is no Peek like method
        // When we want to separate the input into several groups based on some conditions,
        // we need to read the row (MoveNext) and check whether it belongs to the previous group.
        // if it's not, we need to put it into the following group. End() should return true.
        // Call ReloadCurrentRow before the caller handling the next group,
        // or the current row will be missed.
        // it because that caller will read the first row like below:
        // if (End())
        //      MoveNext();
        void ReloadCurrentRow()
        {
            SCOPE_ASSERT(!m_moreRow);
            m_moreRow = true;
        }

        FORCE_INLINE PartitionMetadata * GetMetadata()
        {
            return (*m_getMDPtr)(m_objectPtr);
        }

        FORCE_INLINE void WriteRuntimeStats(TreeNode & root)
        {
            (*m_writeStatsPtr)(m_objectPtr, root);
        }

        FORCE_INLINE LONGLONG GetInclusiveTimeMillisecond()
        {
            return (*m_getInclusiveTimePtr)(m_objectPtr);
        }

    private:
        typedef bool (*GetRowStubType)(void*, Schema &);
        typedef void (*VoidMethodType)(void*);
        typedef PartitionMetadata * (*GetMDStubType)(void*);
        typedef void (*WriteStatsStubType)(void*, TreeNode &);
        typedef LONGLONG (*GetTimeStubType)(void*);
        typedef void (*LoadCheckpointMethodStubType)(void*, BinaryInputStream &);
        typedef void (*DoCheckpointMethodStubType)(void*, BinaryOutputStream &);
        typedef void(*AdjustCtiMethodStubType)(void*, ScopeDateTime& cti);

        // object pointer
        void                   * m_objectPtr;

        // method pointer
        VoidMethodType           m_initPtr;
        VoidMethodType           m_closePtr;
        GetMDStubType            m_getMDPtr;
        VoidMethodType           m_reWindPtr;
        GetRowStubType           m_getNextRowPtr;
        WriteStatsStubType       m_writeStatsPtr;
        GetTimeStubType          m_getInclusiveTimePtr;
        LoadCheckpointMethodStubType m_loadScopeCEPCheckpointPtr;
        DoCheckpointMethodStubType   m_doScopeCEPCheckpointPtr;
        AdjustCtiMethodStubType      m_adjustCtiPtr;

        // Optional row cache if we use MoveNext and GetRow interface.
        Schema                   m_rowCache;
        bool                     m_moreRow;

        template <class T, void (T::*TMethod)(BinaryOutputStream &)>
        FORCE_INLINE static void DoCheckpointMethodStub(void* object_ptr, BinaryOutputStream & a1)
        {
            T* p = static_cast<T*>(object_ptr);
            return (p->*TMethod)(a1);
        }

        template <class T, void (T::*TMethod)(BinaryInputStream &)>
        FORCE_INLINE static void LoadCheckpointMethodStub(void* object_ptr, BinaryInputStream & a1)
        {
            T* p = static_cast<T*>(object_ptr);
            return (p->*TMethod)(a1);
        }

        template <class T, bool (T::*TMethod)(Schema &)>
        FORCE_INLINE static bool GetMethodStub(void* object_ptr, Schema & a1)
        {
            T* p = static_cast<T*>(object_ptr);
            return (p->*TMethod)(a1);
        }

        template <class T, PartitionMetadata * (T::*TMethod)()>
        FORCE_INLINE static PartitionMetadata * GetMDStub(void* object_ptr)
        {
            T* p = static_cast<T*>(object_ptr);
            return (p->*TMethod)();
        }

        template <class T, void (T::*TMethod)()>
        FORCE_INLINE static void VoidMethodStub(void* object_ptr)
        {
            T* p = static_cast<T*>(object_ptr);
            return (p->*TMethod)();
        }

        template <class T, void (T::*TMethod)(TreeNode &)>
        FORCE_INLINE static void WriteStatsStub(void* object_ptr, TreeNode & root)
        {
            T* p = static_cast<T*>(object_ptr);
            return (p->*TMethod)(root);
        }

        template <class T, LONGLONG (T::*TMethod)()>
        FORCE_INLINE static LONGLONG GetTimeStub(void* object_ptr)
        {
            T* p = static_cast<T*>(object_ptr);
            return (p->*TMethod)();
        }

        template <class T, void (T::*TMethod)(ScopeDateTime &)>
        FORCE_INLINE static void AdjustCtiMethodStub(void* object_ptr, ScopeDateTime & cti)
        {
            T* p = static_cast<T*>(object_ptr);
            return (p->*TMethod)(cti);
        }
    };

    template<typename OutputOperator>
    class AutoFlushTimer
    {
        CRITICAL_SECTION  m_lock;
        HANDLE            m_timer;
        OutputOperator*   m_outputer;
        unique_ptr<ExceptionWithStack> m_flushException;
        ULONGLONG           m_lastFlushTick;
        volatile ULONGLONG  m_lastWriteTick;

        const static unsigned int forceFlushMsIfNoWrite = 2000;
        const static unsigned int forceFlushMsIfNoFlush = 10000;
        const static unsigned int timerIntervalMs = 1000;

    public:
        AutoFlushTimer(OutputOperator* outputer)
        {
            m_outputer = outputer;
            InitializeCriticalSection(&m_lock);
            m_timer = NULL;
            m_lastWriteTick = m_lastWriteTick = GetTickCount64();
        }

        ~AutoFlushTimer()
        {
            Stop();
            DeleteCriticalSection(&m_lock);
        }

        bool IsStarted() const
        {
            return m_timer != NULL;
        }


        void Start()
        {
            SCOPE_ASSERT(m_timer == NULL);
            if (!CreateTimerQueueTimer(&m_timer, NULL,
                (WAITORTIMERCALLBACK)OnTimer, this, timerIntervalMs, timerIntervalMs, WT_EXECUTEDEFAULT))
            {
                SCOPE_ASSERT(false);
            }
        }

        CRITICAL_SECTION* GetLock()
        {
            if (m_flushException != nullptr)
            {
                throw m_flushException.release();
            }
            m_lastWriteTick = GetTickCount64();
            return &m_lock;
        }

        void Stop()
        {
            if (m_timer != NULL)
            {
                DeleteTimerQueueTimer(NULL, m_timer, INVALID_HANDLE_VALUE);
                m_timer = NULL;
            }
        }

        bool NeedFlush()
        {
            ULONGLONG now = GetTickCount64();
            if (now - m_lastWriteTick > forceFlushMsIfNoWrite ||
                now - m_lastFlushTick > forceFlushMsIfNoFlush)
            {
                // the last write happen recently, no need to flush
                return true;
            }
            return false;
        }

        static VOID CALLBACK OnTimer(PVOID args, BOOL TimerOrWaitFired)
        {
            AutoFlushTimer* pTimer = reinterpret_cast<AutoFlushTimer*>(args);
            pTimer->AutoFlushRoutineInternal();
        }

        void AutoFlushRoutineInternal()
        {
            if (m_flushException != nullptr)
            {
                // we had a failure in the previous flush, no more flush
                return;
            }

            if (!NeedFlush())
            {
                return;
            }

            if (TryEnterCriticalSection(&m_lock))
            {
                AutoCriticalSection aCs(&m_lock, false);
                try
                {
                    m_outputer->Flush();
                }
                catch (const ExceptionWithStack& ex)
                {
                    m_flushException.reset(ex.Clone());
                }
                m_lastWriteTick = m_lastFlushTick = GetTickCount64();
            }
        }
    };

#pragma endregion OperatorHelpers

#pragma region ManagedOperatorInterfaces
    template<typename InputType, typename OutputSchema>
    class ScopeExtractorManaged
    {
    public:
        virtual void Init() = 0;
        virtual void CreateInstance(const InputFileInfo& input, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize) = 0;
        virtual bool GetNextRow(OutputSchema& output) =0;
        virtual void Close() =0;
        virtual __int64 GetOperatorWaitOnIOTime() = 0;
        virtual __int64 IoStreamInclusiveTime() = 0;
        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output) =0;
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input) =0;

        virtual void WriteRuntimeStats(TreeNode & root) =0;
        virtual OperatorRequirements GetOperatorRequirements() =0;

        virtual ~ScopeExtractorManaged()
        {
            // Virtual destructor which helps tear down the derived class
            // It is declared in a header file to avoid potential mismatch between objects.
        }
    };

    struct ScopeExtractorManagedFactory
    {
        template<typename InputType, typename OutputSchema, int UID, int RunScopeCEPMode>
        static ScopeExtractorManaged<InputType, OutputSchema> * Make(std::string * argv, int argc);

        template<typename InputType, typename OutputSchema, int UID, int RunScopeCEPMode>
        static ScopeExtractorManaged<InputType, OutputSchema> * MakeSqlIp(std::string * argv, int argc);
    };

    template<typename OutputSchema>
    class ScopeSStreamExtractorManaged
    {
    public:
        virtual void Init(const int ssid, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize) =0;
        virtual string GetKeyRangeFileName() = 0;
        virtual bool GetNextRow(OutputSchema& output) =0;
        virtual void Close() =0;
        virtual __int64 GetOperatorWaitOnIOTime() = 0;
        virtual __int64 IoStreamInclusiveTime() = 0;

        virtual void WriteRuntimeStats(TreeNode & root) =0;
        virtual std::vector<std::shared_ptr<BlockDevice>> GetPartitionDevices() = 0;
        virtual ~ScopeSStreamExtractorManaged()
        {
            // Virtual destructor which helps tear down the derived class
            // It is declared in a header file to avoid potential mismatch between objects.
        }
    };

    struct ScopeSStreamExtractorManagedFactory
    {
        template<typename OutputSchema, int UID>
        static ScopeSStreamExtractorManaged<OutputSchema> * Make();
    };

    template<typename InputSchema, typename OutputSchema>
    class ScopeProcessorManaged
    {
    public:
        virtual void Init() =0;
        virtual bool GetNextRow(OutputSchema& output) =0;
        virtual void Close() =0;
        virtual void WriteRuntimeStats(TreeNode & root) =0;
        virtual OperatorRequirements GetOperatorRequirements() =0;
        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output) =0;
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input) =0;

        virtual ~ScopeProcessorManaged()
        {
            // Virtual destructor which helps tear down the derived class
            // It is declared in a header file to avoid potential mismatch between objects.
        }
    };

    struct ScopeProcessorManagedFactory
    {
        template<typename InputSchema, typename OutputSchema, int UID>
        static ScopeProcessorManaged<InputSchema, OutputSchema> * Make(OperatorDelegate<InputSchema> * child);

        template<typename InputSchema, typename OutputSchema, int UID>
        static ScopeProcessorManaged<InputSchema, OutputSchema> * MakeSqlIp(OperatorDelegate<InputSchema> * child);
    };

    struct ScopeApplierManagedFactory
    {
        template<typename InputSchema, typename OutputSchema, int UID>
        static ScopeProcessorManaged<InputSchema, OutputSchema> * Make(OperatorDelegate<InputSchema> * child);

        template<typename InputSchema, typename OutputSchema, int UID>
        static ScopeProcessorManaged<InputSchema, OutputSchema> * MakeSqlIp(OperatorDelegate<InputSchema> * child);
    };

    struct ScopeGrouperManagedFactory
    {
        template<typename InputSchema, typename OutputSchema, int UID>
        static ScopeProcessorManaged<InputSchema, OutputSchema> * Make(OperatorDelegate<InputSchema> * child);

        template<typename InputSchema, typename OutputSchema, int UID>
        static ScopeProcessorManaged<InputSchema, OutputSchema> * MakeSqlIp(OperatorDelegate<InputSchema> * child);
    };

    template<typename InputOperators, typename OutputSchema, int UID>
    class ScopeMultiProcessorManaged
    {
    public:
        virtual void Init() = 0;
        virtual bool GetNextRow(OutputSchema& output) =0;
        virtual void Close() = 0;
        virtual LONGLONG GetInclusiveTimeMillisecond() = 0;
        virtual void WriteRuntimeStats(TreeNode& root) = 0;
        virtual OperatorRequirements GetOperatorRequirements() =0;
        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output) =0;
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input) =0;

        virtual ~ScopeMultiProcessorManaged()
        {
            // Virtual destructor which helps tear down the derived class
            // It is declared in a header file to avoid potential mismatch between objects.
        }
    };

    struct ScopeMultiProcessorManagedFactory
    {
        template<typename InputOperators, typename OutputSchema, int UID>
        static ScopeMultiProcessorManaged<InputOperators, OutputSchema, UID>* Make(
            InputOperators* children,
            string* inputContextFile,
            string* outputContextFile,
            SIZE_T inputBufSize,
            int inputBufCnt,
            SIZE_T outputBufSize,
            int outputBufCnt);
    };

    template<typename InputSchema>
    class ScopeCreateContextManaged
    {
    public:
        virtual void Init(std::string& outputName, SIZE_T bufSize, int bufCnt) = 0;
        virtual void Serialize() = 0;
        virtual void Close() = 0;
        virtual __int64 GetOperatorWaitOnIOTime() = 0;
        virtual __int64 IoStreamInclusiveTime() = 0;

        virtual void WriteRuntimeStats(TreeNode & root) = 0;
        virtual OperatorRequirements GetOperatorRequirements() =0;

        virtual ~ScopeCreateContextManaged()
        {
            // Virtual destructor which helps tear down the derived class
            // It is declared in a header file to avoid potential mismatch between objects.
        }
    };

    struct ScopeCreateContextManagedFactory
    {
        template<typename InputSchema, int UID>
        static ScopeCreateContextManaged<InputSchema>* Make(OperatorDelegate<InputSchema> * child);
    };

    template<typename OutputSchema>
    class ScopeReadContextManaged
    {
    public:
        virtual void Init(std::string& inputName, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize) =0;
        virtual bool GetNextRow(OutputSchema& output) = 0;
        virtual void Close() = 0;
        virtual __int64 GetOperatorWaitOnIOTime() = 0;
        virtual __int64 IoStreamInclusiveTime() = 0;

        virtual void WriteRuntimeStats(TreeNode & root) = 0;

        virtual ~ScopeReadContextManaged()
        {
            // Virtual destructor which helps tear down the derived class
            // It is declared in a header file to avoid potential mismatch between objects.
        }
    };

    struct ScopeReadContextManagedFactory
    {
        template<typename OutputSchema, int UID>
        static ScopeReadContextManaged<OutputSchema>* Make();
    };

    struct ScopeReducerManagedFactory
    {
        template<typename InputSchema, typename OutputSchema, int UID>
        static ScopeProcessorManaged<InputSchema, OutputSchema> * Make(OperatorDelegate<InputSchema> * child);

        template<typename InputSchema, typename OutputSchema, int UID>
        static ScopeProcessorManaged<InputSchema, OutputSchema> * MakeSqlIp(OperatorDelegate<InputSchema> * child);

        template<typename InputSchema, typename OutputSchema, int UID>
        static ScopeProcessorManaged<InputSchema, OutputSchema> * MakeMDF(OperatorDelegate<InputSchema> * child);
    };

    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema>
    class ScopeCombinerManaged
    {
    public:
        virtual void Init() =0;
        virtual bool GetNextRow(OutputSchema& output) =0;
        virtual void Close() =0;
        virtual void WriteRuntimeStats(TreeNode & root) =0;
        virtual OperatorRequirements GetOperatorRequirements() =0;
        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output) =0;
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input) =0;

        virtual ~ScopeCombinerManaged()
        {
            // Virtual destructor which helps tear down the derived class
            // It is declared in a header file to avoid potential mismatch between objects.
        }
    };

    struct ScopeCombinerManagedFactory
    {
        template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, int UID>
        static ScopeCombinerManaged<InputSchemaLeft, InputSchemaRight, OutputSchema> * Make(OperatorDelegate<InputSchemaLeft> * leftChild, OperatorDelegate<InputSchemaRight> * rightChild);
    };

    // SQL IP Combiner
    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, typename LeftKeyPolicy, typename RightKeyPolicy>
    class SqlIpCombinerManaged
    {
    public:

        enum SIDETYPE {LEFT = 1, RIGHT, BOTH, NONE};

        typedef KeyIterator<OperatorDelegate<InputSchemaLeft>, InputSchemaLeft, LeftKeyPolicy> LeftKeyIteratorType;
        typedef KeyIterator<OperatorDelegate<InputSchemaRight>, InputSchemaRight, RightKeyPolicy> RightKeyIteratorType;

        virtual void Init() =0;
        virtual bool GetNextRow(OutputSchema& output, SIDETYPE side) = 0;
        virtual void Close() =0;
        virtual void WriteRuntimeStats(TreeNode & root) =0;
        virtual OperatorRequirements GetOperatorRequirements() =0;
        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output) =0;
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input) =0;
        virtual void SetLeftKeyIterator(LeftKeyIteratorType* iter) = 0;
        virtual void SetRightKeyIterator(RightKeyIteratorType* iter) = 0;

        virtual ~SqlIpCombinerManaged()
        {
            // Virtual destructor which helps tear down the derived class
            // It is declared in a header file to avoid potential mismatch between objects.
        }
    };

	struct SqlIpCombinerManagedFactory
	{
		template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, typename LeftKeyPolicy, typename RightKeyPolicy, int UID>
		static SqlIpCombinerManaged<InputSchemaLeft, InputSchemaRight, OutputSchema, LeftKeyPolicy, RightKeyPolicy> * MakeSqlIp(OperatorDelegate<InputSchemaLeft> * leftChild, OperatorDelegate<InputSchemaRight> * rightChild);
	};

	// SQL IP Combiner
	template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, typename LeftKeyPolicy, typename RightKeyPolicy, typename ThirdInputSchema = None, typename FourthInputSchema = None>
	class SqlIpMultiwayCombinerManaged
	{
	public:

		enum SIDETYPE { LEFT = 1, RIGHT, BOTH, NONE };

		typedef KeyIterator<OperatorDelegate<InputSchemaLeft>, InputSchemaLeft, LeftKeyPolicy> LeftKeyIteratorType;
		typedef KeyIterator<OperatorDelegate<InputSchemaRight>, InputSchemaRight, RightKeyPolicy> RightKeyIteratorType;

		virtual void Init() = 0;
		virtual bool GetNextRow(OutputSchema& output, SIDETYPE side) = 0;
		virtual void Close() = 0;
		virtual void WriteRuntimeStats(TreeNode & root) = 0;
		virtual OperatorRequirements GetOperatorRequirements() = 0;
		virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output) = 0;
		virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input) = 0;
		virtual void SetLeftKeyIterator(LeftKeyIteratorType* iter) = 0;
		virtual void SetRightKeyIterator(RightKeyIteratorType* iter) = 0;

		virtual ~SqlIpMultiwayCombinerManaged()
		{
			// Virtual destructor which helps tear down the derived class
			// It is declared in a header file to avoid potential mismatch between objects.
		}
	};
	
	struct SqlIpMultiwayCombinerManagedFactory
	{
		template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, typename LeftKeyPolicy, typename RightKeyPolicy, int UID, typename ThirdInputSchema = None, typename FourthInputSchema = None>
		static SqlIpMultiwayCombinerManaged<InputSchemaLeft, InputSchemaRight, OutputSchema, LeftKeyPolicy, RightKeyPolicy, ThirdInputSchema, FourthInputSchema> *
			MakeSqlIp(
			OperatorDelegate<InputSchemaLeft> * leftChild,
			OperatorDelegate<InputSchemaRight> * rightChild,
			OperatorDelegate<ThirdInputSchema> * thirdChild = nullptr,
			OperatorDelegate<FourthInputSchema> * fourthChild = nullptr);
	};
	
    template<typename InputSchema>
    class ScopeOutputerManaged
    {
    public:
        virtual void CreateStream(std::string& outputName, SIZE_T bufSize, int bufCnt) =0;
        virtual void Init() =0;
        virtual void Output() =0;
        virtual void Close() =0;
        virtual __int64 GetOperatorWaitOnIOTime() = 0;
        virtual __int64 IoStreamInclusiveTime() = 0;

        virtual void WriteRuntimeStats(TreeNode & root) =0;
        virtual OperatorRequirements GetOperatorRequirements(SIZE_T bufferCount) =0;
        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output) =0;
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input) =0;

        virtual ~ScopeOutputerManaged()
        {
            // Virtual destructor which helps tear down the derived class
            // It is declared in a header file to avoid potential mismatch between objects.
        }
    };

    struct ScopeOutputerManagedFactory
    {
        template<typename InputSchema, int UID, int RunScopeCEPMode>
        static ScopeOutputerManaged<InputSchema> * Make(OperatorDelegate<InputSchema> * child);

        template<typename InputSchema, int UID, int RunScopeCEPMode>
        static ScopeOutputerManaged<InputSchema> * MakeSqlIp(OperatorDelegate<InputSchema> * child);

        template<typename InputSchema, int UID, int RunScopeCEPMode>
        static ScopeOutputerManaged<InputSchema> * MakeSqlIp(OperatorDelegate<InputSchema> * child, bool disableCompression);
    };


#pragma endregion ManagedOperatorInterfaces

} // namespace ScopeEngine


#pragma region StdExtensions
namespace std
{
    //
    // Specialization for STL std::equal_to<T>
    //
    template<typename T>
    class equal_to<class ScopeEngine::FixedArrayType<T> >
    {
    public:
        bool operator()(const ScopeEngine::FixedArrayType<T> & left, const ScopeEngine::FixedArrayType<T> & right) const
        {
            return left.Compare(right) == 0;
        }
    };

    template<typename T>
    class equal_to<class ScopeEngine::NativeNullable<T> >
    {
    public:
        bool operator()(const ScopeEngine::NativeNullable<T> & left, const ScopeEngine::NativeNullable<T> & right) const
        {
            return left == right;
        }
    };

    template<typename T>
    class numeric_limits<class ScopeEngine::NativeNullable<T> >
    {
    public:
        static ScopeEngine::NativeNullable<T> min()
        {
            return (ScopeEngine::NativeNullable<T>(is_floating_point<T>::value ? -numeric_limits<T>::max() : numeric_limits<T>::min()));
        }

        static ScopeEngine::NativeNullable<T> max()
        {
            // return max value
            return ((ScopeEngine::NativeNullable<T>(numeric_limits<T>::max())));
        }
    };

    static const UINT64 x_datetime_ticks_min = 0;
    static const UINT64 x_datetime_ticks_max = 3155378975999999999;

    template<>
    class numeric_limits<class ScopeEngine::ScopeDateTime>
    {
    public:
        static ScopeEngine::ScopeDateTime min()
        {
            return (ScopeEngine::ScopeDateTime(x_datetime_ticks_min));
        }

        static ScopeEngine::ScopeDateTime max()
        {
            return (ScopeEngine::ScopeDateTime(x_datetime_ticks_max));
        }
    };

    template<>
    class numeric_limits<class ScopeEngine::ScopeDecimal>
    {
    public:
        static ScopeEngine::ScopeDecimal min()
        {
            return (ScopeEngine::ScopeDecimal(ScopeEngine::ScopeInt128(ULONG_MAX, ULONG_MAX, ULONG_MAX), 0, 1));
        }

        static ScopeEngine::ScopeDecimal max()
        {
            return (ScopeEngine::ScopeDecimal(ScopeEngine::ScopeInt128(ULONG_MAX, ULONG_MAX, ULONG_MAX), 0, 0));
        }
    };

    template<>
    class numeric_limits<class ScopeEngine::ScopeGuid>
    {
    public:
        static ScopeEngine::ScopeGuid min()
        {
            return (ScopeEngine::ScopeGuid());
        }
        static ScopeEngine::ScopeGuid max()
        {
            ScopeEngine::ScopeGuid guid;
            memset(&guid, 0xff, sizeof(ScopeEngine::ScopeGuid));
            return guid;
        }
    };

    //
    // Specialization for STL std::tr1::hash<T>
    //
    template<typename T>
    class hash<class ScopeEngine::FixedArrayType<T> >
    {
    public:
        size_t operator()(const ScopeEngine::FixedArrayType<T> & value) const
        {
            return value.GetScopeHashCode();
        }
    };

    template<typename T>
    class hash<class ScopeEngine::NativeNullable<T> >
    {
    public:
        size_t operator()(const ScopeEngine::NativeNullable<T> & value) const
        {
            return value.GetStdHashCode();
        }
    };

    template<>
    class hash<class ScopeEngine::ScopeDateTime>
    {
    public:
        size_t operator()(const ScopeEngine::ScopeDateTime & value) const
        {
            return value.GetScopeHashCode();
        }
    };

    template<>
    class hash<class ScopeEngine::ScopeDecimal>
    {
    public:
        size_t operator()(const ScopeEngine::ScopeDecimal& value) const
        {
            return value.GetScopeHashCode();
        }
    };

    template<>
    class hash<class ScopeEngine::ScopeGuid>
    {
    public:
        size_t operator()(const ScopeEngine::ScopeGuid& value) const
        {
            return value.GetScopeHashCode();
        }
    };
} // namespace std
#pragma endregion StdExtensions
