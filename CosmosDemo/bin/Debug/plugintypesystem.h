#pragma once

using namespace std;
#if defined(SCOPE_RUNTIME_EXPORT_DLL)
#define SCOPE_RUNTIME_API __declspec(dllexport)
#define SCOPE_NO_UDT
#elif defined(SCOPE_RUNTIME_IMPORT_DLL)
#define SCOPE_RUNTIME_API __declspec(dllimport)
#define SCOPE_NO_UDT
#else
#define SCOPE_RUNTIME_API
#endif

namespace ScopeEngine
{
    class AutoBuffer;

    enum ConvertResult
    {
        ConvertSuccess,
        ConvertErrorOutOfRange,
        ConvertErrorEmpty,
        ConvertErrorNull,
        ConvertErrorInvalidCharacter,
        ConvertErrorInvalidLength,
        ConvertErrorTooLong,
        ConvertErrorStringToSqlDatetime2,
        ConvertErrorUndefined
    };
}

#ifdef PLUGIN_TYPE_SYSTEM_NAMESPACE
namespace PluginType
{
using namespace ScopeEngine;
#else
namespace ScopeEngine
{
#endif
    class ScopeMapOutputMemoryStream;
    template<typename TextOutputStreamTraits> class TextOutputStream;
    class SStreamDataOutputStream;
    class SCOPE_RUNTIME_API IncrementalAllocator;
    class ScopeMapInputMemoryStream;
    template<typename TextInputStreamTraits, typename InputStreamType> class TextInputStream;
    class FStringWithNull;
    template<typename InputStream> class SCOPE_RUNTIME_API BinaryInputStreamBase;
    template<typename OutputType> class SCOPE_RUNTIME_API BinaryOutputStreamBase;

    // This base class is the guide for any new native types:
    // Any existing scope types or new types need to inherit from this base class
    // Any operators in this base class need to be implemented by derived types
    // Any static methods and enums in CheckStatic need to be defined
    template<typename TYPE_PTS>
    class PluginTypeBase
    {
    public:
        PluginTypeBase()
        {
        }

        int Compare(const TYPE_PTS & y) const
        {
            static_assert(false, "Not implemented");
        }

        // Serialize in BinaryOutputStreamBase::Write
        template<typename OutputType>
        void Serialize(BinaryOutputStreamBase<OutputType>* stream) const
        {
            static_assert(false, "Not implemented");
        }

        // Serialize in, ScopeIO.h TextOutputStream::Write
        template<typename TextOutputStreamTraits>
        void Serialize(TextOutputStream<TextOutputStreamTraits>* stream) const
        {
            static_assert(false, "Not implemented");
        }

        // Serialize in SStreamDataOutputStream::Write
        template<typename BufferType>
        void SStreamDataOutputSerialize(void* buffer) const
        {
            static_assert(false, "Not implemented");
        }

        // Serialize in ScopeMapOutputMemoryStream::Write
        template<typename BufferType>
        void ScopeMapOutputMemoryStreamSerialize(void* buffer) const
        {
            static_assert(false, "Not implemented");
        }

        // DeSerialize in ScopeMapInputMemoryStream::Read
        template<typename AllocatorType>
        void ScopeMapInputMemoryStreamDeserialize(const char* & cur, void* ia)
        {
            static_assert(false, "Not implemented");
        }

        // DeSerialize in BinaryInputStreamBase::Read
        template<typename InputType>
        void Deserialize(BinaryInputStreamBase<InputType>* stream, IncrementalAllocator* alloc)
        {
            static_assert(false, "Not implemented");
        }

        // DeSerialize in TextInputStream::Read
        template<typename TextInputStreamTraits, typename InputStreamType>
        void Deserialize(TextInputStream<TextInputStreamTraits, InputStreamType>* stream, FStringWithNull & str, bool lastEmptyColumn)
        {
            static_assert(false, "Not implemented");
        }

        // Swap the values
        void Swap(TYPE_PTS & y)
        {
            static_assert(false, "Not implemented");
        }

        // The hash function
        size_t GetHashCode() const
        {
            static_assert(false, "Not implemented");
        }

        // TryFStringToT, see scopeio.h
        template<typename StringType>
        ConvertResult FromFString(StringType & str)        
        {
            static_assert(false, "Not implemented");
        }

        // IsNull is conflict with FString::IsNull, so we must use a new name IsNull. We should switch back after runtime switching to Plugin Type System
        bool IsNull() const
        {
            static_assert(false, "Not implemented");
        }

        // For Aggregate_MIN and Aggregate_MAX
        void SetNull()
        {
            static_assert(false, "Not implemented");
        }

        // Deep copier
        void DeepCopyFrom(TYPE_PTS const & value, IncrementalAllocator * ia)
        {
            static_assert(false, "Not implemented");
        }

        // Shallow copier
        void ShallowCopyFrom(TYPE_PTS const & value)
        {
            static_assert(false, "Not implemented");
        }

        // Used by Aggregate_UNCHECKED_SUMx2
        TYPE_PTS Multiply(TYPE_PTS const & value) const
        {
            static_assert(false, "Not implemented");
        }
                
        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        TYPE_PTS & AddByUnchecked(TYPE_PTS const & rhs)
        {
            static_assert(false, "Not implemented");
        }

        // For Aggregate_SUM: Should check overflow; Should skip if the value is null
        TYPE_PTS & AddByChecked(TYPE_PTS const & rhs)
        {
            static_assert(false, "Not implemented");
        }

        // Used by Aggregators like Aggregate_UNCHECKED_SUM
        TYPE_PTS & SetZero()
        {
            static_assert(false, "Not implemented");
        }

        // This method should not be overrided!!!!!
        // Check all static members and enum members
        void static CheckStatic()
        {
            // Return min and max values of a type, see Aggregate_MIN and Aggregate_MAX for the usage
            // static TYPE_PTS min_pts();
            TYPE_PTS::Min();
            TYPE_PTS::Max();

            // The trait indicating whether a type is Nullable. Define in the class like this:
            // enum {isNullablePrimaryType=true};
            cout<<TYPE_PTS::isNullablePrimaryType;

            // Numeric traits
            // is_floating_point should be true for ScopeDecimal
            // For NativeNullable, the result of these traits should be the same as the traits of the data type, which is NativeNullable::value
            cout<<TYPE_PTS::is_floating_point;
            cout<<TYPE_PTS::is_signed;
            cout<<TYPE_PTS::is_unsigned;
            cout<<TYPE_PTS::is_integral;
            cout<<TYPE_PTS::need_deep_copy;
        }
    };
} // namespace
