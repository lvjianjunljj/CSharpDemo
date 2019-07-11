// Common managed c++ code for Scope and SQL
#pragma once

#include "ScopeContainers.h"
#include "ScopeSqlType.h"
#include "ScopeIO.h"

#include <string>

#include <msclr/appdomain.h>
#include <msclr\marshal.h>
#include <msclr\marshal_cppstd.h>

using namespace System;
using namespace System::Collections;
using namespace System::Reflection;
using namespace Microsoft::Analytics::Interfaces;

// workaround common identifiers that clash between a C# and C++
#undef IN
#undef OUT
#undef OPTIONAL

#define SCOPECEP_CHECKPINT_MAGICNUMBER 0x12345678

namespace ScopeEngineManaged
{
    extern INLINE cli::array<String^>^ GroupArguments(std::string * argv, int argc)
    {
        cli::array<String^>^ arr_args = gcnew cli::array<String^>(argc){};
        for(int i=0; i<argc; ++i)
        {
            String^ args = gcnew String(argv[i].c_str());
            arr_args[i] = ScopeEngineManaged::EscapeStrings::UnEscape(args);
        }

        return arr_args;
    } 

    // Initilalize/finalize managed runtime
    extern INLINE void InitializeScopeEngineManaged(cli::array<String^>^ arguments)
    {
        ScopeEngineManaged::Global::Initialize(arguments);
            
#ifndef STREAMING_SCOPE
        ScopeEngineManaged::Global::GCMemoryWatcher->Initialize();
#endif
    }

    extern INLINE void FinalizeScopeEngineManaged(ScopeEngine::CLRMemoryStat& stat)
    {
#ifndef STREAMING_SCOPE
        stat.peakManagedMemory = (UINT64)ScopeEngineManaged::Global::GCMemoryWatcher->PeakGCMemory;
        stat.gen0CollectionsCount = ScopeEngineManaged::Global::GCMemoryWatcher->GetGen0CollectionsCount();
        stat.gen1CollectionsCount = ScopeEngineManaged::Global::GCMemoryWatcher->GetGen1CollectionsCount();
        stat.gen2CollectionsCount = ScopeEngineManaged::Global::GCMemoryWatcher->GetGen2CollectionsCount();
        stat.inducedGCCount = ScopeEngineManaged::Global::GCMemoryWatcher->GetInducedGCCount();
        stat.timeInGCPercent = ScopeEngineManaged::Global::GCMemoryWatcher->CalculateTimeInGCPercent();
        stat.peakGen2HeapSize = ScopeEngineManaged::Global::GCMemoryWatcher->PeakGen2HeapSize;
        stat.peakLargeObjectHeapSize = ScopeEngineManaged::Global::GCMemoryWatcher->PeakLargeObjectHeapSize;
        ScopeEngineManaged::Global::GCMemoryWatcher->Shutdown();
#endif

        ScopeEngineManaged::Global::Shutdown();
    }   
}

namespace ScopeEngine
{
#pragma region ManagedHandle
    
    //
    // Following code is managed implementation of a handle that is safe to declare in native code, thus
    // enabling us to have separate native and managed compilation in the future.
    //
    
    typedef System::Runtime::InteropServices::GCHandle GCHandle;

    INLINE void CheckScopeCEPCheckpointMagicNumber(System::IO::BinaryReader^ checkpoint)
    {
        int magic = checkpoint->ReadInt32();
        if (magic != SCOPECEP_CHECKPINT_MAGICNUMBER)
        {
            throw gcnew InvalidOperationException("Load Checkpoint overflow or underflow");
        }
    }

    template <typename T>
    ScopeManagedHandle::ScopeManagedHandle(T t)
    {
        SCOPE_ASSERT(sizeof(m_handle) == sizeof(GCHandle));
        GCHandle h = GCHandle::Alloc(t);
        memcpy(&m_handle, &h, sizeof(GCHandle));
    }

    template <typename T>
    ScopeManagedHandle::operator T () const
    {
        if (m_handle != NULL)
        {
            // Keep the const-ness on the function and cast it away here since we are still not modifying any memory
            return static_cast<T>(reinterpret_cast<GCHandle *>(const_cast<void **>(&m_handle))->Target);
        }
        else
        {
            return static_cast<T>(nullptr);
        }
    }

    template <typename T>
    ScopeManagedHandle& ScopeManagedHandle::operator=(T t)
    {
        SCOPE_ASSERT(m_handle == NULL);
        GCHandle h = GCHandle::Alloc(t);
        memcpy(&m_handle, &h, sizeof(GCHandle));
        return *this;
    }

    template <typename T>
    T scope_handle_cast(const ScopeManagedHandle & handle)
    {
        return static_cast<T>(handle);
    }

    template<typename T>
    class ScopeTypedManagedHandle
    {

    public:

        ScopeTypedManagedHandle()
        {
        }

        explicit ScopeTypedManagedHandle(T t) : m_handle(t)
        {
        }

        ScopeTypedManagedHandle& operator=(T t)
        {
            m_handle = t;
            return *this;
        }

        operator T () const
        {
            return scope_handle_cast<T>(m_handle);
        }

        T operator->() const
        {
            return scope_handle_cast<T>(m_handle);
        }

        T get() const
        {
            return scope_handle_cast<T>(m_handle);
        }

        void reset()
        {
            m_handle.reset();
        }

        void reset(T t)
        {
            reset();

            if (t != nullptr)
            {
                m_handle = t;
            }
        }

    private:

        ScopeManagedHandle m_handle;
    };
    
#pragma endregion ManagedHandle

    // Wrapper for UDO to be used in native code
    template<int UID>
    struct ManagedUDO
    {
        System::Object^ get();
        System::Collections::Generic::List<System::String^>^ args();
    };

    // Wrapper for UDT to be used in native code
    template<int UserDefinedTypeId>
    struct ManagedUDT
    {
        typedef void Typename;
        System::Object^ get();
    };

    template<typename Schema>
    INLINE void ManagedRowFactory<Schema>::Create(ManagedRow<Schema> * schema)
    {
        // We have allocated fixed size for ManagedRow<Schema>, pointer sized, so any change in size
        // of ManagedRow will have to revisit this code.
        static_assert(sizeof(ManagedRow<Schema>) == sizeof(void *), "Size of ManagedRow class grew beyond pointer size. Please revise this code!");
        new ((char *) schema) ManagedRow<Schema>();
    }

    class ScopeManagedInterop
    {
    public:

        static cli::array<System::Byte>^ CopyToManagedBuffer(const void * buf, int size)
        {
            SCOPE_ASSERT(size >= 0);
            cli::array<System::Byte>^ managedBuf = gcnew cli::array<System::Byte>(size);
            
            if (size)
            {
                pin_ptr<System::Byte> pinnedBuf = &managedBuf[0];
                memcpy(pinnedBuf, buf, size);
            }

            return managedBuf;
        }

        template<typename NT, typename MT>
        static MT ManagedColumnGetter(BYTE *address)
        {
            NT n = *((NT*)address);
            MT m;
            ScopeManagedInterop::CopyToManagedColumn(n, m);
            return m;
        }
    
        template<typename T, typename MT>
        static INLINE void CopyToNativeColumn(T & nativeColumn, MT managedColumn, IncrementalAllocator *)
        {
            nativeColumn = managedColumn;
        }

        template<>
        static INLINE void CopyToNativeColumn<FString,cli::array<System::Byte>^>(FString & nativeColumn, cli::array<System::Byte> ^ managedColumn, IncrementalAllocator * alloc)
        {
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
            }
            else
            {
                int size = managedColumn->Length;
                if (size)
                {
                    pin_ptr<System::Byte> bufsrc = &managedColumn[0];
                    nativeColumn.CopyFrom((char*)bufsrc, size, alloc);
                }
                else
                {
                    nativeColumn.SetEmpty();
                }
            }
        }

        template<>
        static INLINE void CopyToNativeColumn<FString, String^>(FString & nativeColumn, String^ managedColumn, IncrementalAllocator * alloc)
        {
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
            }
            else if (0 == managedColumn->Length)
            {
                nativeColumn.SetEmpty();
            }
            else
            {
                System::Text::UTF8Encoding^ utf8Encoding = ScopeEngineManaged::Utils::UTF8EncodingWithoutBOM;
                int size = utf8Encoding->GetByteCount(managedColumn);

                pin_ptr<const wchar_t> ptrToManaged = PtrToStringChars(managedColumn);
                char *nativeBuf = nativeColumn.Reserve(size, alloc);
                int actualSize = utf8Encoding->GetBytes((wchar_t *)ptrToManaged, managedColumn->Length, (unsigned char *)nativeBuf, size);

                SCOPE_ASSERT(size == actualSize);
            }
        }
        
        template<>
        static INLINE void CopyToNativeColumn<FBinary,cli::array<System::Byte>^>(FBinary & nativeColumn, cli::array<System::Byte> ^ managedColumn, IncrementalAllocator * alloc)
        {
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
            }
            else
            {
                int size = managedColumn->Length;
                if (size)
                {
                    pin_ptr<System::Byte> bufsrc = &managedColumn[0];
                    nativeColumn.CopyFrom((unsigned char*)bufsrc, size, alloc);
                }
                else
                {
                    nativeColumn.SetEmpty();
                }
            }
        }

        template<typename T, typename MT>
        static INLINE void CopyToNativeColumn(NativeNullable<T> & nativeColumn, MT managedColumn, IncrementalAllocator * alloc)
        {
            if (!managedColumn.HasValue)
            {
                nativeColumn.SetNull();
            }
            else
            {
                CopyToNativeColumn(nativeColumn.get(), managedColumn.Value, alloc);
                nativeColumn.ClearNull();
            }
        }

        template<typename T, typename MT>
        static INLINE void CopyToNativeColumn(NativeNullable<T> & nativeColumn, MT^ managedColumn, IncrementalAllocator * alloc)
        {
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
                return;
            }

            CopyToNativeColumn(nativeColumn.get(), managedColumn->Value, alloc);
            nativeColumn.ClearNull();
        }

        ///20151015 partial specialized templates for Scope based SqlType @tonygu
        ///NOTE: given we now cast the sql types to NativeNullable scope types, the template specialization will not pick the typename like "ScopeSqlType::SqlType_Int16"
        ///      but choose "NativeNullable<T>". So the following partial specialized templates are added for the correct interop operations.
        ///      Once we have true SqlType native implementation, we should use remove the following templates.
        template<typename T>
        static INLINE void CopyToNativeColumn(NativeNullable<T> & nativeColumn, Microsoft::Analytics::Types::Sql::SqlBit^ managedColumn, IncrementalAllocator * alloc)
        {
            CopyToNativeColumn(nativeColumn, managedColumn, alloc);
        }

        template<typename T>
        static INLINE void CopyToNativeColumn(NativeNullable<T> & nativeColumn, Microsoft::Analytics::Types::Sql::SqlByte^ managedColumn, IncrementalAllocator * alloc)
        {
            CopyToNativeColumn(nativeColumn, managedColumn, alloc);
        }

        template<typename T>
        static INLINE void CopyToNativeColumn(NativeNullable<T> & nativeColumn, Microsoft::Analytics::Types::Sql::SqlInt16^ managedColumn, IncrementalAllocator * alloc)
        {
            CopyToNativeColumn(nativeColumn, managedColumn, alloc);
        }

        template<typename T>
        static INLINE void CopyToNativeColumn(NativeNullable<T> & nativeColumn, Microsoft::Analytics::Types::Sql::SqlInt32^ managedColumn, IncrementalAllocator * alloc)
        {
            CopyToNativeColumn(nativeColumn, managedColumn, alloc);
        }

        template<typename T>
        static INLINE void CopyToNativeColumn(NativeNullable<T> & nativeColumn, Microsoft::Analytics::Types::Sql::SqlInt64^ managedColumn, IncrementalAllocator * alloc)
        {
            CopyToNativeColumn(nativeColumn, managedColumn, alloc);
        }

        template<typename T>
        static INLINE void CopyToNativeColumn(NativeNullable<T> & nativeColumn, Microsoft::Analytics::Types::Sql::SqlDecimal^ managedColumn, IncrementalAllocator *)
        {
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
                return;
            }

            // TODO: Directly access SqlValue when generated code can access internal methods of SqlDecimal.cs
            Data::SqlTypes::SqlDecimal sqlDecimal = Data::SqlTypes::SqlDecimal(managedColumn->Value);
            nativeColumn.get().SetValue(sqlDecimal.Data[0], sqlDecimal.Data[1], sqlDecimal.Data[2], sqlDecimal.Data[3], sqlDecimal.Precision, sqlDecimal.Scale, sqlDecimal.IsPositive);
            nativeColumn.ClearNull();
        }

        template<typename T>
        static INLINE void CopyToNativeColumn(NativeNullable<T> & nativeColumn, Microsoft::Analytics::Types::Sql::SqlDate^ managedColumn, IncrementalAllocator *)
        {
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
                return;
            }

            // Look at CopyToManaged for clarification on conversion between SqlDate and SqlType_Date
            int year = managedColumn->Value.Year;
            int month = managedColumn->Value.Month;
            int day= managedColumn->Value.Day;
            int hour = managedColumn->Value.Hour;
            int min = managedColumn->Value.Minute;
            int sec = managedColumn->Value.Second;

            nativeColumn = ScopeSqlType::SqlType_Date(ScopeSqlType::SqlDateTime(year, month, day, hour, min, sec, 0));
        }

        ///20151015 partial specialized templates for Scope based SqlType. End

        template<>
        static INLINE void CopyToNativeColumn <ScopeSqlType::SqlType_Bit, Microsoft::Analytics::Types::Sql::SqlBit^>
            (ScopeSqlType::SqlType_Bit & nativeColumn, Microsoft::Analytics::Types::Sql::SqlBit^ managedColumn, IncrementalAllocator * alloc)
        {
            CopyToNativeColumn(nativeColumn, managedColumn, alloc);
        }

        template<>
        static INLINE void CopyToNativeColumn<ScopeSqlType::SqlType_Byte, Microsoft::Analytics::Types::Sql::SqlByte^>
            (ScopeSqlType::SqlType_Byte & nativeColumn, Microsoft::Analytics::Types::Sql::SqlByte^ managedColumn, IncrementalAllocator * alloc)
        {
            CopyToNativeColumn(nativeColumn, managedColumn, alloc);
        }

        template<>
        static INLINE void CopyToNativeColumn<ScopeSqlType::SqlType_Int16, Microsoft::Analytics::Types::Sql::SqlInt16^>
            (ScopeSqlType::SqlType_Int16 & nativeColumn, Microsoft::Analytics::Types::Sql::SqlInt16^ managedColumn, IncrementalAllocator * alloc)
        {
            CopyToNativeColumn(nativeColumn, managedColumn, alloc);
        }

        template<>
        static INLINE void CopyToNativeColumn<ScopeSqlType::SqlType_Int32, Microsoft::Analytics::Types::Sql::SqlInt32^>
            (ScopeSqlType::SqlType_Int32 & nativeColumn, Microsoft::Analytics::Types::Sql::SqlInt32^ managedColumn, IncrementalAllocator * alloc)
        {
            CopyToNativeColumn(nativeColumn, managedColumn, alloc);
        }

        template<class I, typename T, class MT>
        static INLINE void CopyToNativeColumn(ScopeSqlType::ArithmeticSqlType<I, T> & nativeColumn, MT^ managedColumn, IncrementalAllocator *)
        {
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
            }
            else
            {
                nativeColumn.get().setValue(managedColumn->Value);
                nativeColumn.ClearNull();
            }
        }

        template<>
        static INLINE void CopyToNativeColumn<ScopeSqlType::SqlType_Int64, Microsoft::Analytics::Types::Sql::SqlInt64^>
            (ScopeSqlType::SqlType_Int64 & nativeColumn, Microsoft::Analytics::Types::Sql::SqlInt64^ managedColumn, IncrementalAllocator * alloc)
        {
            CopyToNativeColumn(nativeColumn, managedColumn, alloc);
        }

        template<>
        static INLINE void CopyToNativeColumn<ScopeSqlType::SqlType_Decimal, Microsoft::Analytics::Types::Sql::SqlDecimal^>
            (ScopeSqlType::SqlType_Decimal &nativeColumn, Microsoft::Analytics::Types::Sql::SqlDecimal^ managedColumn, IncrementalAllocator *)
        {
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
                return;
            }

            //DEFECT 6184582: This may be the reason for the incorrect resuts in SUM and AVG.
            // TODO: Directly access SqlValue when generated code can access internal methods of SqlDecimal.cs
            Data::SqlTypes::SqlDecimal sqlDecimal = Data::SqlTypes::SqlDecimal(managedColumn->Value);
            nativeColumn.get().SetValue(sqlDecimal.Data[0], sqlDecimal.Data[1], sqlDecimal.Data[2], sqlDecimal.Data[3], sqlDecimal.Precision, sqlDecimal.Scale, sqlDecimal.IsPositive);
            nativeColumn.ClearNull();
        }

        template<>
        static INLINE void CopyToNativeColumn <ScopeSqlType::SqlType_Date, Microsoft::Analytics::Types::Sql::SqlDate^>
            (ScopeSqlType::SqlType_Date & nativeColumn, Microsoft::Analytics::Types::Sql::SqlDate^ managedColumn, IncrementalAllocator *)
        {
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
                return;
            }

            // Look at CopyToManaged for clarification on conversion between SqlDate and SqlType_Date
            int year = managedColumn->Value.Year;
            int month = managedColumn->Value.Month;
            int day = managedColumn->Value.Day;
            int hour = managedColumn->Value.Hour;
            int min = managedColumn->Value.Minute;
            int sec = managedColumn->Value.Second;

            nativeColumn = ScopeSqlType::SqlType_Date(ScopeSqlType::SqlDateTime(year, month, day, hour, min, sec, 0));
        }        

        template<int N>
        static INLINE void CopyToNativeColumn
            (ScopeSqlType::SqlType_MaxString<N> & nativeColumn, Microsoft::Analytics::Types::Sql::SqlString^ managedColumn, IncrementalAllocator * alloc)
        {
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
            }
            else if (0 == managedColumn->Value->Length)
            {
                nativeColumn.get().SetEmpty();
            }
            else
            {
                std::string value = msclr::interop::marshal_as<std::string>(managedColumn->Value);
                nativeColumn = ScopeSqlType::SqlType_MaxString<N>(ScopeSqlType::SqlStringFaceted<N>(value, alloc));
            }
        }

#if !defined(SCOPE_NO_UDT)

        template<int UserDefinedTypeId, template<int> class UserDefinedType, typename MT>
        static INLINE void CopyToNativeColumn(UserDefinedType<UserDefinedTypeId> & nativeColumn, MT managedColumn, IncrementalAllocator *)
        {
            nativeColumn.Set(managedColumn);
        }
        
#endif
        template<>
        static INLINE void CopyToNativeColumn<ScopeDateTime, System::DateTime>(ScopeDateTime & nativeColumn, System::DateTime managedColumn, IncrementalAllocator *)
        {
            nativeColumn = ScopeDateTime::FromBinary(managedColumn.ToBinary());
        }

        template<>
        static INLINE void CopyToNativeColumn<ScopeDecimal,System::Decimal>(ScopeDecimal & nativeColumn, System::Decimal managedColumn, IncrementalAllocator *)
        {
            cli::array<int>^Bits = System::Decimal::GetBits(managedColumn);

            nativeColumn.Reset(Bits[2], Bits[1], Bits[0], Bits[3]);
        }

        template<>
        static INLINE void CopyToNativeColumn<ScopeGuid,System::Guid>(ScopeGuid & nativeColumn, System::Guid managedColumn, IncrementalAllocator *)
        {
            cli::array<System::Byte>^ buffer = managedColumn.ToByteArray();
            pin_ptr<System::Byte> bufsrc = &buffer[0];

            nativeColumn.CopyFrom((unsigned char*)bufsrc);
        }

        template<typename NK, typename NV, typename MK, typename MV>
        static void CopyMapToNativeColumn(ScopeEngine::ScopeMapNative<NK, NV> & nativeColumn, Generic::IEnumerable<Generic::KeyValuePair<MK, MV>>^ managedColumn, IncrementalAllocator * alloc)
        {
            // Note: ScopeMapNative is the same class for both SQLIP as well as SCOPE
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
                return;
            }

            // we had some content created inside udo and never had a chance to hook with native scope map, copy here.
            nativeColumn.Reset(alloc);
            NK nkey;
            NV nvalue;
            for each(Generic::KeyValuePair<MK, MV>^ kv in managedColumn)
            {
                ScopeManagedInterop::CopyToNativeColumn(nkey, kv->Key, alloc);
                ScopeManagedInterop::CopyToNativeColumn(nvalue, kv->Value, alloc);
                nativeColumn.AddShallow(nkey, nvalue);
            }
        }

        template<typename NK, typename NV, typename MK, typename MV>
        static void CopyToNativeColumn(ScopeEngine::ScopeMapNative<NK, NV> & nativeColumn, Microsoft::SCOPE::Types::ScopeMap<MK, MV>^ managedColumn, IncrementalAllocator * alloc)
        {
            // Note: ScopeMapNative is the same class for both SQLIP as well as SCOPE
            CopyMapToNativeColumn<NK, NV, MK, MV>(nativeColumn, managedColumn, alloc);
        }

        template<typename NK, typename NV, typename MK, typename MV>
        static void CopyToNativeColumn(ScopeEngine::ScopeMapNative<NK, NV> & nativeColumn, Microsoft::Analytics::Types::Sql::SqlMap<MK, MV>^ managedColumn, IncrementalAllocator * alloc)
        {
            // Note: ScopeMapNative is the same class for both SQLIP as well as SCOPE
            CopyMapToNativeColumn<NK, NV, MK, MV>(nativeColumn, managedColumn, alloc);
        }

        template<typename NT, typename MT>
        static void CopyArrayToNativeColumn(ScopeEngine::ScopeArrayNative<NT> & nativeColumn, Generic::IEnumerable<MT>^ managedColumn, IncrementalAllocator * alloc)
        {
            // Note: ScopeArrayNative is the same class for both SQLIP as well as SCOPE
            if (managedColumn == nullptr)
            {
                nativeColumn.SetNull();
                return;
            }

            // we had some content created inside udo and never had a chance to hook with native scope array, copy here.
            nativeColumn.Reset(alloc);
            NT nvalue;
            for each(MT mvalue in managedColumn)
            {
                ScopeManagedInterop::CopyToNativeColumn(nvalue, mvalue, alloc);
                nativeColumn.AddShallow(nvalue);
            }
        }

        template<typename NT, typename MT>
        static void CopyToNativeColumn(ScopeEngine::ScopeArrayNative<NT> & nativeColumn, Microsoft::SCOPE::Types::ScopeArray<MT>^ managedColumn, IncrementalAllocator * alloc)
        {
            // Note: ScopeArrayNative is the same class for both SQLIP as well as SCOPE
            CopyArrayToNativeColumn<NT, MT>(nativeColumn, managedColumn, alloc);
        }

        template<typename NT, typename MT>
        static void CopyToNativeColumn(ScopeEngine::ScopeArrayNative<NT> & nativeColumn, Microsoft::Analytics::Types::Sql::SqlArray<MT>^ managedColumn, IncrementalAllocator * alloc)
        {
            // Note: ScopeArrayNative is the same class for both SQLIP as well as SCOPE
            CopyArrayToNativeColumn<NT, MT>(nativeColumn, managedColumn, alloc);
        }

        template<typename NT, typename MT>
        static INLINE void CopyToManagedColumn(const NT & nativeColumn, MT % managedColumn)
        {
            managedColumn = nativeColumn;
        }

        template<typename NT, typename MT>
        static INLINE void CopyToPManagedColumn(const NT & nativeColumn, MT^ % managedColumn)
        {
            if (nativeColumn.IsNull())
            {
                managedColumn = nullptr;
            }
            else
            {
#pragma warning (suppress : 4800) // 'char': forcing value to bool 'true' or 'false' because SqlType_Bit uses __int8 as bool
                managedColumn = gcnew MT(nativeColumn.get().getValue());
            }
        }


#if !defined(SCOPE_NO_UDT)

        template<int UserDefinedTypeId, typename MT>
        static INLINE void CopyToManagedColumn(SqlUserDefinedType<UserDefinedTypeId> & nativeColumn, MT % managedColumn)
        {
            managedColumn = (MT)nativeColumn.Get();
        }
        
#endif

        static INLINE System::String^ CreateManagedString(const char *buffer, int size)
        {
            System::Text::UTF8Encoding^ utf8Encoding = ScopeEngineManaged::Utils::UTF8EncodingWithoutBOM;

            // TODO replace implementation below when we move to .Net 4.6 
            // managedColumn = utf8Encoding->GetString(buffer, size);

            int nchars = utf8Encoding->GetCharCount((unsigned char *)buffer, size);
            std::unique_ptr<wchar_t[]> charBuf(new wchar_t[nchars]);

            int actualChars = utf8Encoding->GetChars((unsigned char *)buffer, size, charBuf.get(), nchars);
            SCOPE_ASSERT(nchars == actualChars);

            return gcnew System::String(charBuf.get(), 0, nchars);
        }

        static INLINE void CopyToManagedColumn(const FString & nativeColumn, System::String^ % managedColumn)
        {
            if (nativeColumn.IsNull())
            {
                managedColumn = nullptr;
            }
            else if (0 == nativeColumn.size())
            {
                managedColumn = System::String::Empty;
            }
            else
            {
                managedColumn = CreateManagedString(nativeColumn.buffer(), nativeColumn.size());
            }
        }

        static INLINE void CopyToManagedColumn(ScopeSqlType::SqlType_Bit & nativeColumn, Microsoft::Analytics::Types::Sql::SqlBit^ % managedColumn)
        {
            CopyToPManagedColumn(nativeColumn, managedColumn);
        }

        static INLINE void CopyToManagedColumn(ScopeSqlType::SqlType_Byte & nativeColumn, Microsoft::Analytics::Types::Sql::SqlByte^ % managedColumn)
        {
            CopyToPManagedColumn(nativeColumn, managedColumn);
        }

        static INLINE void CopyToManagedColumn(ScopeSqlType::SqlType_Int16 & nativeColumn, Microsoft::Analytics::Types::Sql::SqlInt16^ % managedColumn)
        {
            CopyToPManagedColumn(nativeColumn, managedColumn);
        }

        static INLINE void CopyToManagedColumn(ScopeSqlType::SqlType_Int32 & nativeColumn, Microsoft::Analytics::Types::Sql::SqlInt32^ % managedColumn)
        {
            CopyToPManagedColumn(nativeColumn, managedColumn);
        }

        static INLINE void CopyToManagedColumn(ScopeSqlType::SqlType_Int64 & nativeColumn, Microsoft::Analytics::Types::Sql::SqlInt64^ % managedColumn)
        {
            CopyToPManagedColumn(nativeColumn, managedColumn);
        }

        static INLINE void CopyToManagedColumn(ScopeSqlType::SqlType_Single & nativeColumn, Microsoft::Analytics::Types::Sql::SqlSingle^ % managedColumn)
        {
            CopyToPManagedColumn(nativeColumn, managedColumn);
        }

        static INLINE void CopyToManagedColumn(ScopeSqlType::SqlType_Double & nativeColumn, Microsoft::Analytics::Types::Sql::SqlDouble^ % managedColumn)
        {
            CopyToPManagedColumn(nativeColumn, managedColumn);
        }

        static INLINE void CopyToManagedColumn(ScopeSqlType::SqlType_Decimal& nativeColumn, Microsoft::Analytics::Types::Sql::SqlDecimal^ % managedColumn)
        {
            if (nativeColumn.IsNull())
            {
                managedColumn = nullptr;
            }
            else
            {
                // TODO: Directly access constructor that uses System.Data.SqlTypes.SqlDecimal when generated code can access internal methods of SqlDecimal.cs
                System::Data::SqlTypes::SqlDecimal sqlDecimal(
                    nativeColumn.get().BPrec(),
                    nativeColumn.get().BScale(),
                    nativeColumn.get().FPositive(),
                    nativeColumn.get().UlData(0),
                    nativeColumn.get().UlData(1),
                    nativeColumn.get().UlData(2),
                    nativeColumn.get().UlData(3));
                managedColumn = gcnew Microsoft::Analytics::Types::Sql::SqlDecimal(sqlDecimal.Value);
            }
        }

        static INLINE void CopyToManagedColumn(ScopeSqlType::SqlType_Date& nativeColumn, Microsoft::Analytics::Types::Sql::SqlDate^ % managedColumn)
        {
            if (nativeColumn.IsNull())
            {
                managedColumn = nullptr;
            }
            else
            {
                // TODO: vustef
                // Currently, conversion is done between System.Data.SqlTypes.SqlDateTime(which is a backing store of Microsoft.Analytics.Types.SqlDate)
                // and ScopeEngine::ScopeSqlType::SqlDateTime, which represents SQL Server's datetime2 native implementation. The latter has dates no earlier than 1/1/1753
                // while the former support wider range. Also, native implementation supports higher precision. These problems need
                // to be resolved in the future code changes.
                ScopeSqlType::SqlDateTime sqlDateTime = nativeColumn.get();
                System::Data::SqlTypes::SqlDateTime managedSqlDateTime(sqlDateTime.getYear(), sqlDateTime.getMonth(), sqlDateTime.getDay(), sqlDateTime.getHour(), sqlDateTime.getMinute(), sqlDateTime.getSecond());
                System::DateTime managedDateTime = (System::DateTime)managedSqlDateTime;
                managedColumn = gcnew Microsoft::Analytics::Types::Sql::SqlDate(managedDateTime);
            }
        }

        template <int N>
        static INLINE void CopyToManagedColumn(ScopeSqlType::SqlType_MaxString<N> & nativeColumn, Microsoft::Analytics::Types::Sql::SqlString^ % managedColumn)
        {
            if (nativeColumn.IsNull())
            {
                managedColumn = nullptr;
            }
            else if (0 == nativeColumn.get().Size())
            {
                managedColumn = ScopeEngineManaged::Utils::EmptySqlString;
            }
            else
            {
                System::String^ managedString = CreateManagedString(nativeColumn.get().Buffer(), nativeColumn.get().Size());
                managedColumn = gcnew Microsoft::Analytics::Types::Sql::SqlString(managedString);
            }
        }
                
        static INLINE void CopyToManagedColumn(const FBinary & nativeColumn, cli::array<System::Byte>^ % managedColumn)
        {
            if (nativeColumn.IsNull())
            {
                managedColumn = nullptr;
            }
            else
            {
                managedColumn = CopyToManagedBuffer(nativeColumn.buffer(), nativeColumn.size());
            }            
        }   
        
        template<typename NT, typename MT>
        static INLINE void CopyToManagedColumn(const NativeNullable<NT> & nativeColumn, MT % managedColumn)
        {        
            if (nativeColumn.IsNull())
            {
                managedColumn = MT(); // set null
            }
            else
            {
                CopyToManagedColumn(nativeColumn.get(), managedColumn);
            }            
        }     
        
        static INLINE void CopyToManagedColumn(const ScopeDateTime & nativeColumn, System::DateTime % managedColumn)
        {
            managedColumn = System::DateTime::FromBinary(nativeColumn.ToBinary());
        }
       
        static INLINE void CopyToManagedColumn(const ScopeDecimal & nativeColumn, System::Decimal % managedColumn)
        {
            managedColumn = System::Decimal(nativeColumn.Lo32Bit(), 
                                            nativeColumn.Mid32Bit(), 
                                            nativeColumn.Hi32Bit(), 
                                            nativeColumn.Sign() > 0, 
                                            (unsigned char)nativeColumn.Scale());
        }

        static INLINE void CopyToManagedColumn(const ScopeGuid & nativeColumn, System::Guid % managedColumn)
        {
            GUID guid = nativeColumn.get();
            managedColumn = System::Guid(guid.Data1, guid.Data2, guid.Data3, 
                                         guid.Data4[0], guid.Data4[1], guid.Data4[2], guid.Data4[3],
                                         guid.Data4[4], guid.Data4[5], guid.Data4[6], guid.Data4[7]);
        }

        static INLINE void CopyToManagedColumn(const ScopeDateTime & nativeColumn, System::Nullable<System::DateTime> % managedColumn)
        {
            managedColumn = System::DateTime::FromBinary(nativeColumn.ToBinary());
        }
       
        static INLINE void CopyToManagedColumn(const ScopeDecimal & nativeColumn, System::Nullable<System::Decimal> % managedColumn)
        {
            managedColumn = System::Decimal(nativeColumn.Lo32Bit(), 
                                            nativeColumn.Mid32Bit(), 
                                            nativeColumn.Hi32Bit(), 
                                            nativeColumn.Sign() > 0, 
                                            (unsigned char)nativeColumn.Scale());
        }

        static INLINE void CopyToManagedColumn(const ScopeGuid & nativeColumn, System::Nullable<System::Guid> % managedColumn)
        {
            GUID guid = nativeColumn.get();
            managedColumn = System::Guid(guid.Data1, guid.Data2, guid.Data3, 
                                         guid.Data4[0], guid.Data4[1], guid.Data4[2], guid.Data4[3],
                                         guid.Data4[4], guid.Data4[5], guid.Data4[6], guid.Data4[7]);
        }

        template<typename NK, typename NV, typename MK, typename MV>
        static INLINE Generic::Dictionary<MK, MV>^  CopyToManagedDictionary(const ScopeEngine::ScopeMapNative<NK, NV>& nativeColumn)
        {
            // Note: ScopeMapNative is the same class for both SQLIP as well as SCOPE
            if (nativeColumn.IsNull())
            {
                return nullptr;
            }

            /// Copy to managed columns's manage space, did not hook native map.
            /// TODO: optimze to whole copy (native serialize and then manage deserialize) if there are many key/valuses 
            Generic::Dictionary<MK, MV>^ buffer = gcnew Generic::Dictionary<MK, MV>();
            MK mkey;
            MV mvalue;
            for (auto iter = nativeColumn.begin(); iter != nativeColumn.end(); ++iter)
            {
                ScopeManagedInterop::CopyToManagedColumn(iter.Key(), mkey);
                ScopeManagedInterop::CopyToManagedColumn(iter.Value(), mvalue);
                buffer[mkey] = mvalue;
            }

            return buffer;
        }

        template<typename NK, typename NV, typename MK, typename MV>
        static INLINE void CopyToManagedColumn(const ScopeEngine::ScopeMapNative<NK, NV>& nativeColumn, Microsoft::SCOPE::Types::ScopeMap<MK, MV>^ % managedColumn)
        {
            // Note: ScopeMapNative is the same class for both SQLIP as well as SCOPE
            Generic::Dictionary<MK, MV>^ buffer = CopyToManagedDictionary<NK, NV, MK, MV>(nativeColumn);
            managedColumn = buffer == nullptr ? nullptr : gcnew Microsoft::SCOPE::Types::ScopeMap<MK, MV>(buffer);
        }

        template<typename NK, typename NV, typename MK, typename MV>
        static INLINE void CopyToManagedColumn(const ScopeEngine::ScopeMapNative<NK, NV>& nativeColumn, Microsoft::Analytics::Types::Sql::SqlMap<MK, MV>^ % managedColumn)
        {
            // Note: ScopeMapNative is the same class for both SQLIP as well as SCOPE
            Generic::Dictionary<MK, MV>^ buffer = CopyToManagedDictionary<NK, NV, MK, MV>(nativeColumn);
            managedColumn = buffer == nullptr ? nullptr : gcnew Microsoft::Analytics::Types::Sql::SqlMap<MK, MV>(buffer);
        }

        template<typename NT, typename MT>
        static INLINE Generic::List<MT>^ CopyToManagedList(const ScopeEngine::ScopeArrayNative<NT>& nativeColumn)
        {
            // Note: ScopeArrayNative is the same class for both SQLIP as well as SCOPE
            if (nativeColumn.IsNull())
            {
                return nullptr;
            }

            Generic::List<MT>^ buffer = gcnew Generic::List<MT>();
            MT mvalue;
            for (auto iter = nativeColumn.begin(); iter != nativeColumn.end(); ++iter)
            {
                ScopeManagedInterop::CopyToManagedColumn(iter.Value(), mvalue);
                buffer->Add(mvalue);
            }

            return buffer;
        }

        template<typename NT, typename MT>
        static INLINE void CopyToManagedColumn(const ScopeEngine::ScopeArrayNative<NT>& nativeColumn, Microsoft::SCOPE::Types::ScopeArray<MT>^ % managedColumn)
        {
            // Note: ScopeArrayNative is the same class for both SQLIP as well as SCOPE
            Generic::List<MT>^ buffer = CopyToManagedList<NT, MT>(nativeColumn);
            managedColumn = buffer == nullptr ? nullptr : gcnew Microsoft::SCOPE::Types::ScopeArray<MT>(buffer);
        }

        template<typename NT, typename MT>
        static INLINE void CopyToManagedColumn(const ScopeEngine::ScopeArrayNative<NT>& nativeColumn, Microsoft::Analytics::Types::Sql::SqlArray<MT>^ % managedColumn)
        {
            // Note: ScopeArrayNative is the same class for both SQLIP as well as SCOPE
            Generic::List<MT>^ buffer = CopyToManagedList<NT, MT>(nativeColumn);
            managedColumn = buffer == nullptr ? nullptr : gcnew Microsoft::Analytics::Types::Sql::SqlArray<MT>(buffer);
        }
    };

    //
    // ScopeCosmosInputStream implements the CLR Stream interface used by user extractors 
    template<typename InputType>
    ref class ScopeCosmosInputStream : public System::IO::Stream
    {

    protected:

        InputType      *m_input;
        bool            m_isOpened;
        bool            m_ownStream;

    public:

        ScopeCosmosInputStream(const std::string& name, SIZE_T bufSize, int bufCount)
            : m_input(new InputType(name, bufSize, bufCount)),
              m_isOpened(false),
              m_ownStream(true)
        {
        }

        ScopeCosmosInputStream(BlockDevice* device, SIZE_T bufSize, int bufCount)
            : m_input(new InputType(device, bufSize, bufCount)),
              m_isOpened(false),
              m_ownStream(true)        
        {
        }
        
        ScopeCosmosInputStream(InputType* input, SIZE_T /*bufSize*/, int /*bufCount*/)
            : m_input(input),
              m_isOpened(true),
              m_ownStream(false)
        {
        }

        ~ScopeCosmosInputStream()
        {
            this->!ScopeCosmosInputStream();
        }

        !ScopeCosmosInputStream()
        {
            if (m_ownStream)
            {
                delete m_input;
                m_input = nullptr;
            }
        }
        
        void Init()
        {
            m_input->Init();
            m_isOpened = true;
        }

        void SetLowReadLatency(bool val)
        {
            m_input->SetLowLatency(val);
        }

        property bool IsCompressed
        {
            bool get()
            {
                return m_input->IsCompressed();
            }             
        }
        
        //
        // Stream part
        //
        virtual property bool CanRead
        {
            bool get() override
            {
                return true;
            }         
        }

        virtual property bool CanSeek
        {
            bool get() override
            {
                return true;
            }         
        }

        virtual property bool CanWrite
        {
            bool get() override
            {
                return false;
            }         
        }

        virtual property LONGLONG Length
        {
            LONGLONG get() override
            {
                return m_input->Length();
            }
        }

        virtual property LONGLONG Position
        {
            LONGLONG get() override
            {
                return m_input->GetCurrentPosition();
            }

            void set(LONGLONG pos) override
            {
                Seek(pos, System::IO::SeekOrigin::Begin);
            }
        }

        // The Flush actually does not flush anything. It is used to track the row boundaries. 
        // This semantics of Flush is carried over from the old managed runtime :-(
        virtual void Flush() override
        {
            throw gcnew InvalidOperationException("Flush is not supported for read-only streams");
        }

        virtual LONGLONG Seek(LONGLONG pos, System::IO::SeekOrigin origin) override
        {
            ULONGLONG newPosition;
            switch(origin)
            {
            case System::IO::SeekOrigin::Begin: newPosition = pos;
                break;
                
            case System::IO::SeekOrigin::Current: newPosition = Position + pos;
                break;
                
            case System::IO::SeekOrigin::End: newPosition = Length + pos;
                break;
                
            default: newPosition = numeric_limits<ULONGLONG>::max();
                SCOPE_ASSERT(0);
            }

            if (newPosition > (ULONGLONG)Length)
            {
                throw gcnew InvalidOperationException(
                    System::String::Format("seeking past the end, poisition= {0}, stream name={1}", 
                    newPosition, 
                    msclr::interop::marshal_as<System::String^>(GetName().c_str())));
            }

            return m_input->Seek(newPosition);
        }

        virtual void SetLength(LONGLONG) override
        {
            throw gcnew InvalidOperationException("SetLength is not supported for read-only streams");
        }

        virtual int Read(cli::array<unsigned char>^ buffer, int offset, int count) override
        {
            if (count > 0)
            {
                pin_ptr<unsigned char> bufdst = &buffer[0];
                return m_input->Read((char*)(bufdst + offset), count);
            }

            return 0;
        }

        virtual void Write(cli::array<unsigned char>^, int, int) override
        {
            throw gcnew InvalidOperationException("Write is not supported for read-only streams");
        }

        virtual void Close() override
        {
            if (!m_ownStream)
            {
                return;
            }

            if (!m_isOpened)
            {
                return; // it has been closed.
            }
        
            m_isOpened = false;
            m_input->Close();
        }

        __int64 GetTotalIoWaitTime()
        {
            return m_input->GetTotalIoWaitTime();
        }

        __int64 GetInclusiveTimeMillisecond()
        {
            return m_input->GetInclusiveTimeMillisecond();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            m_input->WriteRuntimeStats(root);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            OperatorRequirements result = OperatorRequirements();
            result.Add(m_input->GetOperatorRequirements());
            return result;
        }

        std::string GetName()
        {
            return m_input->StreamName();
        }

        void SaveState(BinaryOutputStream& output, UINT64 position)
        {
            m_input->SaveState(output, position);
        }

        void LoadState(BinaryInputStream& input)
        {
            return m_input->LoadState(input);
        }     
    };

    //
    // ScopeCosmosOutputStream implements the CLR Stream interface used by user outputter
    ref class ScopeCosmosOutputStream : public System::IO::Stream
    {
    protected:
    
        CosmosOutput*   m_output;
        bool            m_isOpened;
        bool            m_ownStream;

    public:

        ScopeCosmosOutputStream(const std::string& name, SIZE_T bufSize, int bufCount, bool maintainBoundaries) : 
            m_output(new CosmosOutput(name, bufSize, bufCount, maintainBoundaries)),
            m_isOpened(false),
            m_ownStream(true)
        {
        }


        ScopeCosmosOutputStream(const std::string& name, SIZE_T bufSize, int bufCount, CosmosOutput::SettingFlags cosmosOutputSettingFlags) :
            m_output(new CosmosOutput(name, bufSize, bufCount, cosmosOutputSettingFlags)),
            m_isOpened(false),
            m_ownStream(true)
        {
        }

        ScopeCosmosOutputStream(BlockDevice* device, SIZE_T bufSize, int bufCount, bool maintainBoundaries) : 
            m_output(new CosmosOutput(device, bufSize, bufCount, maintainBoundaries)),
            m_isOpened(false),
            m_ownStream(true)
        {
        }

        ScopeCosmosOutputStream(BlockDevice* device, SIZE_T bufSize, int bufCount, CosmosOutput::SettingFlags cosmosOutputSettingFlags) :
            m_output(new CosmosOutput(device, bufSize, bufCount, cosmosOutputSettingFlags)),
            m_isOpened(false),
            m_ownStream(true)
        {
        }

        ScopeCosmosOutputStream(CosmosOutput* output, SIZE_T /*bufSize*/, int /*bufCount*/) :
            m_output(output),
            m_isOpened(true),
            m_ownStream(false)
        {
        }

        ~ScopeCosmosOutputStream()
        {
            this->!ScopeCosmosOutputStream();
        }

        !ScopeCosmosOutputStream()
        {
            if (m_ownStream)
            {
                delete m_output;
                m_output = nullptr;
            }
        }

        void Init()
        {
            m_output->Init();
            m_isOpened = true;
        }

        void SetMaintainBoundaryMode(bool val)
        {
            m_output->SetMaintainBoundaryMode(val);
        }

        //
        // Stream part
        //
        virtual property bool CanRead
        {
            bool get() override
            {
                return false;
            }         
        }

        virtual property bool CanSeek
        {
            bool get() override
            {
                // Only read-only streams are seekable
                return false;
            }         
        }

        virtual property bool CanWrite
        {
            bool get() override
            {
                return true;
            }         
        }

        virtual property LONGLONG Length
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Length is supported only for read-only streams");
            }
        }

        virtual property LONGLONG Position
        {
            LONGLONG get() override
            {
                return m_output->GetCurrentPosition();
            }

            void set(LONGLONG pos) override
            {
                Seek(pos, System::IO::SeekOrigin::Begin);
            }
        }

        // The Flush actually does not flush anything. It is used to track the row boundaries. 
        // This semantics of Flush is carried over from the old managed runtime :-(
        virtual void Flush() override
        {
            m_output->Commit();
        }

        virtual LONGLONG Seek(LONGLONG, System::IO::SeekOrigin) override
        {
            throw gcnew InvalidOperationException("Seek is not implemented");
        }

        virtual void SetLength(LONGLONG) override
        {
            throw gcnew InvalidOperationException("SetLength is not implemented");
        }

        virtual int Read(cli::array<unsigned char>^, int, int) override
        {
            throw gcnew InvalidOperationException("Read is not implemented");
        }

        virtual void Write(cli::array<unsigned char>^ buffer, int offset, int count) override
        {
            if (count > 0)
            {
                pin_ptr<unsigned char> bufsrc = &buffer[0];
                m_output->Write((const char*)bufsrc + offset, count);
            }
        }

        virtual void Close() override
        {
            if (!m_ownStream)
            {
                return;
            }

            if (!m_isOpened)
            {
                return; // it has been closed.
            }
        
            m_isOpened = false;
            m_output->Finish();
            m_output->Close();
        }

        __int64 GetTotalIoWaitTime()
        {
            return m_output->GetTotalIoWaitTime();
        }

        __int64 GetInclusiveTimeMillisecond()
        {
            return m_output->GetInclusiveTimeMillisecond();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            m_output->WriteRuntimeStats(root);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }

        std::string GetName()
        {
            return m_output->StreamName();
        }

        void FlushAllData()
        {
            m_output->Flush();
        }

        void SaveState(BinaryOutputStream& output)
        {
            m_output->SaveState(output);
        }

        void LoadState(BinaryInputStream& input)
        {
            return m_output->LoadState(input);
        }
    };

    // in the managed sslib, it'll catch exception to do special handling
    // therefore, it'll lead interop if DeviceException happens
    // default exception interop wrapper will lose some information
    // this class pass the detail native exception information to managed runtime.
    // so far, it's only used in the SStreamV2Extractor wrapper
    // so, it only override Seek, Read and Close methods
    ref class ManagedSSLibScopeCosmosInputStream : public ScopeCosmosInputStream<CosmosInput>
    {

    public:

        ManagedSSLibScopeCosmosInputStream(BlockDevice* device, SIZE_T bufSize, int bufCount)
            : ScopeCosmosInputStream<CosmosInput>(device, bufSize, bufCount)
        {
            ScopeCosmosInputStream<CosmosInput>::Init();
        }

        virtual LONGLONG Seek(LONGLONG pos, System::IO::SeekOrigin origin) override
        {
            try
            {
                return ScopeCosmosInputStream<CosmosInput>::Seek(pos, origin);
            }
            catch (std::exception& ex)
            {
                throw gcnew System::IO::IOException(gcnew System::String(ex.what()));
            }
        }

        virtual int Read(cli::array<unsigned char>^ buffer, int offset, int count) override
        {
            try
            {
                return ScopeCosmosInputStream<CosmosInput>::Read(buffer, offset, count);
            }
            catch (std::exception& ex)
            {
                throw gcnew System::IO::IOException(gcnew System::String(ex.what()));
            }
        }

        virtual void Close() override
        {
            try
            {
                ScopeCosmosInputStream<CosmosInput>::Close();
            }
            catch (std::exception& ex)
            {
                throw gcnew System::IO::IOException(msclr::interop::marshal_as<System::String^>(ex.what()));
            }
        }
    };

    //
    template<class StreamClass>
    ref class ScopeStreamWrapper : public System::IO::Stream
    {
        StreamClass * m_baseStream;
        
    public:

        ScopeStreamWrapper(StreamClass * baseStream) : m_baseStream(baseStream)
        {
        }

        //
        // Stream part
        //
        virtual property bool CanRead
        {
            bool get() override
            {
                throw gcnew InvalidOperationException("CanRead::get is not implemented");
            }
        }

        virtual property bool CanSeek
        {
            bool get() override
            {
                throw gcnew InvalidOperationException("CanSeek::get is not implemented");
            }
        }

        virtual property bool CanWrite
        {
            bool get() override
            {
                throw gcnew InvalidOperationException("CanWrite::get is not implemented");
            }
        }

        virtual property LONGLONG Length
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Length::get is not implemented");
            }
        }

        virtual property LONGLONG Position
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Position::get is not implemented");
            }

            void set(LONGLONG pos) override
            {
                throw gcnew InvalidOperationException("Position::set is not implemented");
            }
        }

        virtual void Flush() override
        {
            throw gcnew InvalidOperationException("Flush is not implemented");
        }

        virtual LONGLONG Seek(LONGLONG pos, System::IO::SeekOrigin origin) override
        {
            throw gcnew InvalidOperationException("Seek is not implemented");
        }

        virtual void SetLength(LONGLONG length) override
        {
            throw gcnew InvalidOperationException("SetLength is not implemented");
        }

        virtual int Read(cli::array<unsigned char>^ buffer, int offset, int count) override
        {
            throw gcnew InvalidOperationException("Read is not implemented");
        }

        virtual void Write(cli::array<unsigned char>^ buffer, int offset, int count) override
        {
            throw gcnew InvalidOperationException("Write is not implemented");
        }

        virtual void Close() override
        {
            throw gcnew InvalidOperationException("Close is not implemented");
        }
    };

    //
    template<typename Input>
    ref class ScopeStreamWrapper<BinaryInputStreamBase<Input>> : public System::IO::Stream
    {
        BinaryInputStreamBase<Input> * m_baseStream;
        
    public:
        ScopeStreamWrapper(BinaryInputStreamBase<Input> * baseStream) : m_baseStream(baseStream)
        {
        }

        //
        // Stream part
        //
        virtual property bool CanRead
        {
            bool get() override
            {
                return true;
            }
        }

        virtual property bool CanSeek
        {
            bool get() override
            {
                return false;
            }
        }

        virtual property bool CanWrite
        {
            bool get() override
            {
                return false;
            }
        }

        virtual property LONGLONG Length
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Length::get is not implemented");
            }
        }

        virtual property LONGLONG Position
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Position::get is not implemented");
            }

            void set(LONGLONG) override
            {
                throw gcnew InvalidOperationException("Position::set is not implemented");
            }
        }

        virtual void Flush() override
        {
            throw gcnew InvalidOperationException("Flush is not implemented");
        }

        virtual LONGLONG Seek(LONGLONG, System::IO::SeekOrigin) override
        {
            throw gcnew InvalidOperationException("Seek is not implemented");
        }

        virtual void SetLength(LONGLONG) override
        {
            throw gcnew InvalidOperationException("SetLength is not implemented");
        }

        virtual int Read(cli::array<unsigned char>^ buffer, int offset, int count) override
        {
            if (count > 0)
            {
                pin_ptr<unsigned char> bufdst = &buffer[0];
                return m_baseStream->Read((char*)(bufdst + offset), count);
            }

            return 0;
        }

        virtual void Write(cli::array<unsigned char>^, int, int) override
        {
            throw gcnew InvalidOperationException("Write is not implemented");
        }

        virtual void Close() override
        {
        }
    };

    // Managed wrapper for binary output streams
    template<typename Output>
    ref class ScopeStreamWrapper<BinaryOutputStreamBase<Output>> : public System::IO::Stream
    {
        BinaryOutputStreamBase<Output> * m_baseStream;
        
    public:

        ScopeStreamWrapper(BinaryOutputStreamBase<Output> * baseStream) : m_baseStream(baseStream)
        {
        }

        //
        // Stream part
        //
        virtual property bool CanRead
        {
            bool get() override
            {
                return false;
            }
        }

        virtual property bool CanSeek
        {
            bool get() override
            {
                return false;
            }
        }

        virtual property bool CanWrite
        {
            bool get() override
            {
                return true;
            }
        }

        virtual property LONGLONG Length
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Length is supported only for write stream");
            }
        }

        virtual property LONGLONG Position
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Position::get is not implemented");
            }

            void set(LONGLONG) override
            {
                throw gcnew InvalidOperationException("Position::set is not implemented");
            }
        }

        virtual void Flush() override
        {
        }

        virtual LONGLONG Seek(LONGLONG, System::IO::SeekOrigin) override
        {
            throw gcnew InvalidOperationException("Seek is not implemented");
        }

        virtual void SetLength(LONGLONG) override
        {
            throw gcnew InvalidOperationException("SetLength is not implemented");
        }

        virtual int Read(cli::array<unsigned char>^, int, int) override
        {
            throw gcnew InvalidOperationException("Read is not implemented");
        }

        virtual void Write(cli::array<unsigned char>^ buffer, int offset, int count) override
        {
            if (count > 0)
            {
                pin_ptr<unsigned char> bufdst = &buffer[0];
                return m_baseStream->Write((char*)(bufdst + offset), count);
            }
        }

        virtual void Close() override
        {
        }
    };

    // Managed wrapper for text output streams
    template<>
    ref class ScopeStreamWrapper<TextOutputStreamBase> : public System::IO::Stream
    {

        TextOutputStreamBase * m_baseStream;
        
    public:

        ScopeStreamWrapper(TextOutputStreamBase * baseStream) : m_baseStream(baseStream)
        {
        }
    
        //
        // Stream part
        //
        virtual property bool CanRead
        {
            bool get() override
            {
                return false;
            }
        }
    
        virtual property bool CanSeek
        {
            bool get() override
            {
                return false;
            }
        }
    
        virtual property bool CanWrite
        {
            bool get() override
            {
                return true;
            }
        }
    
        virtual property LONGLONG Length
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Length is supported only for write stream");
            }
        }
    
        virtual property LONGLONG Position
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Position::get is not implemented");
            }
    
            void set(LONGLONG /*pos*/) override
            {
                throw gcnew InvalidOperationException("Position::set is not implemented");
            }
        }
    
        virtual void Flush() override
        {
        }
    
        virtual LONGLONG Seek(LONGLONG, System::IO::SeekOrigin) override
        {
            throw gcnew InvalidOperationException("Seek is not implemented");
        }
    
        virtual void SetLength(LONGLONG) override
        {
            throw gcnew InvalidOperationException("SetLength is not implemented");
        }
    
        virtual int Read(cli::array<unsigned char>^, int, int) override
        {
            throw gcnew InvalidOperationException("Read is not implemented");
        }
    
        virtual void Write(cli::array<unsigned char>^ buffer, int offset, int count) override
        {
            if (count > 0)
            {
                pin_ptr<unsigned char> bufdst = &buffer[0];
                return m_baseStream->WriteToOutput((char*)(bufdst + offset), count);
            }
        }

        virtual void Close() override
        {
        }
    };


    // Managed wrapper for sstream data output streams
    template<>
    ref class ScopeStreamWrapper<SStreamDataOutputStream> : public System::IO::Stream
    {

        SStreamDataOutputStream * m_baseStream;
        
    public:

        ScopeStreamWrapper(SStreamDataOutputStream * baseStream) : m_baseStream(baseStream)
        {
        }
    
        //
        // Stream part
        //
        virtual property bool CanRead
        {
            bool get() override
            {
                return false;
            }
        }
    
        virtual property bool CanSeek
        {
            bool get() override
            {
                return false;
            }
        }
    
        virtual property bool CanWrite
        {
            bool get() override
            {
                return true;
            }
        }
    
        virtual property LONGLONG Length
        {
            LONGLONG get() override
            {
                throw gcnew InvalidOperationException("Length is supported only for write stream");
            }
        }
    
        virtual property LONGLONG Position
        {
            LONGLONG get() override
            {
                return m_baseStream->GetPosition();
            }
    
            void set(LONGLONG /*pos*/) override
            {
                throw gcnew InvalidOperationException("Position::set is not implemented");
            }
        }
    
        virtual void Flush() override
        {
        }
    
        virtual LONGLONG Seek(LONGLONG, System::IO::SeekOrigin) override
        {
            throw gcnew InvalidOperationException("Seek is not implemented");
        }
    
        virtual void SetLength(LONGLONG) override
        {
            throw gcnew InvalidOperationException("SetLength is not implemented");
        }
    
        virtual int Read(cli::array<unsigned char>^, int, int) override
        {
            throw gcnew InvalidOperationException("Read is not implemented");
        }
    
        virtual void Write(cli::array<unsigned char>^ buffer, int offset, int count) override
        {
            if (count > 0)
            {
                pin_ptr<unsigned char> bufdst = &buffer[0];
                return m_baseStream->Write((char*)(bufdst + offset), count);
            }
        }

        virtual void Close() override
        {
        }
    };
    
    INLINE cli::array<String^>^ ConvertArgsToArray(const std::wstring& wargs)
    {
        String^ args = gcnew String(wargs.c_str());
        cli::array<String^>^ arr_args = args->Length > 0 ? args->Split(gcnew cli::array<wchar_t>(2){L' ', L'\t'}) : gcnew cli::array<String^>(0){};
        for(int i=0; i<arr_args->Length; ++i)
        {
            arr_args[i] = ScopeEngineManaged::EscapeStrings::UnEscape(arr_args[i]);
        }

        return arr_args;
    }        
} // namespace ScopeEngine