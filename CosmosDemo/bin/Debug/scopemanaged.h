#pragma once

// Enable this code once generated C++ files are split
#ifndef __cplusplus_cli
#error "Managed scope operators must be compiled with /clr"
#endif

#include "OperatorRequirementsDef.h"
#include "Managed.h"

using namespace OperatorRequirements;

namespace ScopeEngine
{
    //
    // Special allocator for the Native->Managed Interop
    //
    class InteropAllocator : public IncrementalAllocator
    {
        std::vector<ULONGLONG> m_objectId;
        ULONGLONG m_nextObjectId;

        ULONGLONG GetObjectId(int pos) const
        {
            SCOPE_ASSERT(pos < m_objectId.size());

            return m_objectId[pos];
        }

        ULONGLONG SetObjectId(int pos)
        {
            SCOPE_ASSERT(pos < m_objectId.size());

            return (m_objectId[pos] = ++m_nextObjectId);
        }

        void ResetObjectId(int pos)
        {
            SCOPE_ASSERT(pos < m_objectId.size());

            m_objectId[pos] = 0;
        }

#ifndef SCOPE_INTEROP_NOOPT
        template<typename T>
        bool NeedsCopying(T & obj, int columnId)
        {
            bool doCopy = true;

            USHORT allocId;
            ULONG  allocVersion;
            ULONGLONG destId;

            if (obj.GetSharedPtrInfo(allocId, allocVersion, destId))
            {
                // Ignore allocator version
                if (allocId == Id() && destId == GetObjectId(columnId))
                {
                    // Everything matches, may re-use managed object
                    doCopy = false;
                }
                else
                {
                    ULONGLONG objectId = SetObjectId(columnId);

                    // Update shared pointer and allocator's object table with objectId
                    bool succeed = obj.SetSharedPtrInfo(Id(), Version(), objectId);
                    SCOPE_ASSERT(succeed);
                }
            }
            else
            {
                ResetObjectId(columnId);
            }

            return doCopy;
        }
#else
        template<typename T>
        bool NeedsCopying(T &, int)
        {
            return true;
        }
#endif

     public:

        static System::IO::UnmanagedMemoryStream^ CreateMemoryStream(const void * cbuf, int size)
        {
            SCOPE_ASSERT(size >= 0);
            void* buf = const_cast<void*>(cbuf);
            System::IO::UnmanagedMemoryStream^ stream = gcnew System::IO::UnmanagedMemoryStream(reinterpret_cast<unsigned char*>(buf), size, size, System::IO::FileAccess::Read);

            return stream;
        }

        InteropAllocator(int schemaSize, const char* ownerName) : IncrementalAllocator(1, ownerName), m_nextObjectId(0)
        {
            m_objectId.resize(schemaSize, 0);
        }

        template<typename T>
        void CopyToManagedColumn(T & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int)
        {
            managedColumn->Set(nativeColumn);
        }

        template<>
        void CopyToManagedColumn(FString & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int columnId)
        {
            if (NeedsCopying(nativeColumn, columnId))
            {
                if (nativeColumn.IsNull())
                {
                    managedColumn->SetNull();
                }
                else
                {
                    managedColumn->Set(ScopeManagedInterop::CopyToManagedBuffer(nativeColumn.buffer(), nativeColumn.size()));
                }
            }
        }

        template<>
        void CopyToManagedColumn(FBinary & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int columnId)
        {
            if (NeedsCopying(nativeColumn, columnId))
            {
                if (nativeColumn.IsNull())
                {
                    managedColumn->SetNull();
                }
                else
                {
                    if (nativeColumn.buffer())
                    {
                        managedColumn->Set(CreateMemoryStream(nativeColumn.buffer(), nativeColumn.size()));
                    }
                    else
                    {
                        managedColumn->Set(ScopeManagedInterop::CopyToManagedBuffer(nativeColumn.buffer(), nativeColumn.size()));
                    }
                }
            }
        }

        template<typename T>
        void CopyToManagedColumn(NativeNullable<T> & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int columnId)
        {
            if (nativeColumn.IsNull())
            {
                managedColumn->SetNull();
            }
            else
            {
                CopyToManagedColumn(nativeColumn.get(), managedColumn, columnId);
            }
        }

#if !defined(SCOPE_NO_UDT)

        template<int UserDefinedTypeId, template<int> class ScopeUserDefinedType>
        void CopyToManagedColumn(ScopeUserDefinedType<UserDefinedTypeId> & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int)
        {
            managedColumn->Set((System::Object^)nativeColumn.Get());
            nativeColumn.Reset();
        }

#endif
        template<>
        void CopyToManagedColumn<ScopeDateTime>(ScopeDateTime & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int)
        {
            managedColumn->Set(System::DateTime::FromBinary(nativeColumn.ToBinary()));
        }
        
        template<>
        void CopyToManagedColumn<ScopeDecimal>(ScopeDecimal & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int)
        {
            managedColumn->Set(System::Decimal(nativeColumn.Lo32Bit(), nativeColumn.Mid32Bit(), nativeColumn.Hi32Bit(), nativeColumn.Sign()>0, (unsigned char)nativeColumn.Scale()));
        }

        template<>
        void CopyToManagedColumn<ScopeGuid>(ScopeGuid & nativeColumn, ScopeRuntime::ColumnData^ managedColumn, int)
        {
            GUID guid = nativeColumn.get();
            managedColumn->Set(gcnew System::Guid(guid.Data1, guid.Data2, guid.Data3, guid.Data4[0], guid.Data4[1], guid.Data4[2], guid.Data4[3],
                guid.Data4[4], guid.Data4[5],guid.Data4[6], guid.Data4[7]));
        }

        template<typename NK, typename NV, typename MK, typename MV>
        void CopyToManagedColumn(ScopeEngine::ScopeMapNative<NK,NV> & nativeColumn, ScopeRuntime::MapColumnData<MK,MV>^ managedColumn, int)
        {
            Microsoft::SCOPE::Types::ScopeMap<MK,MV>^ scopeMapManaged = nullptr;
            ScopeManagedInterop::CopyToManagedColumn(nativeColumn, scopeMapManaged);
            managedColumn->Set(scopeMapManaged);
        }

        template<typename NT, typename MT>
        void CopyToManagedColumn(ScopeEngine::ScopeArrayNative<NT> & nativeColumn, ScopeRuntime::ArrayColumnData<MT>^ managedColumn, int)
        {
            Microsoft::SCOPE::Types::ScopeArray<MT>^ scopeArrayManaged = nullptr;
            ScopeManagedInterop::CopyToManagedColumn(nativeColumn, scopeArrayManaged);
            managedColumn->Set(scopeArrayManaged);            
        }
        
        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("InteropAllocator");
            node.AddAttribute(RuntimeStats::MaxCommittedSize(), m_objectId.capacity() * sizeof(ULONGLONG));
            node.AddAttribute(RuntimeStats::AvgCommittedSize(), m_objectId.capacity() * sizeof(ULONGLONG));
            IncrementalAllocator::WriteRuntimeStats(node);
        }
    };

    // Marshal a native row to a managed row
    template<typename RowSchema, int UID>
    class InteropToManagedRowPolicy
    {
    public:        
        static void Marshal(RowSchema& nativeRow, ScopeRuntime::Row^ managedRow, InteropAllocator * alloc);
        static void Marshal(RowSchema& nativeRow, typename KeyComparePolicy<RowSchema,UID>::KeyType& nativeKey, ScopeRuntime::Row^ managedRow, InteropAllocator * alloc);
    };    

    // Marshal a managed row to a native row
    template<typename RowSchema, int>
    class InteropToNativeRowPolicy
    {
    public:
        static void Marshal(ScopeRuntime::Row^ managedRow, RowSchema& nativeRow, IncrementalAllocator* alloc);
    };

    // Wraper for Row object to be used in native code
    template<typename Schema>
    struct ManagedRow
    {
        ScopeRuntime::Row ^ get();
    };

    template<int SchemaId>
    struct ManagedSStreamSchema
    {
        StructuredStream::StructuredStreamSchema^ get();
    };

    template<int SchemaId>
    INLINE ScopeSStreamSchemaStatic<SchemaId>::ScopeSStreamSchemaStatic()
    {
        m_managed = ManagedSStreamSchema<SchemaId>().get();
    }

    //
    // ScopeRowset is a managed class implementing ScopeRuntime.RowSet interface.
    // It can be used in any place where an input RowSet is expected (i.e. Processors, Reducers, Combiners).
    //
    // Input parametes:
    //        GetNextRow function - native function what generates a new row (i.e. pointer to some Operator::GetNextRow)
    //        A native row - passed to the GetNextRow
    //        Schema name - used to construct a managed row
    //
    ref class ScopeRowset abstract : public ScopeRuntime::RowSet, public Generic::IEnumerable<ScopeRuntime::Row^>
    {
        ref class ScopeRowsetEnumerator : public Generic::IEnumerator<ScopeRuntime::Row^>
        {
            bool                      m_hasRow;
            ScopeRowset^              m_inputRowset;
            unsigned __int64          m_rowCount;

        public:
            ScopeRowsetEnumerator(ScopeRowset^ inputRowset) :
                m_inputRowset(inputRowset), 
                m_hasRow(false),
                m_rowCount(0)
            {
            }

            ~ScopeRowsetEnumerator()
            {
            }

            property unsigned __int64 RowCount
            {
                unsigned __int64 get()
                {
                    return m_rowCount;
                }
            }

            property ScopeRuntime::Row^ Current
            {
                virtual ScopeRuntime::Row^ get()
                {
                    return m_hasRow ? m_inputRowset->DefaultRow : nullptr;
                }
            }

            property Object^ CurrentBase
            {
                virtual Object^ get() sealed = IEnumerator::Current::get
                {
                    return Current;
                }
            }
       
            virtual bool MoveNext()
            {
                if ((m_hasRow = m_inputRowset->MoveNext()) == true)
                {
                    m_rowCount++;
                }

                return m_hasRow;
            } 
        
            virtual void Reset()
            {
                throw gcnew NotImplementedException();
            }
        };

        ScopeRowsetEnumerator^ m_enumeratorOutstanding;

    protected:
        //
        // ScopeKeySet which is used for reducer needs distinct enumerator for each key range
        //
        void ResetEnumerator()
        {
            m_enumeratorOutstanding = nullptr;
        }

    public:
        ScopeRowset(ScopeRuntime::Row^ outputrow) : m_enumeratorOutstanding(nullptr)
        {
            // inherited from RowSet class
            _outputRow = outputrow;
            _outputSchema = _outputRow->Schema;
        }

        //
        // ScopeRuntime.RowSet part
        //
        virtual property Generic::IEnumerable<ScopeRuntime::Row^>^ Rows 
        {
            Generic::IEnumerable<ScopeRuntime::Row^>^ get() override
            {
                return this;
            }
        }

        //
        // Amount of rows processed by the iterator
        //
        property unsigned __int64 RowCount
        {
            unsigned __int64 get()
            {
                if (m_enumeratorOutstanding != nullptr)
                {
                    return m_enumeratorOutstanding->RowCount;
                }

                return 0;
            }
        }

        //
        // IEnumerable part
        //
        virtual Generic::IEnumerator<ScopeRuntime::Row^>^ GetEnumerator() sealed = Generic::IEnumerable<ScopeRuntime::Row^>::GetEnumerator
        {
            if (m_enumeratorOutstanding != nullptr)
            {
                throw gcnew InvalidOperationException("User Error: Multiple instances of enumerators are not supported on input RowSet.Rows. The input Rowset.Rows may be enumerated only once. If user code needs to enumerate it multiple times, then all Rows must be cached during the first pass and use cached Rows later.");
            }

            m_enumeratorOutstanding = gcnew ScopeRowsetEnumerator(this);

            // Do not return "this" to avoid object distruction by Dispose()
            return m_enumeratorOutstanding;
        }

        virtual IEnumerator^ GetEnumeratorBase() sealed = IEnumerable::GetEnumerator
        {
            return GetEnumerator();
        }

        //
        // IEnumerator part (to be defined in derived class)
        //
        virtual bool MoveNext() abstract;
    };

    //
    // Enumerate rowset in physical order
    //
    template <typename InputSchema>
    ref class ScopeInputRowset : public ScopeRowset
    {
        bool m_hasMoreRows;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToManagedCallType)(InputSchema&, ScopeRuntime::Row^%, InteropAllocator * alloc);

        InputSchema& m_nativeRow;
        OperatorDelegate<InputSchema> * m_child;
        MarshalToManagedCallType m_marshalToManaged;
        InteropAllocator * m_allocator;
        ScopeRuntime::UDOBase^ m_parentRowset;

    public:
        ScopeInputRowset(InputSchema& nativeRow, OperatorDelegate<InputSchema> * child, ScopeRuntime::UDOBase^ parentRowset, MarshalToManagedCallType marshalToManagedCall) 
            : ScopeRowset(ManagedRow<InputSchema>().get()),
              m_hasMoreRows(true),
              m_nativeRow(nativeRow),
              m_child(child),
              m_parentRowset(parentRowset),
              m_marshalToManaged(marshalToManagedCall)
        {
            m_allocator = new InteropAllocator(_outputSchema->Count, "ScopeInputRowset");
        }

        ~ScopeInputRowset()
        {
            this->!ScopeInputRowset();
        }

        !ScopeInputRowset()
        {
            delete m_allocator;
            m_allocator = nullptr;
        }

        virtual bool MoveNext() override
        {
            if (m_hasMoreRows)
            {
                m_hasMoreRows = m_child->GetNextRow(m_nativeRow);
                SendMemoryLoadNotification(m_parentRowset);
            }

            if (!m_hasMoreRows)
            {
                return false;
            }

            (*m_marshalToManaged)(m_nativeRow, _outputRow, m_allocator);

            return true;
        }

        void MarshalToManagedRow()
        {
            (*m_marshalToManaged)(m_nativeRow, _outputRow, m_allocator);
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("ScopeInputRowset");
            if (m_allocator != nullptr)
            {
                m_allocator->WriteRuntimeStats(node);
            }
            m_child->WriteRuntimeStats(node);
        }

        InputSchema& GetNativeRow()
        {
            SCOPE_ASSERT(m_hasMoreRows);
            return m_nativeRow;
        }

        ScopeRuntime::Row^ GetManagedRow()
        {
            SCOPE_ASSERT(m_hasMoreRows);
            return _outputRow;
        }
    };

    //
    // Enumerate rowset honoring key ranges
    //
    template <typename InputSchema, typename KeyPolicy>
    ref class ScopeInputKeyset : public ScopeRowset
    {
        enum class State { eINITIAL, eSTART, eRANGE, eEND, eEOF };

        State m_state;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToManagedCallType)(InputSchema&, typename KeyPolicy::KeyType&, ScopeRuntime::Row^%, InteropAllocator*);

        InputSchema&                                            m_nativeRow;
        MarshalToManagedCallType                                m_marshalToManaged;
        InteropAllocator *                                      m_allocator;
        KeyIterator<OperatorDelegate<InputSchema>, InputSchema, KeyPolicy> * m_iter;
        ScopeRuntime::UDOBase^ m_parentRowset;

    public:
        ScopeInputKeyset(InputSchema& nativeRow, OperatorDelegate<InputSchema> * child, ScopeRuntime::UDOBase^ parentRowset, MarshalToManagedCallType marshalToManagedCall) 
            : ScopeRowset(ManagedRow<InputSchema>().get()),
              m_state(State::eINITIAL),
              m_nativeRow(nativeRow),
              m_parentRowset(parentRowset),
              m_marshalToManaged(marshalToManagedCall)
        {
            m_allocator = new InteropAllocator(_outputSchema->Count, "ScopeInputKeyset");
            m_iter = new KeyIterator<OperatorDelegate<InputSchema>, InputSchema, KeyPolicy>(child);
        }

        ~ScopeInputKeyset()
        {
            this->!ScopeInputKeyset();
        }

        !ScopeInputKeyset()
        {
            delete m_allocator;
            m_allocator = nullptr;

            delete m_iter;
            m_iter = nullptr;
        }

        bool Init()
        {
            SCOPE_ASSERT(m_state == State::eINITIAL);

            m_iter->ReadFirst();
            m_iter->ResetKey();

            m_state = m_iter->End() ? State::eEOF : State::eSTART;

            return m_state == State::eSTART;
        }

        //
        // End current key range and start next one
        //
        bool NextKey()
        {
            SCOPE_ASSERT(m_state != State::eINITIAL);

            switch(m_state)
            {
            case State::eSTART:
            case State::eRANGE:
                m_iter->Drain();
                // Fallthrough
            case State::eEND:
                m_iter->ResetKey();
                ResetEnumerator();
                m_state = m_iter->End() ? State::eEOF : State::eSTART;
            }

            return m_state == State::eSTART;
        }

        virtual bool MoveNext() override
        {
            SCOPE_ASSERT(m_state != State::eINITIAL);

            switch(m_state)
            {
            case State::eSTART:
                // Row was already read
                m_state = State::eRANGE;
                break;

            case State::eRANGE:
                m_iter->Increment();
                m_state = m_iter->End() ? State::eEND : State::eRANGE;
                break;
            }

            if (m_state != State::eRANGE)
            {
                return false;
            }

            m_nativeRow = *m_iter->GetRow();

            SendMemoryLoadNotification(m_parentRowset);

            (*m_marshalToManaged)(m_nativeRow, *m_iter->GetKey(), _outputRow, m_allocator);

            return true;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("ScopeInputKeyset");
            if (m_allocator != nullptr)
            {
                m_allocator->WriteRuntimeStats(node);
            }
            if (m_iter != nullptr)
            {
                m_iter->WriteRuntimeStats(node);
            }
        }
    };

#pragma region ManagedOperators

    template<typename InputType, typename OutputSchema, int RunScopeCEPMode = SCOPECEP_MODE_NONE>
    class ScopeExtractorManagedImpl : public ScopeExtractorManaged<InputType, OutputSchema>
    {
        friend struct ScopeExtractorManagedFactory;

        static const char* const sm_className;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToNativeCallType)(ScopeRuntime::Row ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<ScopeRuntime::Row^> ^> m_enumerator;
        ScopeTypedManagedHandle<ScopeRuntime::Extractor ^>                  m_extractor;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                   m_args;
        ScopeTypedManagedHandle<System::IO::StreamReader ^>                 m_reader;
        ScopeTypedManagedHandle<ScopeCosmosInputStream<InputType> ^>        m_inputStream;
        MarshalToNativeCallType                                             m_marshalToNative;
        RowEntityAllocator                                                  m_allocator;
        int                                                                 m_streamIdColumnIdx; // column idx (for stream Id) in schema, -1 stands for non exist
        int                                                                 m_streamId;

        ScopeExtractorManagedImpl(ScopeRuntime::Extractor ^ udo, 
                                  Generic::List<String^>^ args, 
                                  int systemIdColumnIdx, 
                                  MarshalToNativeCallType marshalCall):
            m_extractor(udo),
            m_args(args),
            m_streamIdColumnIdx(systemIdColumnIdx),
            m_marshalToNative(marshalCall),
            m_allocator(RowEntityAllocator::RowContent)
        {
        }

    public:
        virtual void CreateInstance(const InputFileInfo& input, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize)
        {
            // No support for stream groups and byte-aligned streams
            SCOPE_ASSERT(!input.HasGroupId());
            SCOPE_ASSERT(input.blockAligned);

            m_inputStream = gcnew ScopeCosmosInputStream<InputType>(input.inputFileName, bufSize, bufCount);
            if (RunScopeCEPMode == SCOPECEP_MODE_NONE) 
            {
                m_reader = gcnew System::IO::StreamReader(m_inputStream.get());
            }
            else
            {
                m_inputStream->SetLowReadLatency(true);
                m_reader = gcnew ScopeRuntime::StreamReaderWithPosition(m_inputStream.get());
            }

            m_allocator.Init(virtualMemSize, sm_className);
            m_streamId = input.streamId;

            SCOPE_ASSERT(m_streamIdColumnIdx == -1 || m_streamIdColumnIdx < m_extractor->DefaultRow->Count);
        }

        virtual void Init()
        {
            m_inputStream->Init();
            m_enumerator = m_extractor->Extract(m_reader.get(), m_extractor->DefaultRow, m_args->ToArray())->GetEnumerator();
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            Generic::IEnumerator<ScopeRuntime::Row^> ^ enumerator = m_enumerator;

            if (enumerator->MoveNext())
            {
                if (enumerator->Current == nullptr || !Object::ReferenceEquals(m_extractor->DefaultRow->GetType(), enumerator->Current->GetType()))
                {
                    throw gcnew InvalidOperationException("Yielded Row object is not the instance passed in as 'outputRow'");
                }

                auto managedRow = enumerator->Current;
                
                // set streamId column
                if (m_streamIdColumnIdx != -1)
                {
                    managedRow[m_streamIdColumnIdx]->Set(m_streamId);
                }

                m_allocator.Reset();
                (*m_marshalToNative)(managedRow, output, &m_allocator);

                SendMemoryLoadNotification(m_extractor);

                return true;
            }

            return false;
        }

        virtual void Close()
        {
            m_reader->Close();
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();

            // Dispose enumerator
            m_enumerator.reset();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            if (m_inputStream)
            {
                m_inputStream->WriteRuntimeStats(node);
            }
        }

        virtual OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::ScopeExtractor__Row_MinMemory)
                .Add(m_inputStream->GetOperatorRequirements());
        }

        virtual ~ScopeExtractorManagedImpl()
        {
            // There only to ensure proper destruction when base class destructor is called
            if (m_inputStream)
            {
                try
                {
                    m_inputStream.reset();
                }
                catch (std::exception&)
                {
                    // ignore any I/O errors as we are destroying the object anyway
                }
            }
        }

        // Time spend in reading
        virtual __int64 GetOperatorWaitOnIOTime()
        {
            if (m_inputStream)
            {
                return m_inputStream->GetTotalIoWaitTime();
            }

            return 0;
        }

        virtual __int64 IoStreamInclusiveTime()
        {
            if (m_inputStream)
            {
                return m_inputStream->GetInclusiveTimeMillisecond();
            }

            return 0;
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            System::IO::BinaryWriter^ checkpoint = gcnew System::IO::BinaryWriter(gcnew ScopeCosmosOutputStream(&output.GetOutputer(), 0x400000, 2));
            m_extractor->DoStreamingCheckpoint(checkpoint);
            checkpoint->Write((int)SCOPECEP_CHECKPINT_MAGICNUMBER);
            checkpoint->Flush();
            unsigned __int64 position = static_cast<ScopeRuntime::StreamReaderWithPosition^>(m_reader.get())->Position;
            if (position == 0)
            {
                position = m_inputStream.get()->Position;
            }
            m_inputStream->SaveState(output, position);

        }
        
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            System::IO::BinaryReader^ checkpoint = gcnew System::IO::BinaryReader(gcnew ScopeCosmosInputStream<CosmosInput>(&input.GetInputer(), 0x400000, 2));
            m_extractor->LoadStreamingCheckpoint(checkpoint);
            CheckScopeCEPCheckpointMagicNumber(checkpoint);
            m_inputStream->LoadState(input);
        }
    };

    template<typename InputType, typename OutputSchema, int RunScopeCEPMode>
    const char* const ScopeExtractorManagedImpl<InputType, OutputSchema, RunScopeCEPMode>::sm_className = "ScopeExtractorManagedImpl";

    template<typename InputType, typename OutputSchema, int UID, int RunScopeCEPMode>
    INLINE ScopeExtractorManaged<InputType, OutputSchema> * ScopeExtractorManagedFactory::Make(std::string * argv, int argc)
    {
        ManagedUDO<UID> managedUDO(argv, argc);
        return new ScopeExtractorManagedImpl<InputType, OutputSchema, RunScopeCEPMode>(managedUDO.get(),
                                                                                       managedUDO.args(),
                                                                                       managedUDO.StreamIdColumnIndex(),
                                                                                       &InteropToNativeRowPolicy<typename OutputSchema,UID>::Marshal);
    }

    template<typename OutputSchema>
    class ScopeSStreamExtractorManagedImpl : public ScopeSStreamExtractorManaged<OutputSchema>
    {
        friend struct ScopeSStreamExtractorManagedFactory;
        static const char* const sm_className;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToNativeCallType)(ScopeRuntime::Row ^, OutputSchema &, IncrementalAllocator *);
        typedef ScopeCosmosInputStream<CosmosInput> InputStreamType;

        std::vector<std::shared_ptr<BlockDevice>>                                             m_partitionDevices;

        ScopeTypedManagedHandle<System::Collections::Generic::List<InputStreamType^>^>        m_streams;
        ScopeTypedManagedHandle<Generic::IEnumerator<ScopeRuntime::Row^> ^>                   m_enumerator;
        ScopeTypedManagedHandle<ScopeRuntime::SStreamExtractor ^>                             m_extractor;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                                     m_args;
        MarshalToNativeCallType                                                               m_marshalToNative;
        RowEntityAllocator                                                                    m_allocator;

        ScopeSStreamExtractorManagedImpl(ScopeRuntime::SStreamExtractor ^ udo, Generic::List<String^>^ args, MarshalToNativeCallType marshalCall):
            m_streams(gcnew System::Collections::Generic::List<InputStreamType^>()),
            m_extractor(udo),
            m_args(args),
            m_marshalToNative(marshalCall),
            m_allocator(RowEntityAllocator::RowContent)
        {
        }

    public:
        virtual void Init(const int ssid, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize)
        {
            m_partitionDevices = IOManager::GetGlobal()->GetSstreamPartitionDevices(ssid);
            auto processingGroupIds = IOManager::GetGlobal()->GetSStreamProcessingGroupIds(m_partitionDevices);
            SCOPE_ASSERT(m_partitionDevices.size() == processingGroupIds.size());

            auto processingGroups = gcnew System::Collections::Generic::SortedList<int, System::Collections::Generic::List<System::Collections::Generic::List<System::IO::Stream^>^>^>();
            for (int i = 0; i < m_partitionDevices.size(); i++)
            {
                auto stream = gcnew ManagedSSLibScopeCosmosInputStream(m_partitionDevices[i].get(), bufSize, bufCount);
                m_streams->Add(stream);
                auto streamList =  gcnew System::Collections::Generic::List<System::IO::Stream^>();
                streamList->Add(StructuredStream::SSLibHelper::GetSyncStream(stream));
                if (processingGroups->Count == 0 || processingGroupIds[i] != processingGroupIds[i - 1])
                {
                    auto processingGroup = gcnew System::Collections::Generic::List<System::Collections::Generic::List<System::IO::Stream^>^>();
                    processingGroup->Add(streamList);
                    processingGroups->Add(processingGroupIds[i], processingGroup);
                }
                else
                {
                    processingGroups[processingGroupIds[i]]->Add(streamList);
                }
            }

            m_extractor->Initialize(processingGroups, m_extractor->Schema, m_args->ToArray(), false);
            m_extractor->OpenPartitionedStreams();
            m_enumerator = m_extractor->Rows->GetEnumerator();
            m_allocator.Init(virtualMemSize, sm_className);
        }

        virtual string GetKeyRangeFileName()
        {
            if (m_extractor->KeyRangeFile == nullptr)
            {
                return "";
            }
            
            return msclr::interop::marshal_as<std::string>(m_extractor->KeyRangeFile);
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            Generic::IEnumerator<ScopeRuntime::Row^> ^ enumerator = m_enumerator;

            if (enumerator->MoveNext())
            {
                if (enumerator->Current == nullptr || !Object::ReferenceEquals(m_extractor->DefaultRow->GetType(), enumerator->Current->GetType()))
                {
                    throw gcnew InvalidOperationException("Yielded Row object is not the instance passed in as 'outputRow'");
                }

                m_allocator.Reset();
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                return true;
            }

            return false;
        }

        virtual void Close()
        {
            m_extractor->CleanupDataUnitReaders();
            for(int i = 0; i < m_streams->Count; i++)
            {
                m_streams.get()[i]->Close();
            }
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
        }

        // Time spend in reading
        virtual __int64 GetOperatorWaitOnIOTime()
        {
            __int64 ioTime = 0;
            for(int i = 0; i < m_streams->Count; i++)
            {
                ioTime += m_streams.get()[i]->GetTotalIoWaitTime();
            }

            return ioTime;
        }

        virtual __int64 IoStreamInclusiveTime()
        {
            __int64 ioTime = 0;
            for (int i = 0; i < m_streams->Count; i++)
            {
                ioTime += m_streams.get()[i]->GetInclusiveTimeMillisecond();
            }

            return ioTime;
        }

        virtual std::vector<std::shared_ptr<BlockDevice>> GetPartitionDevices()
        {
            return m_partitionDevices;
        }
    };

    template<typename OutputSchema>
    const char* const ScopeSStreamExtractorManagedImpl<OutputSchema>::sm_className = "ScopeSStreamExtractorManagedImpl";

    template<typename OutputSchema, int UID>
    INLINE ScopeSStreamExtractorManaged<OutputSchema> * ScopeSStreamExtractorManagedFactory::Make()
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        return new ScopeSStreamExtractorManagedImpl<OutputSchema>(managedUDO.get(),
                                                                  managedUDO.args(),
                                                                  &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal);
    }

    template<typename InputSchema, typename OutputSchema>
    class ScopeProcessorManagedImpl : public ScopeProcessorManaged<InputSchema, OutputSchema>
    {
        friend struct ScopeProcessorManagedFactory;

        static const char* const sm_className;

        InputSchema m_inputRow;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToManagedCallType)(InputSchema &, ScopeRuntime::Row ^%, InteropAllocator *);
        typedef void (*MarshalToNativeCallType)(ScopeRuntime::Row ^, OutputSchema &, IncrementalAllocator *);
        typedef bool (*TransformRowCallType)(InputSchema &, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<ScopeRuntime::Row^> ^> m_enumerator;
        ScopeTypedManagedHandle<ScopeInputRowset<InputSchema> ^>            m_inputRowset;
        ScopeTypedManagedHandle<ScopeRuntime::Processor ^>                  m_processor;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                   m_args;
        MarshalToNativeCallType                                             m_marshalToNative;
        TransformRowCallType                                                m_transformRowCall;
        RowEntityAllocator                                                  m_allocator;

        ScopeProcessorManagedImpl(ScopeRuntime::Processor ^ udo, Generic::List<String^>^ args, OperatorDelegate<InputSchema> * child, MarshalToNativeCallType marshalToNativeCall, MarshalToManagedCallType marshalToManagedCall, TransformRowCallType transformRowCall):
            m_processor(udo),
            m_args(args),
            m_marshalToNative(marshalToNativeCall),
            m_transformRowCall(transformRowCall),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent)
        {
            m_inputRowset = gcnew ScopeInputRowset<InputSchema>(m_inputRow, child, udo, marshalToManagedCall);
        }

    public:
        virtual void Init()
        {
            // Initialize processors (this internally calls InitializeAtRuntime)
            m_processor->Initialize(m_inputRowset.get(), m_processor->DefaultRow->Schema, m_args->ToArray());

            m_enumerator = m_processor->Process(m_inputRowset.get(), m_processor->DefaultRow, m_args->ToArray())->GetEnumerator();
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            Generic::IEnumerator<ScopeRuntime::Row^> ^ enumerator = m_enumerator;

            if (enumerator->MoveNext())
            {
                if (enumerator->Current == nullptr  || !Object::ReferenceEquals(m_processor->DefaultRow->GetType(), enumerator->Current->GetType()))
                {
                    throw gcnew InvalidOperationException("Yielded Row object is not the instance passed in as 'outputRow'");
                }

                m_allocator.Reset();
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);
                (*m_transformRowCall)(m_inputRow, output, nullptr);

                SendMemoryLoadNotification(m_processor);

                return true;
            }

            return false;
        }

        virtual void Close()
        {
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            // Dispose enumerator
            m_enumerator.reset();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            if (m_inputRowset)
            {
                m_inputRowset->WriteRuntimeStats(node);
            }
        }

        virtual OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInRows(OperatorRequirementsConstants::ScopeProcessor__Row_MinMemory);
        }

        virtual ~ScopeProcessorManagedImpl()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            System::IO::BinaryWriter^ checkpoint = gcnew System::IO::BinaryWriter(gcnew ScopeCosmosOutputStream(&output.GetOutputer(), 0x400000, 2));
            m_processor->DoStreamingCheckpoint(checkpoint);
            checkpoint->Write((int)SCOPECEP_CHECKPINT_MAGICNUMBER);
            checkpoint->Flush();
        }
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            System::IO::BinaryReader^ checkpoint = gcnew System::IO::BinaryReader(gcnew ScopeCosmosInputStream<CosmosInput>(&input.GetInputer(), 0x400000, 2));
            m_processor->LoadStreamingCheckpoint(checkpoint);
            CheckScopeCEPCheckpointMagicNumber(checkpoint);
        }
    };

    template<typename InputSchema, typename OutputSchema>
    const char* const ScopeProcessorManagedImpl<InputSchema, OutputSchema>::sm_className = "ScopeProcessorManagedImpl";

    template<typename InputSchema, typename OutputSchema, int UID>
    INLINE ScopeProcessorManaged<InputSchema, OutputSchema> * ScopeProcessorManagedFactory::Make(OperatorDelegate<InputSchema> * child)
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        return new ScopeProcessorManagedImpl<InputSchema, OutputSchema>(managedUDO.get(),
                                                                        managedUDO.args(),
                                                                        child,
                                                                        &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal,
                                                                        &InteropToManagedRowPolicy<InputSchema,UID>::Marshal,
                                                                        &RowTransformPolicy<InputSchema,OutputSchema,UID>::FilterTransformRow);
    }

    template<typename InputSchema, typename OutputSchema, int UID>
    INLINE ScopeProcessorManaged<InputSchema, OutputSchema> * ScopeApplierManagedFactory::Make(OperatorDelegate<InputSchema> * child)
    {
        return ScopeProcessorManagedFactory::Make<InputSchema,OutputSchema,UID>(child);
    }

    template<typename InputSchema, typename OutputSchema, int UID>
    INLINE ScopeProcessorManaged<InputSchema, OutputSchema> * ScopeGrouperManagedFactory::Make(OperatorDelegate<InputSchema> * child)
    {
        return ScopeProcessorManagedFactory::Make<InputSchema,OutputSchema,UID>(child);
    }

    template<typename InputOperators, typename OutputSchema, int UID>
    class MultiProcessorPolicy
    {
    public:
        explicit MultiProcessorPolicy(InputOperators* children);

        void Init();
        void Close();
        Generic::List<ScopeRuntime::RowSet^>^ GetInputRowsets();
        LONGLONG GetInclusiveTimeMillisecond();
        void WriteRuntimeStats(TreeNode& root);
        
        void DoScopeCEPCheckpoint(BinaryOutputStream & output);
        void LoadScopeCEPCheckpoint(BinaryInputStream & output);
    };

    template<typename InputOperators, typename OutputSchema, int UID>
    class ScopeMultiProcessorManagedImpl : public ScopeMultiProcessorManaged<InputOperators, OutputSchema, UID>
    {
        static const char* const sm_className;

        typedef typename MultiProcessorPolicy<InputOperators, OutputSchema, UID> MultiProccessorPolicyType;
        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToNativeCallType)(ScopeRuntime::Row ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<ScopeRuntime::Row^> ^> m_enumerator;
        ScopeTypedManagedHandle<Generic::List<ScopeRuntime::RowSet^> ^>     m_inputRowsets;
        ScopeTypedManagedHandle<ScopeRuntime::MultiProcessor ^>             m_processor;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                   m_args;
        ScopeTypedManagedHandle<ScopeCosmosInputStream<CosmosInput>^>       m_contextInputStream;
        ScopeTypedManagedHandle<ScopeCosmosOutputStream^>                   m_contextOutputStream;
        MarshalToNativeCallType                                             m_marshalToNative;
        RowEntityAllocator                                                  m_allocator;
        MultiProccessorPolicyType                                           m_processorPolicy;

    public:
        ScopeMultiProcessorManagedImpl(ScopeRuntime::MultiProcessor ^ udo, Generic::List<String^>^ args, InputOperators* children, ScopeCosmosInputStream<CosmosInput>^ contextInputStream, ScopeCosmosOutputStream^ contextOutputStream, MarshalToNativeCallType marshalToNativeCall):
            m_processor(udo),
            m_args(args),
            m_contextInputStream(contextInputStream),
            m_contextOutputStream(contextOutputStream),
            m_marshalToNative(marshalToNativeCall),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
            m_processorPolicy(children)
        {
            SCOPE_ASSERT(contextInputStream == nullptr && contextOutputStream == nullptr
                || contextInputStream != nullptr && contextInputStream != nullptr);
            m_inputRowsets = m_processorPolicy.GetInputRowsets(udo);
        }

        virtual void Init()
        {
            if(m_contextInputStream.get() != nullptr)
            {
                m_processor->GetType()->GetMethod("SetContextStreams", BindingFlags::NonPublic | BindingFlags::Instance)->Invoke(m_processor.get(), gcnew cli::array<Object^>(2) {m_contextInputStream.get(), m_contextOutputStream.get()});
            }

            m_processorPolicy.Init();

            // Initialize processors (this internally calls InitializeAtRuntime)
            m_processor->Initialize(m_inputRowsets.get(), m_processor->DefaultRow->Schema, m_args->ToArray());

            if (m_processor->Context != nullptr)
            {
                bool isInitialized = (bool)m_processor->Context->GetType()->GetProperty("IsInitialized", BindingFlags::NonPublic | BindingFlags::Instance)->GetValue(m_processor->Context);
                if (!isInitialized)
                {
                    m_contextInputStream->Init();
                    m_processor->Context->Deserialize(m_contextInputStream.get());
                    m_processor->Context->GetType()->GetProperty("IsInitialized", BindingFlags::NonPublic | BindingFlags::Instance)->SetValue(m_processor->Context, true);
                    ScopeRuntime::ScopeTrace::Status("Initialize context Object Hash {0} Value {1}", m_processor->Context->GetHashCode(), m_processor->Context->ToString());
                }
                else
                {
                    ScopeRuntime::ScopeTrace::Status("Context is already loaded! Object Hash {0} Value {1}", m_processor->Context->GetHashCode(), m_processor->Context->ToString());
                }
                m_contextOutputStream->Init();
            }
            else
            {
                ScopeRuntime::ScopeTrace::Status("Context is null");
            }

            m_enumerator = m_processor->Process(m_inputRowsets.get(), m_processor->DefaultRow, m_args->ToArray())->GetEnumerator();
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            Generic::IEnumerator<ScopeRuntime::Row^> ^ enumerator = m_enumerator;

            if (enumerator->MoveNext())
            {
                if (enumerator->Current == nullptr  || !Object::ReferenceEquals(m_processor->DefaultRow->GetType(), enumerator->Current->GetType()))
                {
                    throw gcnew InvalidOperationException("Yielded Row object is not the instance passed in as 'outputRow'");
                }

                m_allocator.Reset();
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                SendMemoryLoadNotification(m_processor);

                return true;
            }

            return false;
        }

        virtual void Close()
        {
            if (m_processor->Context != nullptr)
            {
                m_contextInputStream->Close();
                m_processor->Context->Serialize(m_contextOutputStream.get());
                m_contextOutputStream->Close();
            }

            m_processorPolicy.Close();
            // Dispose enumerator
            m_enumerator.reset();
        }

        virtual LONGLONG GetInclusiveTimeMillisecond()
        {
            return m_processorPolicy.GetInclusiveTimeMillisecond();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            m_processorPolicy.WriteRuntimeStats(root);
        }

        virtual OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInRows(OperatorRequirementsConstants::ScopeMultiProcessor__Row_MinMemory);
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            System::IO::BinaryWriter^ checkpoint = gcnew System::IO::BinaryWriter(gcnew ScopeCosmosOutputStream(&output.GetOutputer(), 0x400000, 2));
            m_processor->DoStreamingCheckpoint(checkpoint);
            checkpoint->Write((int)SCOPECEP_CHECKPINT_MAGICNUMBER);
            checkpoint->Flush();
            m_processorPolicy.DoScopeCEPCheckpoint(output);
        }
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            System::IO::BinaryReader^ checkpoint = gcnew System::IO::BinaryReader(gcnew ScopeCosmosInputStream<CosmosInput>(&input.GetInputer(), 0x400000, 2));
            m_processor->LoadStreamingCheckpoint(checkpoint);
            CheckScopeCEPCheckpointMagicNumber(checkpoint);
            m_processorPolicy.LoadScopeCEPCheckpoint(input);
        }
    };

    template<typename InputOperators, typename OutputSchema, int UID>
    const char* const ScopeMultiProcessorManagedImpl<InputOperators, OutputSchema, UID>::sm_className = "ScopeMultiProcessorManagedImpl";

    template<typename InputOperators, typename OutputSchema, int UID>
    INLINE ScopeMultiProcessorManaged<InputOperators, OutputSchema, UID>* ScopeMultiProcessorManagedFactory::Make(
            InputOperators* children,
            string* inputContextFile,
            string* outputContextFile,
            SIZE_T inputBufSize,
            int inputBufCnt,
            SIZE_T outputBufSize,
            int outputBufCnt)
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        ScopeCosmosInputStream<CosmosInput>^  contextInputStream = nullptr;
        ScopeCosmosOutputStream^ contextOutputStream = nullptr;
        if (inputContextFile != nullptr && !inputContextFile->empty())
        {
            SCOPE_ASSERT(outputContextFile != nullptr && !outputContextFile->empty());
            contextInputStream = gcnew ScopeCosmosInputStream<CosmosInput>(*inputContextFile, inputBufSize, inputBufCnt);
            contextOutputStream = gcnew ScopeCosmosOutputStream(*outputContextFile, outputBufSize, outputBufCnt, false);
        }

        return new ScopeMultiProcessorManagedImpl<InputOperators, OutputSchema, UID>(managedUDO.get(),
                                                                                     managedUDO.args(),
                                                                                     children,
                                                                                     contextInputStream,
                                                                                     contextOutputStream,
                                                                                     &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal);
    }

    template<typename InputSchema>
    class ScopeCreateContextManagedImpl : public ScopeCreateContextManaged<InputSchema>
    {
        friend struct ScopeCreateContextManagedFactory;
        static const char* const sm_className;

        InputSchema m_inputRow;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToManagedCallType) (InputSchema &, ScopeRuntime::Row ^%, InteropAllocator *);

        ScopeTypedManagedHandle<ScopeInputRowset<InputSchema> ^>  m_inputRowset;
        ScopeTypedManagedHandle<ScopeRuntime::ExecutionContext ^> m_exeContext;
        ScopeTypedManagedHandle<Generic::List<String^> ^>         m_args;
        ScopeTypedManagedHandle<ScopeCosmosOutputStream ^>        m_outputStream;

        ScopeCreateContextManagedImpl(ScopeRuntime::ExecutionContext ^ udo, Generic::List<String^>^ args, OperatorDelegate<InputSchema> * child, MarshalToManagedCallType marshalToManagedCall):
            m_exeContext(udo),
            m_args(args)
        {
            
            m_inputRowset = gcnew ScopeInputRowset<InputSchema>(m_inputRow, child, udo, marshalToManagedCall);
        }

    public:
        virtual void Init(std::string& outputName, SIZE_T bufSize, int bufCnt)
        {
            m_outputStream = gcnew ScopeCosmosOutputStream(outputName, bufSize, bufCnt, false);
            m_outputStream->Init();
            m_exeContext->GetType()->GetMethod("InitializeAtRuntime", BindingFlags::NonPublic | BindingFlags::Instance)->Invoke(
                m_exeContext.get(), gcnew cli::array<Object^>(5) {m_inputRowset.get(), nullptr, m_outputStream.get(), m_args->ToArray(), m_exeContext->Schema});
        }

        virtual void Serialize()
        {
            m_exeContext->GetType()->GetMethod("DoIninitialize", BindingFlags::NonPublic | BindingFlags::Instance)->Invoke(
                m_exeContext.get(), gcnew cli::array<Object^>(0));
        }

        virtual void Close()
        {
            m_outputStream->Close();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            RuntimeStats::WriteRowCount(root, (LONGLONG)m_inputRowset->RowCount);

            auto & node = root.AddElement(sm_className);

            if (m_inputRowset)
            {
                m_inputRowset->WriteRuntimeStats(node);
            }

            if (m_outputStream)
            {
                m_outputStream->WriteRuntimeStats(node);
            }
        }

        virtual OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements(OperatorRequirementsConstants::ScopeCreateContext__Size_MinMemory);
        }

        // Time in writing data
        virtual __int64 GetOperatorWaitOnIOTime()
        {
            if (m_outputStream)
            {
                return m_outputStream->GetTotalIoWaitTime();
            }

            return 0;
        }

        virtual __int64 IoStreamInclusiveTime()
        {
            if (m_outputStream)
            {
                return m_outputStream->GetInclusiveTimeMillisecond();
            }

            return 0;
        }
    };

    template<typename InputSchema>
    const char* const ScopeCreateContextManagedImpl<InputSchema>::sm_className = "ScopeCreateContextManagedImpl";


    template<typename InputSchema, int UID>
    INLINE ScopeCreateContextManaged<InputSchema> * ScopeCreateContextManagedFactory::Make(OperatorDelegate<InputSchema> * child)
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        return new ScopeCreateContextManagedImpl<InputSchema>(managedUDO.get(), 
                                                         managedUDO.args(), 
                                                         child, 
                                                         &InteropToManagedRowPolicy<InputSchema,UID>::Marshal);
    }

    template<typename OutputSchema>
    class ScopeReadContextManagedImpl : public ScopeReadContextManaged<OutputSchema>
    {
        friend struct ScopeReadContextManagedFactory;

        static const char* const sm_className;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToNativeCallType)(ScopeRuntime::Row ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<ScopeRuntime::Row^> ^> m_enumerator;
        ScopeTypedManagedHandle<ScopeRuntime::ExecutionContext ^>           m_exeContext;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                   m_args;
        ScopeTypedManagedHandle<ScopeCosmosInputStream<CosmosInput> ^>      m_inputStream;
        MarshalToNativeCallType                                             m_marshalToNative;
        IncrementalAllocator                                                m_allocator;

        ScopeReadContextManagedImpl(ScopeRuntime::ExecutionContext ^ udo, Generic::List<String^>^ args, MarshalToNativeCallType marshalCall):
            m_exeContext(udo),
            m_args(args),
            m_marshalToNative(marshalCall),
            m_allocator()
        {
        }

    public:
        virtual void Init(std::string& inputName, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize)
        {
            m_inputStream = gcnew ScopeCosmosInputStream<CosmosInput>(inputName, bufSize, bufCount);
            m_inputStream->Init();

            m_exeContext->GetType()->GetMethod("InitializeAtRuntime", BindingFlags::NonPublic | BindingFlags::Instance)->Invoke(
                m_exeContext.get(), gcnew cli::array<Object^>(5) {nullptr, m_inputStream.get(), nullptr, m_args->ToArray(), m_exeContext->Schema});
            m_enumerator = m_exeContext->Rows->GetEnumerator();
            m_allocator.Init(virtualMemSize, sm_className);
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            Generic::IEnumerator<ScopeRuntime::Row^> ^ enumerator = m_enumerator;

            if (enumerator->MoveNext())
            {
                if (enumerator->Current == nullptr  || !Object::ReferenceEquals(m_exeContext->DefaultRow->GetType(), enumerator->Current->GetType()))
                {
                    throw gcnew InvalidOperationException("Yielded Row object is not the instance passed in as 'outputRow'");
                }

                m_allocator.Reset();
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                SendMemoryLoadNotification(m_exeContext);

                return true;
            }

            return false;
        }

        virtual void Close()
        {
            m_inputStream->Close();
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            if (m_inputStream)
            {
                m_inputStream->WriteRuntimeStats(node);
            }
        }

        // Time spend in reading
        virtual __int64 GetOperatorWaitOnIOTime()
        {
            if (m_inputStream)
            {
                return m_inputStream->GetTotalIoWaitTime();
            }

            return 0;
        }

        virtual __int64 IoStreamInclusiveTime()
        {
            if (m_inputStream)
            {
                return m_inputStream->GetInclusiveTimeMillisecond();
            }

            return 0;
        }
    };

    template<typename OutputSchema>
    const char* const ScopeReadContextManagedImpl<OutputSchema>::sm_className = "ScopeReadContextManagedImpl";

    template<typename OutputSchema, int UID>
    INLINE ScopeReadContextManaged<OutputSchema> * ScopeReadContextManagedFactory::Make()
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        return new ScopeReadContextManagedImpl<OutputSchema>(managedUDO.get(),
                                                               managedUDO.args(),
                                                               &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal);
    }

    template<typename InputSchema, typename OutputSchema, typename KeyPolicy>
    class ScopeReducerManagedImpl : public ScopeProcessorManaged<InputSchema, OutputSchema>
    {
        friend struct ScopeReducerManagedFactory;

        static const char* const sm_className;

        InputSchema m_inputRow;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToManagedCallType)(InputSchema &, typename KeyPolicy::KeyType &, ScopeRuntime::Row ^%, InteropAllocator *);
        typedef void (*MarshalToNativeCallType)(ScopeRuntime::Row ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<ScopeRuntime::Row^> ^> m_enumerator;
        ScopeTypedManagedHandle<ScopeInputKeyset<InputSchema,KeyPolicy> ^>  m_inputKeyset;
        ScopeTypedManagedHandle<ScopeRuntime::Reducer ^>                    m_reducer;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                   m_args;
        MarshalToNativeCallType                                             m_marshalToNative;
        RowEntityAllocator                                                  m_allocator;
        bool                                                                m_hasMoreRows;

        ScopeReducerManagedImpl(ScopeRuntime::Reducer ^ udo, Generic::List<String^>^ args, OperatorDelegate<InputSchema> * child, MarshalToNativeCallType marshalToNativeCall, MarshalToManagedCallType marshalToManagedCall):
            m_reducer(udo),
            m_args(args),
            m_marshalToNative(marshalToNativeCall),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
            m_hasMoreRows(false)
        {
            m_inputKeyset = gcnew ScopeInputKeyset<InputSchema,KeyPolicy>(m_inputRow, child, udo, marshalToManagedCall);
        }

    public:
        virtual void Init()
        {
            // Let reducer do custom initialization
            m_reducer->Initialize(m_inputKeyset->Schema, m_reducer->Schema, m_args->ToArray());

            m_hasMoreRows = m_inputKeyset->Init();

            if (m_hasMoreRows)
            {
                m_enumerator = m_reducer->Reduce(m_inputKeyset.get(), m_reducer->DefaultRow, m_args->ToArray())->GetEnumerator();
            }
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            while(m_hasMoreRows)
            {
                Generic::IEnumerator<ScopeRuntime::Row^> ^ enumerator = m_enumerator;

                if (enumerator->MoveNext())
                {
                    if (enumerator->Current == nullptr  || !Object::ReferenceEquals(m_reducer->DefaultRow->GetType(), enumerator->Current->GetType()))
                    {
                        throw gcnew InvalidOperationException("Yielded Row object is not the instance passed in as 'outputRow'");
                    }

                    m_allocator.Reset();
                    (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                    SendMemoryLoadNotification(m_reducer);

                    return true;
                }
                else if (m_inputKeyset->NextKey())
                {
                    // Proceed to the next key and create enumerator for it
                    m_enumerator.reset(m_reducer->Reduce(m_inputKeyset.get(), m_reducer->DefaultRow, m_args->ToArray())->GetEnumerator());
                }
                else
                {
                    // EOF
                    m_hasMoreRows = false;
                }
            }

            return false;
        }

        virtual void Close()
        {
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            // Dispose enumerator
            m_enumerator.reset();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            if (m_inputKeyset)
            {
                m_inputKeyset->WriteRuntimeStats(node);
            }
        }

        virtual OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInRows(OperatorRequirementsConstants::ScopeProcessor_ManagedReducer__Row_MinMemory);
        }

        virtual ~ScopeReducerManagedImpl()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            System::IO::BinaryWriter^ checkpoint = gcnew System::IO::BinaryWriter(gcnew ScopeCosmosOutputStream(&output.GetOutputer(), 0x400000, 2));
            m_reducer->DoStreamingCheckpoint(checkpoint);
            checkpoint->Write((int)SCOPECEP_CHECKPINT_MAGICNUMBER);
            checkpoint->Flush();
        }
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            System::IO::BinaryReader^ checkpoint = gcnew System::IO::BinaryReader(gcnew ScopeCosmosInputStream<CosmosInput>(&input.GetInputer(), 0x400000, 2));
            m_reducer->LoadStreamingCheckpoint(checkpoint);
            CheckScopeCEPCheckpointMagicNumber(checkpoint);
        }
    };

    template<typename InputSchema, typename OutputSchema, typename KeyPolicy>
    const char* const ScopeReducerManagedImpl<InputSchema, OutputSchema, KeyPolicy>::sm_className = "ScopeReducerManagedImpl";

    template<typename InputSchema, typename OutputSchema, int UID>
    INLINE ScopeProcessorManaged<InputSchema, OutputSchema> * ScopeReducerManagedFactory::Make(OperatorDelegate<InputSchema> * child)
    {
        typedef KeyComparePolicy<InputSchema, UID> KeyPolicy;

        ManagedUDO<UID> managedUDO(nullptr, 0);
        return new ScopeReducerManagedImpl<InputSchema, OutputSchema, KeyPolicy>(managedUDO.get(),
                                                                                 managedUDO.args(),
                                                                                 child,
                                                                                 &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal,
                                                                                 &InteropToManagedRowPolicy<InputSchema,UID>::Marshal);
    }

    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema>
    class ScopeCombinerManagedImpl : public ScopeCombinerManaged<InputSchemaLeft, InputSchemaRight, OutputSchema>
    {
        friend struct ScopeCombinerManagedFactory;

        static const char* const sm_className;

        InputSchemaLeft m_inputRowLeft;
        InputSchemaRight m_inputRowRight;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToManagedCallTypeLeft) (InputSchemaLeft &, ScopeRuntime::Row ^%, InteropAllocator *);
        typedef void (*MarshalToManagedCallTypeRight) (InputSchemaRight &, ScopeRuntime::Row ^%, InteropAllocator *);
        typedef void (*MarshalToNativeCallType)(ScopeRuntime::Row ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<ScopeRuntime::Row^> ^> m_enumerator;
        ScopeTypedManagedHandle<ScopeInputRowset<InputSchemaLeft> ^>        m_inputRowsetLeft;
        ScopeTypedManagedHandle<ScopeInputRowset<InputSchemaRight> ^>       m_inputRowsetRight;
        ScopeTypedManagedHandle<ScopeRuntime::RowSet ^>                     m_combiner;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                   m_args;
        MarshalToNativeCallType                                             m_marshalToNative;
        RowEntityAllocator                                                  m_allocator;

        ScopeCombinerManagedImpl
        (
            ScopeRuntime::RowSet ^ udo,
            Generic::List<String^>^ args,
            OperatorDelegate<InputSchemaLeft> * leftChild,
            OperatorDelegate<InputSchemaRight> * rightChild,
            MarshalToNativeCallType marshalToNativeCall, 
            MarshalToManagedCallTypeLeft marshalToManagedCallLeft,
            MarshalToManagedCallTypeRight marshalToManagedCallRight
        ) : m_combiner(udo), m_args(args), m_marshalToNative(marshalToNativeCall), m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent)
        {
            m_inputRowsetLeft = gcnew ScopeInputRowset<InputSchemaLeft>(m_inputRowLeft, leftChild, udo, marshalToManagedCallLeft);
            m_inputRowsetRight = gcnew ScopeInputRowset<InputSchemaRight>(m_inputRowRight, rightChild, udo, marshalToManagedCallRight);
        }

    public:
        virtual void Init()
        {
            // Set the table names of the left and right schema (same as DoCombine in the old runtime)
            cli::array<String^>^ args_arr = m_args->ToArray();
            Generic::List<String^>^ unusedArgs = gcnew Generic::List<String^>();
            for (int i = 0; i < args_arr->Length; ++i)
            {
                if (args_arr[i] == "-leftTable")
                {
                    m_inputRowsetLeft.get()->Schema->SetTable(args_arr[++i]);
                }
                else if (args_arr[i] == "-rightTable")
                {
                    m_inputRowsetRight.get()->Schema->SetTable(args_arr[++i]);
                }
                else
                {
                    unusedArgs->Add(args_arr[i]);
                }
            }

            m_combiner->Initialize(m_inputRowsetLeft.get(), m_inputRowsetRight.get(), unusedArgs->ToArray());
            m_enumerator = m_combiner->Rows->GetEnumerator();
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            Generic::IEnumerator<ScopeRuntime::Row^> ^ enumerator = m_enumerator;

            if (enumerator->MoveNext())
            {
                if (enumerator->Current == nullptr  || !Object::ReferenceEquals(m_combiner->DefaultRow->GetType(), enumerator->Current->GetType()))
                {
                    throw gcnew InvalidOperationException("Yielded Row object is not the instance passed in as 'outputRow'");
                }

                m_allocator.Reset();
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                SendMemoryLoadNotification(m_combiner);

                return true;
            }

            return false;
        }

        virtual void Close()
        {
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            // Dispose enumerator
            m_enumerator.reset();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            if (m_inputRowsetLeft)
            {
                m_inputRowsetLeft->WriteRuntimeStats(node);
            }
            if (m_inputRowsetRight)
            {
                m_inputRowsetRight->WriteRuntimeStats(node);
            }
        }

        virtual OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInRows(OperatorRequirementsConstants::ScopeCombiner__Row_MinMemory);
        }

        virtual ~ScopeCombinerManagedImpl()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            System::IO::BinaryWriter^ checkpoint = gcnew System::IO::BinaryWriter(gcnew ScopeCosmosOutputStream(&output.GetOutputer(), 0x400000, 2));
            m_combiner->DoStreamingCheckpoint(checkpoint);
            checkpoint->Write((int)SCOPECEP_CHECKPINT_MAGICNUMBER);
            checkpoint->Flush();
        }
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            System::IO::BinaryReader^ checkpoint = gcnew System::IO::BinaryReader(gcnew ScopeCosmosInputStream<CosmosInput>(&input.GetInputer(), 0x400000, 2));
            m_combiner->LoadStreamingCheckpoint(checkpoint);
            CheckScopeCEPCheckpointMagicNumber(checkpoint);
        }
    };

    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema>
    const char* const ScopeCombinerManagedImpl<InputSchemaLeft, InputSchemaRight, OutputSchema>::sm_className = "ScopeCombinerManagedImpl";

    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, int UID>
    INLINE ScopeCombinerManaged<InputSchemaLeft, InputSchemaRight, OutputSchema> * ScopeCombinerManagedFactory::Make
    (
        OperatorDelegate<InputSchemaLeft> * leftChild,
        OperatorDelegate<InputSchemaRight> * rightChild
    )
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        return new ScopeCombinerManagedImpl<InputSchemaLeft, InputSchemaRight, OutputSchema>(managedUDO.get(),
                                                                                             managedUDO.args(),
                                                                                             leftChild,
                                                                                             rightChild,
                                                                                             &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal,
                                                                                             &InteropToManagedRowPolicy<InputSchemaLeft,UID>::Marshal,
                                                                                             &InteropToManagedRowPolicy<InputSchemaRight,UID>::Marshal);
    }

    template<typename InputSchema>
    class ScopeOutputerManagedImpl : public ScopeOutputerManaged<InputSchema>
    {
    protected:
        friend struct ScopeOutputerManagedFactory;

        InputSchema m_inputRow;

        // Function pointer type for Marshal call methods.
        typedef void (*MarshalToManagedCallType) (InputSchema &, ScopeRuntime::Row ^%, InteropAllocator *);

        ScopeTypedManagedHandle<ScopeInputRowset<InputSchema> ^> m_inputRowset;
        ScopeTypedManagedHandle<ScopeRuntime::Outputter ^>       m_outputer;
        ScopeTypedManagedHandle<Generic::List<String^> ^>        m_args;
        ScopeTypedManagedHandle<ScopeCosmosOutputStream ^>       m_outputStream;
        ScopeTypedManagedHandle<System::IO::StreamWriter ^>      m_writer;

        ScopeOutputerManagedImpl(ScopeRuntime::Outputter ^ udo, Generic::List<String^>^ args, OperatorDelegate<InputSchema> * child, MarshalToManagedCallType marshalToManagedCall):
            m_outputer(udo),
            m_args(args)
        {
            m_inputRowset = gcnew ScopeInputRowset<InputSchema>(m_inputRow, child, udo, marshalToManagedCall);
        }

    public:

        virtual void CreateStream(std::string& outputName, SIZE_T bufSize, int bufCnt)
        {   
            m_outputStream = gcnew ScopeCosmosOutputStream(outputName, bufSize, bufCnt, true);
        }

        virtual void Init()
        {
            m_outputStream->Init();

            m_writer = gcnew System::IO::StreamWriter(m_outputStream.get());

            // This method is deprecated but many UDOs are bound to it
            m_outputer->InitializeAtRuntime(m_inputRowset.get(), m_args->ToArray(), m_outputer->Schema);
        }

        virtual void Output()
        {
            m_outputer->Output(m_inputRowset.get(), m_writer.get(), m_args->ToArray());
        }

        virtual void Close()
        {
            if (m_writer)
            {
                m_writer->Close();
            }
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            RuntimeStats::WriteRowCount(root, (LONGLONG)m_inputRowset->RowCount);

            auto & node = root.AddElement("ScopeOutputerManagedImpl");

            if (m_inputRowset)
            {
                m_inputRowset->WriteRuntimeStats(node);
            }

            if (m_outputStream)
            {
                m_outputStream->WriteRuntimeStats(node);
            }
        }

        virtual OperatorRequirements GetOperatorRequirements(SIZE_T bufferCount)
        {
            return OperatorRequirements(OperatorRequirementsConstants::ScopeOutputer__Size_MinMemory)
                .AddMemoryForIOStreams(OperatorRequirementsConstants::ScopeOutputer__Count_OutputUStream, 
                    Configuration::GetGlobal().GetMaxOnDiskRowSize(), 
                    bufferCount, 
                    OperatorRequirementsConstants::Output_UStream__Size_FormatterMemory);
        }

        virtual ~ScopeOutputerManagedImpl()
        {
            // There only to ensure proper destruction when base class destructor is called
            if (m_outputStream)
            {
                try
                {
                    m_outputStream.reset();
                }
                catch (std::exception&)
                {
                    // ignore any I/O errors as we are destroying the object anyway
                }
            }
        }

        // Time in writing data
        virtual __int64 GetOperatorWaitOnIOTime()
        {
            if (m_outputStream)
            {
                return m_outputStream->GetTotalIoWaitTime();
            }

            return 0;
        }

        virtual __int64 IoStreamInclusiveTime()
        {
            if (m_outputStream)
            {
                return m_outputStream->GetInclusiveTimeMillisecond();
            }

            return 0;
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            throw gcnew NotImplementedException("ScopeManagedOutputer doesn't support CEP checkpoint");
        }
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            throw gcnew NotImplementedException("ScopeManagedOutputer doesn't support CEP checkpoint");
        }
    };
    
    template<typename InputSchema>
    class StreamingOutputerManagedImpl;
    // specialized rowset for streaming outputer. It splits the upstream rowset into multiple
    // batches, based on the checkpoint and the output channel flush requirement.
    // when split is needed, m_needPauseEnumeration will be set to true, so next round of GetNext will
    // return false and the execution will go back to ScopeOutputerManaged. The Outputer may flush the output stream
    // or do checkpoint based on what triggers the split, and need call Resume to start the next batch.
    template <typename InputSchema>
    ref class RowsetAdapterForStreamingManagedOutputer: public ScopeRowset
    {
        ScopeInputRowset<InputSchema>^ m_inputRowset;
        StreamingOutputChannel* m_pOutputChannel;
        AutoFlushTimer<StreamingOutputerManagedImpl<InputSchema>>*  m_autoFlushTimer;
        int m_switchToNext;
        bool m_needCheckpoint;
        bool m_hasMoreRows;
        bool m_fromCheckpoint;

    public:
        RowsetAdapterForStreamingManagedOutputer(
            StreamingOutputChannel* pOutputChannel,
            ScopeInputRowset<InputSchema>^ inputRowset,
            AutoFlushTimer<StreamingOutputerManagedImpl<InputSchema>>* autoFlushTimer)
            : ScopeRowset(ManagedRow<InputSchema>().get()), m_autoFlushTimer(autoFlushTimer)
        {
             m_inputRowset = inputRowset;
             m_pOutputChannel = pOutputChannel;
             m_hasMoreRows = true;
             if (!g_scopeCEPCheckpointManager->GetStartScopeCEPState().empty())
             {
                ScopeDateTime startTime = g_scopeCEPCheckpointManager->GetStartCTITime();
                (m_inputRowset->GetNativeRow()).ResetScopeCEPStatus(startTime, startTime, SCOPECEP_CTI_CHECKPOINT);
                m_inputRowset->MarshalToManagedRow();
                _outputRow = m_inputRowset->GetManagedRow();
                m_fromCheckpoint = true;
             }

             SCOPE_ASSERT(m_autoFlushTimer != nullptr);
        }

        virtual bool MoveNext() override
        {
            LeaveCriticalSection(m_autoFlushTimer->GetLock());
            bool fRet = false;
            if (m_fromCheckpoint)
            {
                m_fromCheckpoint = false;
                fRet = true;
            }
            else if (m_switchToNext == 1)
            {
                m_switchToNext = 2;
                fRet = false;
            }
            else if (m_switchToNext == 2)
            {
                m_switchToNext = 0;
                m_needCheckpoint = false;
                fRet = true;
            }
            else if (m_needCheckpoint)
            {
                m_needCheckpoint = false;
                fRet = true;
            }
            else
            {
                m_hasMoreRows = m_inputRowset->MoveNext();
                if (m_hasMoreRows)
                {
                    _outputRow = m_inputRowset->GetManagedRow();
                    if (m_inputRowset->GetNativeRow().IsScopeCEPCTI())
                    {
                        if (!m_pOutputChannel->TryAdvanceCTI(m_inputRowset->GetNativeRow().GetScopeCEPEventStartTime(), ScopeDateTime::MaxValue, true))
                        {
                            m_switchToNext = 1;
                        }

                        if (m_inputRowset->GetNativeRow().GetScopeCEPEventType() == (UINT8)SCOPECEP_CTI_CHECKPOINT &&
                            g_scopeCEPCheckpointManager->IsWorthyToDoCheckpoint(m_inputRowset->GetNativeRow().GetScopeCEPEventStartTime()))
                        {
                            m_needCheckpoint = true;
                        }
                        g_scopeCEPCheckpointManager->UpdateLastCTITime(m_inputRowset->GetNativeRow().GetScopeCEPEventStartTime());
                    }
                }

                fRet = ((m_switchToNext > 0) || (m_hasMoreRows && !m_needCheckpoint));
            }

            EnterCriticalSection(m_autoFlushTimer->GetLock());
            return fRet;
        }

        void Resume()
        {
            ResetEnumerator();
        }

        bool NeedSwitchToNext() { return m_switchToNext > 0; }
        bool NeedCheckpoint() { return m_needCheckpoint; }
        bool HasMoreRows() { return m_hasMoreRows || m_fromCheckpoint; }

        void CompleteCheckpoint()
        {
        }

        void CompleteSwitch()
        {
            SCOPE_ASSERT(m_pOutputChannel->TryAdvanceCTI(m_inputRowset->GetNativeRow().GetScopeCEPEventStartTime(), ScopeDateTime::MaxValue, false));
        }
    };
    
    template<typename InputSchema>
    class StreamingOutputerManagedImpl: public ScopeOutputerManagedImpl<InputSchema>
    {
        StreamingOutputChannel* m_pOutputChannel;
        int               m_runScopeCEPMode;
        OperatorDelegate<InputSchema> * m_child;
        std::string m_outputName;
    public:
        StreamingOutputerManagedImpl(ScopeRuntime::Outputter ^ udo, Generic::List<String^>^ args, OperatorDelegate<InputSchema> * child, MarshalToManagedCallType marshalToManagedCall, int runScopeCEPMode):
            ScopeOutputerManagedImpl(udo, args, child, marshalToManagedCall)
        {
            m_runScopeCEPMode = runScopeCEPMode;
            m_child = child;
        }

        virtual ~StreamingOutputerManagedImpl()
        {
        }

        void Flush()
        {
            m_outputStream->FlushAllData();
        }

        virtual void CreateStream(std::string& outputName, SIZE_T bufSize, int bufCnt)
        {
            m_outputName = outputName;
            m_outputStream = gcnew ScopeCosmosOutputStream(outputName, bufSize, bufCnt, true);
        }

        virtual void Init() override
        {
            ScopeOutputerManagedImpl<InputSchema>::Init();
            m_pOutputChannel = IOManager::GetGlobal()->GetStreamingOutputChannel(m_outputName);
            m_pOutputChannel->SetAllowDuplicateRecord(true);
        }

        virtual void Output() override
        {
            AutoFlushTimer<StreamingOutputerManagedImpl<InputSchema>> autoFlushTimer(this);
            HANDLE handle = INVALID_HANDLE_VALUE;

            if (m_runScopeCEPMode == SCOPECEP_MODE_REAL)
            {
                autoFlushTimer.Start();
            }

            ScopeTypedManagedHandle<RowsetAdapterForStreamingManagedOutputer<InputSchema>^> rowsetAdpater;
            rowsetAdpater = gcnew RowsetAdapterForStreamingManagedOutputer<InputSchema>(m_pOutputChannel, m_inputRowset, &autoFlushTimer);

            while (rowsetAdpater->HasMoreRows())
            {
                EnterCriticalSection(autoFlushTimer.GetLock());
                m_outputer->Output(rowsetAdpater.get(), m_writer.get(), m_args->ToArray());
                LeaveCriticalSection(autoFlushTimer.GetLock());

                if (rowsetAdpater->NeedCheckpoint())
                {
                    EnterCriticalSection(autoFlushTimer.GetLock());
                    // flush the encoder in case it has data left
                    m_writer->Flush(); 
                    // flush the real stream --the Flush method on ScopeCosmosOutputStream is changed 
                    // not to flush the data but only mark the line boundaries
                    // so we need call FlushAllData to ensure all the data really flush into the device
                    m_outputStream->FlushAllData(); 
                    LeaveCriticalSection(autoFlushTimer.GetLock());

                    std::string uri = g_scopeCEPCheckpointManager->GenerateCheckpointUri();
                    IOManager::GetGlobal()->AddOutputStream(uri, uri, "", ScopeTimeSpan::TicksPerWeek);
                    BinaryOutputStream checkpoint(uri, 0x400000, 2);
                    checkpoint.Init();
                    DoScopeCEPCheckpoint(checkpoint);
                    checkpoint.Finish();
                    checkpoint.Close();
                    g_scopeCEPCheckpointManager->UpdateCurrentScopeCEPState(&uri);
                    rowsetAdpater->CompleteCheckpoint();
                }

                if (rowsetAdpater->NeedSwitchToNext())
                {
                    EnterCriticalSection(autoFlushTimer.GetLock());
                    // flush the encoder in case it has data left
                    m_writer->Flush(); 
                    // flush the real stream --the Flush method on ScopeCosmosOutputStream is changed 
                    // not to flush the data but only mark the line boundaries
                    // so we need call FlushAllData to ensure all the data really flush into the device
                    m_outputStream->FlushAllData(); 
                    rowsetAdpater->CompleteSwitch();
                    LeaveCriticalSection(autoFlushTimer.GetLock());
                }

                if (rowsetAdpater->HasMoreRows())
                {
                    rowsetAdpater->Resume();
                }
            }
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            Flush();
            m_outputStream->SaveState(output);
            System::IO::BinaryWriter^ checkpoint = gcnew System::IO::BinaryWriter(gcnew ScopeCosmosOutputStream(&output.GetOutputer(), 0x400000, 2));
            m_outputer->DoStreamingCheckpoint(checkpoint);
            checkpoint->Write((int)SCOPECEP_CHECKPINT_MAGICNUMBER);
            checkpoint->Flush();
            m_child->DoScopeCEPCheckpoint(output);
        }
        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            m_outputStream->LoadState(input);
            System::IO::BinaryReader^ checkpoint = gcnew System::IO::BinaryReader(gcnew ScopeCosmosInputStream<CosmosInput>(&input.GetInputer(), 0x400000, 2));
            m_outputer->LoadStreamingCheckpoint(checkpoint);
            CheckScopeCEPCheckpointMagicNumber(checkpoint);
            m_child->LoadScopeCEPCheckpoint(input);
        }
    };

    template<typename InputSchema, int UID, int RunScopeCEPMode>
    ScopeOutputerManaged<InputSchema> * ScopeOutputerManagedFactory::Make(OperatorDelegate<InputSchema> * child)
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        if (RunScopeCEPMode == SCOPECEP_MODE_NONE)
        {
            return new ScopeOutputerManagedImpl<InputSchema>(managedUDO.get(), 
                                                             managedUDO.args(), 
                                                             child, 
                                                             &InteropToManagedRowPolicy<InputSchema,UID>::Marshal);
        }
        else
        {
            return new StreamingOutputerManagedImpl<InputSchema>(managedUDO.get(), 
                managedUDO.args(), 
                child, 
                &InteropToManagedRowPolicy<InputSchema,UID>::Marshal,
                RunScopeCEPMode);
        }
    }

    INLINE void SendMemoryLoadNotification(ScopeRuntime::UDOBase^ rowset)
    {

#ifndef STREAMING_SCOPE
        unsigned long loadPercent;
        unsigned __int64 availableBytes;
        MemoryStatusResult result = MemoryManager::GetGlobal()->GetMemoryLoadStat(loadPercent, availableBytes);
        if (result == MSR_Failure)
            return;

        if (result == MSR_NoClrHosting)
        {
            unsigned __int64 maxClrMemoryUsageSize = 5ULL * 1024 * 1024 *1024; // 5GB
            unsigned __int64 currentMemorySize = (UINT64)ScopeEngineManaged::Global::GCMemoryWatcher->LastGCMemory;
            if (maxClrMemoryUsageSize <= currentMemorySize)
            {
                availableBytes = 0;
                loadPercent = 100;
            }
            else
            {
                availableBytes = maxClrMemoryUsageSize - currentMemorySize;
                loadPercent = (unsigned long)((double)currentMemorySize / maxClrMemoryUsageSize * 100);
            }
        }
        rowset->MemoryLoadNotification(loadPercent, availableBytes);
#endif

    }
    
#pragma endregion ManagedOperators

#if !defined(SCOPE_NO_UDT)
#pragma region ScopeUDT

    template<int UserDefinedTypeId>
    template<typename ColumnData>
    INLINE ColumnData ScopeUserDefinedType<UserDefinedTypeId>::GetColumnData() const
    {
        ColumnData columnData = scope_handle_cast<ColumnData>(m_managed);
        SCOPE_ASSERT(columnData != nullptr);
        return columnData;
    }

    template<int UserDefinedTypeId>
    ScopeUserDefinedType<UserDefinedTypeId>::ScopeUserDefinedType()
    {
        m_managed = ManagedUDT<UserDefinedTypeId>().get();
    }

    template<int UserDefinedTypeId>
    ScopeUserDefinedType<UserDefinedTypeId>::ScopeUserDefinedType(const ScopeUserDefinedType<UserDefinedTypeId> & c)
    {
        m_managed = ManagedUDT<UserDefinedTypeId>().get();

        // point the columndata to the same UDT object.
        this->Set(c.Get());
    }

    template<int UserDefinedTypeId>
    void ScopeUserDefinedType<UserDefinedTypeId>::Set(const ScopeManagedHandle & value)
    {
        this->GetColumnData<ScopeRuntime::ColumnData^>()->Set((System::Object^)value);
    }

    template<int UserDefinedTypeId>
    ScopeManagedHandle ScopeUserDefinedType<UserDefinedTypeId>::Get() const
    {
        return this->GetColumnData<ScopeRuntime::ColumnData^>()->Value;
    }

    template<int UserDefinedTypeId>
    void ScopeUserDefinedType<UserDefinedTypeId>::Reset()
    {
        this->GetColumnData<ScopeRuntime::ColumnData^>()->Reset();
    }

    template<int UserDefinedTypeId>
    bool ScopeUserDefinedType<UserDefinedTypeId>::IsNull() const
    {
        return this->GetColumnData<ScopeRuntime::ColumnData^>()->IsNull();
    }

    template<int UserDefinedTypeId>
    void ScopeUserDefinedType<UserDefinedTypeId>::SetNull()
    {
        this->GetColumnData<ScopeRuntime::ColumnData^>()->SetNull();
    }

    template<typename ReaderOrWriterType, typename StreamType>
    static INLINE ReaderOrWriterType^ GetOrCreateReaderWriter(StreamType * baseStream)
    {
        // Lazily construct managed wrapper
        if (!baseStream->Wrapper())
        {
            baseStream->Wrapper().reset(new ScopeManagedHandle(gcnew ReaderOrWriterType(gcnew ScopeStreamWrapper<StreamType>(baseStream))));
        }

        return scope_handle_cast<ReaderOrWriterType^>(*baseStream->Wrapper());
    }

    template<int UserDefinedTypeId>
    template<typename WriterType, typename StreamType>
    INLINE void ScopeUserDefinedType<UserDefinedTypeId>::Serialize(StreamType * stream) const
    {
        WriterType^ writer = GetOrCreateReaderWriter<WriterType>(stream);
        this->GetColumnData<ScopeRuntime::ColumnData^>()->Serialize(writer);
        writer->Flush();
    }

    template<int UserDefinedTypeId>
    template<typename ReaderType, typename StreamType>
    INLINE void ScopeUserDefinedType<UserDefinedTypeId>::Deserialize(StreamType * stream)
    {
        try
        {
            ReaderType^ reader = GetOrCreateReaderWriter<ReaderType>(stream);
            this->GetColumnData<ScopeRuntime::ColumnData^>()->Deserialize(reader);
        }
        catch (System::IO::EndOfStreamException^)
        {
            //Translate EndOfStreamException exception to ScopeStreamException.
            throw ScopeStreamException(ScopeStreamException::EndOfFile);
        }
    }

    template<int UserDefinedTypeId>
    void ScopeUserDefinedType<UserDefinedTypeId>::BinarySerialize(BinaryOutputStreamBase<CosmosOutput> * stream,const ScopeManagedHandle & serializationContext) const
    {
        this->Serialize<System::IO::BinaryWriter>(stream);
    }

    template<int UserDefinedTypeId>
    void ScopeUserDefinedType<UserDefinedTypeId>::BinarySerialize(BinaryOutputStreamBase<MemoryOutput> * stream,const ScopeManagedHandle & serializationContext) const
    {
        this->Serialize<System::IO::BinaryWriter>(stream);
    }

    template<int UserDefinedTypeId>
    void ScopeUserDefinedType<UserDefinedTypeId>::BinarySerialize(BinaryOutputStreamBase<DummyOutput> * stream,const ScopeManagedHandle & serializationContext) const
    {
        this->Serialize<System::IO::BinaryWriter>(stream);
    }

    template<int UserDefinedTypeId>
    void ScopeUserDefinedType<UserDefinedTypeId>::BinarySerialize(SStreamDataOutputStream * stream,const ScopeManagedHandle & serializationContext) const
    {
        this->Serialize<System::IO::BinaryWriter>(stream);
    }

    template<int UserDefinedTypeId>
    void ScopeUserDefinedType<UserDefinedTypeId>::BinaryDeserialize(BinaryInputStreamBase<CosmosInput> * stream,const ScopeManagedHandle & serializationContext)
    {
        this->Deserialize<System::IO::BinaryReader>(stream);
    }

    template<int UserDefinedTypeId>
    void ScopeUserDefinedType<UserDefinedTypeId>::BinaryDeserialize(BinaryInputStreamBase<MemoryInput> * stream,const ScopeManagedHandle & serializationContext)
    {
        this->Deserialize<System::IO::BinaryReader>(stream);
    }

    // Serialize UDT into the TextOutputStream
    template<int UserDefinedTypeId>
    void ScopeUserDefinedType<UserDefinedTypeId>::TextSerialize(TextOutputStreamBase * stream) const
    {
        this->Serialize<System::IO::StreamWriter>(stream);
    }

    // Parse UDT using IScopeSerializableText interface defined on the UDT.
    template<int UserDefinedTypeId>
    void ScopeUserDefinedType<UserDefinedTypeId>::TextDeserialize(const char * str)
    {
        try
        {
            System::String^ token = gcnew System::String(str);
            this->GetColumnData<ScopeRuntime::ColumnData^>()->UnsafeSet(token);
        }
        catch (System::Exception^)
        {
            throw ScopeStreamException(ScopeStreamException::BadConvert);
        }
    }

    template<int UserDefinedTypeId>
    void ScopeUserDefinedType<UserDefinedTypeId>::SSLibSerialize(SStreamDataOutputStream * baseStream) const
    {
        // Lazily construct managed wrapper
        if (!baseStream->Wrapper())
        {
            baseStream->Wrapper().reset(new ScopeManagedHandle(gcnew ScopeStreamWrapper<SStreamDataOutputStream>(baseStream)));
        }

        System::IO::Stream^ stream = scope_handle_cast<System::IO::Stream^>(*baseStream->Wrapper());
        this->GetColumnData<ScopeRuntime::ColumnData^>()->SSLIBFastSerializeV3(stream);
        stream->Flush();
    }

    template<int UserDefinedTypeId>
    void ScopeUserDefinedType<UserDefinedTypeId>::SSLibDeserialize(BYTE* buffer, int offset, int length, ScopeSStreamSchema& schema)
    {
        cli::array<Byte>^ byteArray = gcnew cli::array<Byte>(length + 2);
        System::Runtime::InteropServices::Marshal::Copy((IntPtr)buffer, byteArray, offset, length);

        StructuredStream::StructuredStreamSchema^ sschema = scope_handle_cast<StructuredStream::StructuredStreamSchema^>(schema.ManagedSchema());
        SCOPE_ASSERT(sschema != nullptr);

        this->GetColumnData<ScopeRuntime::ColumnData^>()->SSLIBFastDeserializeV3(byteArray, offset, length, sschema);
    }


#pragma endregion ScopeUDT
#endif // SCOPE_NO_UDT

#pragma region CEP

    ref class ScopeCosmosOutputStreamWithRealFlush : public ScopeCosmosOutputStream
    {
   
    public:
   
        ScopeCosmosOutputStreamWithRealFlush(const std::string& name, SIZE_T bufSize, int bufCount)
             : ScopeCosmosOutputStream(name, bufSize, bufCount, false)
        {
        }

        virtual void Flush() override
        {
            m_output->Flush(true);
        }
    };

    ref class CosmosIOFactory : public ScopeRuntime::IOFactory
    {

    private:

        System::String^ m_base;

    public:

        CosmosIOFactory(System::String^ base)
        {
            m_base = base;
        }

        System::IO::Stream^ CreateStream(System::String^ uri, System::IO::FileMode mode, System::IO::FileAccess access) override
        {
            SCOPE_ASSERT(
                (mode == System::IO::FileMode::Create && access == System::IO::FileAccess::Write) ||
                (mode == System::IO::FileMode::Open && access == System::IO::FileAccess::Read)
                );

            std::string path = msclr::interop::marshal_as<std::string>(m_base + "/" + uri);

            if (mode == System::IO::FileMode::Create && access == System::IO::FileAccess::Write)
            {
                IOManager::GetGlobal()->AddOutputStream(path, path, "", ScopeTimeSpan::TicksPerWeek);
                ScopeCosmosOutputStream^ stream = gcnew ScopeCosmosOutputStreamWithRealFlush(path, 0x400000, 2);
                stream->Init();
                return stream;
            }
            else
            {
                IOManager::GetGlobal()->AddInputStream(path, path);
                ScopeCosmosInputStream<CosmosInput>^ stream = gcnew ScopeCosmosInputStream<CosmosInput>(path, 0x400000, 2);
                stream->Init();
                return stream;
            }
        }
    };
    
#pragma endregion CEP

}// namespace ScopeEngine
