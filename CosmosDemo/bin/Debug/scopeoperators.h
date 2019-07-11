#pragma once

#include "OperatorRequirementsDef.h"
#include "ScopeContainers.h"
#include "ScopeSqlType.h"
#include "ScopeUtils.h"
#include "ScopeIO.h"
#include "ScopeNative.h"
#include "Hashtable.h"

#include <stack>
#include <unordered_set>
#include <numeric>

#define CACHELINE_SIZE 64

#define DEFAULT_IMPLEMENT_SCOPECEP_CHECKPOINT \
    void DoScopeCEPCheckpointImpl(BinaryOutputStream & output) \
    { \
    m_child->DoScopeCEPCheckpoint(output); \
    } \
    \
    void LoadScopeCEPCheckpointImpl(BinaryInputStream & input) \
    { \
    m_child->LoadScopeCEPCheckpoint(input); \
    }

#define DEFAULT_IMPLEMENT_SCOPECEP_CHECKPOINT_VIRTUAL \
    virtual void DoScopeCEPCheckpointImpl(BinaryOutputStream & output) \
    { \
    SCOPE_ASSERT(FALSE); \
    } \
    \
    virtual void LoadScopeCEPCheckpointImpl(BinaryInputStream & input) \
    { \
    SCOPE_ASSERT(FALSE); \
    }

#define DEFAULT_IMPLEMENT_SCOPECEP_ADJUSTCTI \
    void AdjustCtiImpl(ScopeDateTime& cti) \
        { \
        m_child->AdjustCti(cti); \
        } \

using namespace OperatorRequirements;
// TODO please remove the above line
// and replace OperatorRequirementsConstants::x with
// ::OperatorRequirements::OperatorRequirementsConstants::x

namespace ScopeEngine
{
    class OperatorOutOfMemoryException : public RuntimeException
    {
    public:
        SCOPE_ENGINE_API OperatorOutOfMemoryException(int operatorId);
        SCOPE_ENGINE_API OperatorOutOfMemoryException(const OperatorOutOfMemoryException& other);
        SCOPE_ENGINE_API virtual ~OperatorOutOfMemoryException() override;
        SCOPE_ENGINE_API virtual ExceptionWithStack* Clone() const override;
    };

    class JoinCrossLimitExceededException : public RuntimeException
    {
    public:
        SCOPE_ENGINE_API JoinCrossLimitExceededException(int operatorId, int limit);
        SCOPE_ENGINE_API JoinCrossLimitExceededException(const JoinCrossLimitExceededException& other);
        SCOPE_ENGINE_API virtual ~JoinCrossLimitExceededException() override;
        SCOPE_ENGINE_API virtual ExceptionWithStack* Clone() const override;
    };

    // It would be cleaner to define an independent class and inherit Operator from both
    // ExecutionStats and OperatorLifecycleDebugHelper. Unfortunately, it does not work because
    // multiple inheritance of Operator breaks reinterpret_casts in OperatorDelegate::FromOperator().
    class OperatorLifecycleDebugHelper : public ExecutionStats
    {
    public:
        static int& DebugLifecycleTraceLevel()
        {
            static int s_level = 0;
            return s_level;
        }
    };

#define NO_OPERATOR_ID -1

    // This is the base class that defines an operator.
    // It defines the interface it supports.
    template<typename DerivedType, typename Schema, int UID, typename CorrelatedParametersSchema = NullSchema>
    class Operator : public OperatorLifecycleDebugHelper
    {
        // "external" id for this operator to establish binding with ScopeVertexDef.xml
        // id == -1 is reserved to denote "no id" (some Operator derived classes have no matching Stage object)
        int m_operatorId;

        // We need precise lifecylcle control. If, for example, we would have only INITIALIZED state (no INIT_ENTERED), then
        // if InitImpl() throws:
        // - If we set m_lifecycleState to INITIALIZED before InitImpl(), subsequent call to Close() would be allowed, which is BAD.
        // - If we set m_lifecycleState to INITIALIZED after InitImpl(), repeated call to Init() would be allowed, which is BAD.
        // Note that even when InitImpl() has thrown, there are various destructors still waiting to screw up the things.
        enum EState
        {
            CREATED,
            INIT_ENTERED,
            INITIALIZED,
            CLOSE_ENTERED,
            CLOSED
        };

        // Enforces Operator's lifecycle. The following event sequences are valid:
        //      ctor,    then dtor
        //      ctor,    then Init()
        //      Init(),  then dtor
        //      Init(),  then Close()
        //      Close(), then dtor
        // - Any out of sequence calls are SCOPE_ASSERT'ed.
        // - Init() stands for both overloads, Init() and Init(const CorrelatedParametersSchema & params)
        //   The Operator must be Init'ed only once, with either one the other.
        // - We intentionally do not enforce sequence of GetNextRow() to avoid performance hit.
        // ToDo: Should we extend lifecycle control mechanism to other methods, like ReWind(), GetMetadata()?
        // ToDo: Task 7023767 : Re-enable ScopeEngine::Operator lifecycle asserts
        EState m_lifecycleState = CREATED;

        void DebugLifecycleTrace(char direction, const char* site)
        {
#if 0
            if (direction == '>') ++DebugLifecycleTraceLevel();

            std::cout << "---- this " << this << " | state " << m_lifecycleState << " | site ";
            std::cout.width(DebugLifecycleTraceLevel());
            std::cout << right << direction << ' ';
            std::cout.width(24 - DebugLifecycleTraceLevel()); // Space for longer site names, like "Init(params)". Negative width is OK
            std::cout << left << site;
            std::cout << " | type " << typeid(DerivedType).name() << std::endl;

            if (direction == '<') --DebugLifecycleTraceLevel();
#else
            UNREFERENCED_PARAMETER(site);
            UNREFERENCED_PARAMETER(direction);
#endif
        }

    public:
        Operator(int operatorId) : m_operatorId(operatorId)
        {
        }

        // init the operator.
        void Init()
        {
            DebugLifecycleTrace('>', "Init()");
            // Temporarily disabled to unblock March 2016 release.
            // ToDo: Task 7023767 : Re-enable ScopeEngine::Operator lifecycle asserts
            //SCOPE_ASSERT(m_lifecycleState == CREATED || !"Init() is called in the wrong state.");
            m_lifecycleState = INIT_ENTERED;

            static_cast<DerivedType*>(this)->InitImpl();

            m_lifecycleState = INITIALIZED;
            DebugLifecycleTrace('<', "Init()");
        }

        // init with parameters
        void Init(const CorrelatedParametersSchema & params)
        {
            DebugLifecycleTrace('>', "Init(params)");
            // Temporarily disabled to unblock March 2016 release.
            // ToDo: Task 7023767 : Re-enable ScopeEngine::Operator lifecycle asserts
            //SCOPE_ASSERT(m_lifecycleState == CREATED || !"Init(params) is called in the wrong state.");
            m_lifecycleState = INIT_ENTERED;

            static_cast<DerivedType*>(this)->InitImpl(params);

            m_lifecycleState = INITIALIZED;
            DebugLifecycleTrace('<', "Init(params)");
        }

        // rewind the operator to the begin.
        void ReWind()
        {
            static_cast<DerivedType*>(this)->ReWindImpl();
        }

        // Returns metadata for the rowset (should be called after Init)
        PartitionMetadata * GetMetadata()
        {
            return static_cast<DerivedType*>(this)->GetMetadataImpl();
        }

        // Get next row.
        // Return false when it reaches the end.
        bool GetNextRow(Schema & output)
        {
            return static_cast<DerivedType*>(this)->GetNextRowImpl(output);
        }

        // close the operator.
        // All resources should be released after close call.
        void Close()
        {
            DebugLifecycleTrace('>', "Close()");
            // Temporarily disabled to unblock March 2016 release.
            // ToDo: Task 7023767 : Re-enable ScopeEngine::Operator lifecycle asserts
            //SCOPE_ASSERT(m_lifecycleState == INITIALIZED || !"Close() is called in the wrong state.");
            m_lifecycleState = CLOSE_ENTERED;

            static_cast<DerivedType*>(this)->CloseImpl();

            m_lifecycleState = CLOSED;
            DebugLifecycleTrace('<', "Close()");
        }

        void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            static_cast<DerivedType*>(this)->LoadScopeCEPCheckpointImpl(input);
        }

        void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            static_cast<DerivedType*>(this)->DoScopeCEPCheckpointImpl(output);
        }

        void AdjustCti(ScopeDateTime& cti)
        {
            static_cast<DerivedType*>(this)->AdjustCtiImpl(cti);
        }

        // Writers runtime statistics for this operator and its children
        void WriteRuntimeStats(TreeNode & root)
        {
            static_cast<DerivedType*>(this)->WriteRuntimeStatsImpl(root);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            static_cast<DerivedType*>(this)->GetOperatorRequirementsImpl();
        }

        int GetOperatorId() const
        {
            return m_operatorId;
        }

        bool HasOperatorId() const
        {
            return GetOperatorId() != NO_OPERATOR_ID;
        }

        // -------------Following are default implementation for optional operator interface.  --------------


        // Default ReWind implementation. Operator needs to overide it if it provides such functionality
        void ReWindImpl()
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "ReWindImpl is not implemented for the operator.");
        }

        // Default Init(const ParametersSchema&) implementation. Operator needs to overide it if it provides such functionality
        void InitImpl(const CorrelatedParametersSchema & params)
        {
            UNREFERENCED_PARAMETER(params);

            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "InitImpl(const ParametersSchema&) is not implemented for the operator.");
        }

        void DoScopeCEPCheckpointImpl(BinaryOutputStream & /*output*/)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "DoScopeCEPCheckpointImpl is not implemented for the operator.");
        }

        void LoadScopeCEPCheckpointImpl(BinaryInputStream & /*input*/)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "LoadScopeCEPCheckpointImpl is not implemented for the operator.");
        }

        void AdjustCtiImpl(ScopeDateTime & /*cti*/)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "AdjustCtiImpl is not implemented for the operator.");
        }

        // Default WriteRuntimeStatsImpl
        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            root.AddElement("NotYetImplemented");
        }

        // Default GetOperatorRequirementsImpl
        OperatorRequirements GetOperatorRequirementsImpl()
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "GetOperatorRequirementsImpl is not implemented for the operator.");
        }
    };

    template<typename InputOperator, typename InputSchema, int UID>
    class DedupScopeCEPCTIProcessor : public Operator<DedupScopeCEPCTIProcessor<InputOperator, InputSchema, UID>, InputSchema, UID>
    {
        InputOperator* m_child;
        ScopeDateTime m_lastCTI;
        bool m_outputCTIWithCheckpoint;
    public:
        DedupScopeCEPCTIProcessor(InputOperator* input, int operatorId) :
            Operator(operatorId),
            m_child(input)
        {
            m_lastCTI = ScopeDateTime::MinValue;
            m_outputCTIWithCheckpoint = false;
        }

        void InitImpl()
        {
            AutoExecStats stats(this);
            m_child->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(InputSchema & output)
        {
            AutoExecStats stats(this);
            while (m_child->GetNextRow(output))
            {
                bool isCTI = output.IsScopeCEPCTI();
                if (isCTI)
                {
                    if ((output.GetScopeCEPEventStartTime() > m_lastCTI) ||
                        (!m_outputCTIWithCheckpoint && output.GetScopeCEPEventStartTime() == m_lastCTI))
                    {
                        if (output.GetScopeCEPEventStartTime() > m_lastCTI)
                        {
                            m_outputCTIWithCheckpoint = false;
                        }

                        if (output.GetScopeCEPEventType() == SCOPECEP_CTI_CHECKPOINT)
                        {
                            if (m_outputCTIWithCheckpoint)
                            {
                                continue;
                            }
                            else
                            {
                                m_outputCTIWithCheckpoint = true;
                            }
                        }

                        if (output.GetScopeCEPEventType() == SCOPECEP_CTI && output.GetScopeCEPEventStartTime() == m_lastCTI)
                        {
                            continue;
                        }

                        stats.IncreaseRowCount(1);
                        m_lastCTI = output.GetScopeCEPEventStartTime();
                        return true;
                    }
                }
                else
                {
                    stats.IncreaseRowCount(1);
                    return true;
                }
            }
            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("DedupScopeCTIProcessor");
            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_child->WriteRuntimeStats(node);
        }

        void DoScopeCEPCheckpoint(BinaryOutputStream& output)
        {
            output.Write(m_lastCTI);
            output.Write(m_outputCTIWithCheckpoint);
            m_child->DoScopeCEPCheckpoint(output);
        }

        void LoadScopeCEPCheckpoint(BinaryInputStream& input)
        {
            input.Read(m_lastCTI);
            input.Read(m_outputCTIWithCheckpoint);
            m_child->LoadScopeCEPCheckpoint(input);
        }

        DEFAULT_IMPLEMENT_SCOPECEP_ADJUSTCTI
    };

    template<typename InputOperator, typename InputSchema, typename AssertPolicy, int UID>
    class Asserter : public Operator<Asserter<InputOperator, InputSchema, AssertPolicy, UID>, InputSchema, UID>
    {
        InputOperator* m_child;
        AssertPolicy m_policy;
        RowEntityAllocator m_allocator;

    public:
        Asserter(InputOperator* input, int operatorId) :
            Operator(operatorId),
            m_child(input),
            m_allocator(Configuration::GetGlobal().GetMaxOnDiskRowSize(), "AssertRowOrderPolicy", RowEntityAllocator::RowContent)
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(InputSchema & output)
        {
            AutoExecStats stats(this);

            if (!m_child->GetNextRow(output))
            {
                return false;
            }

            m_policy.CheckAssert(output, m_allocator);
            stats.IncreaseRowCount(1);

            return true;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("Asserter");
            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Asserter__Row_MinMemory);
        }

        DEFAULT_IMPLEMENT_SCOPECEP_CHECKPOINT
    };

    class SlowRowTracker
    {
        ScopeDateTime m_lastRowTime;
        BYTE          m_lastRowType;
        std::string   m_operatorName;
        ULONGLONG     m_startTime;
        ULONGLONG     m_slowRowThreshold;
    public:
        SlowRowTracker(const char* name, int slowRowThreshold = 10000)
        {
            m_lastRowType = SCOPECEP_NORMAL;
            m_lastRowTime = ScopeDateTime::MinValue;
            m_operatorName = name;
            m_startTime = GetTickCount64();
            m_slowRowThreshold = slowRowThreshold;
        }

        void StartNextRow()
        {
            m_startTime = GetTickCount64();
        }

        void FinishNextRow(const ScopeDateTime& newRowStartTime, BYTE newRowType)
        {
            ULONGLONG end = GetTickCount64();
            if (end - m_startTime > m_slowRowThreshold)
            {
                char previousTimeStr[MAX_PATH], newTimeStr[MAX_PATH];
                m_lastRowTime.ToString(previousTimeStr, sizeof(previousTimeStr));
                newRowStartTime.ToString(newTimeStr, sizeof(newTimeStr));
                SCOPE_LOG_FMT_INFO("", "%s is taking long time:%I64u ms, previousTime:%s, previousType:%u, newTime:%s, newType:%u",
                    m_operatorName.c_str(), end - m_startTime,
                    previousTimeStr, m_lastRowType,
                    newTimeStr, newRowType);
            }
            m_lastRowType = newRowType;
            m_lastRowTime = newRowStartTime;
        }
    };

    template<typename OutputSchema, typename ExtractorType, typename InputStream = BinaryInputStream>
    class Extractor : public Operator<Extractor<OutputSchema, ExtractorType, InputStream>, OutputSchema, -1>
    {
        static const char* const sm_className;

        InputStream                        m_input;
        InputFileInfo                      m_inputFileInfo;

        RowEntityAllocator                 m_allocator;

        const bool                         m_needMetadata;
        PartitionMetadata * volatile       m_metadata;
        volatile LONG                      m_metadataDone;
        CRITICAL_SECTION                   m_cs;

    public:
        Extractor(const InputFileInfo& input, bool needMetadata, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize, int operatorId) :
            Operator(operatorId),
            m_input(input, &m_allocator, bufSize, bufCount),
            m_inputFileInfo(input),
            m_allocator(virtualMemSize, sm_className, RowEntityAllocator::RowContent),
            m_needMetadata(needMetadata),
            m_metadata(nullptr),
            m_metadataDone(FALSE)
        {
            InitializeCriticalSection(&m_cs);
        }

        template <typename ParametersType>
        Extractor(const InputFileInfo& input, bool needMetadata, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize, ParametersType&& inputStreamParams, int operatorId) :
            Operator(operatorId),
            m_input(input, &m_allocator, bufSize, bufCount, std::forward<ParametersType>(inputStreamParams)),
            m_inputFileInfo(input),
            m_allocator(virtualMemSize, sm_className, RowEntityAllocator::RowContent),
            m_needMetadata(needMetadata),
            m_metadata(nullptr),
            m_metadataDone(FALSE)
        {
            InitializeCriticalSection(&m_cs);
        }

        Extractor(SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize, const std::string& hostName, unsigned short port, const std::string& ext, const std::string& httpInputParameterSets, unsigned int partitionId, int operatorId) :
            Operator(operatorId),
            m_input(&m_allocator, hostName, port, ext, httpInputParameterSets, partitionId),
            m_allocator(virtualMemSize, sm_className, RowEntityAllocator::RowContent),
            m_needMetadata(false),
            m_metadata(nullptr),
            m_metadataDone(FALSE)
        {
            InitializeCriticalSection(&m_cs);
        }

        Extractor(SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize, const std::string& hostName, unsigned short port, const std::string& ext, const std::string& httpInputParameterSets, const std::vector<BlockDevice*>& devices, int operatorId) :
            Operator(operatorId),
            m_input(&m_allocator, hostName, port, ext, httpInputParameterSets, devices),
            m_allocator(virtualMemSize, sm_className, RowEntityAllocator::RowContent),
            m_needMetadata(false),
            m_metadata(nullptr),
            m_metadataDone(FALSE)
        {
            InitializeCriticalSection(&m_cs);
        }

        ~Extractor()
        {
            DeleteCriticalSection(&m_cs);
            delete m_metadata;
        }

        void InitImpl()
        {
            AutoExecStats stats(this);
            m_input.Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            if (m_metadataDone == FALSE)
            {
                // Critical section will execute interlocked operations at the beginning and the end
                // of the protected region ensuring that stores to volatile variables are visible to
                // other threads immediately, making this code safe for multithreading.
                AutoCriticalSection aCS(&m_cs);

                if (m_metadataDone == FALSE)
                {
                    AutoExecStats stats(this);

                    if (m_needMetadata)
                    {
                        m_metadata = m_input.ReadMetadata();
                    }
                    else
                    {
                        m_input.DiscardMetadata();
                    }

                    m_metadataDone = TRUE;
                }
            }

            return m_metadata;
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            // ensure metadata is extracted from the input stream
            GetMetadataImpl();

            AutoExecStats stats(this);

            m_allocator.Reset();

            try
            {
                if (ExtractorType::Deserialize(&m_input, output))
                {
                    stats.IncreaseRowCount(1);
                    return true;
                }

                return false;
            }
            catch (ScopeStreamExceptionWithEvidence & e)
            {
                IOManager::GetGlobal()->SetStreamParseErrorState(m_inputFileInfo.inputFileName, e.what());
                throw;
            }
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);
            m_input.Close();
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
        }

        void AggregateToOuterMemoryStatistics(IncrementalAllocator::Statistics& stats)
        {
            m_allocator.AggregateToOuterStatistics(stats);
        }

        LONGLONG GetTotalIoWaitTime()
        {
            return m_input.GetTotalIoWaitTime();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            AppendRuntimeStats(node);
        }

        void AppendRuntimeStats(TreeNode & node)
        {
            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_input.GetTotalIoWaitTime());
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_input.GetTotalIoWaitTime());
            node.AddAttribute(RuntimeStats::IoStreamInclusiveTime(), m_input.GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_input.WriteRuntimeStats(node);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
        }

        // Rewind the input stream
        void ReWindImpl()
        {
            if (m_needMetadata)
            {
                throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Rewind is not supported on the stream has metadata payload.");
            }

            m_input.ReWind();
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::Extractor__Row_MinMemory)
                .Add(m_input.GetOperatorRequirements());
        }
    };
    template<typename OutputSchema, typename ExtractorType, typename InputStream>
    const char* const Extractor<OutputSchema, ExtractorType, InputStream>::sm_className = "Extractor";

    template<typename OutputSchema, typename ExtractorType, typename InputStream, typename OutputType, typename OutputStream, int RunScopeCEPMode>
    class StreamingExtractor : public Operator<StreamingExtractor<OutputSchema, ExtractorType, InputStream, OutputType, OutputStream, RunScopeCEPMode>, OutputSchema, -1>
    {
        static const char* const           sm_className;
        InputStream                        m_input;
        RowEntityAllocator                 m_allocator;
        StreamingInputParams*              m_streamingInputParams;
        volatile long                      m_extractorCnt;
        std::unique_ptr<SlowRowTracker>    m_slowRowTracker;
        StreamingOutputChannel*            m_streamingChannel; // need rename class name
        std::string                        m_inputName;
        ScopeDateTime                      m_adjustedCti;

    public:
        StreamingExtractor(const InputFileInfo& input, bool needMetadata, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize, StreamingInputParams* streamingInputParams, int operatorId) :
            Operator(operatorId),
            m_input(input, &m_allocator, bufSize, bufCount),
            m_allocator(virtualMemSize, sm_className, RowEntityAllocator::RowContent),
            m_streamingInputParams(streamingInputParams),
            m_extractorCnt(0),
            m_inputName(input.inputFileName)
        {
        }

        StreamingExtractor(const InputFileInfo& input, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize, const InputStreamParameters & inputStreamParams, StreamingInputParams* streamingInputParams, int operatorId) :
            Operator(operatorId),
            m_input(input, &m_allocator, bufSize, bufCount, inputStreamParams),
            m_allocator(virtualMemSize, sm_className, RowEntityAllocator::RowContent),
            m_streamingInputParams(streamingInputParams),
            m_extractorCnt(0),
            m_inputName(input.inputFileName)
        {
        }

        ~StreamingExtractor()
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);
            m_adjustedCti = ScopeDateTime::MinValue;
            m_input.Init();
            m_slowRowTracker.reset(new SlowRowTracker(m_input.GetInputer().StreamName().c_str()));
            m_streamingChannel = IOManager::GetGlobal()->GetStreamingOutputChannel(m_inputName);
        }

        PartitionMetadata * GetMetadataImpl()
        {
            SCOPE_ASSERT(false);
            return nullptr;
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);
            SCOPE_ASSERT(RunScopeCEPMode != SCOPECEP_MODE_NONE);
            UINT64 sn = 0;
            bool outputsn = false;
            ScopeGuard guard(&m_extractorCnt);
            while (true)
            {
                m_allocator.Reset();
                try
                {
                    m_slowRowTracker->StartNextRow();
                    m_input.Read(sn);
                    if (outputsn)
                    {
                        SCOPE_LOG_FMT_INFO("", "recover: after SN is:%I64u", sn);
                        outputsn = false;
                    }
                    if (sn == ScopeCEPCheckpointManager::SYSTEM_RESERVED_SN_FOR_CACHE_METADATA)
                    {
                        UINT32 cacheMetadataLength;
                        m_input.Read(cacheMetadataLength);
                        SCOPE_ASSERT(cacheMetadataLength < 4 * 1024 * 1024 - sizeof(UINT32));
                        std::unique_ptr<char> buffer(new char[cacheMetadataLength]);
                        ((BinaryInputStream*)&m_input)->Read(buffer.get(), cacheMetadataLength);
                        m_streamingChannel->UpdateCacheMetadata(buffer.get(), cacheMetadataLength);
                        continue;
                    }
                    else
                    {
                        if (!ExtractorType::Deserialize(&m_input, output))
                        {
                            return false;
                        }

                        if (output.GetScopeCEPEventType() == SCOPECEP_FINAL_ROW)
                        {
                            m_streamingChannel->EnableEofPolling(false);
                            return false;
                        }
                    }

                    if (sn > m_streamingInputParams->Sn)
                    {
                        m_slowRowTracker->FinishNextRow(output.GetScopeCEPEventStartTime(), output.GetScopeCEPEventType());
                        m_streamingInputParams->Sn = sn;
                        stats.IncreaseRowCount(1);
                        if (output.GetScopeCEPEventStartTime() < m_adjustedCti)
                        {
                            output.SetScopeCEPEventStartTime(m_adjustedCti);
                            output.SetScopeCEPEventType(output.GetScopeCEPEventType() | SCOPECEP_CTI_ADJUST);
                        }
                        return true;
                    }
                }
                catch (DeviceException&)
                {
                    if (m_input.GetInputer().TryRecover())
                    {
                        SCOPE_LOG_FMT_INFO("", "recover: before SN is:%I64u", sn);
                        outputsn = true;
                        continue;
                    }
                    throw;
                }
                catch (ScopeStreamException &e)
                {
                    SCOPE_ASSERT(e.Error() == ScopeStreamException::EndOfFile);
                    // we reach the end of file
                    return false;
                }
            }

            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);
            m_input.Close();
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
        }

        void AggregateToOuterMemoryStatistics(IncrementalAllocator::Statistics& stats)
        {
            m_allocator.AggregateToOuterStatistics(stats);
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            AppendRuntimeStats(node);
        }

        void AppendRuntimeStats(TreeNode & node)
        {
            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_input.GetTotalIoWaitTime());
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_input.GetTotalIoWaitTime());
            node.AddAttribute(RuntimeStats::IoStreamInclusiveTime(), m_input.GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_input.WriteRuntimeStats(node);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
        }

        // Rewind the input stream
        void ReWindImpl()
        {
            SCOPE_ASSERT(false);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::StreamingExtractor__Row_MinMemory)
                .Add(m_input.GetOperatorRequirements());
        }

        void DoScopeCEPCheckpointImpl(BinaryOutputStream & output)
        {
            SCOPE_ASSERT(m_extractorCnt == 0);
            m_input.GetInputer().SaveState(output, m_input.GetInputer().GetCurrentPosition());
        }

        void LoadScopeCEPCheckpointImpl(BinaryInputStream & input)
        {
            m_input.GetInputer().LoadState(input);
            if (m_streamingInputParams->NeedSkipInputRecovery)
            {
                m_input.GetInputer().ClearState();
            }
        }

        void AdjustCtiImpl(ScopeDateTime& cti)
        {
            OutputStream os(m_inputName, sizeof(OutputSchema)+1024, 1);
            os.Init();

            OutputSchema record;
            record.ResetScopeCEPStatus(cti, cti, SCOPECEP_CTI_ADJUST | SCOPECEP_CTI);

            OutputType::Serialize(&os, record);
            os.Finish();
            os.Close();
            m_adjustedCti = cti;
        }
    };

    template<typename OutputSchema, typename ExtractorType, typename InputStream, typename OutputType, typename OutputStream, int RunScopeCEPMode>
    const char* const StreamingExtractor<OutputSchema, ExtractorType, InputStream, OutputType, OutputStream, RunScopeCEPMode>::sm_className = "StreamingExtractor";

    template <typename Schema, bool truncated>
    class KeyRangeMetafile
    {
        static const char* const sm_className;

        RowEntityAllocator                      m_allocator;
        ResourceInputStream                     m_input;

        std::unique_ptr<bool[]>                 m_includeLow;
        std::unique_ptr<bool[]>                 m_includeHigh;
        std::unique_ptr<bool[]>                 m_normalLow;
        std::unique_ptr<bool[]>                 m_normalHigh;
        std::unique_ptr<AutoRowArray<Schema>>   m_rowLow;      // auto grow array for partition range
        std::unique_ptr<AutoRowArray<Schema>>   m_rowHigh;     // auto grow array for partition range

        bool ReadRow(Schema& row, bool& includeBoundary)
        {
            bool normal = false;
            char weight;

            m_input.Read(weight);
            if (weight == 0 /*normal value*/)
            {
                // deserialize a row here
                BinaryExtractPolicy<Schema>::DeserializeKey(&m_input, row);
                normal = true;
            }
            m_input.Read(includeBoundary);

            return normal;
        }

    public:
        KeyRangeMetafile(const string& filename) :
            m_allocator(Configuration::GetGlobal().GetMaxOnDiskRowSize(), sm_className, RowEntityAllocator::RowContent),
            m_input(&m_allocator, filename)
        {
        }

        Schema* Low(int pid) const
        {
            if (!m_normalLow[pid])
            {
                return nullptr;
            }

            SCOPE_ASSERT(m_includeLow[pid]);
            return &((*m_rowLow)[pid]);
        }

        bool LowIncluded(int pid) const
        {
            return m_includeLow[pid];
        }

        Schema* High(int pid) const
        {
            if (!m_normalHigh[pid])
            {
                return nullptr;
            }

            SCOPE_ASSERT(!m_includeHigh[pid] || (truncated && m_includeHigh[pid]));
            return &((*m_rowHigh)[pid]);
        }

        bool HighIncluded(int pid) const
        {
            return m_includeHigh[pid];
        }


        // this is a "copy" of KeyFormatter.Deserialize
        void Read()
        {
            m_input.Init();
            FString sSchemaDef;
            __int64 nRanges;

            m_input.Read(sSchemaDef); // string is allocated from m_allocator
            m_input.Read(nRanges);

            // Now we know the number of key ranges allocate the row arrays for them
            m_rowLow.reset(new AutoRowArray<Schema>("KeyRangeMetafile_Low", nRanges, Configuration::GetGlobal().GetKeyRangeMaxMemorySize()));
            m_rowHigh.reset(new AutoRowArray<Schema>("KeyRangeMetafile_High", nRanges, Configuration::GetGlobal().GetKeyRangeMaxMemorySize()));

            if (nRanges > 0)
            {
                m_includeLow.reset(new bool[nRanges]);
                m_includeHigh.reset(new bool[nRanges]);
                m_normalLow.reset(new bool[nRanges]);
                m_normalHigh.reset(new bool[nRanges]);
            }

            // deserialize all ranges;
            for (int i = 0; i<nRanges; ++i)
            {
                Schema low, high;
                bool normalLow, normalHigh;
                bool includeLow, includeHigh;

                m_allocator.Reset();
                // deserialize a low boundary
                normalLow = ReadRow(low, includeLow);
                // deserialize a high boundary
                normalHigh = ReadRow(high, includeHigh);

                m_includeLow[i] = includeLow;
                m_includeHigh[i] = includeHigh;
                m_normalLow[i] = normalLow;
                m_normalHigh[i] = normalHigh;

                // if cache is full, we will bail out.
                if (!m_rowLow->AddRow(low) || !m_rowHigh->AddRow(high))
                {
                    throw RuntimeException(E_SYSTEM_ERROR, "Key range file does not have enough memory to hold range boundaries.");
                }
            }
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            m_input.Close();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            if (m_rowLow)
            {
                m_rowLow->WriteRuntimeStats(node);
            }
            if (m_rowHigh)
            {
                m_rowHigh->WriteRuntimeStats(node);
            }
            m_allocator.WriteRuntimeStats(node, sizeof(Schema));
            m_input.WriteRuntimeStats(node);
        }

        LONGLONG GetTotalIoWaitTime()
        {
            return m_input.GetTotalIoWaitTime();
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements(Configuration::GetGlobal().GetKeyRangeMaxMemorySize() * 2).AddMemoryInRows(1);
        }
    };

    template <typename Schema, bool truncated>
    const char* const KeyRangeMetafile<Schema, truncated>::sm_className = "KeyRangeMetafile";

    //
    // Template for RowGenerator operator
    //
    template<typename OutputSchema, typename GeneratePolicy, int UID = -1>
    class RowGenerator : public Operator<RowGenerator<OutputSchema, GeneratePolicy, UID>, OutputSchema, UID>
    {
        int m_partitionId;

        unsigned __int64 m_cRows;

        std::unique_ptr<KeyRangeMetafile<typename GeneratePolicy::PartitionSchema, GeneratePolicy::m_truncatedRangeKey>> m_keyRangeFile;

        std::unique_ptr<PartitionMetadata> m_metadata;

    public:
        RowGenerator(int partitionId, const string& keyRangeName, int operatorId) :
            Operator(operatorId),
            m_partitionId(partitionId),
            m_cRows(GeneratePolicy::m_cRows)
        {
            if (!keyRangeName.empty())
            {
                m_keyRangeFile.reset(new KeyRangeMetafile<typename GeneratePolicy::PartitionSchema, GeneratePolicy::m_truncatedRangeKey>(keyRangeName));
            }
        }

        // Initialize child
        void InitImpl()
        {
            AutoExecStats stats(this);
            if (m_keyRangeFile)
            {
                m_keyRangeFile->Read();
            }
        }

        PartitionMetadata * GetMetadataImpl()
        {
            if (GeneratePolicy::m_generateMetadata)
            {
                if (m_keyRangeFile)
                {
                    SCOPE_ASSERT(GeneratePolicy::m_partitioning == RangePartition);
                    m_metadata.reset(new PartitionPayloadMetadata<typename GeneratePolicy::PartitionSchema, UID>(m_partitionId, m_keyRangeFile->Low(m_partitionId), m_keyRangeFile->High(m_partitionId)));
                    return m_metadata.get();
                }
                else
                {
                    if (GeneratePolicy::m_partitioning == HashPartition || GeneratePolicy::m_partitioning == DirectHashPartition || GeneratePolicy::m_partitioning == RandomPartition)
                    {
                        m_metadata.reset(new PartitionPayloadMetadata<typename GeneratePolicy::PartitionSchema, UID>(m_partitionId));
                        return m_metadata.get();
                    }
                    else if (GeneratePolicy::m_partitioning == RangePartition)
                    {
                        throw MetadataException("RangePartition not yet implemented");
                    }
                    else
                    {
                        throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Invalid partitioning type");
                    }
                }
            }

            return nullptr;
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);
            if (m_cRows)
            {
                --m_cRows;
                stats.IncreaseRowCount(1);
                return true;
            }

            return false;
        }

        // Release all resources of children
        void CloseImpl()
        {
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("RowGenerator");
            LONGLONG ioTime = m_keyRangeFile ? m_keyRangeFile->GetTotalIoWaitTime() : 0;

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - ioTime);
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), ioTime);
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            if (m_keyRangeFile)
            {
                m_keyRangeFile->WriteRuntimeStats(node);
            }
            if (m_metadata)
            {
                m_metadata->WriteRuntimeStats(node);
            }
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            OperatorRequirements result = OperatorRequirements();

            if (m_keyRangeFile)
            {
                result.Add(m_keyRangeFile.GetOperatorRequirements());
            }
            if (m_metadata)
            {
                result.Add(m_metadata->GetOperatorRequirements());
            }

            return result;
        }
    };

    //
    // SStream Extractor operator template
    //
    template<typename OutputSchema, typename ExtractPolicy, int UID = -1, typename CorrelatedParametersSchema = NullSchema, bool hasParameterizedPredicate = false>
    class SStreamExtractor : public Operator<SStreamExtractor<OutputSchema, ExtractPolicy, UID, CorrelatedParametersSchema, hasParameterizedPredicate>, OutputSchema, UID, CorrelatedParametersSchema>
    {
        class PartitionsIterator
        {
        private:
            const SSLibV3::DataUnitDescriptor*                       m_dataUnitDesc;

            const std::vector<int>*                                            m_processingGroupIds;
            std::vector<std::shared_ptr<SSLibV3::DataUnitScanner>>*            m_scanners;

            SSIZE_T m_current;
            SSIZE_T m_opened;

        private:
            void OpenNext()
            {
                Close();
                Get()->Open(*m_dataUnitDesc);
                m_opened = m_current;
            }

        public:
            PartitionsIterator(const SSLibV3::DataUnitDescriptor* dataUnitDesc,
                const std::vector<int>* processingGroupIds,
                std::vector<std::shared_ptr<SSLibV3::DataUnitScanner>>* scanners) :
                m_dataUnitDesc(dataUnitDesc),
                m_processingGroupIds(processingGroupIds),
                m_scanners(scanners),
                m_current(0),
                m_opened(-1)
            {
                SCOPE_ASSERT(m_dataUnitDesc);
                SCOPE_ASSERT(m_processingGroupIds);
                SCOPE_ASSERT(m_scanners);
            }

            bool MoveToFirst()
            {
                m_current = 0;
                if (IsValid())
                {
                    // open for the first time or
                    // close the last one and open the first one
                    // if there is more then one  scanner
                    if (m_opened != m_current)
                    {
                        OpenNext();
                    }

                    return true;
                }

                return false;
            }

            bool MoveToNext()
            {
                ++m_current;
                if (IsValid()) //leave the last one opened
                {
                    OpenNext();

                    return true;
                }

                return false;
            }

            SSLibV3::DataUnitScanner* Get() const
            {
                return (*m_scanners)[m_current].get();
            }

            void Close()
            {
                if (m_opened >= 0 && m_scanners->size() > 0)
                {
                    (*m_scanners)[m_opened]->Close();
                    m_opened = -1;
                }
            }

            bool IsValid() const
            {
                return m_current < (SSIZE_T)m_scanners->size();
            }

            int ProcessingGroupId() const
            {
                return (*m_processingGroupIds)[m_current];
            }
        };

        int m_getssid;

        typedef typename ExtractPolicy::PredFn PredFn;

        std::vector<std::shared_ptr<BlockDevice>> m_partitionDevices;
        std::vector<std::shared_ptr<SSLibV3::DataUnitScanner>> m_scanners;
        std::vector<int> m_processingGroupIds;
        unique_ptr<PartitionsIterator> m_partitionIt;

        UINT m_rowid;
        UINT m_rows;
        PredFn* m_predicateLow;
        PredFn* m_predicateHi;
        UINT  m_predicateCnt;
        UINT m_predicateIdx;
        CorrelatedParametersSchema m_predicateParameters;

        SSLibV3::Block* m_block;
        const BYTE* m_blockEnd;

        std::unique_ptr<SSLibV3::ColumnIterator[]> m_columns;

        SSLibV3::DataUnitDescriptor m_dataUnitDesc;

        RowEntityAllocator                 m_allocator;
        RowEntityAllocator                 m_paramsAllocator;

        Scanner::Statistics                m_statistics;

        std::unique_ptr<KeyRangeMetafile<typename ExtractPolicy::PartitionSchema, ExtractPolicy::m_truncatedRangeKey>> m_keyRangeFile;

        std::unique_ptr<PartitionMetadata>      m_metadata;

        bool m_isReInit;

        UINT64 m_skippedDataLength;

    private:
        void InitScanners(int getssid)
        {
            // Every SStream extractor is given a set of partitions (data units in the sslib parlance)
            // Each partition is represented by a block device
            m_partitionDevices = IOManager::GetGlobal()->GetSstreamPartitionDevices(getssid);
            for (SIZE_T i = 0; i < m_partitionDevices.size(); ++i)
            {
                std::shared_ptr<SSLibV3::DataUnitScanner> spScanner(SSLibV3::DataUnitScanner::CreateScanner(m_partitionDevices[i].get(), MemoryManager::GetGlobal(), BufferPool::GetGlobal()), SSLibV3::DataUnitScanner::DeleteScanner);
                m_scanners.push_back(spScanner);
            }

            m_processingGroupIds = IOManager::GetGlobal()->GetSStreamProcessingGroupIds(m_partitionDevices);
            SCOPE_ASSERT(m_partitionDevices.size() == m_processingGroupIds.size());
        }

    public:
        SStreamExtractor(int getssid, const string& keyRangeName, int operatorId) :
            Operator(operatorId),
            m_getssid(getssid),
            m_rowid(0),
            m_rows(0),
            m_predicateLow(nullptr),
            m_predicateHi(nullptr),
            m_predicateCnt(0),
            m_block(nullptr),
            m_blockEnd(nullptr),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), "SStreamExtractor", RowEntityAllocator::RowContent),
            m_paramsAllocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), "SStreamExtractor_Params", RowEntityAllocator::RowContent),
            m_statistics(),
            m_isReInit(false),
            m_skippedDataLength(0)
        {
            if (!keyRangeName.empty())
            {
                m_keyRangeFile.reset(new KeyRangeMetafile<typename ExtractPolicy::PartitionSchema, ExtractPolicy::m_truncatedRangeKey>(keyRangeName));
            }

            InitScanners(m_getssid);
            m_partitionIt.reset(new PartitionsIterator(&m_dataUnitDesc, &m_processingGroupIds, &m_scanners));

            // prepare the descriptor
            m_dataUnitDesc.m_dataColumnSizes = ExtractPolicy::DataColumnSizes();
            m_dataUnitDesc.m_dataColumnCnt = ExtractPolicy::m_dataColumnSizesCnt;
            m_dataUnitDesc.m_indexColumnSizes = ExtractPolicy::IndexColumnSizes();
            m_dataUnitDesc.m_indexColumnCnt = ExtractPolicy::m_indexColumnSizesCnt;
            m_dataUnitDesc.m_sortKeys = ExtractPolicy::SortKeys();
            m_dataUnitDesc.m_sortKeysCnt = ExtractPolicy::m_sortKeysCnt;
            m_dataUnitDesc.m_descending = ExtractPolicy::m_descending;
            m_dataUnitDesc.m_skipUnavailable = ExtractPolicy::m_skipUnavailable;
            m_dataUnitDesc.m_dataSchema = ExtractPolicy::DataSchemaString();

            // 8 if used with nested loop join, 4 by default
            m_dataUnitDesc.m_numOfBuffers = ExtractPolicy::m_numOfBuffers;

            m_predicateLow = ExtractPolicy::PredicatesLow();
            m_predicateHi = ExtractPolicy::PredicatesHi();
            m_predicateCnt = ExtractPolicy::m_predicateCnt;

            // Descending scan is not currently fully implemented/tested as the optimizer is not generating such plans yet
            // When it does then we finish/test everything and remove the assert
            SCOPE_ASSERT(!m_dataUnitDesc.m_descending);

            m_columns.reset(new SSLibV3::ColumnIterator[m_dataUnitDesc.m_dataColumnCnt]);
        }

        ~SStreamExtractor()
        {
        }


        // workaround the lambda bug in the C++ compiler (should be fixed in dev12)
        template<bool keyRangeFile = false, bool keyRangeLow = false>
        class PredicateFunctor
        {
            PredicateFunctor(); // omit default construction
            PredicateFunctor& operator=(const PredicateFunctor&);

            IncrementalAllocator& m_allocator;
            PredFn     m_pred;
            CorrelatedParametersSchema* m_predicateParameters;

            typename ExtractPolicy::PartitionSchema* m_keyFileValue;
            bool m_included;
        public:
            PredicateFunctor(IncrementalAllocator& allocator, PredFn pred, CorrelatedParametersSchema* predicateParameters, typename ExtractPolicy::PartitionSchema* keyFileValue = nullptr, bool included = true) :
                m_allocator(allocator),
                m_pred(pred),
                m_predicateParameters(predicateParameters),
                m_keyFileValue(keyFileValue),
                m_included(included)
            {
            }

            PredicateFunctor(const PredicateFunctor& other) :
                m_allocator(other.m_allocator),
                m_pred(other.m_pred),
                m_predicateParameters(other.m_predicateParameters),
                m_keyFileValue(other.m_keyFileValue),
                m_included(other.m_included)
            {
            }

            bool operator() (SSLibV3::ColumnIterator* iters)
            {
                m_allocator.Reset();

                // check if we have a range predicate coming from a user script (WHERE clause)
                if (m_pred)
                {
                    // ... and if we have a key file too
                    if (keyRangeFile && keyRangeLow && m_keyFileValue)
                    {
                        // .... then set both user and low key file predicates
                        if (m_included)
                        {
                            return m_pred(iters, &m_allocator, m_predicateParameters) && typename ExtractPolicy::LowIncludedPredicateRefineSeek(iters, &m_allocator, *m_keyFileValue);
                        }
                        else
                        {
                            return m_pred(iters, &m_allocator, m_predicateParameters) && typename ExtractPolicy::LowExcludedPredicateRefineSeek(iters, &m_allocator, *m_keyFileValue);
                        }
                    }
                    else if (keyRangeFile && !keyRangeLow && m_keyFileValue)
                    {
                        // .... or user and hi key file predicates
                        if (m_included)
                        {
                            return m_pred(iters, &m_allocator, m_predicateParameters) && typename ExtractPolicy::HiIncludedPredicateRefineSeek(iters, &m_allocator, *m_keyFileValue);
                        }
                        else
                        {
                            return m_pred(iters, &m_allocator, m_predicateParameters) && typename ExtractPolicy::HiExcludedPredicateRefineSeek(iters, &m_allocator, *m_keyFileValue);
                        }
                    }
                    else
                    {
                        // ... no key file -> call simply a user predicate
                        return m_pred(iters, &m_allocator, m_predicateParameters);
                    }
                }
                else // no user predicate below
                {
                    // set key file predicates if provided
                    if (keyRangeFile && keyRangeLow && m_keyFileValue)
                    {
                        if (m_included)
                        {
                            return typename ExtractPolicy::LowIncludedPredicateRefineSeek(iters, &m_allocator, *m_keyFileValue);
                        }
                        else
                        {
                            return typename ExtractPolicy::LowExcludedPredicateRefineSeek(iters, &m_allocator, *m_keyFileValue);
                        }
                    }
                    else if (keyRangeFile && !keyRangeLow && m_keyFileValue)
                    {
                        if (m_included)
                        {
                            return typename ExtractPolicy::HiIncludedPredicateRefineSeek(iters, &m_allocator, *m_keyFileValue);
                        }
                        else
                        {
                            return typename ExtractPolicy::HiExcludedPredicateRefineSeek(iters, &m_allocator, *m_keyFileValue);
                        }
                    }
                }

                // no predicates at all -> all rows qualify -> return true
                return true;
            }
        };

        void SetKeyRangeSeek()
        {
            if (ExtractPolicy::m_keyRangeSeek)
            {
                SCOPE_ASSERT(m_keyRangeFile);

                int partitionId = m_partitionIt->ProcessingGroupId();

                if (m_predicateIdx < m_predicateCnt)
                {
                    m_partitionIt->Get()->SetLowBound(PredicateFunctor<true, true>(m_allocator, m_predicateLow[m_predicateIdx], &m_predicateParameters, m_keyRangeFile->Low(partitionId), m_keyRangeFile->LowIncluded(partitionId)));
                    m_partitionIt->Get()->SetHiBound(PredicateFunctor<true, false>(m_allocator, m_predicateHi[m_predicateIdx], &m_predicateParameters, m_keyRangeFile->High(partitionId), m_keyRangeFile->HighIncluded(partitionId)));
                    ++m_predicateIdx;
                }
                else
                {
                    m_partitionIt->Get()->SetLowBound(PredicateFunctor<true, true>(m_allocator, nullptr, nullptr, m_keyRangeFile->Low(partitionId), m_keyRangeFile->LowIncluded(partitionId)));
                    m_partitionIt->Get()->SetHiBound(PredicateFunctor<true, false>(m_allocator, nullptr, nullptr, m_keyRangeFile->High(partitionId), m_keyRangeFile->HighIncluded(partitionId)));
                }
            }
            else if (m_predicateIdx < m_predicateCnt)
            {
                m_partitionIt->Get()->SetLowBound(PredicateFunctor<false, true>(m_allocator, m_predicateLow[m_predicateIdx], &m_predicateParameters));
                m_partitionIt->Get()->SetHiBound(PredicateFunctor<false, false>(m_allocator, m_predicateHi[m_predicateIdx], &m_predicateParameters));
                ++m_predicateIdx;
            }
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            InitImplInner(CorrelatedParametersSchema());
        }

        void InitImpl(const CorrelatedParametersSchema & params)
        {
            AutoExecStats stats(this);

            InitImplInner(params);
        }

    private:
        void InitImplInner(const CorrelatedParametersSchema & params)
        {
            if (!m_isReInit)
            {
                if (m_keyRangeFile)
                {
                    m_keyRangeFile->Read();
                }

                m_isReInit = true;
            }

            m_paramsAllocator.Reset();
            m_predicateParameters = CorrelatedParametersSchema(params, &m_paramsAllocator);

            if (m_partitionIt->MoveToFirst())
            {
                m_block = nullptr;
                m_predicateIdx = 0;

                // set the first range here (if any)
                SetKeyRangeSeek();
            }

            m_skippedDataLength = 0;
        }

    public:
        void ReWindImpl()
        {
            InitImplInner(m_predicateParameters);
        }

        PartitionMetadata * GetMetadataImpl()
        {
            if (ExtractPolicy::m_generateMetadata && m_scanners.size() > 0)
            {
                SCOPE_ASSERT(m_processingGroupIds.size() > 0);
                int partitionId = m_processingGroupIds[0];
                // make sure that all processing groups belong to the same partition
                for (SIZE_T i = 1; i < m_processingGroupIds.size(); ++i)
                {
                    SCOPE_ASSERT(m_processingGroupIds[i] == partitionId);
                }

                if (m_keyRangeFile)
                {
                    SCOPE_ASSERT(ExtractPolicy::m_partitioning == RangePartition);
                    m_metadata.reset(new PartitionPayloadMetadata<typename ExtractPolicy::PartitionSchema, UID>(partitionId, m_keyRangeFile->Low(partitionId), m_keyRangeFile->High(partitionId)));
                    return m_metadata.get();
                }
                else
                {
                    if (ExtractPolicy::m_partitioning == HashPartition || ExtractPolicy::m_partitioning == DirectHashPartition || ExtractPolicy::m_partitioning == RandomPartition)
                    {
                        m_metadata.reset(new PartitionPayloadMetadata<typename ExtractPolicy::PartitionSchema, UID>(partitionId));
                        return m_metadata.get();
                    }
                    else if (ExtractPolicy::m_partitioning == RangePartition)
                    {
                        throw MetadataException("RangePartition not yet implemented");
                    }
                    else
                    {
                        throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Invalid partitioning type");
                    }
                }
            }

            return nullptr;
        }


        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            //for each partition
            //  open a partition if not yet opened (i.e. read metadata, schema - probably DataUnit::LoadMetadata() )
            //
            //  for each data block in a partition
            //
            //    for each row in a data block
            //
            //      copy out the row to the output row
            //

            for (;;)
            {
                if (!m_partitionIt->IsValid())
                {
                    return false;
                }

                if (m_block && m_rowid != m_rows)
                {
                    // refine Range partitioning residual filter
                    if (ExtractPolicy::m_residualFilterForKeyRange)
                    {
                        m_allocator.Reset();
                        SCOPE_ASSERT(m_keyRangeFile);

                        int partitionId = m_partitionIt->ProcessingGroupId();

                        auto low = m_keyRangeFile->Low(partitionId);
                        auto high = m_keyRangeFile->High(partitionId);

                        bool validLow = low ?
                            (m_keyRangeFile->LowIncluded(partitionId) ?
                            ExtractPolicy::LowIncludedPredicateRefineResidual(&m_columns[0], &m_allocator, *low)
                            : ExtractPolicy::LowExcludedPredicateRefineResidual(&m_columns[0], &m_allocator, *low)
                            )
                            : true;
                        bool validHigh = high ?
                            (m_keyRangeFile->HighIncluded(partitionId) ?
                            ExtractPolicy::HiIncludedPredicateRefineResidual(&m_columns[0], &m_allocator, *high)
                            : ExtractPolicy::HiExcludedPredicateRefineResidual(&m_columns[0], &m_allocator, *high)
                            )
                            : true;

                        if (!(validLow && validHigh))
                        {
                            // the current row did not pass the filter
                            // so advance to the next row
                            ExtractPolicy::SkipRow(&m_columns[0]);
                            ++m_rowid;
                            //go back to the top of the loop
                            continue;
                        }
                    }

                    if (ExtractPolicy::m_residualFilterForRefinedHash)
                    {
                        m_allocator.Reset();
                        SCOPE_ASSERT(ExtractPolicy::m_numberOfPartitionAfterRefinedHash > 0);

                        int numberOfPartitions = ExtractPolicy::m_numberOfPartitionAfterRefinedHash;
                        int partitionId = m_partitionIt->ProcessingGroupId();

                        bool valid = ExtractPolicy::PredicateRefinedHashResidual(&m_columns[0], &m_allocator, partitionId, numberOfPartitions);

                        if (!valid)
                        {
                            // the current row does not belong to this partition
                            // so advance to the next row
                            ExtractPolicy::SkipRow(&m_columns[0]);
                            ++m_rowid;
                            //go back to the top of the loop
                            continue;
                        }
                    }

                    m_allocator.Reset();
                    if (hasParameterizedPredicate)
                    {
                        bool skipRow = false;
                        // we can't remove intersection for the parameterized predicates until it's in runtime.
                        // it'll return duplicated data if there are intersections between predicates.
                        // When there are overlaps in predicates, the execution for earlier predicates may have returned the row already.
                        // Check each of the earlier predicates to make sure the current row is not in the range already processed.
                        for (UINT i = 0; i < m_predicateIdx - 1; i++)
                        {
                            if (m_predicateLow[i](&m_columns[0], &m_allocator, &m_predicateParameters)
                                && m_predicateHi[i](&m_columns[0], &m_allocator, &m_predicateParameters))
                            {
                                // the row has been returned in previous reads
                                // so advance to the next row
                                ExtractPolicy::SkipRow(&m_columns[0]);
                                ++m_rowid;
                                skipRow = true;
                                break;
                            }
                            m_allocator.Reset();
                        }

                        if (skipRow)
                        {
                            //go back to the top of the loop
                            continue;
                        }
                    }
                    ExtractPolicy::Deserialize(&m_columns[0], output, &m_allocator, m_blockEnd);
                    ++m_rowid;
                    stats.IncreaseRowCount(1);
                    break;
                }
                else
                {
                    m_block = m_partitionIt->Get()->GetNextBlock(m_block);
                    if (m_block)
                    {
                        m_rowid = 0;
                        m_rows = m_partitionIt->Get()->GetRowCount(m_block);
                        m_blockEnd = m_partitionIt->Get()->GetBlockBoundary(m_block);

                        // do not setup iterators if we do not have any rows
                        for (UINT i = 0; m_rows && i<m_dataUnitDesc.m_dataColumnCnt; ++i)
                        {
                            m_columns[i] = m_partitionIt->Get()->GetIterator(m_block, i);
                        }
                    }
                }

                m_skippedDataLength += m_partitionIt->Get()->GetSkippedDataLength();

                if (!m_block)
                {
                    // here is the place where the next range is set on (*m_position)
                    // only after we run out of all ranges we should close the currect data unit (aka partition) and move to the next
                    if (m_predicateIdx < m_predicateCnt)
                    {
                        SetKeyRangeSeek();
                    }
                    else
                    {
                        if (m_partitionIt->MoveToNext())
                        {
                            m_block = nullptr;
                            m_predicateIdx = 0;

                            // set the first range here (if any)
                            SetKeyRangeSeek();
                        }
                    }
                }
            }

            return true;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_partitionIt->Close();
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("SStreamExtract");

            LONGLONG sumIOWaitTime = m_keyRangeFile ? m_keyRangeFile->GetTotalIoWaitTime() : 0;
            node.AddAttribute(RuntimeStats::MaxInputCount(), m_scanners.size());
            node.AddAttribute(RuntimeStats::AvgInputCount(), m_scanners.size());
            for (SIZE_T i = 0; i < m_scanners.size(); ++i)
            {
                auto & ioNode = node.AddElement("ProcessingGroup");
                sumIOWaitTime += m_scanners[i]->WriteRuntimeStats(ioNode);
            }

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - sumIOWaitTime);
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), sumIOWaitTime);
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            if (m_keyRangeFile)
            {
                m_keyRangeFile->WriteRuntimeStats(node);
            }
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            m_paramsAllocator.WriteRuntimeStats(node);
            if (m_metadata)
            {
                m_metadata->WriteRuntimeStats(node);
            }
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            OperatorRequirements result = OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::SStreamExtractor__Row_MinMemory)
                .AddMemoryForInputSStreams(OperatorRequirementsConstants::SStreamExtractor__Count_InputSStream, ExtractPolicy::m_numOfBuffers);

            if (ExtractPolicy::m_hasCorrelatedSchema)
            {
                result.AddMemoryInRows(1, 1);
            }
            if (m_keyRangeFile)
            {
                result.Add(m_keyRangeFile->GetOperatorRequirements());
            }
            if (m_metadata)
            {
                result.Add(m_metadata->GetOperatorRequirements());
            }

            return result;
        }

        UINT64 GetSkippedDataLength() const
        {
            return m_skippedDataLength;
        }

        void MarkRemainderUnavailable() const
        {
            while (m_partitionIt->IsValid())
            {
                m_partitionIt->Get()->MarkRemainderUnavailable();
                m_partitionIt->MoveToNext();
            }
        }
    };

    //
    // Intermediate SStream Extractor operator template
    //
    template<typename OutputSchema, typename ExtractPolicy, typename InputStream, int UID = -1>
    class IntermediateSStreamExtractor : public Operator<IntermediateSStreamExtractor<OutputSchema, ExtractPolicy, InputStream, UID>, OutputSchema, UID>
    {
        std::shared_ptr<PartitionMetadata>          m_partitionMetadata;
        int                                         m_partitionIndex;
        __int64                                     m_payloadLength;
        SIZE_T                                      m_bufferSize;
        int                                         m_bufferCount;
        RowEntityAllocator                          m_allocator;
        std::unique_ptr<IncrementalAllocator>       m_partitionMetaAllocator;
        string                                      m_fileName;
        bool                                        m_needMetadata;

        BlockDevice*                                m_device;
        std::shared_ptr<SSLibV3::DataUnitScanner>   m_dataunitScanner;

        UINT                                        m_rowid;
        UINT                                        m_rows;
        SSLibV3::Block*                             m_block;
        const BYTE*                                 m_blockEnd;
        std::unique_ptr<SSLibV3::ColumnIterator[]>  m_columns;
        SSLibV3::DataUnitDescriptor                 m_dataUnitDesc;

        std::unique_ptr<Extractor<OutputSchema, BinaryExtractPolicy<OutputSchema>, InputStream>>        m_binaryExtractor;

        ConcurrentCache<IntermediateSStreamExtractor<OutputSchema, ExtractPolicy, InputStream, UID>*>*  m_extractorCache;

        static bool LowBoundPredicate(SSLibV3::ColumnIterator* iters, __int64 partitionIndex)
        {
            int curPartitionIdx = iters[0].Data<int>();
            return curPartitionIdx >= partitionIndex;
        }

        static bool HighBoundPredicate(SSLibV3::ColumnIterator* iters, __int64 partitionIndex)
        {
            int curPartitionIdx = iters[0].Data<int>();
            return curPartitionIdx <= partitionIndex;
        }

        typedef bool(*PredFn)(SSLibV3::ColumnIterator* iters, __int64 partitionIndex);

        class PredicateFunctor
        {
            PredFn                  m_pred;
            __int64                 m_partitionIndex;
        public:
            PredicateFunctor(PredFn pred, __int64 partitionIndex)
                : m_pred(pred), m_partitionIndex(partitionIndex)
            {
            }

            bool operator() (SSLibV3::ColumnIterator* iters) const
            {
                return m_pred(iters, m_partitionIndex);
            }
        };

    public:
        IntermediateSStreamExtractor(const std::string & fileName, SIZE_T bufSize, int bufCount, int partitionIdx, SIZE_T virtualMemSize, bool isStructuredStream, bool needMetadata
            , ConcurrentCache<IntermediateSStreamExtractor<OutputSchema, ExtractPolicy, InputStream, UID>*>* extractorCache, int operatorId)
            : Operator(operatorId), m_partitionIndex(partitionIdx), m_payloadLength(0), m_bufferSize(bufSize), m_bufferCount(bufCount), m_allocator(RowEntityAllocator::RowContent)
            , m_fileName(fileName), m_needMetadata(needMetadata), m_rowid(0), m_rows(0), m_block(nullptr), m_blockEnd(nullptr), m_extractorCache(extractorCache)
        {
            if (!isStructuredStream)
            {
                m_binaryExtractor.reset(new Extractor<OutputSchema, BinaryExtractPolicy<OutputSchema>, InputStream>(fileName, needMetadata, bufSize, bufCount, virtualMemSize, operatorId));
            }
            else
            {
                SCOPE_ASSERT(partitionIdx >= 0);
                m_allocator.Init(Configuration::GetGlobal().GetMaxInMemoryRowSize(), "IntermediateSStreamExtractor");
                m_device = IOManager::GetGlobal()->GetDevice(fileName);

                // prepare the descriptor
                m_dataUnitDesc.m_dataColumnSizes = ExtractPolicy::DataColumnSizes();
                m_dataUnitDesc.m_dataColumnCnt = ExtractPolicy::m_dataColumnSizesCnt;
                m_dataUnitDesc.m_indexColumnSizes = ExtractPolicy::IndexColumnSizes();
                m_dataUnitDesc.m_indexColumnCnt = ExtractPolicy::m_indexColumnSizesCnt;
                m_dataUnitDesc.m_sortKeys = ExtractPolicy::SortKeys();
                m_dataUnitDesc.m_sortKeysCnt = ExtractPolicy::m_sortKeysCnt;
                m_dataUnitDesc.m_descending = ExtractPolicy::m_descending;
                m_dataUnitDesc.m_dataSchema = ExtractPolicy::DataSchemaString();

                // hardcode for now
                m_dataUnitDesc.m_numOfBuffers = OperatorRequirementsConstants::Input_SStream__Size_BufferCount;
                m_dataUnitDesc.m_blockSize = SSLibV3::DEFAULT_SS_BLOCK_SIZE;

                SCOPE_ASSERT(!m_dataUnitDesc.m_descending);
                SCOPE_ASSERT(!m_dataUnitDesc.m_skipUnavailable);

                m_columns.reset(new SSLibV3::ColumnIterator[m_dataUnitDesc.m_dataColumnCnt]);
            }
        }

        ~IntermediateSStreamExtractor()
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            if (m_binaryExtractor != nullptr)
            {
                m_binaryExtractor->Init();
            }
            else
            {
                ReadPartitionMetadata();
                m_dataunitScanner.reset(SSLibV3::DataUnitScanner::CreateScanner(m_device, m_payloadLength, MemoryManager::GetGlobal(), BufferPool::GetDummy()), SSLibV3::DataUnitScanner::DeleteScanner);

                m_dataunitScanner->SetLowBound(PredicateFunctor(LowBoundPredicate, m_partitionIndex));
                m_dataunitScanner->SetHiBound(PredicateFunctor(HighBoundPredicate, m_partitionIndex));

                m_dataunitScanner->Open(m_dataUnitDesc);
            }
        }

        PartitionMetadata * GetMetadataImpl()
        {
            if (m_binaryExtractor != nullptr)
            {
                return m_binaryExtractor->GetMetadataImpl();
            }

            return m_partitionMetadata.get();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            if (m_binaryExtractor != nullptr)
            {
                return m_binaryExtractor->GetNextRowImpl(output);
            }

            if (m_dataunitScanner == nullptr)
            {
                return false;
            }

            for (;;)
            {
                if (m_block && m_rowid != m_rows)
                {
                    m_allocator.Reset();
                    ExtractPolicy::Deserialize(&m_columns[0], output, &m_allocator, m_blockEnd);
                    ++m_rowid;
                    stats.IncreaseRowCount(1);
                    break;
                }
                else
                {
                    m_block = m_dataunitScanner->GetNextBlock(m_block);
                    if (m_block)
                    {
                        m_rowid = 0;
                        m_rows = m_dataunitScanner->GetRowCount(m_block);
                        m_blockEnd = m_dataunitScanner->GetBlockBoundary(m_block);

                        // do not setup iterators if we do not have any rows
                        for (UINT i = 0; m_rows && i<m_dataUnitDesc.m_dataColumnCnt; ++i)
                        {
                            m_columns[i] = m_dataunitScanner->GetIterator(m_block, i);
                        }
                    }
                }

                if (!m_block)
                {
                    return false;
                }
            }
            return true;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            if (m_binaryExtractor != nullptr)
            {
                m_binaryExtractor->Close();
            }

            if (m_dataunitScanner != nullptr)
            {
                m_dataunitScanner->Close();
            }

            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("IntermediateSStreamExtractor");
            if (m_binaryExtractor != nullptr)
            {
                m_binaryExtractor->AppendRuntimeStats(node);
                return;
            }
            else if (m_partitionMetadata != nullptr)
            {
                m_partitionMetadata->WriteRuntimeStats(node);
            }

            auto & ioNode = node.AddElement("ProcessingGroup");
            LONGLONG sumIOWaitTime = m_dataunitScanner->WriteRuntimeStats(ioNode);

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - sumIOWaitTime);
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), sumIOWaitTime);
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            if (m_binaryExtractor != nullptr)
            {
                return m_binaryExtractor->GetOperatorRequirementsImpl();
            }

            OperatorRequirements result = OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::IntermediateSStreamExtractor__Row_MinMemory)
                .AddMemoryForInputSStreams(OperatorRequirementsConstants::IntermediateSStreamExtractor__Count_InputSStream, OperatorRequirementsConstants::Input_SStream__Size_BufferCount);

            if (m_partitionMetadata != nullptr)
            {
                result.Add(m_partitionMetadata->GetOperatorRequirements());
            }

            return result;
        }

    private:
        void ReadPartitionMetadata()
        {
            string channelName = GetChannelName(m_fileName);
            IntermediateSStreamExtractor<OutputSchema, ExtractPolicy, InputStream, UID>* extractor;
            if (m_extractorCache->GetItem(channelName, extractor))
            {
                // in the indexed partition case, payload could be large percent of total reading data (especially for range partition).
                // payload from the same channel should be same,
                // so we need to avoid duplicate reading.
                m_payloadLength = extractor->m_payloadLength;
                m_partitionMetadata = extractor->m_partitionMetadata;
                return;
            }

            if (m_partitionMetaAllocator == nullptr)
            {
                m_partitionMetaAllocator.reset(new IncrementalAllocator(MemoryManager::x_maxMemSize, "PartitionMetadataExtractor"));
            }
            InputStream input(m_fileName, m_partitionMetaAllocator.get(), 1024 * 1024, m_bufferCount);
            input.Init(false);
            SIZE_T pos = input.Position();
            if (m_needMetadata)
            {
                std::vector<std::shared_ptr<PartitionMetadata>> partitionMetadataList;
                input.ReadIndexedPartitionMetadata(partitionMetadataList);

                // it could happen that partition index is larger than the number of partition in the metadata.
                // if it's true, just return empty partition.
                // in other words, such partition doesn't real exist.
                if (m_partitionIndex < partitionMetadataList.size())
                {
                    m_partitionMetadata = partitionMetadataList[m_partitionIndex];
                    SCOPE_ASSERT(m_partitionMetadata != nullptr && (m_partitionIndex == m_partitionMetadata->GetPartitionId() || PartitionMetadata::PARTITION_NOT_EXIST == m_partitionMetadata->GetPartitionId()));
                }
                else
                {
                    m_partitionMetadata.reset(NonExistentPartitionMetadata::CreateNonExistentPartitionMetadata());
                }
            }
            else
            {
                input.DiscardIndexedPartitionMetadata();
            }
            m_payloadLength = input.Position() - pos;
            input.Close();

            m_extractorCache->AddItem(channelName, this);
        }
    };

    //
    // Forward declaration
    //
    template<typename OutputSchema> class SortingBucket;

    //
    // k-way merge using loser tree
    //
    template<typename InputOperator, typename InputSchema, int UID = -1>
    class ScopeLoserTree
    {
    protected:
        typedef InputSchema RowType;
        typedef KeyComparePolicy<InputSchema, UID> KeyPolicy;
        typedef RowIterator<InputOperator, InputSchema>* NodeType;
        // Input iterators
        NodeType*                    m_nodes;
        NodeType*                    m_originals;
        ULONG                        m_count;

        // variable for tournament tree
        ULONG                        m_k;
        ULONG                        m_kDone;
        ULONG                      * m_treeEntry;
        ULONG                        m_winner;

        enum MergerState
        {
            UnInit,
            InitState,
            GetRow,
            FinishWinner,
            Finished,
        };

        MergerState                  m_state;

    private:
        void InitNode(InputOperator* op, ULONG nodeIdx)
        {
            // add operator to row iterator
            m_nodes[nodeIdx] = new RowIterator<InputOperator, InputSchema>();
            m_originals[nodeIdx] = m_nodes[nodeIdx];
            m_nodes[nodeIdx]->SetOperator(op);
            ReadFirstRow(nodeIdx);
        }

    protected:
        virtual void ReadFirstRow(ULONG nodeIdx)
        {
            // get the first row
            m_nodes[nodeIdx]->ReadFirst();
        }

    public:
        ScopeLoserTree(ULONG capacity) : m_state(UnInit)
        {
            // since we only deal with limited number of input stream, we use new allocator for now.
            // will move to new allocator once it is available.
            m_nodes = new NodeType[capacity];
            m_originals = new NodeType[capacity];
            ZeroMemory(m_nodes, sizeof(NodeType)* capacity);
            ZeroMemory(m_originals, sizeof(NodeType)* capacity);
            m_treeEntry = new ULONG[capacity << 1];
            m_count = capacity;
        }

        virtual ~ScopeLoserTree()
        {
            SCOPE_ASSERT(m_nodes != nullptr);
            SCOPE_ASSERT(m_originals != nullptr);

            for (ULONG i = 0; i < m_count; i++)
            {
                delete m_originals[i];
            }
            delete[] m_originals;
            delete[] m_nodes;
            delete[] m_treeEntry;

        }

        // Initialize the loser tree
        void Init(std::vector<InputOperator *> & ops)
        {
            SCOPE_ASSERT(ops.size() == m_count);
            Init(&ops[0]);
        }

        // Initialize the loser tree
        void Init(std::vector<std::shared_ptr<InputOperator>> & ops)
        {
            SCOPE_ASSERT(ops.size() == m_count);

            for (ULONG i = 0; i < m_count; i++)
            {
                InitNode(ops[i].get(), i);
            }

            SetupTree();
        }

        // Initialize the loser tree
        void Init(InputOperator ** ops)
        {
            for (ULONG i = 0; i < m_count; i++)
            {
                InitNode(ops[i], i);
            }

            SetupTree();
        }

        void SetupTree()
        {
            m_k = m_count;

            // Compact tree
            ULONG from = 0;
            ULONG to = 0;
            for (; from < m_k; from++)
            {
                if (!m_nodes[from]->End())
                {
                    m_nodes[to] = m_nodes[from];
                    to++;
                }
            }

            m_k = to;
            m_kDone = 0;

            if (m_k == 0)
            {
                m_state = Finished;
            }
            else
            {
                //InitWinner is only valid when m_k > 0.
                m_treeEntry[0] = InitWinner(1);
                m_winner = m_treeEntry[0];

                // If all inputs are empty, then we are done.
                if (m_nodes[m_winner]->End())
                {
                    m_state = Finished;
                }
                else
                {
                    m_state = InitState;
                }
            }
        }

        void Close()
        {
        }

        int NodeCompareInternal(RowIterator<InputOperator, InputSchema> * n1, RowIterator<InputOperator, InputSchema> * n2)
        {
            if (n1->End())
            {
                return 1;
            }
            else if (n2->End())
            {
                return (-1);
            }

            return KeyPolicy::Compare(n1->GetRow(), n2->GetRow());
        }

        template<typename T, typename Schema>
        FORCE_INLINE int NodeCompare(RowIterator<T, Schema> * n1, RowIterator<T, Schema> * n2)
        {
            return NodeCompareInternal(n1, n2);
        }

        template<typename T>
        FORCE_INLINE int NodeCompare(RowIterator<SortingBucket<T>, T> * n1, RowIterator<SortingBucket<T>, T> * n2)
        {
            int result = NodeCompareInternal(n1, n2);

            if (result != 0)
            {
                return result;
            }
            else
            {
                // compare SortingBucket's serial number for stable sort
                return n1->GetOperator()->GetSerialNumber() - n2->GetOperator()->GetSerialNumber();
            }
        }

        // find winner in the tree
        ULONG InitWinner(ULONG root)
        {
            if (root >= m_k)
            {
                return root - m_k;
            }
            else
            {
                ULONG leftWinner = InitWinner(root << 1);
                ULONG rightWinner = InitWinner((root << 1) + 1);

                if (leftWinner > rightWinner)
                {
                    rightWinner ^= leftWinner;
                    leftWinner ^= rightWinner;
                    rightWinner ^= leftWinner;
                }

                if (NodeCompare(m_nodes[leftWinner], m_nodes[rightWinner]) <= 0)
                {
                    // right loses
                    m_treeEntry[root] = rightWinner;
                    return leftWinner;
                }
                else
                {
                    // left loses
                    m_treeEntry[root] = leftWinner;
                    return rightWinner;
                }
            }
        }

        // Get next row from top of the merger
        // If there is no more row, return false
        bool GetNextRow(RowType & output)
        {
            switch (m_state)
            {
                //First time after initialization, the winner is already setup
            case InitState:
            {
                              SCOPE_ASSERT(!m_nodes[m_winner]->End());
                              output = *(m_nodes[m_winner]->GetRow());
                              if (m_k == 1)
                              {
                                  // if we only have one input, just finish the winner
                                  m_state = FinishWinner;
                              }
                              else
                              {
                                  m_state = GetRow;
                              }
                              return true;
            }

            case GetRow:
            {
                           // Except for the first read which the tree is setup during Init.
                           // We need to call Increment on the winner iterator and adjust the tree
                           // before reading next winner.
                           m_nodes[m_winner]->Increment();

                           // If we have exhausted winner input, we need to shuffle the tree first.
                           if (m_nodes[m_winner]->End())
                           {
                               m_kDone++;

                               // if there is only element left, find the next winner which is not ended.
                               if (m_k - m_kDone == 1)
                               {
                                   m_winner = 0;
                                   for (; m_winner < m_k; m_winner++)
                                   {
                                       if (!m_nodes[m_winner]->End())
                                       {
                                           break;
                                       }
                                   }

                                   if (!m_nodes[m_winner]->End())
                                   {
                                       m_state = FinishWinner;
                                       output = *(m_nodes[m_winner]->GetRow());
                                       return true;
                                   }
                                   else
                                   {
                                       m_state = Finished;
                                       return false;
                                   }
                               }
                               else if ((m_kDone >= (m_k * 3 / 5)) && (m_k > 1))
                               {
                                   // Compact tree
                                   ULONG from = 0;
                                   ULONG to = 0;
                                   for (; from < m_k; from++)
                                   {
                                       if (!m_nodes[from]->End())
                                       {
                                           m_nodes[to] = m_nodes[from];
                                           to++;
                                       }
                                   }

                                   m_k = to;
                                   m_treeEntry[0] = InitWinner(1);
                                   m_kDone = 0;

                                   m_winner = m_treeEntry[0];

                                   SCOPE_ASSERT(!m_nodes[m_winner]->End());
                                   output = *(m_nodes[m_winner]->GetRow());
                                   return true;
                               }
                           }

                           // Go up the tree
                           for (ULONG i = ((m_winner + m_k) >> 1); i > 0; i >>= 1)
                           {
                               int nCompareResult = NodeCompare(m_nodes[m_treeEntry[i]], m_nodes[m_winner]);
                               if (nCompareResult < 0 || nCompareResult == 0 && m_treeEntry[i] < m_winner)
                               {
                                   m_treeEntry[i] ^= m_winner;
                                   m_winner ^= m_treeEntry[i];
                                   m_treeEntry[i] ^= m_winner;
                               }
                           }

                           SCOPE_ASSERT(!m_nodes[m_winner]->End());
                           output = *(m_nodes[m_winner]->GetRow());
                           return true;
            }

            case Finished:
                return false;

            case FinishWinner:
            {
                                 m_nodes[m_winner]->Increment();

                                 // If we have exhausted winner input, we need to shuffle the tree first.
                                 if (!m_nodes[m_winner]->End())
                                 {
                                     output = *(m_nodes[m_winner]->GetRow());
                                     return true;
                                 }
                                 else
                                 {
                                     m_state = Finished;
                                     return false;
                                 }
            }
            }

            SCOPE_ASSERT(!"Invalid Loser Tree state");
            return false;
        }
    };

    template<typename InputOperator, typename InputSchema, int UID = -1>
    class StreamingLoserTree : public ScopeLoserTree<InputOperator, InputSchema, UID>
    {
        const static int SKIP_PERCENTAGE = 0; // 0 means that it's disabled
        const static int SKIP_TIMEOUT_SECONDS = 30;
        const static int SKIP_TIMER_INTERVAL_SECONDS = 5;
        const static int SKIP_TIME_LIMIT_SECONDS = 120;

        RowType* m_headsFromCheckpoint;
        bool*    m_endsFromCheckpoint;
        std::vector<std::pair<ScopeDateTime, int>> m_smallestInputs;
        std::vector<ScopeDateTime> m_eventStartTimes;
        std::shared_ptr<void> m_skipTimer;
        std::shared_ptr<void> m_event;
        int m_skipTimeoutRetries;
        int m_skipPercentage;
        int m_skipTimeoutSeconds;
        int m_skipTimerIntervalSeconds;
        int m_skipTimeLimitSeconds;

        static inline HANDLE CreateTimer(DWORD dueTimeMs, DWORD periodTimeMs, WAITORTIMERCALLBACK timerCallback, PVOID parameter)
        {
            HANDLE hTimer = nullptr;

            bool success = ::CreateTimerQueueTimer(&hTimer, nullptr, timerCallback, parameter, dueTimeMs, periodTimeMs, WT_EXECUTEDEFAULT) != 0;
            SCOPE_ASSERT(success);

            return hTimer;
        }

        static inline void DeleteTimer(HANDLE hTimer)
        {
            ::DeleteTimerQueueTimer(nullptr, hTimer, INVALID_HANDLE_VALUE);
        }

        static VOID CALLBACK OnSkipTimerExpired(PVOID lpParameter, BOOLEAN timerOrWaitFired)
        {
            StreamingLoserTree * loserTree = (StreamingLoserTree *)lpParameter;
            DWORD res = WaitForSingleObject(loserTree->m_event.get(), 1000);
            if (res != WAIT_OBJECT_0)
            {
                if (res == WAIT_TIMEOUT)
                {
                    if (loserTree->m_skipTimeoutRetries-- <= 1)
                    {
                        loserTree->AdjustCti();
                    }
                }
            }
        }

        void AdjustCti()
        {
            for (auto itr = m_smallestInputs.begin(); itr != m_smallestInputs.end(); ++itr)
            {
                itr->first = ScopeDateTime::MaxValue;
            }

            // the smallest inputs
            for (int i = 0; i < m_eventStartTimes.size(); i++)
            {
                for (auto itr = m_smallestInputs.begin(); itr != m_smallestInputs.end();)
                {
                    if (itr->first > m_eventStartTimes[i])
                    {
                        m_smallestInputs.insert(itr, std::pair<ScopeDateTime, int>(m_eventStartTimes[i], i));
                        m_smallestInputs.resize(m_smallestInputs.size() - 1);
                        break;
                    }

                    ++itr;
                }
            }

            int idx = 0;
            for (; idx < m_smallestInputs.size() - 1; idx++)
            {
                if (m_smallestInputs[idx].second == (int)m_winner)
                {
                    break;
                }
            }

            if (idx < m_smallestInputs.size() - 1)
            {
                ScopeDateTime cti = m_smallestInputs[idx].first;
                if (m_eventStartTimes[m_winner].AddSeconds(m_skipTimeLimitSeconds) < cti)
                {
                    m_nodes[m_winner]->AdjustCti(cti);
                }
                else
                {
                    SCOPE_LOG_FMT_INFO("Adjust cti", "The input (%lu) is too slow compared with others", m_winner);
                }
            }
            else
            {
                SCOPE_LOG_FMT_INFO("Adjust cti", "Too many inputs (%I) have been adjusted.", m_smallestInputs.size() - 1);
            }
        }

    protected:
        virtual void ReadFirstRow(ULONG nodeIdx)
        {
            if (m_headsFromCheckpoint != nullptr)
            {
                if (m_endsFromCheckpoint[nodeIdx])
                {
                    m_nodes[nodeIdx]->SetEnd();
                }
                else
                {
                    *m_nodes[nodeIdx]->GetRow() = m_headsFromCheckpoint[nodeIdx];
                }
            }
            else
            {
                ScopeLoserTree::ReadFirstRow(nodeIdx);
            }

            if (m_event != nullptr)
            {
                ScopeDateTime startTime = m_nodes[nodeIdx]->GetRow()->GetScopeCEPEventStartTime();
                m_eventStartTimes[nodeIdx] = startTime;
            }
        }

    public:
        StreamingLoserTree(ULONG capacity, int skipPercentage = SKIP_PERCENTAGE, int skipTimeoutSeconds = SKIP_TIMEOUT_SECONDS, int skipTimerIntervalSeconds = SKIP_TIMER_INTERVAL_SECONDS, int skipTimeLimitSeconds = SKIP_TIME_LIMIT_SECONDS)
            : ScopeLoserTree(capacity), m_headsFromCheckpoint(nullptr), m_endsFromCheckpoint(nullptr),
            m_skipPercentage(skipPercentage), m_skipTimeoutSeconds(skipTimeoutSeconds), m_skipTimerIntervalSeconds(skipTimerIntervalSeconds), m_skipTimeLimitSeconds(skipTimeLimitSeconds)
        {
            if (capacity * m_skipPercentage / 100 > 0)
            {
                m_smallestInputs.resize(capacity * m_skipPercentage / 100 + 1);
                m_eventStartTimes.resize(capacity);
                m_event.reset(::CreateEvent(nullptr, TRUE, TRUE, nullptr), ::CloseHandle);
                SCOPE_ASSERT(m_event);
                m_skipTimer.reset(CreateTimer(0, m_skipTimerIntervalSeconds * 1000, (WAITORTIMERCALLBACK)OnSkipTimerExpired, (PVOID)this), DeleteTimer);
                m_skipTimeoutRetries = m_skipTimeoutSeconds / m_skipTimerIntervalSeconds;
            }
        }

        virtual ~StreamingLoserTree()
        {
            if (m_headsFromCheckpoint)
            {
                delete[] m_headsFromCheckpoint;
                m_headsFromCheckpoint = nullptr;
            }

            if (m_endsFromCheckpoint)
            {
                delete[] m_endsFromCheckpoint;
                m_endsFromCheckpoint = nullptr;
            }
        }

        // Get next row from top of the merger
        // If there is no more row, return false
        bool GetNextRow(RowType & output)
        {
            if (m_event == nullptr)
            {
                return ScopeLoserTree::GetNextRow(output);
            }

            ULONG lastWinner = m_winner;
            ResetEvent(m_event.get());
            bool fRet = ScopeLoserTree::GetNextRow(output);
            SetEvent(m_event.get());
            m_skipTimeoutRetries = m_skipTimeoutSeconds / m_skipTimerIntervalSeconds;

            //GetNextRow will read next row in last winner
            if (!m_nodes[lastWinner]->End())
            {
                RowType* record = m_nodes[lastWinner]->GetRow();
                if ((record->GetScopeCEPEventType() & SCOPECEP_CTI_ADJUST) == 0)
                {
                    m_eventStartTimes[lastWinner] = record->GetScopeCEPEventStartTime();
                }
            }
            else
            {
                m_eventStartTimes[lastWinner] = ScopeDateTime::MaxValue;
            }

            return fRet;
        }

        void DoScopeCEPCheckpoint(BinaryOutputStream& output)
        {
            SCOPE_LOG_FMT_INFO("CheckpointManager", "LoserTree Dump Checkpoint pos %I64u", output.GetOutputer().GetCurrentPosition());
            output.Write(m_count);
            for (ULONG i = 0; i < m_count; i++)
            {
                output.Write(m_originals[i]->End());
                if (!m_originals[i]->End())
                {
                    BinaryOutputPolicy<RowType>::Serialize(&output, *(m_originals[i]->GetRow()));
                }
            }
            SCOPE_LOG_FMT_INFO("CheckpointManager", "LoserTree Dump Checkpoint pos %I64u", output.GetOutputer().GetCurrentPosition());
        }

        void LoadScopeCEPCheckpoint(BinaryInputStream& input)
        {
            SCOPE_LOG_FMT_INFO("CheckpointManager", "LoserTree Load Checkpoint pos %I64u", input.GetInputer().GetCurrentPosition());
            ULONG count;
            input.Read(count);
            SCOPE_ASSERT(count > 1);
            m_headsFromCheckpoint = new RowType[count];
            m_endsFromCheckpoint = new bool[count];
            for (ULONG i = 0; i < count; i++)
            {
                input.Read(m_endsFromCheckpoint[i]);
                if (!m_endsFromCheckpoint[i])
                {
                    BinaryExtractPolicy<RowType>::Deserialize(&input, m_headsFromCheckpoint[i]);
                }
            }
            SCOPE_LOG_FMT_INFO("CheckpointManager", "LoserTree Load Checkpoint pos %d", input.GetInputer().GetCurrentPosition());
        }
    };

    //
    // k-way union all using round robin
    //
    template<typename InputOperator, typename InputSchema>
    class ScopeUnionAll
    {
        typedef InputSchema RowType;

        // Input iterators
        RowIterator<InputOperator, InputSchema> * m_nodes;
        ULONG                        m_count;    // total number of children
        ULONG                        m_nodeDone;  // child that has finished during one round robin pass
        ULONG                        m_k;         // number of children are not ended.
        ULONG                        m_winner;    // next reading child

        enum MergerState
        {
            UnInit,
            GetRow,
            FinishWinner,
            Finished,
        };

        MergerState                  m_state;

    public:
        ScopeUnionAll(ULONG capacity) : m_state(UnInit)
        {
            // since we only deal with limited number of input stream, we use new allocator for now.
            // will move to new allocator once it is available.
            m_nodes = new RowIterator<InputOperator, InputSchema>[capacity];
            m_count = capacity;
        }

        ~ScopeUnionAll()
        {
            SCOPE_ASSERT(m_nodes != nullptr);

            delete[] m_nodes;
        }

        // Initialize the union all tree
        void Init(std::vector<InputOperator *> & ops)
        {
            SCOPE_ASSERT(ops.size() == m_count);

            for (ULONG i = 0; i < m_count; i++)
            {
                // add operator to row iterator
                m_nodes[i].SetOperator(ops[i]);
            }

            SetupFirstRead();
        }

        // Initialize the union all tree
        void Init(std::vector<std::shared_ptr<InputOperator>> & ops)
        {
            SCOPE_ASSERT(ops.size() == m_count);

            for (ULONG i = 0; i < m_count; i++)
            {
                // add operator to row iterator
                m_nodes[i].SetOperator(ops[i].get());
            }

            SetupFirstRead();
        }

        // Initialize the union all tree
        void Init(InputOperator ** ops)
        {
            for (ULONG i = 0; i < m_count; i++)
            {
                // add operator to row iterator
                m_nodes[i].SetOperator(ops[i]);
            }

            SetupFirstRead();
        }

        void SetupFirstRead()
        {

            m_winner = 0;
            m_nodeDone = 0;
            m_k = m_count;

            if (m_k == 1)
            {
                m_state = FinishWinner;
            }
            else if (m_k == 0)
            {
                m_state = Finished;
            }
            else
            {
                m_state = GetRow;
            }
        }

        void Close()
        {
        }

        // Get next row from top of the merger
        // If there is no more row, return false
        bool GetNextRow(RowType & output)
        {
            switch (m_state)
            {
            case GetRow:
            {
                           for (;;)
                           {
                               // if current node is not ended, we will try to read one row
                               if (!m_nodes[m_winner].End())
                               {
                                   // read one row
                                   m_nodes[m_winner].Increment();

                                   // if we get a row, we return the row and move the winner
                                   if (!m_nodes[m_winner].End())
                                   {
                                       output = *(m_nodes[m_winner].GetRow());

                                       // move the winner for round-robin
                                       m_winner = (m_winner + 1) % m_k;
                                       return true;
                                   }
                               }

                               m_nodeDone++;

                               m_winner = (m_winner + 1) % m_k;

                               // if we have wrapped around, we need to check how many input are done and
                               // we may need to compact the input stream array
                               if (m_winner == 0)
                               {
                                   // if more than 3/5 node are ended, we need to compact the array
                                   if ((m_nodeDone >= (m_k * 3 / 5)) && (m_k > 1))
                                   {
                                       // Compact tree
                                       ULONG from = 0;
                                       ULONG to = 0;
                                       for (; from < m_k; from++)
                                       {
                                           if (!m_nodes[from].End())
                                           {
                                               m_nodes[to] = m_nodes[from];
                                               to++;
                                           }
                                       }

                                       m_k = to;
                                   }

                                   // only one node left
                                   if (m_k == 1)
                                   {
                                       // if the remaining node is ended, then we finished.
                                       if (m_nodes[m_winner].End())
                                       {
                                           m_state = Finished;
                                           return false;
                                       }

                                       // change the state now.
                                       m_state = FinishWinner;
                                   }
                                   else if (m_k == 0)
                                   {
                                       m_state = Finished;
                                       return false;
                                   }

                                   // reset nodeDone for next round of round robin
                                   m_nodeDone = 0;
                               }
                           }
            }
                break;

            case Finished:
                return false;

            case FinishWinner:
            {
                                 m_nodes[m_winner].Increment();

                                 if (!m_nodes[m_winner].End())
                                 {
                                     output = *(m_nodes[m_winner].GetRow());
                                     return true;
                                 }
                                 else
                                 {
                                     m_state = Finished;
                                     return false;
                                 }
            }
            }

            SCOPE_ASSERT(!"Invalid Union All Tree state");
            return false;
        }

        void DoScopeCEPCheckpoint(BinaryOutputStream& output)
        {
        }

        void LoadScopeCEPCheckpoint(BinaryInputStream& input)
        {
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements();
        }
    };

    ///
    /// merger operator template.
    ///
    template<typename InputOperator, typename OutputSchema, class MergerType, int UID = -1>
    class Merger : public Operator<Merger<InputOperator, OutputSchema, MergerType, UID>, OutputSchema, UID>
    {
        InputOperator ** m_children;  // Array of child operator
        ULONG            m_count;     // number of child operator
        MergerType       m_merger;    // merger class to merge the result from child operator.

    public:
        Merger(InputOperator ** inputs, ULONG count, int operatorId) :
            Operator(operatorId),
            m_merger(count),
            m_children(inputs),
            m_count(count)
        {
        }

        // Initialize children and merger
        void InitImpl()
        {
            AutoExecStats stats(this);

            for (ULONG i = 0; i < m_count; i++)
                m_children[i]->Init();

            m_merger.Init(m_children);
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return PartitionMetadata::MergeMetadata(m_children, m_count);
        }

        /// Get row from merger
        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            if (m_merger.GetNextRow(output))
            {
                stats.IncreaseRowCount(1);
                return true;
            }

            return false;
        }

        // Release all resources of children
        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_merger.Close();

            for (ULONG i = 0; i < m_count; i++)
                m_children[i]->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("Merge");

            LONGLONG sumChildInclusiveTime = 0;
            node.AddAttribute(RuntimeStats::MaxInputCount(), m_count);
            node.AddAttribute(RuntimeStats::AvgInputCount(), m_count);
            for (SIZE_T i = 0; i < m_count; i++)
            {
                m_children[i]->WriteRuntimeStats(node);
                sumChildInclusiveTime += m_children[i]->GetInclusiveTimeMillisecond();
            }

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - sumChildInclusiveTime);
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }
        }

#ifdef STREAMING_SCOPE
        void DoScopeCEPCheckpointImpl(BinaryOutputStream & output)
        {
            m_merger.DoScopeCEPCheckpoint(output);
            ULONG i = 0;
            while (i < m_count)
            {
                m_children[i++]->DoScopeCEPCheckpoint(output);
            }
        }

        void LoadScopeCEPCheckpointImpl(BinaryInputStream & input)
        {
            m_merger.LoadScopeCEPCheckpoint(input);
            ULONG i = 0;
            while (i < m_count)
            {
                m_children[i++]->LoadScopeCEPCheckpoint(input);
            }
        }
#endif
        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements();
        }
    };

    ///
    /// histogram merger operator template
    ///
    template<typename InputOperator, typename OutputSchema, class MergerType, int UID = -1>
    class HistogramMerger : public Operator<HistogramMerger<InputOperator, OutputSchema, MergerType, UID>, typename OutputSchema, UID>
    {
        std::vector<InputOperator*> m_children; // Array of child operator
        MergerType             m_merger;   // merger class to merge the result from child operator.

        ULONGLONG m_rowCount;
        ULONGLONG m_dataSize;
        bool m_firstRow;

    public:
        HistogramMerger(InputOperator ** inputs, ULONG count, int operatorId) :
            Operator(operatorId),
            m_merger(count),
            m_rowCount(0),
            m_dataSize(0),
            m_firstRow(true)
        {
            // Randomize inputs
            m_children.resize(count);
            for (ULONG i = 0; i < count; i++)
            {
                m_children[i] = inputs[i];
            }
        }

        // Initialize children and merger
        void InitImpl()
        {
            AutoExecStats stats(this);

            OutputSchema output;

            for (ULONG i = 0; i < m_children.size(); i++)
            {
                m_children[i]->Init();

                // Read first row from the inputs to calculate overall row count
                bool succeed = m_children[i]->GetNextRow(output);
                SCOPE_ASSERT(succeed);

                m_rowCount += output.GetBucketNumRec();
                m_dataSize += output.GetBucketDataSize();
            }

            // Init merger
            m_merger.Init(m_children);
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return PartitionMetadata::MergeMetadata(m_children, m_children.size());
        }

        /// Get row from merger
        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            if (m_firstRow)
            {
                // output rowCount row
                stats.IncreaseRowCount(m_children.size());
                output.SetBucketNumRec(m_rowCount);
                output.SetBucketDataSize(m_dataSize);
                m_firstRow = false;
                return true;
            }
            else if (m_merger.GetNextRow(output))
            {
                stats.IncreaseRowCount(1);
                return true;
            }
            else
            {
                return false;
            }
        }

        // Release all resources of children
        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_merger.Close();

            for (ULONG i = 0; i < m_children.size(); i++)
            {
                m_children[i]->Close();
            }
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("HistogramMerge");

            LONGLONG sumChildInclusiveTime = 0;
            node.AddAttribute(RuntimeStats::MaxInputCount(), m_children.size());
            node.AddAttribute(RuntimeStats::AvgInputCount(), m_children.size());
            for (SIZE_T i = 0; i < m_children.size(); i++)
            {
                m_children[i]->WriteRuntimeStats(node);
                sumChildInclusiveTime += m_children[i]->GetInclusiveTimeMillisecond();
            }

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - sumChildInclusiveTime);
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements();
        }
    };

    ///
    /// coordinated join operator template.
    ///
    template<typename InputOperators, typename OutputSchema, int UID = -1, typename CorrelatedParametersSchema = NullSchema>
    class CoordinatedJoin : public Operator<CoordinatedJoin<InputOperators, OutputSchema, UID, CorrelatedParametersSchema>, OutputSchema, UID, CorrelatedParametersSchema>
    {
        typedef typename CoordinatedJoinPolicy<InputOperators, OutputSchema, CorrelatedParametersSchema, UID> CoordinatedJoinPolicyType;

        CoordinatedJoinPolicyType m_cjpolicy;
        CorrelatedParametersSchema m_predicateParameters;

    public:
        CoordinatedJoin(InputOperators* inputs, int operatorId) :
            Operator(operatorId),
            m_cjpolicy(inputs)
        {
        }

        // Initialize children and merger
        void InitImpl()
        {
            AutoExecStats stats(this);

            m_cjpolicy.Init(CorrelatedParametersSchema());
        }

        void InitImpl(const CorrelatedParametersSchema & params)
        {
            AutoExecStats stats(this);

            m_cjpolicy.Init(params);
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_cjpolicy.GetMetadata();
        }

        /// Get row from merger
        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            return m_cjpolicy.GetNextRow(output);
        }

        // Release all resources of children
        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_cjpolicy.Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("CoordinatedJoin");

            LONGLONG sumChildInclusiveTime = 0;
            m_cjpolicy.WriteRuntimeStats(node, sumChildInclusiveTime);

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - sumChildInclusiveTime);
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::CoordinatedJoin__Size_MinMemory);
        }
    };

    ///
    /// Outputer operator template.
    ///
    template<typename InputOperator, typename InputSchema, typename OutputType, typename OutputStream = BinaryOutputStream, bool needMetadata = false>
    class Outputer : public Operator<Outputer<InputOperator, InputSchema, OutputType, OutputStream, needMetadata>, InputSchema, -1>
    {
    protected:
        InputOperator  *  m_child;  // left child operator
        OutputStream      m_output;
        SIZE_T          m_bufSize;
        int             m_bufCount;

    public:
        Outputer(InputOperator * input, std::string filename, SIZE_T bufSize, int bufCnt, int operatorId, bool maintainBoundaries = false) :
            Operator(operatorId),
            m_child(input),
            m_bufSize(bufSize),
            m_bufCount(bufCnt),
            m_output(filename, bufSize, bufCnt, maintainBoundaries)
        {
        }

        Outputer(InputOperator * input, std::string filename, SIZE_T bufSize, int bufCnt, const OutputStreamParameters & outputStreamParams, int operatorId, bool maintainBoundaries = false) :
            Operator(operatorId),
            m_child(input),
            m_output(filename, bufSize, bufCnt, outputStreamParams, maintainBoundaries)
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_output.Init();

            m_child->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            return nullptr;
        }

        bool GetNextRowImpl(InputSchema & output)
        {
            AutoExecStats stats(this);

            // calling GetMetadata() implies validating metadata which may result in false negatives when metadata is not needed
            if (needMetadata)
            {
                m_output.WriteMetadata(m_child->GetMetadata());
            }

            int count = DoOutput(output);

            // flush all remaining bytes from buffer.
            m_output.Finish();

            stats.IncreaseRowCount(count);

            return false;
        }

        virtual int DoOutput(InputSchema & output)
        {
            int count = 0;
            while (m_child->GetNextRow(output))
            {
                OutputType::Serialize(&m_output, output);
                count++;
            }
            return count;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_output.Close();

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("Output");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_output.GetTotalIoWaitTime() - m_child->GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_output.GetTotalIoWaitTime());
            node.AddAttribute(RuntimeStats::IoStreamInclusiveTime(), m_output.GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_output.WriteRuntimeStats(node);

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            SIZE_T maxBufSize = maintainBoundaries ? Configuration::GetGlobal().GetMaxOnDiskRowSize() : m_bufSize;
            return OperatorRequirements(OperatorRequirementsConstants::Outputer__Size_MinMemory)
                .AddMemoryForOutputUStreams(OperatorRequirementsConstants::Outputer__Count_OutputUStream, maxBufSize, m_bufCount);
        }

        DEFAULT_IMPLEMENT_SCOPECEP_CHECKPOINT_VIRTUAL
    };

    template<typename InputOperator, typename InputSchema, typename OutputType, typename OutputStream = BinaryOutputStream, bool needMetadata = false, int RunScopeCEPMode = SCOPECEP_MODE_NONE, bool generateSN = false, bool checkOutput = false>
    class StreamingOutputer : public Outputer<InputOperator, InputSchema, OutputType, OutputStream, needMetadata>
    {
        StreamingOutputChannel* m_streamingChannel;
        StreamingOutputCTIProcessing<StreamingOutputer, InputSchema, OutputType, OutputStream, generateSN> m_ctiProcessing;
        StreamingOutputChecking<OutputStream, InputSchema>  m_streamingChecking;
        SlowRowTracker m_slowRowTracker;

    public:
        typedef OutputType ROW;
        typedef OutputStream OUTPUT;

        StreamingOutputer(InputOperator * input, std::string filename, SIZE_T bufSize, int bufCnt, int operatorId) :
            Outputer(input, filename, bufSize, bufCnt, operatorId, RunScopeCEPMode == SCOPECEP_MODE_REAL),
            m_slowRowTracker("OUTPUT")
        {
            Initialize(filename);
        }

        StreamingOutputer(InputOperator * input, std::string filename, SIZE_T bufSize, int bufCnt, const OutputStreamParameters & outputStreamParams, int operatorId) :
            Outputer(input, filename, bufSize, bufCnt, outputStreamParams, operatorId, RunScopeCEPMode == SCOPECEP_MODE_REAL),
            m_slowRowTracker("OUTPUT")
        {
            Initialize(filename);
        }

        void Initialize(std::string& filename)
        {
            m_streamingChannel = IOManager::GetGlobal()->GetStreamingOutputChannel(filename);
            m_streamingChannel->SetAllowDuplicateRecord(true);
        }

        void Flush()
        {
            m_output.Flush();
        }

        int DoOutput(InputSchema & output) override
        {
            int count = 0;
            AutoFlushTimer<StreamingOutputer> m_autoFlushTimer(this);

            bool fromCheckpoint = false;
            if (!g_scopeCEPCheckpointManager->GetStartScopeCEPState().empty())
            {
                ScopeDateTime startTime = g_scopeCEPCheckpointManager->GetStartCTITime();
                output.ResetScopeCEPStatus(startTime, startTime, SCOPECEP_CTI_CHECKPOINT);
                fromCheckpoint = true;
                g_scopeCEPCheckpointManager->DecrementSeqNumber();
            }

            while (fromCheckpoint || m_child->GetNextRow(output))
            {
                m_slowRowTracker.FinishNextRow(output.GetScopeCEPEventStartTime(), output.GetScopeCEPEventType());
                if (!m_autoFlushTimer.IsStarted() && !fromCheckpoint && RunScopeCEPMode == SCOPECEP_MODE_REAL)
                {
                    m_autoFlushTimer.Start();
                }

                AutoCriticalSection aCs(m_autoFlushTimer.GetLock());

                if (output.IsScopeCEPCTI())
                {
                    g_scopeCEPCheckpointManager->UpdateLastCTITime(output.GetScopeCEPEventStartTime());
                    g_scopeCEPCheckpointManager->IncrementSeqNumber();
                    m_ctiProcessing.DispatchCTIToOutput(output, m_streamingChannel, &m_output);
                    m_output.Commit();

                    if (!fromCheckpoint && output.GetScopeCEPEventType() == (UINT8)SCOPECEP_CTI_CHECKPOINT && g_scopeCEPCheckpointManager->IsWorthyToDoCheckpoint(output.GetScopeCEPEventStartTime()))
                    {
                        m_output.Flush(true);
                        if (checkOutput)
                        {
                            m_streamingChecking.SetCheckpoint(g_scopeCEPCheckpointManager->InitiateCheckPointChainInternal(this));
                        }
                        else
                        {
                            g_scopeCEPCheckpointManager->InitiateCheckPointChain(this);
                        }
                    }
                }
                else
                {
                    g_scopeCEPCheckpointManager->IncrementSeqNumber();
                    if (generateSN)
                    {
                        m_output.Write(g_scopeCEPCheckpointManager->GetCurrentSeqNumber());
                    }

                    SIZE_T curPos = m_output.GetOutputer().GetCurrentPosition();
                    OutputType::Serialize(&m_output, output);
#ifdef SCOPE_DEBUG
                    if (count == 0)
                    {
                        cout << output << endl;
                    }
#endif
                    int rowSize = (int)(m_output.GetOutputer().GetCurrentPosition() - curPos);
                    if (checkOutput && RunScopeCEPMode == SCOPECEP_MODE_REAL)
                    {
                        m_streamingChecking.CheckFirstRow(m_output, rowSize);
                        m_streamingChecking.WriteRowToCheckpoint(m_output, output, rowSize);
                    }

                    m_output.Commit();
                }

                fromCheckpoint = false;
                count++;
                m_slowRowTracker.StartNextRow();
            }

            m_ctiProcessing.WriteFinalRow(&m_output);

            return count;
        }

        virtual void DoScopeCEPCheckpointImpl(BinaryOutputStream & output) override
        {
            m_output.GetOutputer().SaveState(output);
            m_child->DoScopeCEPCheckpoint(output);
        }

        virtual void LoadScopeCEPCheckpointImpl(BinaryInputStream & input) override
        {
            m_output.GetOutputer().LoadState(input);
            m_child->LoadScopeCEPCheckpoint(input);
            if (checkOutput && RunScopeCEPMode == SCOPECEP_MODE_REAL)
            {
                m_streamingChecking.GetFirstRowFromCheckpoint(input);
            }
        }
    };

    ///
    /// Native SStreamOutputer
    ///
    template<typename InputOperator, typename InputSchema, int UID>
    class SStreamOutputer : public Operator<SStreamOutputer<InputOperator, InputSchema, UID>, InputSchema, -1>
    {
        InputOperator*  m_child;  // child operator
        SStreamPartitionWriter<InputSchema, UID> m_partitionWriter;

    public:
        SStreamOutputer(InputOperator * input, std::string& filenames, int fileCnt, SIZE_T bufSize, int bufCnt, const string& outputMetadata, bool preferSSD, int operatorId, bool enableBloomFilter) :
            Operator(operatorId),
            m_child(input),
            m_partitionWriter(&filenames, fileCnt, bufSize, bufCnt, outputMetadata, preferSSD, enableBloomFilter)
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
            m_partitionWriter.Init();
            
            auto metadata = m_child->GetMetadata();
            if (metadata != nullptr)
            {
                m_partitionWriter.GetPartitionInfo(metadata);
            }
            else
            {
                // devnote: Better to rewrite operator's Init() method return true/false.
                SCOPE_LOG_FMT_INFO("SStreamOutputer", "InitImpl status %s", "child returns GetMetadata nullptr");
            }
        }

        PartitionMetadata * GetMetadataImpl()
        {
            return nullptr;
        }

        bool GetNextRowImpl(InputSchema & output)
        {
            // bailout if we are asked to process an invalid partition.
            if (!m_partitionWriter.ValidPartition()) return false;

            AutoExecStats stats(this);

            int count = 0;

            while (m_child->GetNextRow(output))
            {
                m_partitionWriter.AppendRow(output);
                count++;
            }

            stats.IncreaseRowCount(count);

            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);
            m_child->Close();
            m_partitionWriter.Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("SStreamOutput");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_partitionWriter.WriteRuntimeStats(node);

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements(OperatorRequirementsConstants::SStreamOutputer__Size_MinMemory)
                .Add(m_partitionWriter.GetOperatorRequirements());
        }
    };

    ///
    /// Native IntermediateSStreamOutputer.
    ///
    template<typename InputOperator, typename InputSchema, typename PolicyType, int MetadataId>
    class IntermediateSStreamOutputer : public Operator<IntermediateSStreamOutputer<InputOperator, InputSchema, PolicyType, MetadataId>, InputSchema, -1>
    {
        InputOperator  *m_child;  // child operator
        std::string     m_filename;
        SIZE_T          m_bufSize;
        int             m_bufCount;
        SStreamOutputStream<InputSchema, PolicyType, InputSchema, MetadataId> m_outputStream;

    public:
        IntermediateSStreamOutputer(InputOperator * input, std::string& filename, SIZE_T bufSize, int bufCnt, int operatorId) :
            Operator(operatorId),
            m_child(input),
            m_filename(filename),
            m_bufSize(bufSize),
            m_bufCount(bufCnt),
            m_outputStream(filename, 0, bufSize, bufCnt, false, false)
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();

            Scanner::ScannerType stype;
            if (MetadataId != -1)
            {
                // Write partition payload at the starting of output file.
                WritePartitionPayload(m_filename);
                stype = Scanner::STYPE_OpenExistAndAppend;
            }
            else
            {
                stype = Scanner::STYPE_Create;
            }

            m_outputStream.Init(stype);
        }

        void WritePartitionPayload(const std::string& filename)
        {
            std::unique_ptr<BinaryOutputStream> output(new BinaryOutputStream(filename, m_bufSize, m_bufCount));
            output->Init();

            // Get metadata for partition payload.
            PartitionMetadata* p = m_child->GetMetadata();
            SCOPE_ASSERT(p != nullptr);

            p->Serialize(output.get());
            output->Finish();
            output->Close();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            return nullptr;
        }

        bool GetNextRowImpl(InputSchema & output)
        {
            AutoExecStats stats(this);

            int count = 0;
            int prevPartitionIdx = 0;

            while (m_child->GetNextRow(output))
            {
                int partitionIdx = PolicyType::GetPartitionIndex(output);

                if (partitionIdx != prevPartitionIdx)
                {
                    m_outputStream.Flush();
                    prevPartitionIdx = partitionIdx;
                }

                m_outputStream.AppendRow(output);
                count++;
            }

            stats.IncreaseRowCount(count);

            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);
            m_child->Close();
            m_outputStream.Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("IntermediateSStreamOutput");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_outputStream.GetTotalIoWaitTime() - m_child->GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_outputStream.GetTotalIoWaitTime());
            node.AddAttribute(RuntimeStats::IoStreamInclusiveTime(), m_outputStream.GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_outputStream.WriteRuntimeStats(node);

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements(OperatorRequirementsConstants::IntermediateSStreamOutputer__Size_MinMemory)
                .AddMemoryForOutputSStreams(OperatorRequirementsConstants::IntermediateSStreamOutputer__Count_OutputSStream,
                Configuration::GetGlobal().GetMaxOnDiskRowSize(),
                m_bufCount);
        }
    };

    ///
    /// SStreamMetadataOutputer operator template.
    ///
    template<typename InputOperator, typename InputSchema, typename PartitionSchema, int UID>
    class SStreamMetadataOutputer : public Operator<SStreamMetadataOutputer<InputOperator, InputSchema, PartitionSchema, UID>, InputSchema, -1>
    {
        template <typename T> friend class SStreamMetadataOutputer_TestHelper;
        InputOperator*  m_child;
        BlockDevice*    m_device;
        std::string     m_baseStreamGuid;
        bool            m_preferSSD;
        bool            m_checkingPartitionInfo;
        LONGLONG        m_operatorWaitOnIOTime;

    public:
        SStreamMetadataOutputer(InputOperator * input, const std::string& filename, const char* pcszBaseStreamGuid, bool preferSSD, bool checkingPartitionInfo, int operatorId) :
            Operator(operatorId),
            m_child(input),
            m_baseStreamGuid(pcszBaseStreamGuid),
            m_preferSSD(preferSSD),
            m_checkingPartitionInfo(checkingPartitionInfo),
            m_operatorWaitOnIOTime(0)
        {
            m_device = IOManager::GetGlobal()->GetDevice(filename);
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(InputSchema & output)
        {
            AutoExecStats stats(this);
            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            SStreamMetadata* streamMetadata = dynamic_cast<SStreamMetadata*>(GetMetadataImpl());
            if (streamMetadata != nullptr)
            {
                if (m_checkingPartitionInfo)
                {
                    const std::vector<SSLibV3::PartitionInfoTableRow>& partitionInfo = *streamMetadata->GetPartitionInfo();
                    if (!CheckPartitionInfo(partitionInfo))
                    {
                        UnifyPartitionSpec(partitionInfo);
                    }
                }

                streamMetadata->UpdateDataUnitOffset();
                if (!m_baseStreamGuid.empty())
                {
                    std::shared_ptr<SSLibV3::Provenance> provenance(SSLibV3::Provenance::CreateProvenance(m_baseStreamGuid));
                    streamMetadata->SetProvenance(provenance);
                }

                streamMetadata->PreferSSD(m_preferSSD);
                streamMetadata->Write(m_device);
                m_operatorWaitOnIOTime = streamMetadata->GetTotalIoWaitTime();
            }
            else
            {
                // write a empty file and it's consistent with the managed runtime implementation
                std::unique_ptr<Scanner> scanner(Scanner::CreateScanner(m_device, MemoryManager::GetGlobal(), Scanner::STYPE_Create, 1024, 1024, 16));
                scanner->Open();
                scanner->Start();
                scanner->Close();
            }

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("SStreamMetadataOutputer");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_operatorWaitOnIOTime);
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_operatorWaitOnIOTime);
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements(OperatorRequirementsConstants::SStreamMetadataOutputer__Row_MinMemory)
                .Union(streamMetadata->GetOperatorRequirements());
        }

    private:
        static bool CheckPartitionInfo(const std::vector<SSLibV3::PartitionInfoTableRow>& partitionInfo)
        {
            SCOPE_ASSERT(UID != -1);
            size_t partitionCnt = partitionInfo.size();
            // ideally, partition count should be larger than 1 for range partitioned structured stream.
            // however, in current native runtime implementation (HistogramCollect/Reducer), it could generate partition spec which has only one partition [min, max) if row count is small
            //SCOPE_ASSERT( partitionCnt > 1); // partition count should be at least 2
            IncrementalAllocator allocator[2];
            allocator[0].Init(Configuration::GetGlobal().GetMaxKeySize(), "CheckPartitionInfo allocator0");
            allocator[1].Init(Configuration::GetGlobal().GetMaxKeySize(), "CheckPartitionInfo allocator1");
            int allocatorIdx = 0;
            PartitionSchema lowerKey;
            PartitionSchema upperKey;
            bool isValid = true;
            for (int i = 0; i < partitionCnt; i++)
            {
                const SSLibV3::PartitionInfoTableRow& partitionInfoRow = partitionInfo[i];
                MemoryInputStream inputStream(&allocator[allocatorIdx], const_cast<char*>(partitionInfoRow.m_PartitionKeyRange.data()), partitionInfoRow.m_PartitionKeyRange.length());
                char weight = 0;
                inputStream.Read(weight);
                if (i == 0)
                {
                    if (weight != x_keyWeightMin)
                    {
                        SCOPE_LOG_WARNING("Partition Keyrange checking", "Lower key of the first key range should be minimal key");
                        isValid = false;
                        break;
                    }
                }
                else
                {
                    SCOPE_ASSERT(weight == x_keyWeightNormal);
                    BinaryExtractPolicy<PartitionSchema>::DeserializeKeyForSS(&inputStream, lowerKey);

                    // Now, the upperKey is previous partition's upper key
                    int ret = KeyComparePolicy<PartitionSchema, UID>::Compare(&lowerKey, &upperKey);
                    if (ret < 0)
                    {
                        throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Current partition's lower key is less than previous partition's upper key");
                    }
                    else if (ret > 0)
                    {
                        SCOPE_LOG_WARNING("Partition Keyrange checking", "Partition key range should be continuous");
                        isValid = false;
                    }
                }
                allocatorIdx = (allocatorIdx + 1) % 2;
                allocator[allocatorIdx].Reset();

                bool includeBoundary = false;
                inputStream.Read(includeBoundary);
                if (!includeBoundary)
                {
                    SCOPE_LOG_WARNING("Partition Keyrange checking", "In partition key range, lower key should be inclusive");
                    isValid = false;
                    break;
                }

                inputStream.Read(weight);
                if (i == partitionCnt - 1)
                {
                    if (weight != x_keyWeightMax)
                    {
                        SCOPE_LOG_WARNING("Partition Keyrange checking", "Upper key of the last key range should be maximal key");
                        isValid = false;
                        break;
                    }
                }
                else
                {
                    SCOPE_ASSERT(weight == x_keyWeightNormal);
                    BinaryExtractPolicy<PartitionSchema>::DeserializeKeyForSS(&inputStream, upperKey);

                    // Now, the upperKey is current partition's upper key
                    if (i > 0 && KeyComparePolicy<PartitionSchema, UID>::Compare(&lowerKey, &upperKey) > 0)
                    {
                        throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Current partition's lower key is greater than its upper key");
                    }
                }

                inputStream.Read(includeBoundary);
                if (includeBoundary)
                {
                    SCOPE_LOG_WARNING("Partition Keyrange checking", "In partition key range, upper key should be exclusive");
                    isValid = false;
                    break;
                }
            }

            return isValid;
        }

        static void UnifyPartitionSpec(const std::vector<SSLibV3::PartitionInfoTableRow>& partitionInfo)
        {
            PartitionSchema lowerKey;
            IncrementalAllocator allocator;
            allocator.Init(Configuration::GetGlobal().GetMaxKeySize(), "UnifyPartitionSpec allocator");
            unique_ptr<MemoryOutputStream> memos(new MemoryOutputStream());
            memos->Write(x_keyWeightMin);
            // lower inclusive
            memos->Write(true);
            int idx = 1;
            for (; idx < partitionInfo.size(); idx++)
            {
                const SSLibV3::PartitionInfoTableRow& partitionInfoRow = partitionInfo[idx];
                MemoryInputStream inputStream(&allocator, const_cast<char*>(partitionInfoRow.m_PartitionKeyRange.data()), partitionInfoRow.m_PartitionKeyRange.length());
                char weight = 0;
                inputStream.Read(weight);
                SCOPE_ASSERT(weight == x_keyWeightNormal);
                memos->Write(weight);
                BinaryExtractPolicy<PartitionSchema>::DeserializeKeyForSS(&inputStream, lowerKey);
                BinaryOutputPolicy<PartitionSchema>::SerializeKeyForSS(memos.get(), lowerKey);
                // upper exclusive
                memos->Write(false);
                memos->Flush();
                auto buffer = (char*)memos->GetOutputer().Buffer();
                auto len = memos->GetOutputer().Tellp();
                const_cast<SSLibV3::PartitionInfoTableRow&>(partitionInfo[idx - 1]).m_PartitionKeyRange = string(buffer, len);

                memos.reset(new MemoryOutputStream());
                memos->Write(weight);
                BinaryOutputPolicy<PartitionSchema>::SerializeKeyForSS(memos.get(), lowerKey);
                // lower inclusive
                memos->Write(true);
                memos->Flush();

                allocator.Reset();
            }

            memos->Write(x_keyWeightMax);
            // upper exclusive
            memos->Write(false);
            memos->Flush();
            auto buffer = (char*)memos->GetOutputer().Buffer();
            auto len = memos->GetOutputer().Tellp();
            const_cast<SSLibV3::PartitionInfoTableRow&>(partitionInfo[idx - 1]).m_PartitionKeyRange = string(buffer, len);
        }
    };

    template<typename Schema, bool inlineRow>
    class AutoRowArrayInlinePolicy
    {
    public:
        typedef typename Schema CacheRowType;
        typedef AutoRowArray<CacheRowType> AutoRowArrayType;
        typedef void(*SortingMethodType)(CacheRowType *, SIZE_T);

        static Schema & GetRow(AutoRowArrayType * rowCache, SIZE_T index)
        {
            return rowCache->operator[](index);
        }

        static bool PushRow(OperatorDelegate<Schema> * input, AutoRowArrayType * rowCache)
        {
            // If cache is full, we need to raise exception and fail the execution
            return rowCache->AddRow(input->CurrentRow());
        }
    };

    template<typename Schema>
    class AutoRowArrayInlinePolicy<Schema, false>
    {
    public:
        typedef typename Schema * CacheRowType;
        typedef AutoRowArray<CacheRowType> AutoRowArrayType;
        typedef void(*SortingMethodType)(CacheRowType *, SIZE_T);

        static Schema & GetRow(AutoRowArrayType * rowCache, SIZE_T index)
        {
            return *(rowCache->operator[](index));
        }

        static bool PushRow(OperatorDelegate<Schema> * input, AutoRowArrayType * rowCache)
        {
            // If cache is full, we need to raise exception and fail the execution
            Schema * rowPtr = input->CurrentRowPtr();
            return rowCache->AddRow(rowPtr);
        }
    };

    /// New Efficient Sorting bucket implemenation which will not cause code bloat.
    ///
    /// Sorting bucket operator. This operator will cache input rows and sort in memory.
    /// The operator will cache up to a predefine number of rows or reach its memory quota.
    /// If the memory presure detected by the caller, the whole bucket may be spilled to disk using binary outputer.
    ///
    template<typename OutputSchema>
    class SortingBucket : public ExecutionStats
    {
        typedef AutoRowArrayInlinePolicy<OutputSchema, (sizeof(OutputSchema) <= CACHELINE_SIZE)> AutoRowArrayPolicyType;
        typedef typename AutoRowArrayPolicyType::AutoRowArrayType CacheType;

        LONGLONG         m_spillTime;  // track time spend in spilling
        SIZE_T           m_rowOutput;
        SIZE_T           m_size;
        SIZE_T           m_maxRowSize;
        int              m_serialNumber; // caller-assigned unique number. normally we use this field to tell sequence among multiple buckets.

        std::unique_ptr<CacheType> m_rowCache; // auto grow array to (nullptr if bucket is spilled))

        // Spill-related objects
        RowEntityAllocator m_spillAllocator; // allocator for spilled data
        std::unique_ptr<BinaryInputStream> m_spillInput; // input stream for spilled bucket

        static const SIZE_T x_ioBufferSize = 4 * 1 << 20; //4Mb
        static const SIZE_T x_ioBufferCount = 2; // minimum amount of buffers

        void InitSpilled(string & streamName)
        {
            stringstream ss;
            ss << "INPUT_" << streamName;
            string node = ss.str();

            IOManager::GetGlobal()->AddInputStream(node, streamName);
            m_spillAllocator.Init(Configuration::GetGlobal().GetMaxInMemoryRowSize(), "SortingBucket_Spill");
            SCOPE_ASSERT(!m_spillInput);
            m_spillInput.reset(new BinaryInputStream(ScopeEngine::InputFileInfo(node), &m_spillAllocator, x_ioBufferSize, x_ioBufferCount));
        }

        static SIZE_T GetManagedAvailableBytes()
        {
#ifndef SCOPE_NO_UDT
            unsigned long loadPercent;
            unsigned __int64 availableBytes;
            MemoryStatusResult result = MemoryManager::GetGlobal()->GetActualMemoryLoadStat(loadPercent, availableBytes);
            return result != MSR_Success ? 0 : availableBytes;
#else
            return 0;
#endif
        }

#ifndef SCOPE_NO_UDT
        // lastAvailableBytes stores size of available managed memory at the beginning of bucket loading
        bool HasManagedMemoryPressure(SIZE_T maxBucketSize, SIZE_T lastAvailableBytes)
        {
            unsigned long loadPercent;
            unsigned __int64 availableBytes;
            MemoryStatusResult result = MemoryManager::GetGlobal()->GetMemoryLoadStat(loadPercent, availableBytes);
            unsigned long actualLoadPercent;
            unsigned __int64 actualAvailableBytes;
            MemoryStatusResult actualResult = MemoryManager::GetGlobal()->GetActualMemoryLoadStat(actualLoadPercent, actualAvailableBytes);

            SIZE_T currentRowCount = m_rowCache->Size();
            if (result != MSR_Success || actualResult != MSR_Success)
            {
                // if ScopeHost is not used for code execution then we check memory status every 256 rows
                if ((currentRowCount & 0xFF) != 0)
                {
                    return false;
                }

                PROCESS_MEMORY_COUNTERS mi;
                BOOL success = ::GetProcessMemoryInfo(::GetCurrentProcess(), &mi, sizeof(mi));
                SCOPE_ASSERT(success);

                // This triggers spilling when "TotalMemory > 4Gb AND ManagedMemory > 2Gb" (assuming x_vertexMemoryLimit == 6Gb and x_vertexReserveMemory == 2Gb)
                // Given that we check this condition every 256 rows in practice we will spill somewhere in between 4Gb and 6Gb memory consumption
                // Actual value depends on the ratio of native and managed memory used by the process (in theory we still can go over 6Gb due to 256 rows window)
                return mi.PagefileUsage > MemoryManager::x_vertexMemoryLimit - MemoryManager::x_vertexReserveMemory && GCTotalMemory() > MemoryManager::x_vertexReserveMemory;
            }

            SIZE_T rowDelta = currentRowCount;
            if (availableBytes > Configuration::GetGlobal().GetMaxInMemoryRowSize() || rowDelta == 0)
            {
                // we have space for at least one row in vertex managed memory or bucket is empty
                return false;
            }

            // Scope CLR hosting reports 0 available memory at the lower bound than real memory limit
            // It can be a case when UDO consumes more memory than its quota
            // Instead of consuming all managed memory up to a real limit
            // we will try to estimate native plus managed memory usage by rows in bucket
            // and limit it with max bucket size

            SIZE_T managedMemoryDelta = lastAvailableBytes > actualAvailableBytes ? lastAvailableBytes - actualAvailableBytes : 0;
            SIZE_T freeBucketSize = maxBucketSize - MemorySize();
            freeBucketSize = freeBucketSize > managedMemoryDelta ? freeBucketSize - managedMemoryDelta : 0;
            if (actualAvailableBytes < Configuration::GetGlobal().GetMaxInMemoryRowSize() || freeBucketSize < Configuration::GetGlobal().GetMaxInMemoryRowSize())
            {
                return true;
            }

            return false;
        }
#endif

    public:
        typedef OutputSchema Schema;
        typedef typename AutoRowArrayPolicyType::SortingMethodType SortingMethodType;

        SortingBucket(int serial) :
            m_size(0),
            m_spillTime(0),
            m_rowOutput(0),
            m_maxRowSize(0),
            m_serialNumber(serial),
            m_spillAllocator(RowEntityAllocator::RowContent)
        {
            m_rowCache.reset(new CacheType("SortingBucket"));
        }

        SortingBucket(string & streamName, SIZE_T size, SIZE_T readCost, int serial) :
            m_size(size),
            m_spillTime(0),
            m_rowOutput(0),
            m_serialNumber(serial),
            m_spillAllocator(RowEntityAllocator::RowContent)
        {
            m_maxRowSize = readCost > x_ioBufferSize * x_ioBufferCount ? readCost - x_ioBufferSize * x_ioBufferCount : 0;
            InitSpilled(streamName);
        }

        //
        // Serial number of this bucket.
        //
        int GetSerialNumber() const
        {
            return m_serialNumber;
        }

        void SetSerialNumber(int serialNumber)
        {
            m_serialNumber = serialNumber;
        }

        //
        // Amount of rows in the bucket
        //
        SIZE_T Size() const
        {
            return m_size;
        }

        //
        // Total memory size (sum of all rows in memory)
        //
        SIZE_T MemorySize() const
        {
            if (m_rowCache)
            {
                return m_rowCache->MemorySize();
            }
            else
            {
                return 0;
            }
        }

        //
        // Amount of memory required to read bucket row by row from disk
        //
        LONGLONG RequiresIoMemory() const
        {
            return x_ioBufferSize * x_ioBufferCount + m_maxRowSize;
        }

        // load rows into bucket until it all done or it hits capacity.
        // return false if there no more rows in input.
        // return true if there is more rows in input left.
        bool LoadingPhase(OperatorDelegate<OutputSchema> * input, bool & managedMemoryFull, SIZE_T maxBucketSize = MemoryManager::x_maxMemSize)
        {
            AutoExecStats stats(this);
            managedMemoryFull = false;
            SIZE_T lastManagedAvailableBytes = GetManagedAvailableBytes();

            if (input == nullptr)
            {
                SCOPE_ASSERT(!"Sorting bucket has null input");
                return false;
            }

            SIZE_T bucketSize = 0;
            while (!m_rowCache->FFull() && bucketSize <= maxBucketSize && !input->End())
            {
                SIZE_T tmp = bucketSize;

#ifndef SCOPE_NO_UDT
                // Only check GC if we have UDT in the schema.
                if (OutputSchema::containsUDT)
                {
                    managedMemoryFull = HasManagedMemoryPressure(maxBucketSize, lastManagedAvailableBytes);
                    if (managedMemoryFull)
                    {
                        break;
                    }
                }
#endif

                // If cache is full, we need to raise exception and fail the execution
                if (!AutoRowArrayPolicyType::PushRow(input, m_rowCache.get()))
                {
                    break;
                }

                bucketSize = MemorySize();

                // just an upper estimation of row size, actual value may be much smaller
                m_maxRowSize = max<SIZE_T>(m_maxRowSize, bucketSize - tmp);

                input->MoveNext();
            }

            m_size = m_rowCache->Size();
            stats.IncreaseRowCount(m_rowCache->Size());

            return !input->End();
        }

        // sort loaded rows
        void SortingPhase(SortingMethodType sortingMethod)
        {
            AutoExecStats stats(this);

            // there is no need to sort if size is less than 2.
            if (m_rowCache->Size() < 2)
            {
                return;
            }

            // sorting the bucket
            (*sortingMethod)(m_rowCache->Begin(), m_rowCache->Size());
        }

        // write bucket to disk
        void SpillingPhase()
        {
            AutoExecStats stats(this);

            SCOPE_ASSERT(m_rowCache);

            string outputStreamName = IOManager::GetTempStreamName();

            // spill bucket to disk
            stringstream ss;
            ss << "OUTPUT_" << outputStreamName;
            string node = ss.str();

            IOManager::GetGlobal()->AddOutputStream(node, outputStreamName);
            BinaryOutputStream output(node, x_ioBufferSize, x_ioBufferCount);
            output.Init();

            // write bucket to disk
            for (SIZE_T i = 0; i < m_rowCache->Size(); ++i)
            {
                BinaryOutputPolicy<OutputSchema>::Serialize(&output, AutoRowArrayPolicyType::GetRow(m_rowCache.get(), i));
            }

            // flush all remaining bytes from buffer.
            output.Finish();
            output.Close();

            m_spillTime += output.GetTotalIoWaitTime();

            // release memory
            m_rowCache.reset();

            // prepare input stream for later reading
            InitSpilled(outputStreamName);
        }

        // Input operator maybe reused, the caller is responsible to initialize it.
        void Init()
        {
            AutoExecStats stats(this);

            if (m_spillInput)
            {
                // initialize input stream for spilled bucket
                m_spillInput->Init();
            }
        }

        /// Get row from the sorting bucket
        FORCE_INLINE bool GetNextRow(OutputSchema & output)
        {
            AutoExecStats stats(this);

            if (m_rowOutput < m_size)
            {
                if (m_rowCache)
                {
                    // in memory
                    output = AutoRowArrayPolicyType::GetRow(m_rowCache.get(), m_rowOutput);
                }
                else
                {
                    // on disk
                    m_spillAllocator.Reset();
                    bool succeed = BinaryExtractPolicy<OutputSchema>::Deserialize(m_spillInput.get(), output);
                    SCOPE_ASSERT(succeed);
                }

                m_rowOutput++;
                return true;
            }
            else
            {
                // end of bucket, no need to hold it in memory anymore
                Close();

                return false;
            }
        }

        // Input operator maybe reused, the caller is responsible to close it.
        void Close()
        {
            AutoExecStats stats(this);

            if (m_rowCache)
            {
                m_rowCache->Reset();
            }

            if (m_spillInput)
            {
                // on-disk bucket is done
                m_spillAllocator.Reset();
                m_spillInput->Close();

                m_spillTime += m_spillInput->GetTotalIoWaitTime();

                m_spillInput.reset();
            }
        }

        // supposely, it'll only be called for in-memory bucket.
        void Reset(bool reclaimMemory)
        {
            m_size = 0;
            m_rowOutput = 0;
            m_maxRowSize = 0;

            SCOPE_ASSERT(m_rowCache);
            SCOPE_ASSERT(m_rowCache->Size() == 0);

            // Bucket has already been reset in Close(), we do "hard" reset decomitting all memory only if requested
            if (reclaimMemory)
            {
                m_rowCache->Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            }
        }

        LONGLONG OperatorWaitOnIOTime() const
        {
            return m_spillTime;
        }

        void AggregateToOuterMemoryStatistics(IncrementalAllocator::Statistics& stats)
        {
            if (m_spillAllocator.HasStatistics())
            {
                m_spillAllocator.AggregateToOuterStatistics(stats);
            }
        }
    };

    // Delegate class for ScopeLoserTree.
    // The class will delegate all operator interface with one direct function invocation.
    template<typename Schema>
    class ScopeLoserTreeDelegate
    {
    public:
        ScopeLoserTreeDelegate() : m_objectPtr(nullptr),
            m_initPtr(nullptr),
            m_closePtr(nullptr),
            m_createPtr(nullptr),
            m_deletePtr(nullptr),
            m_getNextRowPtr(nullptr)
        {}

        // we will not scope loser tree object in copy constructor.
        ScopeLoserTreeDelegate(const ScopeLoserTreeDelegate & c)
        {
            m_createPtr = c.m_createPtr;
            m_initPtr = c.m_initPtr;
            m_closePtr = c.m_closePtr;
            m_deletePtr = c.m_deletePtr;
            m_getNextRowPtr = c.m_getNextRowPtr;
            m_objectPtr = nullptr;

        }

        ~ScopeLoserTreeDelegate()
        {
            if (m_objectPtr != nullptr)
            {
                Delete();
            }
        }

        template <int UID>
        static ScopeLoserTreeDelegate * CreateDelegate()
        {
            typedef ScopeLoserTree<SortingBucket<Schema>, Schema, UID> ScopeLoserTreeType;

            std::unique_ptr<ScopeLoserTreeDelegate> d(new ScopeLoserTreeDelegate());

            d->m_createPtr = &CreateScopeLoserTreeStub<ScopeLoserTreeType>;
            d->m_initPtr = &InitMethodStub<ScopeLoserTreeType, &ScopeLoserTreeType::Init>;
            d->m_closePtr = &VoidMethodStub<ScopeLoserTreeType, &ScopeLoserTreeType::Close>;
            d->m_deletePtr = &DeleteScopeLoserTreeStub<ScopeLoserTreeType>;
            d->m_getNextRowPtr = &GetMethodStub<ScopeLoserTreeType, &ScopeLoserTreeType::GetNextRow>;

            return d.release();
        }

        FORCE_INLINE void Create(ULONG capacity)
        {
            if (m_objectPtr != nullptr)
            {
                Delete();
            }

            m_objectPtr = (*m_createPtr)(capacity);
        }

        FORCE_INLINE void Init(std::vector<std::shared_ptr<SortingBucket<Schema>>> & ops)
        {
            return (*m_initPtr)(m_objectPtr, ops);
        }

        FORCE_INLINE void Close()
        {
            return (*m_closePtr)(m_objectPtr);
        }

        FORCE_INLINE bool GetNextRow(Schema & output)
        {
            return (*m_getNextRowPtr)(m_objectPtr, output);
        }

        FORCE_INLINE void Delete()
        {
            if (m_objectPtr != nullptr)
            {
                (*m_deletePtr)(m_objectPtr);
                m_objectPtr = nullptr;
            }
        }

    private:
        typedef bool(*GetRowStubType)(void*, Schema &);
        typedef void(*VoidMethodType)(void*);
        typedef void(*InitMethodType)(void*, std::vector<std::shared_ptr<SortingBucket<Schema>>> &);
        typedef void* (*CreateMethodType)(ULONG);

        // object pointer
        void                   * m_objectPtr;

        // method pointer
        CreateMethodType         m_createPtr;
        InitMethodType           m_initPtr;
        VoidMethodType           m_closePtr;
        VoidMethodType           m_deletePtr;
        GetRowStubType           m_getNextRowPtr;

        template <class T, bool (T::*TMethod)(Schema &)>
        FORCE_INLINE static bool GetMethodStub(void* object_ptr, Schema & a1)
        {
            T* p = static_cast<T*>(object_ptr);
            return (p->*TMethod)(a1);
        }

        template <class T, void (T::*TMethod)()>
        FORCE_INLINE static void VoidMethodStub(void* object_ptr)
        {
            T* p = static_cast<T*>(object_ptr);
            return (p->*TMethod)();
        }

        template <class T, void (T::*TMethod)(std::vector<std::shared_ptr<SortingBucket<Schema>>> &)>
        FORCE_INLINE static void InitMethodStub(void* object_ptr, std::vector<std::shared_ptr<SortingBucket<Schema>>> & ops)
        {
            T* p = static_cast<T*>(object_ptr);
            return (p->*TMethod)(ops);
        }

        template <class T>
        FORCE_INLINE static void * CreateScopeLoserTreeStub(ULONG capacity)
        {
            return static_cast<void*>(new T(capacity));
        }

        template <class T>
        FORCE_INLINE static void DeleteScopeLoserTreeStub(void* object_ptr)
        {
            T* p = static_cast<T*>(object_ptr);
            delete p;
        }
    };

    //
    // Sorter template
    //
    template<typename OutputSchema>
    class Sorter : public Operator<Sorter<OutputSchema>, OutputSchema, -1>
    {
    public:
        //
        // Following needs to be public for unit testing
        //
        typedef SortingBucket<OutputSchema> SortingBucketType;
        typedef ScopeLoserTreeDelegate<OutputSchema> ScopeLoserTreeDelegateType;
        typedef typename SortingBucketType::SortingMethodType SortingMethodType;

        void ForceThreadPoolInit()
        {
            m_sortingPool.Init();
        }

        //
        // Performs iterative merge of a set of buckets.
        // Stops when all buckets may be merged at once (i.e. all i/o buffers may fit into the available memory)
        // Returns amount of merging iterations
        //
        SIZE_T MergeUntilFitsMemory(std::vector<std::shared_ptr<SortingBucketType>> & onDiskBuckets, LONGLONG availableMemory)
        {
            SIZE_T iterations = 0; // amount of iterations

            LONGLONG totalCost = SumReadCost(onDiskBuckets);

            //  if honor stable sort, order bucket by DESC serial number, first bucket has min serial number
            //  otherwise "buckets" is a DESC priority queue, first bucket has minumum amount of rows
            //  TODO: we can merge these two cases if OrderByRowCountDesc does not bring more benefit
            OrderType order = m_stable ? OrderBySerialNumberDesc : OrderByRowCountDesc;
            BucketCompareFunction functor(order);

            priority_queue<std::shared_ptr<SortingBucketType>, deque<std::shared_ptr<SortingBucketType>>, BucketCompareFunction> buckets(functor);

            for (int idx = 0; idx < onDiskBuckets.size(); ++idx)
            {
                buckets.push(onDiskBuckets[idx]);
            }

            onDiskBuckets.clear(); // clear now to decrement bucket ref coutner

            // split buckets onto two vectors such that at least one of them (or both) fit into the memory
            while (buckets.size() > 2 && totalCost > availableMemory)
            {
                std::vector<std::shared_ptr<SortingBucketType>> mergingBuckets;

                // move two buckets (this is minumum amount of buckets to be merged at each iteration)
                mergingBuckets.push_back(buckets.top()); totalCost -= buckets.top()->RequiresIoMemory(); buckets.pop();
                mergingBuckets.push_back(buckets.top()); totalCost -= buckets.top()->RequiresIoMemory(); buckets.pop();

                // this loop has two exit conditions:
                // 1) "SumReadCost(mergingBuckets) + buckets.top()->RequiresIoMemory()" checks if we can merge more buckets in the current iteration (we cannot exceed m_availableMemory). This is UPPER mergingBuckets limit.
                // 2) "totalCost + MaxReadCost(mergingBuckets)" checks if we can stop moving buckets since after we complete this iteration we will be ready for final merge. This is LOWER mergingBuckets limit.
                while (SumReadCost(mergingBuckets) + buckets.top()->RequiresIoMemory() <= availableMemory &&
                    totalCost + MaxReadCost(mergingBuckets) > availableMemory)
                {
                    mergingBuckets.push_back(buckets.top());
                    totalCost -= buckets.top()->RequiresIoMemory();
                    buckets.pop();
                }

                SIZE_T inMemorySizePreFetch = m_sorterQuotaSize - availableMemory + SumReadCost(mergingBuckets);
                std::shared_ptr<SortingBucketType> bucket = MergeAndDump(mergingBuckets);
                inMemorySizePreFetch += DumpCost();
                m_peakInMemorySizePreFetch = max<SIZE_T>(m_peakInMemorySizePreFetch, inMemorySizePreFetch);
                buckets.push(bucket);
                totalCost += bucket->RequiresIoMemory();

                iterations++;

                SCOPE_LOG_FMT_INFO("Sorter", "%I64u: Re-merged %I64u buckets. Rows in output bucket %I64u", iterations, mergingBuckets.size(), bucket->Size());
            }

            while (!buckets.empty())
            {
                onDiskBuckets.push_back(buckets.top());
                buckets.pop();
            }

            return iterations;
        }

        //
        // Checks if we need to release some memory before loading next bucket.
        // Returns amount of memory that needs to be released, returns zero if freeing memory is not needed.
        //
        LONGLONG CheckMemoryPressure(LONGLONG availableMemory, __int64 multiplier, bool managedMemFull) const
        {
            // do we have enough memory to load next bucket?
            if (availableMemory < x_bucketThreshold || managedMemFull)
            {
                // overall amount of memory occupied by all in-memory buckets
                LONGLONG totalMemoryUsed = m_sorterQuotaSize - availableMemory;

                SCOPE_LOG_FMT_INFO("Sorter", "Low memory: TotalUsed: %I64u, InMemoryBuckets: %I64u, OnDiskBuckets: %I64u", totalMemoryUsed, m_inMemoryBuckets.size(), m_onDiskBuckets.size());

                if (managedMemFull)
                {
                    return totalMemoryUsed;
                }
                else
                {
                    LONGLONG needsMemory = x_bucketThreshold - availableMemory;

                    // we want to avoid producing lots of small buckets in case we constantly hit "low memory"
                    // we do it by increasing amount of memory we free in case of low memory (i.e. increase the value we pass to FreeMemory)
                    // we use m_onDiskBuckets.size() as multiplier value to estimate how much Sorter spills
                    for (int i = 0; i < multiplier && needsMemory < totalMemoryUsed; ++i)
                    {
                        needsMemory += x_bucketThreshold;
                    }

                    return needsMemory;
                }
            }

            return 0;
        }

        static const LONGLONG x_bucketThreshold = 100 * 1024 * 1024; // minimum amount of memory we need to have before loading the bucket

        const LONGLONG m_sorterQuotaSize;
        SIZE_T m_peakInMemorySizeRead;
        SIZE_T m_peakInMemorySizePreFetch;
        SIZE_T m_peakInMemorySizeFetch;

        SIZE_T m_fillNewBucketCount;
        SIZE_T m_mergeBucketCount;
        SIZE_T m_spillBucketCount;
        SIZE_T m_finalBucketCount;
        IncrementalAllocator::Statistics m_spilledStat;
        SIZE_T m_inMemoryBucketsCreated;

    private:

        enum OrderType
        {
            OrderByMemorySizeAsc = 1,
            OrderByRowCountDesc,
            OrderBySerialNumberDesc,
        };

        class BucketCompareFunction
        {

        public:

            BucketCompareFunction(OrderType order)
            {
                switch (order)
                {
                case OrderByMemorySizeAsc:
                    compareFunc = &IsMemorySizeAsc;
                    break;

                case OrderByRowCountDesc:
                    compareFunc = &IsRowCountDesc;
                    break;

                case OrderBySerialNumberDesc:
                    compareFunc = &IsSerialNumberDesc;
                    break;

                default:
                    SCOPE_ASSERT(!"Invalid bucket order type");
                }
            }

            bool operator()(const std::shared_ptr<SortingBucketType>& left, const std::shared_ptr<SortingBucketType>& right) const
            {
                return compareFunc(left, right);
            }

        private:

            static bool IsMemorySizeAsc(const std::shared_ptr<SortingBucketType>& left, const std::shared_ptr<SortingBucketType>& right)
            {
                return left->MemorySize() < right->MemorySize();
            }

            static bool IsRowCountDesc(const std::shared_ptr<SortingBucketType>& left, const std::shared_ptr<SortingBucketType>& right)
            {
                return right->Size() < left->Size();
            }

            static bool IsSerialNumberDesc(const std::shared_ptr<SortingBucketType>& left, const std::shared_ptr<SortingBucketType>& right)
            {
                return right->GetSerialNumber() < left->GetSerialNumber();
            }

            bool(*compareFunc)(const std::shared_ptr<SortingBucketType>& left, const std::shared_ptr<SortingBucketType>& right);

        };

        priority_queue<std::shared_ptr<SortingBucketType>, deque<std::shared_ptr<SortingBucketType>>, BucketCompareFunction> m_inMemoryBuckets;
        std::vector<std::shared_ptr<SortingBucketType>> m_onDiskBuckets;

        // if sorter operator was called many many times (e.g., PrefixSort),
        // it'll encounter the exception - IncrementalAllocator run out of ID.
        // To reuse the sorting bucket, we hold the bucket used before and reuse it in the next call.
        stack<std::shared_ptr<SortingBucketType>> m_sortingBucketPool;

        LONGLONG m_availableMemory; // negative value means overcommit
        static const LONGLONG x_ioBufferSize = 4 * 1 << 20; //4Mb
        static const LONGLONG x_ioBufferCount = 2; // 2 - minimum amount of buffers accepted by Scanner
        static const int x_syncSortingRowCountLimit = 1024;

        enum SorterState
        {
            Initial,
            Merge,
            Fetch,
            Done
        };

        SorterState m_state;

        OperatorDelegate<OutputSchema> * m_child;  // child operator

        std::shared_ptr<ScopeLoserTreeDelegateType> m_merger;

        PrivateThreadPool m_sortingPool; // private thread pool for parallel sorting.

        SortingMethodType m_sortMethod;

        bool m_stable;  // flag indicating whether honor stable sort when doing n-way merge sort

        LONGLONG m_spillTime; // time spent in IO (reading/writing buckets)

        int m_serial;

        int m_reinitCount;

        LONGLONG m_rowCount;

        struct CallBackParam
        {
            PVOID param1;        // call back take two params this contains point to sorter object
            PVOID param2;        // additional param for callback

            CallBackParam(PVOID p1, PVOID p2) :param1(p1), param2(p2)
            {
            }
        };

        std::shared_ptr<SortingBucketType> GetSortingBucket()
        {
            if (m_sortingBucketPool.size() > 0)
            {
                auto sortingBucket = m_sortingBucketPool.top();
                m_sortingBucketPool.pop();
                // to support stable sort, serial number should be incremental.
                sortingBucket->SetSerialNumber(++m_serial);
                return sortingBucket;
            }

            std::shared_ptr<SortingBucketType> bucket(new SortingBucketType(++m_serial));
            ++m_inMemoryBucketsCreated;
            return bucket;
        }

        void PutSortingBucket(std::shared_ptr<SortingBucketType> sortingBucket)
        {
            m_sortingBucketPool.push(sortingBucket);
        }

        //
        // Worker function to sort bucket
        //
        static void SortingCallback(PVOID Param)
        {
            std::unique_ptr<CallBackParam> callParam((CallBackParam*)Param);

            SortingBucketType * bucket = reinterpret_cast<SortingBucketType*>(callParam->param2);

            bucket->SortingPhase((SortingMethodType)(callParam->param1));

            //cout << "Sorted one bucket. Size = "<< bucket->Size() << ". Serial = " << bucket->GetSerialNumber() << endl;
        }

        //
        // Reads and sorts (spills if necessary) input data
        // Returns total amount of rows
        //
        LONGLONG ReadAndSort()
        {
            bool moreRows = true;
            m_rowCount = 0;
            bool managedMemFull = false;

            // Initial the first read.
            if (m_child->End())
            {
                m_child->MoveNext();
            }

            while (moreRows)
            {
                // do we have enough memory to load next bucket?
                if (LONGLONG needsMemory = CheckMemoryPressure(m_availableMemory, m_onDiskBuckets.size(), managedMemFull))
                {
                    // finish all sorting work before starting cleanup
                    m_sortingPool.WaitForAllCallbacks();

                    m_peakInMemorySizeRead = max<SIZE_T>(m_peakInMemorySizeRead, m_sorterQuotaSize - m_availableMemory);

                    FreeMemory(needsMemory);
                }

                // The loading phase will populate bucket sequentially so bucket with smaller serial number
                // will get populated first. The serial number will be used when doing n-way merge to keep
                // sorting is stable cross multiple buckets.
                std::shared_ptr<SortingBucketType> bucket = GetSortingBucket();

                // load bucket
                moreRows = bucket->LoadingPhase(m_child, managedMemFull, m_availableMemory);
                m_availableMemory -= bucket->MemorySize();
                m_rowCount += bucket->Size();

                //cout << "Loaded one bucket. Size = "<< bucket->Size()
                //     << ". MemorySize = " << bucket->MemorySize()
                //     << ". ReadCost = " << bucket->RequiresIoMemory()
                //     << ". Serial = "  << bucket->GetSerialNumber() << endl;

                // store bucket
                if (bucket->Size() > 0)
                {
                    // store bucket
                    m_inMemoryBuckets.push(bucket);
                    m_fillNewBucketCount++;

                    if (bucket->Size() < x_syncSortingRowCountLimit)
                    {
                        bucket->SortingPhase(m_sortMethod);
                    }
                    else
                    {
                        std::unique_ptr<CallBackParam> param(new CallBackParam((PVOID)m_sortMethod, (PVOID)(bucket.get())));

                        // queue bucket sorting work
                        m_sortingPool.QueueUserWorkItem(SortingCallback, (PVOID)param.release());
                    }
                }
            }

            // wait for all outstanding sorting works to complete
            m_sortingPool.WaitForAllCallbacks();

            m_peakInMemorySizeRead = max<SIZE_T>(m_peakInMemorySizeRead, m_sorterQuotaSize - m_availableMemory);
            //cout << "Sorted finished reading data. In memory = " << m_inMemoryBuckets.size() << ". On disk = " << m_onDiskBuckets.size() << endl;

            return m_rowCount;
        }

        //
        // Takes several buckets, merges them and writes single stream.
        // Returns "spilled" bucket to later read the merged data
        //
        std::shared_ptr<SortingBucketType> MergeAndDump(std::vector<std::shared_ptr<SortingBucketType>> & buckets)
        {
            SCOPE_ASSERT(buckets.size() > 1);

            LONGLONG readCost = 0;
            SIZE_T rowCount = 0;
            for (size_t i = 0; i < buckets.size(); ++i)
            {
                rowCount += buckets[i]->Size();
                readCost = max<LONGLONG>(readCost, buckets[i]->RequiresIoMemory());

                buckets[i]->Init();
            }

            // create merger
            std::shared_ptr<ScopeLoserTreeDelegateType> merger(new ScopeLoserTreeDelegateType(*m_merger));
            merger->Create((ULONG)buckets.size());
            merger->Init(buckets);

            // target stream
            string outputStreamName = IOManager::GetTempStreamName();
            stringstream ss;
            ss << "OUTPUT_" << outputStreamName;
            string node = ss.str();

            IOManager::GetGlobal()->AddOutputStream(node, outputStreamName);
            BinaryOutputStream output(node, x_ioBufferSize, x_ioBufferCount);
            output.Init();

            // merge and write rows
            OutputSchema row;
            while (merger->GetNextRow(row))
            {
                BinaryOutputPolicy<OutputSchema>::Serialize(&output, row);
            }

            // close everything
            output.Finish();
            output.Close();
            merger->Close();
            for (size_t i = 0; i < buckets.size(); ++i)
            {
                buckets[i]->Close();

                // Merged buckets are released after MergeAndDump, save their I/O time for statistics
                m_spillTime += buckets[i]->OperatorWaitOnIOTime();
            }

            m_mergeBucketCount += buckets.size();
            m_fillNewBucketCount++;
            m_spillBucketCount++;
            std::shared_ptr<SortingBucketType> bucket(new SortingBucketType(outputStreamName, rowCount, readCost, buckets[0]->GetSerialNumber())); // reuse serial number from one of merged bucket

            return bucket;
        }

        //
        // Tries to free requested amount of memory by spilling buckets
        // Returns actual amount of memory freed
        //
        LONGLONG FreeMemory(LONGLONG amount)
        {
            // find out how many buckets we need to spill to release required amount of memory
            std::vector<std::shared_ptr<SortingBucketType>> buckets;
            LONGLONG released = 0;
            LONGLONG readCost = 0;
            while (released < amount + readCost && !m_inMemoryBuckets.empty())
            {
                // consider cost of reading merged bucket - it's a maximum of a read costs of all merged buckets
                released += m_inMemoryBuckets.top()->MemorySize();
                readCost = max<LONGLONG>(readCost, m_inMemoryBuckets.top()->RequiresIoMemory());

                buckets.push_back(m_inMemoryBuckets.top());
                m_inMemoryBuckets.pop();
            }

            if (!buckets.empty())
            {
                if (Configuration::GetGlobal().GetRestrictOperatorMemorySpilling())
                {
                    throw OperatorOutOfMemoryException(GetOperatorId());
                }

                if (buckets.size() > 1)
                {
                    std::shared_ptr<SortingBucketType> bucket = MergeAndDump(buckets);
                    m_onDiskBuckets.push_back(bucket);

                    // "hard" reset all buckets reclaiming all memory and return buckets to the pool
                    for (int i = 0; i < buckets.size(); ++i)
                    {
                        buckets[i]->Reset(true);
                        PutSortingBucket(buckets[i]);
                    }
                }
                else
                {
                    // just one bucket
                    buckets[0]->SpillingPhase();
                    m_onDiskBuckets.push_back(buckets[0]);
                    m_spillBucketCount++;
                }

                m_availableMemory += released;
#ifndef SCOPE_NO_UDT
                // do a gc collection after release
                GCCollect();
#endif
                SCOPE_LOG_FMT_INFO("Sorter", "Spilled %I64u bucktes. Rleased = %I64u", buckets.size(), released);
            }

            return released;
        }

        //
        // Returns SUM of read costs of all buckets in the container
        //
        LONGLONG SumReadCost(std::vector<std::shared_ptr<SortingBucketType>> & buckets)
        {
            LONGLONG result = 0;
            for (size_t i = 0; i < buckets.size(); i++)
            {
                result += buckets[i]->RequiresIoMemory();
            }
            return result;
        }

        //
        // Returns MAX of read costs of all buckets in the container
        //
        LONGLONG MaxReadCost(std::vector<std::shared_ptr<SortingBucketType>> & buckets)
        {
            LONGLONG result = 0;
            for (size_t i = 0; i < buckets.size(); i++)
            {
                result = max<LONGLONG>(result, buckets[i]->RequiresIoMemory());
            }
            return result;
        }

        //
        // Amount of memory required to dump merged rows to disk
        //
        SIZE_T DumpCost() const
        {
            return x_ioBufferSize * x_ioBufferCount;
        }

        //
        // Prepares outputting sorted data
        //
        SorterState PrepareFetch()
        {
            if (m_spillBucketCount > 0)
            {
                m_peakInMemorySizeRead += DumpCost();
            }

            // find out how much memory we need to read spilled buckets
            LONGLONG readCost = SumReadCost(m_onDiskBuckets);
            if (readCost > m_availableMemory)
            {
                LONGLONG released = FreeMemory(readCost - m_availableMemory);

                if (released < readCost - m_availableMemory)
                {
                    SCOPE_LOG_FMT_INFO("Sorter", "Sorter needs interative merge: %I64u", m_onDiskBuckets.size());

                    SCOPE_ASSERT(m_inMemoryBuckets.empty());

                    // perform iterative merge
                    MergeUntilFitsMemory(m_onDiskBuckets, m_availableMemory);
                }
            }
            m_peakInMemorySizeFetch = m_sorterQuotaSize - m_availableMemory + SumReadCost(m_onDiskBuckets);

            // put all buckets into single array
            while (!m_inMemoryBuckets.empty())
            {
                m_onDiskBuckets.push_back(m_inMemoryBuckets.top());
                m_inMemoryBuckets.pop();
            }

            // init buckets
            for (size_t i = 0; i < m_onDiskBuckets.size(); ++i)
            {
                m_onDiskBuckets[i]->Init();
            }

            m_finalBucketCount = m_onDiskBuckets.size();

            // create merger
            if (m_onDiskBuckets.size() > 1)
            {
                m_merger->Create((ULONG)m_onDiskBuckets.size());
                m_merger->Init(m_onDiskBuckets);
                return Merge;
            }

            return Fetch;
        }

    public:

        // "sortMethod" is the sorting method for inside-bucket sorting, e.g. MKQSort, stdsort or stablesort.
        // "stable" is a flag to indicate whether honor stable sort when n-way merging from buckets. When sortMethod = stablesort, be sure to set stable = true.
        // Need a refactor to fuse these two parameters to avoid mismatching. e.g. MKQSort + true

        Sorter(OperatorDelegate<OutputSchema> *input, SortingMethodType sortMethod, ScopeLoserTreeDelegateType * merger, bool stable, LONGLONG memoryQuota, int operatorId) :
            Operator(operatorId),
            m_child(input),
            m_sorterQuotaSize(memoryQuota),
            m_availableMemory(memoryQuota),
            m_state(Initial),
            m_sortMethod(sortMethod),
            m_inMemoryBuckets(BucketCompareFunction(stable ? OrderBySerialNumberDesc : OrderByMemorySizeAsc)),
            m_stable(stable),
            m_spillTime(0),
            m_serial(0),
            m_peakInMemorySizeRead(0),
            m_peakInMemorySizePreFetch(0),
            m_peakInMemorySizeFetch(0),
            m_fillNewBucketCount(0),
            m_mergeBucketCount(0),
            m_spillBucketCount(0),
            m_finalBucketCount(0),
            m_inMemoryBucketsCreated(0),
            m_reinitCount(0),
            m_rowCount(-1),
            m_sortingPool(true)
        {
            m_sortingPool.SetThreadpoolMax(12);
            m_sortingPool.SetThreadpoolMin(4);
            m_merger.reset(merger);
        }

        ~Sorter()
        {
            // in case of exception stop threads before cleaning objects
            m_sortingPool.CancelAllCallbacks();
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        LONGLONG TotalRowCount()
        {
            // make sure it's called after ReadAndSort
            SCOPE_ASSERT(m_state != Initial);
            return m_rowCount;
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            while (m_state != Done)
            {
                switch (m_state)
                {
                case Initial:
                    ReadAndSort();

                    if (m_inMemoryBuckets.empty() && m_onDiskBuckets.empty())
                    {
                        m_state = Done;
                    }
                    else
                    {
                        m_state = PrepareFetch();
                    }
                    break;

                case Fetch:
                    if (m_onDiskBuckets[0]->GetNextRow(output))
                    {
                        stats.IncreaseRowCount(1);
                        return true;
                    }
                    m_state = Done;
                    break;

                case Merge:
                    if (m_merger->GetNextRow(output))
                    {
                        stats.IncreaseRowCount(1);
                        return true;
                    }
                    m_state = Done;
                    break;

                default:
                    SCOPE_ASSERT(!"Sorter: illegal state");
                }
            }

            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            SCOPE_ASSERT(m_inMemoryBuckets.empty());

            for (size_t i = 0; i < m_onDiskBuckets.size(); ++i)
            {
                // since the last merge contains all rows anyway we don't need statistics from intermediate merges
                m_onDiskBuckets[i]->AggregateToOuterMemoryStatistics(m_spilledStat);
                m_onDiskBuckets[i]->Close();
                m_spillTime += m_onDiskBuckets[i]->OperatorWaitOnIOTime();
            }

            while (m_sortingBucketPool.size() > 0)
            {
                auto sortingBucket = m_sortingBucketPool.top();
                m_sortingBucketPool.pop();
                sortingBucket->Close();
            }

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("Sort");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond() - m_spillTime);
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_spillTime);
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            node.AddAttribute(RuntimeStats::MaxPeakInMemorySizeRead(), m_peakInMemorySizeRead);
            node.AddAttribute(RuntimeStats::AvgPeakInMemorySizeRead(), m_peakInMemorySizeRead);
            node.AddAttribute(RuntimeStats::MaxPeakInMemorySizePreFetch(), m_peakInMemorySizePreFetch);
            node.AddAttribute(RuntimeStats::AvgPeakInMemorySizePreFetch(), m_peakInMemorySizePreFetch);
            node.AddAttribute(RuntimeStats::MaxPeakInMemorySizeFetch(), m_peakInMemorySizeFetch);
            node.AddAttribute(RuntimeStats::AvgPeakInMemorySizeFetch(), m_peakInMemorySizeFetch);

            node.AddAttribute(RuntimeStats::MaxFillNewBucketCount(), m_fillNewBucketCount);
            node.AddAttribute(RuntimeStats::AvgFillNewBucketCount(), m_fillNewBucketCount);
            node.AddAttribute(RuntimeStats::MaxMergeBucketCount(), m_mergeBucketCount);
            node.AddAttribute(RuntimeStats::AvgMergeBucketCount(), m_mergeBucketCount);
            node.AddAttribute(RuntimeStats::MaxSpillBucketCount(), m_spillBucketCount);
            node.AddAttribute(RuntimeStats::AvgSpillBucketCount(), m_spillBucketCount);
            node.AddAttribute(RuntimeStats::MaxFinalBucketCount(), m_finalBucketCount);
            node.AddAttribute(RuntimeStats::AvgFinalBucketCount(), m_finalBucketCount);

            node.AddAttribute(RuntimeStats::MaxNewInMemoryBucketCount(), m_inMemoryBucketsCreated);

            if (!m_spilledStat.IsEmpty())
            {
                m_spilledStat.WriteRuntimeStats(node, sizeof(OutputSchema));
            }

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements().AddMemoryInRows(OperatorRequirementsConstants::Sorter_Row_MinMemory)
                .AddMemoryInRows(OperatorRequirementsConstants::RowBuffer_Sorter__Row_MinMemory)
                .AddMemory(0, OperatorRequirementsConstants::RowBuffer_Sorter__Size_OptimalMemory)
                .AddMemoryForOutputUStreams(OperatorRequirementsConstants::RowBuffer_Sorter__Count_OutputUStream, x_ioBufferSize, x_ioBufferCount);
        }

        void Reinit()
        {
            AutoExecStats stats(this);
            ++m_reinitCount;
            SCOPE_ASSERT(m_inMemoryBuckets.empty());
            // Supposely, we won't call Reinit unless it's in Done state
            SCOPE_ASSERT(m_state == Done);
            for (size_t i = 0; i < m_onDiskBuckets.size(); ++i)
            {
                if (m_onDiskBuckets[i]->MemorySize() > 0)
                {
                    m_onDiskBuckets[i]->Reset(false);
                    PutSortingBucket(m_onDiskBuckets[i]);
                }
                m_spillTime += m_onDiskBuckets[i]->OperatorWaitOnIOTime();
            }
            m_onDiskBuckets.clear();
            m_state = Initial;

            // Since we haven't reclaimed all memory from the buckets just put in bucket pool,
            // we can still have some amount of memory allocated there:
            // (COMMIT_BLOCK_SIZE * AmortizeMemoryAllocationPolicy::x_reservedPageCnt) * BucketsCount
            // This memory will be used by Sorter when loading buckets on the next iteration,
            // but it's not tracked anywhere and my lead to (minor) Soter memory quota excess.
            m_availableMemory = m_sorterQuotaSize;
        }
    };

    ///
    /// PrefixSorter template
    ///
    template<typename OutputSchema, typename KeyPrefixPolicy>
    class PrefixSorter : public Operator<PrefixSorter<OutputSchema, KeyPrefixPolicy>, OutputSchema, -1>
    {
    public:
        typedef ScopeLoserTreeDelegate<OutputSchema> ScopeLoserTreeDelegateType;
        typedef SortingBucket<OutputSchema> SortingBucketType;
        typedef typename SortingBucketType::SortingMethodType SortingMethodType;

    private:
        // it'll will iterate all the rows with the same key prefix
        class KeyPrefixIterator : public Operator<KeyPrefixIterator, OutputSchema, -1>
        {
        private:
            OperatorDelegate<OutputSchema>* m_pChild;
            typename KeyPrefixPolicy::KeyType m_keyPrefix;
            RowEntityAllocator m_allocator; // for deep-copying of the key (if key tracking is requested)
            bool m_isFirstRow;
            bool m_hasMoreRows;
            LONGLONG m_keyCount;

        public:
            KeyPrefixIterator(OperatorDelegate<OutputSchema>* pChild) :
                Operator(NO_OPERATOR_ID),
                m_pChild(pChild),
                m_allocator(Configuration::GetGlobal().GetMaxKeySize(), "KeyPrefixIterator", RowEntityAllocator::KeyContent),
                m_isFirstRow(true),
                m_hasMoreRows(true),
                m_keyCount(0)
            {
            }

            bool HasMoreRows()
            {
                return m_hasMoreRows;
            }

            void InitImpl()
            {
                AutoExecStats stats(this);

                m_pChild->Init();
            }

            PartitionMetadata * GetMetadataImpl()
            {
                AutoExecStats stats(this);

                return m_pChild->GetMetadata();
            }

            bool GetNextRowImpl(OutputSchema & output)
            {
                AutoExecStats stats(this);

                m_hasMoreRows = m_pChild->GetNextRow(output);

                if (m_isFirstRow && m_hasMoreRows)
                {
                    m_allocator.Reset();
                    new ((char*)&m_keyPrefix) KeyPrefixPolicy::KeyType(output, &m_allocator);

                    m_isFirstRow = false;
                    stats.IncreaseRowCount(1);
                    m_keyCount = 1;
                    return true;
                }

                if (!m_hasMoreRows)
                {
                    return false;
                }

                stats.IncreaseRowCount(1);
                int compareResult = KeyPrefixPolicy::Compare(output, m_keyPrefix);

                if (compareResult < 0)
                {
#ifdef SCOPE_DEBUG
                    cout << "Current row: " << endl;
                    cout << output << endl;
                    cout << "Previous key: " << endl;
                    cout << m_keyPrefix << endl;
#endif
                    throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Input data for the PrefixSorter is not sorted");
                }

                bool keyChanged = compareResult != 0;

                if (keyChanged)
                {
                    m_allocator.Reset();
                    new ((char*)&m_keyPrefix) KeyPrefixPolicy::KeyType(output, &m_allocator);
                    ++m_keyCount;
                }

                return !keyChanged;
            }

            void CloseImpl()
            {
                AutoExecStats stats(this);
                m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
                m_pChild->Close();
            }

            void WriteRuntimeStatsImpl(TreeNode & root)
            {
                auto & node = root.AddElement("KeyPrefixIterator");

                node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
                node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_pChild->GetInclusiveTimeMillisecond());
                RuntimeStats::WriteRowCount(node, GetRowCount());
                RuntimeStats::WriteKeyCount(node, m_keyCount);

                m_allocator.WriteRuntimeStats(node, sizeof(KeyPrefixPolicy::KeyType));

                m_pChild->WriteRuntimeStats(node);
            }
        };

    private:
        KeyPrefixIterator m_keyPrefixIterator;
        std::unique_ptr<OperatorDelegate<OutputSchema>> m_delegateKeyPrefixIteratorPtr;
        std::unique_ptr<Sorter<OutputSchema>> m_sorterPtr;

    public:
        PrefixSorter(OperatorDelegate<OutputSchema>* pInput, SortingMethodType sortMethod, ScopeLoserTreeDelegateType* pMerger, bool isStable, LONGLONG memoryQuota, int operatorId) :
            Operator(operatorId),
            m_keyPrefixIterator(pInput)
        {
            m_delegateKeyPrefixIteratorPtr.reset(new OperatorDelegate<OutputSchema>(OperatorDelegate<OutputSchema>::FromOperator(&m_keyPrefixIterator)));
            m_sorterPtr.reset(new Sorter<OutputSchema>(m_delegateKeyPrefixIteratorPtr.get(), sortMethod, pMerger, isStable, memoryQuota, operatorId));
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_sorterPtr->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_sorterPtr->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);
            while (true)
            {
                bool moreRows = m_sorterPtr->GetNextRowImpl(output);
                if (moreRows)
                {
                    stats.IncreaseRowCount(1);
                    return true;
                }

                if (!m_keyPrefixIterator.HasMoreRows())
                {
                    break;
                }

                // reinitialize the sorter, and it'll load the following the data from the input.
                m_sorterPtr->Reinit();

                // the current the row is a valid the row
                // ReloadCurrentRow will set moreRows flag
                // or it'll be skipped by the following reading first row logic
                // if (End())
                //    MoveNext();
                m_delegateKeyPrefixIteratorPtr->ReloadCurrentRow();
            }

            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);
            m_sorterPtr->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("PrefixSort");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_sorterPtr->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_sorterPtr->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements().AddMemoryInRows(OperatorRequirementsConstants::PrefixSorter_Row_MinMemory)
                .AddMemoryInRows(OperatorRequirementsConstants::RowBuffer_Sorter__Row_MinMemory)
                .AddMemory(0, OperatorRequirementsConstants::RowBuffer_Sorter__Size_OptimalMemory)
                .AddMemoryForOutputUStreams(OperatorRequirementsConstants::RowBuffer_Sorter__Count_OutputUStream, x_ioBufferSize, x_ioBufferCount);
        }
    };

    ///
    /// UnionAllSorter template
    ///
    template<typename OutputSchema>
    class UnionAllSorter : public Operator<UnionAllSorter<OutputSchema>, OutputSchema, -1>
    {
        typedef UnionAllSorter<OutputSchema> SorterType;
        typedef SortingBucket<OutputSchema> SortingBucketType;
        typedef ScopeLoserTreeDelegate<OutputSchema> ScopeLoserTreeDelegateType;
        typedef typename SortingBucketType::SortingMethodType SortingMethodType;
        typedef OperatorDelegate<OutputSchema> InputOperator;

        InputOperator ** m_children;  // Array of child operator
        ULONG            m_count;     // number of child operator

        std::vector<std::shared_ptr<SortingBucketType>>  m_sortingBuckets;
        CRITICAL_SECTION                                 m_cs;

        std::shared_ptr<ScopeLoserTreeDelegateType> m_merger;

        SortingMethodType m_sortMethod;

        enum GetRowState
        {
            UnInit,
            Fetch,
            NoMergerFetch,
            Finished,
        };

        GetRowState     m_state;

        PrivateThreadPool m_loadingPool; // private thread pool for parallel loading.
        PrivateThreadPool m_sortingPool; // private thread pool for parallel sorting.

        static const int x_bucketSize = 500000;

        struct CallBackParam
        {
            PVOID param1;        // call back take two params this contains point to sorter object
            PVOID param2;        // additional param for callback

            CallBackParam(PVOID p1, PVOID p2) :param1(p1), param2(p2)
            {
            }
        };


        static void LoadingCallback(PVOID Param)
        {
            std::unique_ptr<CallBackParam> callParam((CallBackParam*)Param);
            SorterType * sorter = (SorterType *)(callParam->param1);
            InputOperator * input = (InputOperator *)(callParam->param2);

            // We do delay init to throttle the memory usage.
            // Otherwise, all the memory buffer will gets created upfront and cause problems.
            input->Init();

            // Initial the first read.
            if (input->End())
            {
                input->MoveNext();
            }

            bool moreRows = true;
            bool managedMemFull = false;

            int serial = 0;
            do
            {
                std::unique_ptr<SortingBucketType> bucket(new SortingBucketType(++serial));

                moreRows = bucket->LoadingPhase(input, managedMemFull);

                SCOPE_LOG_FMT_INFO("Sorter", "Loaded one bucket. size = %I64u. Serial = %I64u", bucket->Size(), bucket->GetSerialNumber());

                std::unique_ptr<CallBackParam> param(new CallBackParam((PVOID)sorter, (PVOID)(bucket.release())));

                sorter->m_sortingPool.QueueUserWorkItem(SortingCallback, (PVOID)param.release());
            } while (moreRows);
        }

        static void SortingCallback(PVOID Param)
        {
            std::unique_ptr<CallBackParam> callParam((CallBackParam*)Param);
            SorterType * sorter = (SorterType *)(callParam->param1);
            std::shared_ptr<SortingBucketType> bucket;

            bucket.reset((SortingBucketType*)(callParam->param2));

            bucket->SortingPhase(sorter->m_sortMethod);

            SCOPE_LOG_FMT_INFO("Sorter", "Loaded one bucket. size = %I64u. Serial = %I64u", bucket->Size(), bucket->GetSerialNumber());

            sorter->AddBucket(bucket);

            return;
        }

        void AddBucket(std::shared_ptr<SortingBucketType> & bucket)
        {
            AutoCriticalSection aCS(&m_cs);

            m_sortingBuckets.push_back(bucket);
        }

    public:
        UnionAllSorter(InputOperator ** inputs, ULONG count, SortingMethodType sortMethod, ScopeLoserTreeDelegateType * merger, int operatorId) :
            Operator(operatorId),
            m_children(inputs),
            m_count(count),
            m_state(UnInit),
            m_loadingPool(true),
            m_sortingPool(true)
        {
            // Initialize the critical section one time only.
            InitializeCriticalSection(&m_cs);

            m_merger.reset(merger);
            m_sortMethod = sortMethod;
        }

        ~UnionAllSorter()
        {
            m_loadingPool.CancelAllCallbacks();
            m_sortingPool.CancelAllCallbacks();

            // Release resources used by the critical section object.
            DeleteCriticalSection(&m_cs);
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_loadingPool.SetThreadpoolMax(8);
            m_loadingPool.SetThreadpoolMin(4);
            m_sortingPool.SetThreadpoolMax(12);
            m_sortingPool.SetThreadpoolMin(4);
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return PartitionMetadata::MergeMetadata(m_children, m_count);
        }

        /// GetRow Implmentation for combiner. The combiner expected the rows get from left and right child are sorted on joining key.
        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            for (;;)
            {
                switch (m_state)
                {
                case UnInit:
                {
                               // Initial phase
                               // Read input into buffer and sort.
                               // Overflow to disk if necessary
                               for (ULONG i = 0; i < m_count; i++)
                               {
                                   std::unique_ptr<CallBackParam> param(new CallBackParam((PVOID)this, (PVOID)(m_children[i])));

                                   m_loadingPool.QueueUserWorkItem(LoadingCallback, (PVOID)param.release());
                               }

                               // wait for all task to finish
                               m_loadingPool.WaitForAllCallbacks();
                               m_sortingPool.WaitForAllCallbacks();

                               if (m_sortingBuckets.size() > 1)
                               {
                                   m_merger->Create((ULONG)m_sortingBuckets.size());
                                   m_merger->Init(m_sortingBuckets);

                                   m_state = Fetch;
                               }
                               else if (m_sortingBuckets.size() == 1)
                               {
                                   m_state = NoMergerFetch;
                               }
                               else
                               {
                                   // there is no row.
                                   return false;
                               }

                               break;
                }

                case Fetch:
                {
                              if (m_merger->GetNextRow(output))
                              {
                                  stats.IncreaseRowCount(1);
                                  return true;
                              }

                              return false;
                }

                case NoMergerFetch:
                {
                                      if (m_sortingBuckets[0]->GetNextRow(output))
                                      {
                                          stats.IncreaseRowCount(1);
                                          return true;
                                      }

                                      return false;
                }
                default:
                    SCOPE_ASSERT(!"invalid default state for combiner");
                    return false;
                }
            }
        }

        // Release all resources of children
        void CloseImpl()
        {
            AutoExecStats stats(this);

            for (ULONG i = 0; i< m_count; i++)
                m_children[i]->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("UnionAllSort");

            IncrementalAllocator::Statistics spilledStat;
            for (size_t i = 0; i < m_sortingBuckets.size(); ++i)
            {
                // since the last merge contains all rows anyway we don't need statistics from intermediate merges
                m_sortingBuckets[i]->AggregateToOuterMemoryStatistics(spilledStat);
            }
            if (!spilledStat.IsEmpty())
            {
                spilledStat.WriteRuntimeStats(node);
            }

            LONGLONG maxChildInclusiveTime = 0;
            node.AddAttribute(RuntimeStats::MaxInputCount(), m_count);
            node.AddAttribute(RuntimeStats::AvgInputCount(), m_count);
            for (SIZE_T i = 0; i < m_count; i++)
            {
                maxChildInclusiveTime = std::max<LONGLONG>(maxChildInclusiveTime, m_children[i]->GetInclusiveTimeMillisecond());
                m_children[i]->WriteRuntimeStats(node);
            }

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - maxChildInclusiveTime);
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }
        }
    };

    //Sorting template for using std::sort
    template <class T, bool inlineRow = false>
    class StdSortLessPolicy
    {
    public:
        typedef T* RowType;

        // Compare function for std::sort
        template<class KeyPolicy>
        static bool Less(T* & n1, T* & n2)
        {
            return KeyPolicy::Compare(n1, n2) < 0;
        }
    };

    //Sorting template for using std::sort
    template <class T>
    class StdSortLessPolicy<T, true>
    {
    public:
        typedef T RowType;

        // Compare function for std::sort
        template<class KeyPolicy>
        static bool Less(T & n1, T & n2)
        {
            return KeyPolicy::Compare(&n1, &n2) < 0;
        }
    };

    //Sorting template for using std::sort
    template <class T>
    class StdSort
    {
    public:
        template<class KeyPolicy, bool inlineRow>
        static void Sort(typename StdSortLessPolicy<T, inlineRow>::RowType * begin, SIZE_T N)
        {
            //Using std::sort
            sort(begin, begin + N, StdSortLessPolicy<T, inlineRow>::Less<KeyPolicy>);
        }
    };


    //Sorting template for using std::stable_sort
    template <class T, bool inlineRow = false>
    class StableSortLessPolicy
    {
    public:
        typedef T* RowType;

        // Compare function for std::stable_sort
        template<class KeyPolicy>
        static bool Less(T* const n1, T* const n2)
        {
            return KeyPolicy::Compare(n1, n2) < 0;
        }
    };

    //Sorting template for using std::stable_sort
    template <class T>
    class StableSortLessPolicy<T, true>
    {
    public:
        typedef T RowType;

        // Compare function for std::stable_sort
        template<class KeyPolicy>
        static bool Less(const T & n1, const T & n2)
        {
            return KeyPolicy::Compare((T*)&n1, (T*)&n2) < 0;
        }
    };

    //Sorting template for using std::stable_sort
    template <class T>
    class StableSort
    {
    public:
        template<class KeyPolicy, bool inlineRow>
        static void Sort(typename StableSortLessPolicy<T, inlineRow>::RowType * begin, SIZE_T N)
        {
            //Using std::stable_sort
            stable_sort(begin, begin + N, StableSortLessPolicy<T, inlineRow>::Less<KeyPolicy>);
        }
    };

    ///
    /// SampleCollector operator template
    ///
    template<typename InputOperator, typename InputSchema, typename OutputSchema, typename SampleSchema, int UID = -1>
    class SampleCollector : public Operator<SampleCollector<InputOperator, InputSchema, OutputSchema, SampleSchema, UID>, typename OutputSchema, UID>
    {
        // Input and Output schemas are the same
        typedef KeyComparePolicy<InputSchema, UID> KeyPolicy;

        InputOperator* m_child;
        BinaryOutputStream m_sampleOutput;

        ULONGLONG m_rowCount;
        int m_maxSamples;
        int m_selectedCount;
        std::vector<std::shared_ptr<SampleSchema>> m_samples;
        IncrementalAllocator m_allocator[2];
        int m_currentAllocator;
        ScopeRandom m_random;

        static const int c_defaultMaxSamples = 4000; //it's same value with managed runtime
    public:
        SampleCollector(InputOperator* input, const std::string & sampleOutputName, int operatorId, int numSamples = c_defaultMaxSamples) :
            Operator(operatorId),
            m_child(input),
            m_sampleOutput(sampleOutputName, IOManager::x_defaultOutputBufSize, IOManager::x_defaultOutputBufCount),
            m_maxSamples(numSamples),
            m_rowCount(0),
            m_selectedCount(0),
            m_currentAllocator(0),
            m_random(0) //setting the seed to 0 to make it to be deterministic
        {
            m_allocator[0].Init(Configuration::GetGlobal().GetSampleMaxMemorySize(), "SampleCollector_Row0");
            m_allocator[1].Init(Configuration::GetGlobal().GetSampleMaxMemorySize(), "SampleCollector_Row1");
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_sampleOutput.Init();

            m_child->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & outputRow)
        {
            AutoExecStats stats(this);

            if (m_child->GetNextRow(outputRow))
            {
                stats.IncreaseRowCount(1);
                m_rowCount++;

                SampleSchema sample;
                RowTransformPolicy<InputSchema, SampleSchema, UID>::FilterTransformRow(outputRow, sample, nullptr);
                UpdateSamples(sample);
                return true;
            }

            for (int i = 0; i < m_samples.size(); i++)
            {
                BinaryOutputPolicy<SampleSchema>::Serialize(&m_sampleOutput, *m_samples[i]);
            }

            m_sampleOutput.Finish();
            return false;
        }

        /// Note! the sample algorithm must be deterministic, otherwise the results will be violated if
        /// the vertex rerun due to some failure.
        ///
        /// http://en.wikipedia.org/wiki/Reservoir_sampling
        /// Fill up the 'reservoir' with the first k items from S
        /// For every item S[j] where j > k:
        /// Choose an integer r between 0 and j
        /// If r is less than k then replace element r in the reservoir with S[j]
        void UpdateSamples(const SampleSchema& sample)
        {
            if (m_rowCount <= m_maxSamples)
            {
                ++m_selectedCount;
                std::shared_ptr<SampleSchema> row = make_shared<SampleSchema>(sample, &m_allocator[m_currentAllocator]);
                m_samples.push_back(row);
                return;
            }

            int r = (int)(m_random.Next() % m_rowCount);

            // r could be less than 0 if m_rowCount is larger than int.MaxValue.
            if (r >= m_maxSamples || r < 0)
            {
                return;
            }

            ++m_selectedCount;
            if (m_selectedCount % m_maxSamples == 0)
            {
                // there are much garbage in the current allocator
                int nextAllocator = (m_currentAllocator + 1) % 2;
                for (int i = 0; i < m_samples.size(); i++)
                {
                    m_samples[i] = make_shared<SampleSchema>(*m_samples[i], &m_allocator[nextAllocator]);
                }
                m_allocator[m_currentAllocator].Reset();
                m_currentAllocator = nextAllocator;
            }

            m_samples[r] = make_shared<SampleSchema>(sample, &m_allocator[m_currentAllocator]);
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_sampleOutput.Close();

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("SampleCollect");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_sampleOutput.GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_allocator[0].WriteRuntimeStats(node);
            m_allocator[1].WriteRuntimeStats(node);
            m_sampleOutput.WriteRuntimeStats(node);

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements(Configuration::GetGlobal().GetSampleMaxMemorySize() * OperatorRequirementsConstants::HistogramCollector__ConstSampleMaxMemory_MinMemory)
                .AddMemoryForOutputUStreams(OperatorRequirementsConstants::SampleCollector__Count_OutputUStream,
                IOManager::x_defaultOutputBufSize,
                IOManager::x_defaultOutputBufCount);
        }
    };

    ///
    /// HistogramCollector operator template.
    ///
    template<typename InputOperator, typename InputSchema, typename OutputSchema, typename HistogramSchema, int UID = -1>
    class HistogramCollector : public Operator<HistogramCollector<InputOperator, InputSchema, OutputSchema, HistogramSchema, UID>, typename OutputSchema, UID>
    {
        // Input and Output schemas are the same
        typedef KeyComparePolicy<InputSchema, UID> KeyPolicy;
        typedef typename KeyPolicy::KeyType KeySchema;

        InputOperator * m_child;        // upstream operator
        BinaryOutputStream m_histogramOutput; // histogram goes here

        ULONGLONG m_rowCount;
        int       m_maxBuckets;

        bool      m_rowSizeBasedSampling;

        LONGLONG  m_minRowsInBucket;

        ULONGLONG m_dataSize;
        LONGLONG  m_minDataSizeInBucket;
        static const int c_maxRowCountThreshold = 2;
        DummyOutputStream m_dummyOutput; // use it calcuate the row size.

        //
        // This number guarantees that each node produces MIN(RowCount / c_initialBucketThreshold, [c_defaultMaxBuckets / 2 .. c_defaultMaxBuckets]) intervals
        // HistogramCoordinator later merges them into desired number of partitions
        //
        static const int c_defaultMaxBuckets = 8192;

        //
        // This is the amount of rows we initially put into each bucket
        //
        static const int c_initialRowCountBucketThreshold = 33;

        //
        // This is the amount of data size we initially put into each bucket, estimated by 32M/c_defaultMaxBuckets.
        //
        static const int c_initialDataSizeBucketThreshold = 4 * 1024;

        double m_bucketMergeParamter;

        // Declare operator to avoid warning C4512: 'ScopeEngine::HistogramCollector<InputOperator,HistogramSchema>' : assignment operator could not be generated
        HistogramCollector & operator=(const HistogramCollector & from);

    public:
        HistogramCollector(InputOperator * input, const std::string & histogramOutputName, int operatorId, bool rowSizeBasedSampling = false, int numPartitions = c_defaultMaxBuckets) :
            Operator(operatorId),
            m_child(input),
            m_histogramOutput(histogramOutputName, IOManager::x_defaultOutputBufSize, IOManager::x_defaultOutputBufCount),
            m_rowCount(0),
            m_currentBucket(-1), // Initial value must be -1 for correct processing of zero size input
            m_rowSizeBasedSampling(rowSizeBasedSampling),
            m_dataSize(0),
            m_currentAllocator(0)
        {
            m_maxBuckets = m_rowSizeBasedSampling ? numPartitions * 2 : numPartitions;

            if (m_maxBuckets % 2 != 0)
            {
                throw std::string("Number of max buckets in histogram must be an even number");
            }

            m_bucketMergeParamter = 1.1;

            m_minRowsInBucket = c_initialRowCountBucketThreshold;
            m_minDataSizeInBucket = c_initialDataSizeBucketThreshold;

            m_boundaryBuffer.reset(new char[sizeof(KeySchema)* m_maxBuckets]);
            m_boundaries = (KeySchema*)m_boundaryBuffer.get();
            m_rowsInBucket.reset(new LONGLONG[m_maxBuckets]);
            m_dataSizeInBucket.reset(new LONGLONG[m_maxBuckets]);

            m_rowAllocator[0].reset(new RowEntityAllocator(Configuration::GetGlobal().GetHistogramMaxMemorySize(), "HistogramCollector_Row0", RowEntityAllocator::KeyContent));
            m_rowAllocator[1].reset(new RowEntityAllocator(Configuration::GetGlobal().GetHistogramMaxMemorySize(), "HistogramCollector_Row1", RowEntityAllocator::KeyContent));
            m_boundaryAllocator.reset(new RowEntityAllocator(Configuration::GetGlobal().GetMaxKeySize(), "HistogramCollector_Boundary", RowEntityAllocator::KeyContent));
            m_keyAllocator.reset(new RowEntityAllocator(Configuration::GetGlobal().GetMaxKeySize(), "HistogramCollector_Key", RowEntityAllocator::KeyContent));
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_histogramOutput.Init();

            m_child->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & outputRow)
        {
            AutoExecStats stats(this);

            if (m_child->GetNextRow(outputRow))
            {
                stats.IncreaseRowCount(1);

                UpdateHistogram(outputRow);

                m_rowCount++;

                return true;
            }

            //
            // Write histogram
            //
            // - first row is merely used to pass total amount of rows so bottom and top boundaries are assined with empty value
            // - for each bucket current histogram merging operator uses only BOTTOM part of the bucket so high part is assigned with "empty" value always
            //

            KeySchema dummy;

            // first row is used to tell amount of rows
            HistogramSchema headerRow;
            if (m_rowSizeBasedSampling)
            {
                BucketRowPolicy<InputSchema, HistogramSchema, UID>::MakeBucketRow(&dummy, &dummy, m_dataSize, m_rowCount, &headerRow);
                BinaryOutputPolicy<HistogramSchema>::Serialize(&m_histogramOutput, headerRow);

                // histogram buckets
                for (int i = 0; i <= m_currentBucket; ++i)
                {
                    HistogramSchema intervalRow;
                    BucketRowPolicy<InputSchema, HistogramSchema, UID>::MakeBucketRow(i == 0 ? &m_leftBoundary : &m_boundaries[i - 1], &dummy, m_dataSizeInBucket[i], m_rowsInBucket[i], &intervalRow);
                    BinaryOutputPolicy<HistogramSchema>::Serialize(&m_histogramOutput, intervalRow);
                }
            }
            else
            {
                BucketRowPolicy<InputSchema, HistogramSchema, UID>::MakeBucketRow(&dummy, &dummy, 0L, m_rowCount, &headerRow);
                BinaryOutputPolicy<HistogramSchema>::Serialize(&m_histogramOutput, headerRow);

                // histogram buckets
                for (int i = 0; i <= m_currentBucket; ++i)
                {
                    HistogramSchema intervalRow;
                    BucketRowPolicy<InputSchema, HistogramSchema, UID>::MakeBucketRow(i == 0 ? &m_leftBoundary : &m_boundaries[i - 1], &dummy, 0L, m_rowsInBucket[i], &intervalRow);
                    BinaryOutputPolicy<HistogramSchema>::Serialize(&m_histogramOutput, intervalRow);
                }
            }

            // flush all remaining bytes from buffer.
            m_histogramOutput.Finish();

            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_histogramOutput.Close();
            m_rowAllocator[0]->Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            m_rowAllocator[1]->Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            m_boundaryAllocator->Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            m_keyAllocator->Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("HistogramCollect");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_histogramOutput.GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_rowAllocator[0]->WriteRuntimeStats(node, sizeof(KeySchema));
            m_rowAllocator[1]->WriteRuntimeStats(node, sizeof(KeySchema));
            m_boundaryAllocator->WriteRuntimeStats(node, sizeof(KeySchema));
            m_keyAllocator->WriteRuntimeStats(node, sizeof(KeySchema));
            m_histogramOutput.WriteRuntimeStats(node);

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements(Configuration::GetGlobal().GetHistogramMaxMemorySize() * OperatorRequirementsConstants::HistogramCollector__ConstHistogramMaxMemory_MinMemory)
                .AddMemoryInRows(OperatorRequirementsConstants::HistogramCollector__Row_MinMemory)
                .AddMemoryForOutputUStreams(OperatorRequirementsConstants::HistogramCollector__Count_OutputStream,
                IOManager::x_defaultOutputBufSize,
                IOManager::x_defaultOutputBufCount);
        }

    private:
        int MergeBucketByDataSize(bool useAvgDataSize = false)
        {
            int nextAllocator = (m_currentAllocator + 1) % 2;

            m_minDataSizeInBucket *= 2;

            if (useAvgDataSize)
            {
                LONGLONG avgDataSize = m_dataSize / (m_maxBuckets / 2);
                if (m_minDataSizeInBucket < avgDataSize)
                {
                    m_minDataSizeInBucket = avgDataSize;
                }
            }

            int index = 0;
            for (int start = 0; start <= m_currentBucket;)
            {
                LONGLONG curDataSize = m_dataSizeInBucket[start];
                LONGLONG curRowCount = m_rowsInBucket[start];
                int end = start + 1;
                while (end <= m_currentBucket && ((curDataSize + m_dataSizeInBucket[end]) < m_minDataSizeInBucket * m_bucketMergeParamter))
                {
                    curDataSize += m_dataSizeInBucket[end];
                    curRowCount += m_rowsInBucket[end];
                    end++;
                }

                new ((char*)&m_boundaries[index]) KeySchema(m_boundaries[end - 1], m_rowAllocator[nextAllocator].get());
                m_dataSizeInBucket[index] = curDataSize;
                m_rowsInBucket[index] = curRowCount;
                index++;

                start = end;
            }

            m_rowAllocator[m_currentAllocator]->Reset();
            m_currentAllocator = nextAllocator;

            return index;
        }

        void OpenBucketAndMerge(InputSchema & row)
        {
            // save bucket boundary
            try
            {
                new (&m_boundaries[m_currentBucket]) KeySchema(row, m_rowAllocator[m_currentAllocator].get());
            }
            catch (RuntimeMemoryException &)
            {
                // Throw more meaningful exception in case all the ranges do not fit into the allocator
                throw RuntimeException(E_USER_OUT_OF_MEMORY, "Not enough memory to evaluate histogram");
            }

            if (m_currentBucket < m_maxBuckets - 1)
            {
                ++m_currentBucket;
            }
            else
            {
                int index = MergeBucketByDataSize();
                while (index > m_currentBucket)
                {
                    index = MergeBucketByDataSize();
                }

                m_currentBucket = index;
            }

            m_rowsInBucket[m_currentBucket] = 0;
            m_dataSizeInBucket[m_currentBucket] = 0;
        }

        ///
        /// Iteratively builds histogram until all rows are processed.
        /// For each bucket left (low) boundary is inclusive, while right (high) is exclusive
        /// i.e. row goes into this bucket if it belongs to [low..high)
        /// low == -infinity for the left bucket and high == +infinity for the right bucket
        /// In thiese tow cases inclusion/exclusion is not distinguished
        ///
        void UpdateHistogram(InputSchema & row)
        {
            if (m_rowSizeBasedSampling)
            {
                SIZE_T rowSize = 0;
                SIZE_T rowStart = m_dummyOutput.GetOutputer().GetCurrentPosition();
                BinaryOutputPolicy<OutputSchema>::Serialize(&m_dummyOutput, row);
                rowSize = m_dummyOutput.GetOutputer().GetCurrentPosition() - rowStart;
                m_dataSize += rowSize;

                if (m_currentBucket == -1)
                {
                    // first row
                    m_currentBucket = 0;

                    // left boundary is simply the key of first row
                    new ((char*)&m_leftBoundary) KeySchema(row, m_boundaryAllocator.get());
                    m_rowsInBucket[0] = 0;
                    m_dataSizeInBucket[0] = 0;
                }
                else if (m_dataSizeInBucket[m_currentBucket] < m_minDataSizeInBucket)
                {
                    // bucket is not yet full, proceeding with filling bucket
                }
                else
                {
                    OpenBucketAndMerge(row);
                }

                m_rowsInBucket[m_currentBucket]++;
                m_dataSizeInBucket[m_currentBucket] += rowSize;
            }
            else
            {
                if (m_currentBucket == -1)
                {
                    // first row
                    m_currentBucket = 0;

                    // left boundary is simply the key of first row
                    new ((char*)&m_leftBoundary) KeySchema(row, m_boundaryAllocator.get());
                    m_rowsInBucket[0] = 0;
                }
                else if (m_rowsInBucket[m_currentBucket] < m_minRowsInBucket)
                {
                    // bucket is not yet full, proceeding with filling bucket
                }
                else if (m_rowsInBucket[m_currentBucket] == m_minRowsInBucket)
                {
                    // bucket reached minimum level, save row key for tracking key change
                    StoreKey(row);
                }
                else if (!IsKeyChanged(row))
                {
                    // filling bucket over the capacity but we have to do it until key changes
                }
                else
                {
                    // save bucket boundary
                    try
                    {
                        new (&m_boundaries[m_currentBucket]) KeySchema(row, m_rowAllocator[m_currentAllocator].get());
                    }
                    catch (RuntimeMemoryException &)
                    {
                        // Throw more meaningful exception in case all the ranges do not fit into the allocator
                        throw RuntimeException(E_USER_OUT_OF_MEMORY, "Not enough memory to evaluate histogram");
                    }

                    if (m_currentBucket < m_maxBuckets - 1)
                    {
                        m_rowsInBucket[++m_currentBucket] = 0;
                    }
                    else
                    {
                        // filled all buckets and rows still coming, reduce bucket boundaries to free buckets
                        // - first half of buckets get filled with data from original full list of buckets (second half of buckets becomes available)
                        // - each bucket gets 2 consecutive buckets worth of data from original and copies it into a new allocator as it goes
                        int nextAllocator = (m_currentAllocator + 1) % 2;

                        // merge two consequent buckets into one
                        for (int i = 0; i < m_maxBuckets / 2; i++)
                        {
                            m_rowsInBucket[i] = m_rowsInBucket[2 * i] + m_rowsInBucket[2 * i + 1];

                            new ((char*)&m_boundaries[i]) KeySchema(m_boundaries[2 * i + 1], m_rowAllocator[nextAllocator].get());
                        }

                        m_rowAllocator[m_currentAllocator]->Reset();
                        m_currentAllocator = nextAllocator;

                        // Increase bucket capacity twice
                        m_minRowsInBucket *= 2;
                        m_currentBucket = m_maxBuckets / 2;
                        m_rowsInBucket[m_currentBucket] = 0;
                    }
                }

                m_rowsInBucket[m_currentBucket]++;
            }
        }

        FORCE_INLINE bool IsKeyChanged(InputSchema & input)
        {
            int compareResult = KeyPolicy::Compare(input, m_thresholdKey);

            if (compareResult < 0)
            {
#ifdef SCOPE_DEBUG
                cout << "Current row: " << endl;
                cout << input << endl;
                cout << "Previous row: " << endl;
                cout << m_thresholdKey << endl;
#endif
                throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Input data for the HistogramCollector is not sorted on partition key");
            }

            return compareResult != 0;
        }

        FORCE_INLINE void StoreKey(InputSchema & input)
        {
            m_keyAllocator->Reset();
            new ((char*)&m_thresholdKey) KeySchema(input, m_keyAllocator.get());
        }

        KeySchema m_thresholdKey;            // key of the threshold row
        KeySchema m_leftBoundary;              // first row (lower boundary of #0 bucket
        std::unique_ptr<char> m_boundaryBuffer;     // buffer for boundaries
        KeySchema * m_boundaries;              // inner and upper boundaries
        std::unique_ptr<LONGLONG[]> m_rowsInBucket; // number of rows in buckets
        std::unique_ptr<LONGLONG[]> m_dataSizeInBucket; //Data Size in buckets
        int m_currentBucket;                   // id of current bucket
        int m_currentAllocator;                // id of the current allocator (0 or 1)

        std::shared_ptr<RowEntityAllocator> m_keyAllocator;
        std::shared_ptr<RowEntityAllocator> m_boundaryAllocator; // stores left most sample boundary
        std::shared_ptr<RowEntityAllocator> m_rowAllocator[2];   // stores all the boundaries except the left most one (allocators are swapped on every reduction)
    };

    ///
    /// Sample reducer (coordinator) operator template
    ///
    template<typename InputOperator, typename InputSchema, typename OutputSchema, int UID = -1>
    class SampleReducer : public Operator<SampleReducer<InputOperator, InputSchema, OutputSchema, UID>, OutputSchema, UID>
    {
        typedef KeyComparePolicy<InputSchema, UID> KeyPolicy;

        InputOperator * m_child;        // child operator

        ULONG m_bucketCount;  // number of partitions
        ULONG m_currentRowIndex;

        double m_totalRowCount;
        double m_threashold;
        ULONGLONG m_rowCount;

        KeyIterator<InputOperator, InputSchema, KeyPolicy> m_keyIterator;

    public:
        SampleReducer(InputOperator * input, ULONG bucketCount, int operatorId) :
            Operator(operatorId),
            m_child(input),
            m_bucketCount(bucketCount),
            m_currentRowIndex(0),
            m_totalRowCount(0.0),
            m_threashold(0.0),
            m_rowCount(0),
            m_keyIterator(input)
        {
            // ideally, partition number should be larger than 1 for any range partition structured streams.
            // that is, bucket count should be larger than 1
            // however, there is a corner case - empty structured stream
            // That is, there is not any data which will be written into structured stream.
            // in this case, JM will set number of buckets to 1
            SCOPE_ASSERT(m_bucketCount > 0);
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);
            if (m_bucketCount == 1)
            {
                // bail out early as we are not doing anything if there is only 1 partition
                return false;
            }

            if (m_currentRowIndex >= m_bucketCount - 1)
            {
                return false;
            }

            if (m_currentRowIndex == 0)
            {
                m_keyIterator.ReadFirst();

                // in current implementation, the previous stage of SampleCoordindator is sort stage
                // and sort stage knows total row count before it returns the first row.
                // it'll fail to compile if this assumption is broken for there is no TotalRowCount implementation for other operators
                m_totalRowCount = (double)m_child->TotalRowCount();
                m_threashold = m_totalRowCount / m_bucketCount;
                if (m_keyIterator.HasMoreRows())
                {
                    m_keyIterator.ResetKey();
                    m_rowCount = 1;
                }
            }
            else
            {
                m_rowCount += m_keyIterator.Drain();
                m_keyIterator.ResetKey();
            }

            while (m_keyIterator.HasMoreRows())
            {
                if (m_rowCount >= m_threashold)
                {
                    stats.IncreaseRowCount(1);
                    output = *(m_keyIterator.GetRow());
                    ++m_currentRowIndex;
                    m_threashold = m_rowCount + (m_totalRowCount - m_rowCount) / (m_bucketCount - m_currentRowIndex);

                    // Drain could overwrite output
                    // so return here and call Drain in next GetNextRow if some sampled key are selected
                    return true;
                }

                m_rowCount += m_keyIterator.Drain();
                m_keyIterator.ResetKey();
            }

            if (m_currentRowIndex == 0)
            {
                // range partition must have more than 1 partition, however, there is not enough unique sampled key
                // it just returns one key - by default, all columns are default value if caller doesn't do special initialization.
                ++m_currentRowIndex;
                stats.IncreaseRowCount(1);
                return true;
            }

            return false;
        }

        // Release all resources of child
        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("SampleReduce");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_keyIterator.WriteRuntimeStats(node);
        }
    };


    ///
    /// Histogram reducer (coordinator) operator template
    ///
    template<typename InputOperator, typename InputSchema, typename OutputSchema, int UID = -1>
    class HistogramReducer : public Operator<HistogramReducer<InputOperator, InputSchema, OutputSchema, UID>, OutputSchema, UID>
    {
        typedef KeyComparePolicy<InputSchema, UID> KeyPolicy;

        InputOperator * m_child;        // child operator

        // Declare operator to avoid warning C4512: 'ScopeEngine::HistogramReducer<InputOperator,OutputSchema>' : assignment operator could not be generated
        HistogramReducer & operator=(const HistogramReducer & from);

    public:
        HistogramReducer(InputOperator * input, ULONG bucketCount, int operatorId, bool rowSizeBasedSampling) :
            Operator(operatorId),
            m_child(input),
            m_bucketCount(bucketCount),
            m_bucketProcessed(0),
            m_bucketCapacity(0),
            m_rowCount(0),
            m_rowSizeBasedSampling(rowSizeBasedSampling),
            m_rowProcessed(0),
            m_rowLimit(0),
            m_dataSize(0),
            m_dataSizeProcessed(0),
            m_dataSizeLimit(0),
            m_rowCountInCurrentBucket(0),
            m_dataSizeInCurrentBucket(0),
            m_rowCountThreshold(0),
            m_firstRow(true)
        {
            SCOPE_ASSERT(m_bucketCount > 0);
            m_allocator.reset(new RowEntityAllocator(Configuration::GetGlobal().GetMaxKeySize(), "HistogramReducer", RowEntityAllocator::KeyContent));
        }

        // Initialize child
        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();

            if (m_bucketCount == 1)
            {
                // bail out early as we are not doing anything if there is only 1 partition
                return;
            }

            InputSchema input;

            // read total amount of rows
            bool succeed = m_child->GetNextRow(input);
            SCOPE_ASSERT(succeed);
            stats.IncreaseRowCount(1);
            m_rowCount = input.GetBucketNumRec();

            if (m_rowSizeBasedSampling)
            {
                m_dataSize = input.GetBucketDataSize();
                m_bucketCapacity = m_dataSize / m_bucketCount;
                m_dataSizeLimit = m_bucketCapacity * (m_bucketCount - 2) + (m_dataSize - m_bucketCapacity * (m_bucketCount - 2)) / 2;
                m_rowCountThreshold = c_maxRowCountThreshold * m_rowCount / m_bucketCount;

                // init key boundary
                if (m_child->GetNextRow(input))
                {
                    stats.IncreaseRowCount(1);

                    StoreKey(input);

                    m_dataSizeProcessed += input.GetBucketDataSize();
                    m_rowCountInCurrentBucket += input.GetBucketNumRec();
                    m_dataSizeInCurrentBucket += input.GetBucketDataSize();
                }
            }
            else
            {
                // reserve 1 bucket to amortize skewed data
                m_bucketCapacity = m_rowCount / (m_bucketCount - 1);
                m_rowLimit = m_bucketCapacity * (m_bucketCount - 2) + (m_rowCount - m_bucketCapacity * (m_bucketCount - 2)) / 2;

                // init key boundary
                if (m_child->GetNextRow(input))
                {
                    stats.IncreaseRowCount(1);

                    StoreKey(input);

                    m_rowProcessed += input.GetBucketNumRec();
                }
            }
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            if (m_bucketCount == 1)
            {
                // bail out early as we are not doing anything if there is only 1 partition
                return false;
            }

            if (m_rowSizeBasedSampling)
            {
                if (m_dataSizeProcessed <= m_dataSizeLimit)
                {
                    InputSchema input;

                    while (m_child->GetNextRow(input))
                    {
                        stats.IncreaseRowCount(1);

                        //
                        // This interval merge algorithm does not consider interval overlapping which in extreme cases may result in partition skew
                        // Interval right boundary is not taken into account and hence left empty by HistogramCollector
                        // In future if more elaborated algorightm is used that considers interval length fix HistogramCollector to generate right boundary
                        //
                        if (IsKeyChanged(input))
                        {
                            if (m_bucketProcessed < m_bucketCount - 1 && (m_dataSizeInCurrentBucket > m_bucketCapacity || m_rowCountInCurrentBucket > m_rowCountThreshold))
                            {
                                m_bucketProcessed++;

                                //
                                // we output m_bucketCount - 1 rows that divide (-infinity..+inifinity) into m_bucketCount intervals
                                //
                                RowTransformPolicy<InputSchema, OutputSchema, UID>::FilterTransformRow(input, output, nullptr);

                                StoreKey(input);
#ifdef SCOPE_DEBUG
                                cout << output;
#endif

                                m_bucketCapacity = (m_dataSize - m_dataSizeProcessed) / (m_bucketCount - m_bucketProcessed);
                                m_firstRow = false;

                                m_dataSizeProcessed += input.GetBucketDataSize();
                                m_rowCountInCurrentBucket = input.GetBucketNumRec();
                                m_dataSizeInCurrentBucket = input.GetBucketDataSize();

                                return true;
                            }

                            StoreKey(input);
                        }

                        m_dataSizeProcessed += input.GetBucketDataSize();
                        m_rowCountInCurrentBucket += input.GetBucketNumRec();
                        m_dataSizeInCurrentBucket += input.GetBucketDataSize();
                    }

                    // fallthrough and return false
                }
            }
            else
            {
                if (m_rowProcessed <= m_rowLimit)
                {
                    InputSchema input;

                    while (m_child->GetNextRow(input))
                    {
                        stats.IncreaseRowCount(1);

                        //
                        // This interval merge algorithm does not consider interval overlapping which in extreme cases may result in partition skew
                        // Interval right boundary is not taken into account and hence left empty by HistogramCollector
                        // In future if more elaborated algorightm is used that considers interval length fix HistogramCollector to generate right boundary
                        //
                        if (IsKeyChanged(input))
                        {
                            StoreKey(input);

                            if ((m_bucketProcessed < m_bucketCount - 2 && m_rowProcessed >(m_bucketProcessed + 1) * m_bucketCapacity)
                                ||
                                m_rowProcessed > m_rowLimit)
                            {
                                m_bucketProcessed++;
                                m_rowProcessed += input.GetBucketNumRec();

                                //
                                // we output m_bucketCount - 1 rows that divide (-infinity..+inifinity) into m_bucketCount intervals
                                //

                                RowTransformPolicy<InputSchema, OutputSchema, UID>::FilterTransformRow(input, output, nullptr);

#ifdef SCOPE_DEBUG
                                cout << output;
#endif
                                m_firstRow = false;
                                return true;
                            }
                        }

                        m_rowProcessed += input.GetBucketNumRec();
                    }

                    // fallthrough and return false
                }
            }

            if (m_firstRow)
            {
                m_firstRow = false;
                return true;
            }
            else
            {
                return false;
            }
        }

        // Release all resources of child
        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_allocator->Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("HistogramReduce");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            if (m_allocator)
            {
                m_allocator->WriteRuntimeStats(node, sizeof(InputSchema));
            }

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements().AddMemoryInRows(OperatorRequirementsConstants::HistogramReducer__Row_MinMemory);
        }

    private:

        bool IsKeyChanged(InputSchema & input)
        {
            int compareResult = KeyPolicy::Compare(&m_keyBoundary, &input);

            if (compareResult > 0)
            {
#ifdef SCOPE_DEBUG
                cout << "Current row: " << endl;
                cout << input << endl;
                cout << "Previous row: " << endl;
                cout << m_keyBoundary << endl;
#endif
                throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Input data for the HistogramReducer is not sorted");
            }

            return compareResult != 0;
        }

        FORCE_INLINE void StoreKey(InputSchema & input)
        {
            m_allocator->Reset();
            new ((char*)&m_keyBoundary) InputSchema(input, m_allocator.get());
        }


        ULONG       m_bucketCount;  // number of partitions
        ULONG       m_bucketProcessed;
        ULONGLONG   m_bucketCapacity;
        ULONGLONG   m_rowCount;     // number of rows

        bool        m_rowSizeBasedSampling;

        //Row count based sampling related
        ULONGLONG   m_rowProcessed;
        ULONGLONG   m_rowLimit;

        //Row size based sampling related
        ULONGLONG   m_dataSize;     // data size
        ULONGLONG   m_dataSizeProcessed;
        ULONGLONG   m_dataSizeLimit;
        ULONGLONG   m_rowCountInCurrentBucket;
        ULONGLONG   m_dataSizeInCurrentBucket;
        ULONGLONG   m_rowCountThreshold;
        static const int c_maxRowCountThreshold = 2; // Max 2X row number to the average.

        bool        m_firstRow;
        InputSchema m_keyBoundary;

        std::shared_ptr<RowEntityAllocator> m_allocator;
    };

    // A mini-extractor for the partition spec meta file
    template <typename Schema>
    class PartSpecMetafile : public ExecutionStats
    {
        RowEntityAllocator                 m_allocator;
        ResourceInputStream                m_input;
        int                                m_rows;
        int                                m_current;

    public:
        typedef typename Schema Schema;

        PartSpecMetafile(const string& filename) :
            m_allocator(Configuration::GetGlobal().GetMaxOnDiskRowSize(), "PartSpecMetafile", RowEntityAllocator::RowContent), m_input(&m_allocator, filename),
            m_rows(-1), m_current(-1)
        {
        }

        void Init()
        {
            AutoExecStats stats(this);

            m_input.Init();
            m_input.Read(m_rows);
            m_current = 0;
        }

        bool GetNextRow(Schema & output)
        {
            AutoExecStats stats(this);

            if (m_current == m_rows)
            {
                return false;
            }

            m_allocator.Reset();

            // deserialize a row here
            BinaryExtractPolicy<Schema>::DeserializePartitionSpec(&m_input, output);
            ++m_current;

            stats.IncreaseRowCount(1);

            return true;
        }

        void Close()
        {
            AutoExecStats stats(this);

            m_input.Close();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("PartSpecMetafile");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_input.GetTotalIoWaitTime());
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_input.GetTotalIoWaitTime());
            node.AddAttribute(RuntimeStats::IoStreamInclusiveTime(), m_input.GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());

            m_allocator.WriteRuntimeStats(node);
            m_input.WriteRuntimeStats(node);
        }
    };

    ///
    /// Sorting bucket operator. This operator will cache input rows and sort in memory.
    /// The operator will cache up to a predefine number of rows or reach its memory quota.
    /// If the memory presure detected by the caller, the whole bucket may be spilled to disk using binary outputer.
    ///
    template<typename InputOperator, typename InputSchema, typename RowSchema, int UID = -1>
    class RangePartitioner : public ExecutionStats
    {
        static const char* const sm_className;
        static const SIZE_T x_maxNumOfPartitions = 1000000; // some big number since compiler sets limit on partition number

        InputOperator                      * m_child;    // input operator which contains partition range
        AutoRowArray<InputSchema>            m_partitionKeyCache;      // auto grow array for partition range
        int                                  m_keyCount;
        RowEntityAllocator                   m_allocator;

        typedef InputSchema RangeRowSchema;

        struct KeyComparator
        {
            INLINE bool operator()(const RowSchema & d1, const RangeRowSchema & d2) const
            {
                return RowComparePolicy<RowSchema, RangeRowSchema, UID>::Compare(&d1, &d2) < 0;
            }
        };

    public:
        RangePartitioner(InputOperator * input) :
            m_child(input),
            m_partitionKeyCache(sm_className, x_maxNumOfPartitions, Configuration::GetGlobal().GetRangeBoundsMaxMemorySize()),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent)
        {
        }

        bool LoadBuckets(int& bucketCountCap)
        {
            AutoExecStats stats(this);

            if (m_child == nullptr)
            {
                SCOPE_ASSERT(!"range partitioner has no input stream");
                return false;
            }

            try
            {
                RangeRowSchema row;

                // No global memory throttling yet.
                while (!m_partitionKeyCache.FFull() && m_child->GetNextRow(row))
                {
                    // if cache is full, we will bail out.
                    if (!m_partitionKeyCache.AddRow(row))
                    {
                        throw RuntimeException(E_USER_OUT_OF_MEMORY, "Range partitioner does not have enough memory to hold range boundaries.");
                    }
                }

                if (m_partitionKeyCache.FFull() && m_child->GetNextRow(row))
                {
                    stringstream ss;
                    ss << "The number of range boundaries is beyond the range partitioners max limit " << m_partitionKeyCache.Limit();
                    throw RuntimeException(E_USER_TOO_MANY_RANGES, ss.str().c_str());
                }

                stats.IncreaseRowCount(m_partitionKeyCache.Size());

                if (bucketCountCap != -1)
                {
                    //For range partition to get n bucket we only need n-1 partition key.
                    int bucketCount = bucketCountCap - 1;

                    // reduce bucket count if it has too many ranges
                    if (bucketCount < m_partitionKeyCache.Size())
                    {
                        for (int i = 0; i < bucketCount; i++)
                        {
                            m_partitionKeyCache[i] = m_partitionKeyCache[(int)(i * m_partitionKeyCache.Size() / bucketCount)];
                        }

                        m_keyCount = bucketCount;
                    }
                    else
                    {
                        m_keyCount = (int)(m_partitionKeyCache.Size());
                    }
                }
                else
                {
                    // Dynamic range partitioning
                    m_keyCount = (int)(m_partitionKeyCache.Size());
                    bucketCountCap = m_keyCount + 1;
                }

            }
            catch (char * ex)
            {
                cout << "hit exception: " << ex << endl;

                // we hit OOM
                // TODO: for OOM case we will dump the bucket to disk and continue.
                return false;
            }

            // return true if cache is full, since there is more rows in input operator
            return true;

        }

        // Creates intermediate payload metadata for the provided partition (by index)
        std::unique_ptr<PartitionMetadata> CreateMetadata(int partitionIdx)
        {
            RangeRowSchema * lb = nullptr;
            RangeRowSchema * ub = nullptr;

            if (partitionIdx <= m_keyCount)
            {
                if (partitionIdx > 0)
                {
                    lb = &m_partitionKeyCache[partitionIdx - 1];
                }

                if (partitionIdx < m_keyCount)
                {
                    ub = &m_partitionKeyCache[partitionIdx];
                }
            }
            else
            {
                partitionIdx = PartitionMetadata::PARTITION_NOT_EXIST;
            }

            std::unique_ptr<PartitionMetadata> metadata(new PartitionPayloadMetadata<RangeRowSchema, UID>(partitionIdx, lb, ub, m_allocator));
            return metadata;
        }

        // get partition index
        int GetPartitionIndex(RowSchema & row)
        {
            AutoExecStats stats(this);

            int i = (int)(std::upper_bound(m_partitionKeyCache.Begin(), m_partitionKeyCache.Begin() + m_keyCount, row, KeyComparator()) - m_partitionKeyCache.Begin());

            SCOPE_ASSERT(i >= 0 && i <= m_keyCount);

            return i;

        }

        // Input operator maybe reused, the caller is responsible to initialize it.
        void Init()
        {
            m_child->Init();
        }

        // Input operator maybe reused, the caller is responsible to close it.
        void Close()
        {
            m_child->Close();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());

            m_partitionKeyCache.WriteRuntimeStats(node);
            m_allocator.WriteRuntimeStats(node);

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements(bool isIndexedPartiton)
        {
            SIZE_T rangeBoundsMaxMemory = isIndexedPartiton
                ? OperatorRequirementsConstants::IndexedPartitionProcessor_RangePartitioner__ConstRangeBoundsMaxMemory_MinMemory
                : OperatorRequirementsConstants::PartitionOutputer_RangePartitioner__ConstRangeBoundsMaxMemory_MinMemory;
            SIZE_T rowCount = isIndexedPartiton
                ? OperatorRequirementsConstants::IndexedPartitionProcessor_RangePartitioner__Row_MinMemory
                : OperatorRequirementsConstants::PartitionOutputer_RangePartitioner__Row_MinMemory;
            return OperatorRequirements(Configuration::GetGlobal().GetRangeBoundsMaxMemorySize() * rangeBoundsMaxMemory).AddMemoryInRows(rowCount);
        }
    };

    template<typename InputOperator, typename InputSchema, typename RowSchema, int UID>
    const char* const RangePartitioner<InputOperator, InputSchema, RowSchema, UID>::sm_className = "RangePartitioner";

    template<typename PartitionSchema, typename RowSchema, int UID = -1>
    class HashPartitioner : public ExecutionStats
    {
        int        m_keyCount;

    public:
        HashPartitioner() : m_keyCount(-1)
        {
        }

        bool LoadBuckets(int& bucketCountCap)
        {
            m_keyCount = bucketCountCap;

            return true;
        }

        // Creates intermediate payload metadata for the provided partition (by index)
        std::unique_ptr<PartitionMetadata> CreateMetadata(int partitionIdx)
        {
            if (partitionIdx >= m_keyCount)
            {
                partitionIdx = PartitionMetadata::PARTITION_NOT_EXIST;
            }

            std::unique_ptr<PartitionMetadata> metadata(new PartitionPayloadMetadata<PartitionSchema, UID>(partitionIdx));
            return metadata;
        }

        // get partition index
        int GetPartitionIndex(RowSchema & row)
        {
            AutoExecStats stats(this);

            // due to direct hash function, it returns negative value which in turn causes GetParttionIndex() to return negative value.
            return (int)((RowHashPolicy<RowSchema, UID>::Hash(&row) % m_keyCount + m_keyCount) % m_keyCount);
        }

        void Init()
        {
        }

        void Close()
        {
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("HashPartition");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
        }

        OperatorRequirements GetOperatorRequirements(bool isIndexedPartiton)
        {
            SIZE_T memorySize = isIndexedPartiton
                ? OperatorRequirementsConstants::IndexedPartitionProcessor_HashPartitioner__Size_MinMemory
                : OperatorRequirementsConstants::PartitionOutputer_HashPartitioner__Size_MinMemory;
            return OperatorRequirements(memorySize);
        }
    };

    ///
    /// PartitionOutputer operator template.
    ///
    template<typename InputOperator, typename InputSchema, typename Partitioner, typename OutputType, typename OutputStream = BinaryOutputStream, bool needMetadata = false>
    class PartitionOutputer : public Operator<PartitionOutputer<InputOperator, InputSchema, Partitioner, OutputType, OutputStream, needMetadata>, int, -1>
    {
    protected:
        InputOperator  *  m_child;  // child operator
        auto_ptr<OutputStream>  m_outputs;
        SIZE_T m_outputBufSize;
        int m_outputBufCnt;
        int               m_outputCount;
        Partitioner    *  m_partitioner;
        std::vector<LONGLONG>  m_rowsPerPartition;

    public:
        PartitionOutputer(InputOperator * input, Partitioner * partitioner, std::string * fileNames, int fileCount, SIZE_T outputBufSize, int outputBufCnt, int operatorId, bool maintainBoundaries = false) :
            Operator(operatorId),
            m_child(input),
            m_outputBufSize(outputBufSize),
            m_outputBufCnt(outputBufCnt),
            m_outputCount(fileCount),
            m_partitioner(partitioner)
        {
            //allocate memory for output stream
            m_outputs.reset((OutputStream*)new char[m_outputCount*sizeof(OutputStream)]);

            // Just allocate the memory and call in place new to initialize OutputStream
            for (int i = 0; i < m_outputCount; i++)
            {
                new (m_outputs.get() + i) OutputStream(fileNames[i], outputBufSize, outputBufCnt, maintainBoundaries);
            }

            m_rowsPerPartition.resize(m_outputCount, 0);
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_partitioner->Init();

            m_child->Init();

            for (int i = 0; i < m_outputCount; i++)
            {
                m_outputs.get()[i].Init();
            }
        }

        PartitionMetadata * GetMetadataImpl()
        {
            return nullptr;
        }

        int SelectBucket(InputSchema& row)
        {
            return m_partitioner->GetPartitionIndex(row);
        }

        bool GetNextRowImpl(int & output)
        {
            AutoExecStats stats(this);

            // if fail to load buckets, we need to exit.
            if (!m_partitioner->LoadBuckets(m_outputCount))
            {
                return false;
            }

            // calling GetMetadata() implies validating metadata which may result in false negatives when metadata is not needed
            if (needMetadata)
            {
                for (int i = 0; i < m_outputCount; i++)
                {
                    m_outputs.get()[i].WriteMetadata(m_partitioner->CreateMetadata(i).get());
                }
            }

            int count = DoOutput();

            // flush all remaining bytes from buffer.
            for (int i = 0; i < m_outputCount; i++)
            {
                m_outputs.get()[i].Finish();
            }

            output = count;
            stats.IncreaseRowCount(count);

            return false;
        }

        virtual int DoOutput()
        {
            int count = 0;
            InputSchema row;
            while (m_child->GetNextRow(row))
            {
                int bucket = SelectBucket(row);
                OutputType::Serialize(&(m_outputs.get()[bucket]), row);
                m_rowsPerPartition[bucket]++;
                count++;
            }
            return count;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            for (int i = 0; i < m_outputCount; i++)
            {
                m_outputs.get()[i].Close();
            }

            m_child->Close();

            m_partitioner->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("PartitionOutput");

            LONGLONG sumOutputInclusiveTime = 0;
            node.AddAttribute(RuntimeStats::MaxOutputCount(), m_outputCount);
            node.AddAttribute(RuntimeStats::AvgOutputCount(), m_outputCount);
            for (SIZE_T i = 0; i < m_outputCount; i++)
            {
                auto & outputNode = node.AddElement("PartitionBucket");
                m_outputs.get()[i].WriteRuntimeStats(outputNode);
                RuntimeStats::WriteRowCount(outputNode, m_rowsPerPartition[i]);

                sumOutputInclusiveTime += m_outputs.get()[i].GetInclusiveTimeMillisecond();
            }

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond() - m_partitioner->GetInclusiveTimeMillisecond() - sumOutputInclusiveTime);
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_partitioner->WriteRuntimeStats(node);

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return m_partitioner->GetOperatorRequirements(false)
                .AddMemoryForOutputUStreams(m_outputCount, m_outputBufSize, m_outputBufCnt);
        }

        DEFAULT_IMPLEMENT_SCOPECEP_CHECKPOINT_VIRTUAL
    };

    template<typename InputOperator, typename InputSchema, typename Partitioner, typename OutputType, typename OutputStream = BinaryOutputStream, bool needMetadata = false, int RunScopeCEPMode = SCOPECEP_MODE_NONE, bool checkOutput = false>
    class StreamingPartitionOutputer : public PartitionOutputer<InputOperator, InputSchema, Partitioner, OutputType, OutputStream, needMetadata>
    {
        vector<StreamingOutputChannel*> m_streamingChannels;
        StreamingOutputCTIProcessing<StreamingPartitionOutputer, InputSchema, OutputType, OutputStream, true> m_ctiProcessing;
        StreamingOutputChecking<OutputStream, InputSchema> m_streamingChecking;
        SlowRowTracker m_slowRowTracker;

    public:
        typedef OutputType ROW;
        typedef OutputStream OUTPUT;

        StreamingPartitionOutputer(InputOperator * input, Partitioner * partitioner, std::string * fileNames, int fileCount, SIZE_T outputBufSize, int outputBufCnt, int operatorId) :
            PartitionOutputer(input, partitioner, fileNames, fileCount, outputBufSize, outputBufCnt, operatorId, RunScopeCEPMode == SCOPECEP_MODE_REAL),
            m_slowRowTracker("OUTPUT")
        {
            m_streamingChannels.reserve(m_outputCount);

            for (int i = 0; i < fileCount; i++)
            {
                StreamingOutputChannel* pChannel = IOManager::GetGlobal()->GetStreamingOutputChannel(fileNames[i]);
                SCOPE_ASSERT(pChannel != nullptr);
                pChannel->SetAllowDuplicateRecord(true);
                m_streamingChannels.push_back(pChannel);
            }
        }

        void Flush()
        {
            for (int i = 0; i < m_outputCount; i++)
            {
                m_outputs.get()[i].Flush();
            }
        }

        virtual int DoOutput() override
        {
            int count = 0;
            InputSchema row;

            AutoFlushTimer<StreamingPartitionOutputer> autoFlushTimer(this);

            bool fromCheckpoint = false;
            if (!g_scopeCEPCheckpointManager->GetStartScopeCEPState().empty())
            {
                ScopeDateTime startTime = g_scopeCEPCheckpointManager->GetStartCTITime();
                row.ResetScopeCEPStatus(startTime, startTime, SCOPECEP_CTI_CHECKPOINT);
                fromCheckpoint = true;
                g_scopeCEPCheckpointManager->DecrementSeqNumber();
            }

            while (fromCheckpoint || m_child->GetNextRow(row))
            {
                if (!autoFlushTimer.IsStarted() && !fromCheckpoint && RunScopeCEPMode == SCOPECEP_MODE_REAL)
                {
                    autoFlushTimer.Start();
                }

                m_slowRowTracker.FinishNextRow(row.GetScopeCEPEventStartTime(), row.GetScopeCEPEventType());
                AutoCriticalSection aCs(autoFlushTimer.GetLock());
                if (row.IsScopeCEPCTI())
                {
                    g_scopeCEPCheckpointManager->IncrementSeqNumber();
                    for (int i = 0; i < m_outputCount; i++)
                    {
                        m_ctiProcessing.DispatchCTIToOutput(row, m_streamingChannels[i], m_outputs.get() + i);
                        m_outputs.get()[i].Commit();
                        m_rowsPerPartition[i]++;
                    }
                    g_scopeCEPCheckpointManager->UpdateLastCTITime(row.GetScopeCEPEventStartTime());
                    if (!fromCheckpoint && row.GetScopeCEPEventType() == (UINT8)SCOPECEP_CTI_CHECKPOINT && g_scopeCEPCheckpointManager->IsWorthyToDoCheckpoint(row.GetScopeCEPEventStartTime()))
                    {
                        for (int i = 0; i < m_outputCount; i++)
                        {
                            (m_outputs.get() + i)->Flush(false);
                        }
                        for (int i = 0; i < m_outputCount; i++)
                        {
                            (m_outputs.get() + i)->Flush(true);
                        }

                        if (checkOutput)
                        {
                            m_streamingChecking.SetCheckpoint(g_scopeCEPCheckpointManager->InitiateCheckPointChainInternal(this));
                        }
                        else
                        {
                            g_scopeCEPCheckpointManager->InitiateCheckPointChain(this);
                        }
                    }
                }
                else
                {
                    g_scopeCEPCheckpointManager->IncrementSeqNumber();
                    int bucket = SelectBucket(row);
                    INT64 sn = g_scopeCEPCheckpointManager->GetCurrentSeqNumber();
                    m_outputs.get()[bucket].Write(sn);

                    SIZE_T curPos = (m_outputs.get()[bucket]).GetOutputer().GetCurrentPosition();
                    OutputType::Serialize(&(m_outputs.get()[bucket]), row);
                    int rowSize = (int)((m_outputs.get()[bucket]).GetOutputer().GetCurrentPosition() - curPos);
                    if (checkOutput && RunScopeCEPMode == SCOPECEP_MODE_REAL)
                    {
                        m_streamingChecking.CheckFirstRow((m_outputs.get()[bucket]), rowSize);
                        m_streamingChecking.WriteRowToCheckpoint(m_outputs.get()[bucket], row, rowSize);
                    }

                    m_rowsPerPartition[bucket]++;
                    m_outputs.get()[bucket].Commit();
                }

                fromCheckpoint = false;
                count++;
                m_slowRowTracker.StartNextRow();
            }

            for (int i = 0; i < m_outputCount; i++)
            {
                m_ctiProcessing.WriteFinalRow(m_outputs.get() + i);
            }

            return count;
        }

        virtual void DoScopeCEPCheckpointImpl(BinaryOutputStream & output) override
        {
            for (int i = 0; i < m_outputCount; i++)
            {
                m_outputs.get()[i].GetOutputer().SaveState(output);
            }
            m_child->DoScopeCEPCheckpoint(output);
        }

        virtual void LoadScopeCEPCheckpointImpl(BinaryInputStream & input) override
        {
            for (int i = 0; i < m_outputCount; i++)
            {
                m_outputs.get()[i].GetOutputer().LoadState(input);
            }
            m_child->LoadScopeCEPCheckpoint(input);

            if (checkOutput && RunScopeCEPMode == SCOPECEP_MODE_REAL)
            {
                m_streamingChecking.GetFirstRowFromCheckpoint(input);
            }

        }
    };

    ///
    /// IndexedPartitionProcessor operator template.
    ///
    template<typename InputOperator, typename InputSchema, typename Partitioner, typename OutputSchema, int UID = -1>
    class IndexedPartitionProcessor : public Operator<IndexedPartitionProcessor<InputOperator, InputSchema, Partitioner, OutputSchema, UID>, OutputSchema, UID>
    {
        InputOperator  *  m_child;  // child operator
        Partitioner    *  m_partitioner;
        int               m_bucketCount;
        bool              m_bucketLoaded;
        InputSchema       m_row;
        std::unique_ptr<PartitionMetadataContainer<UID>>       m_metadata;

    public:
        IndexedPartitionProcessor(InputOperator * input, Partitioner * partitioner, int bucketCount, int operatorId) :
            Operator(operatorId),
            m_child(input),
            m_partitioner(partitioner),
            m_bucketCount(bucketCount),
            m_bucketLoaded(false)
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_partitioner->Init();

            m_child->Init();

            if (m_partitioner->LoadBuckets(m_bucketCount))
            {
                // Assert that the bucket number is returned for dynamic range partitioning
                SCOPE_ASSERT(m_bucketCount != -1);
                m_bucketLoaded = true;
            }
        }

        PartitionMetadata * GetMetadataImpl()
        {
            if (!m_bucketLoaded)
            {
                return nullptr;
            }

            if (!m_metadata)
            {
                m_metadata.reset(new PartitionMetadataContainer<UID>(m_bucketCount));
                for (int i = 0; i < m_bucketCount; i++)
                {
                    m_metadata->AddOnePartitionMetadata(std::move(m_partitioner->CreateMetadata(i)));
                }
            }

            return m_metadata.get();
        }

        bool GetNextRowImpl(OutputSchema& output)
        {
            AutoExecStats stats(this);

            if (!m_bucketLoaded)
            {
                return false;
            }

            if (m_child->GetNextRow(m_row))
            {
                int bucket = m_partitioner->GetPartitionIndex(m_row);

                // Attach partition ID
                IndexedPartitionRowPolicy<OutputSchema, UID>::AttachPartitionID(m_row, output, bucket);

                stats.IncreaseRowCount(1);
                return true;
            }

            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();

            m_partitioner->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("IndexedPartitionProcess");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_partitioner->WriteRuntimeStats(node);
            if (m_metadata != nullptr)
            {
                m_metadata->WriteRuntimeStats(node);
            }

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return m_partitioner->GetOperatorRequirements(true);
        }
    };

    ///
    /// Filter/Transformer operator template.
    ///
    template<typename InputOperator, typename InputSchema, typename OutputSchema, int UID = -1>
    class FilterTransformer : public Operator<FilterTransformer<InputOperator, InputSchema, OutputSchema, UID>, OutputSchema, UID>
    {
        InputOperator * m_child; // child operator

        RowEntityAllocator m_allocator;

    public:
        FilterTransformer(InputOperator * input, int operatorId) :
            Operator(operatorId),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), "FilterTransformer", RowEntityAllocator::RowContent),
            m_child(input)
        {
        }

        // Initialize child
        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            InputSchema input;

            while (m_child->GetNextRow(input))
            {
                m_allocator.Reset();

                if (RowTransformPolicy<InputSchema, OutputSchema, UID>::FilterTransformRow(input, output, &m_allocator))
                {
                    stats.IncreaseRowCount(1);

                    return true;
                }
            }

            return false;
        }

        // Release all resources of children
        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("FilterTransform");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return RowTransformPolicy<InputSchema, OutputSchema, UID>::GetOperatorRequirements();
        }

        DEFAULT_IMPLEMENT_SCOPECEP_CHECKPOINT
            DEFAULT_IMPLEMENT_SCOPECEP_ADJUSTCTI
    };

    template<typename InputOperator, typename InputSchema, typename OutputSchema, int UID = -1>
    class LocalHashAggregator : public Operator<LocalHashAggregator<InputOperator, InputSchema, OutputSchema, UID>, OutputSchema, UID>
    {
    private:
        typedef          RowIterator<InputOperator, InputSchema>               InputIterator;

        typedef typename HashAggregationPolicy<InputSchema,
            OutputSchema,
            UID>               Policy;

    private:
        template <typename KeySchema, typename ValueSchema>
        struct HashtablePolicy
        {
            typedef typename Policy::Hash                                 Hash;
            typedef typename Policy::EqualTo                              Pred;
            typedef          LFUEvictionStats                             EvictionStats;
            typedef typename Policy::Allocator                            DataAllocator;
            typedef          FixedArrayTypeMemoryManager<DataAllocator>   DeepDataMemoryManager;

            static const SIZE_T m_containerAllocTax = Policy::m_containerAllocTax;
        };

    private:
        typedef typename Policy::KeySchema                        KeySchema;
        typedef typename Policy::StateSchema                      StateSchema;

        typedef typename Hashtable<KeySchema,
            StateSchema,
            MutableValueContainer,
            HashtablePolicy>               Hashtable;
        typedef typename Hashtable::ConstIterator                 HashtableIterator;

    private:
        enum State
        {
            UnInit,
            Aggregate,
            InitReturnAggregated,
            ReturnAggregated
        };

    private:
        InputOperator *      m_input;
        InputIterator        m_rowIterator;
        State                m_state;

        Hashtable            m_hashtable;
        HashtableIterator    m_aggIterator;

        UINT                 m_htResetCount;
        SIZE_T               m_htMaxTotalMemory;
        SIZE_T               m_htMaxDataMemory;
        SIZE_T               m_htInsertCount;
        SIZE_T               m_htUpdateCount;

    public:
        LocalHashAggregator(InputOperator * input, int operatorId) :
            Operator(operatorId),
            m_input(input),
            m_rowIterator(input),
            m_state(UnInit),
            m_hashtable(Policy::m_memoryQuota, "LocalHashAggregator", Policy::m_initialSize),
            m_htResetCount(0),
            m_htMaxTotalMemory(0),
            m_htMaxDataMemory(0),
            m_htInsertCount(0),
            m_htUpdateCount(0)
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_input->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_input->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            for (;;)
            {
                switch (m_state)
                {
                case UnInit:
                {
                               m_rowIterator.ReadFirst();
                               m_state = Aggregate;

                               break;
                }
                case Aggregate:
                {
                                  if (m_rowIterator.End())
                                  {
                                      return false;
                                  }

                                  // destroy iterator before cleaning up the memory
                                  m_aggIterator = HashtableIterator();
                                  m_hashtable.Clear(); //release all the memory used by the hashtable

                                  KeySchema key;
                                  StateSchema defaultState;

                                  // the row iterator is at either one of the following two positions:
                                  // 1) at the beginning
                                  // 2) at the last row that was processed but not inserted
                                  //    by the previous "Aggregate" state run
                                  while (!m_rowIterator.End())
                                  {
                                      Policy::GetKey(*m_rowIterator.GetRow(), key);
                                      Policy::GetDefaultState(*m_rowIterator.GetRow(), defaultState);

                                      Hashtable::EResult res = Policy::InsertOrUpdateState(key, defaultState, *m_rowIterator.GetRow(), m_hashtable);

                                      if (res == Hashtable::OK_UPDATE)
                                      {
                                          ++m_htUpdateCount;
                                          m_rowIterator.Increment();
                                          continue;
                                      }
                                      else if (res == Hashtable::OK_INSERT)
                                      {
                                          ++m_htInsertCount;
                                          m_rowIterator.Increment();
                                          continue;
                                      }
                                      else if (res == Hashtable::FAILED_OUT_OF_MEMORY)
                                      {
                                          break;
                                      }
                                      else
                                      {
                                          SCOPE_ASSERT(res == Hashtable::OK_INSERT || res == Hashtable::OK_UPDATE || res == Hashtable::FAILED_OUT_OF_MEMORY);
                                      }
                                  }

                                  {
                                      ++m_htResetCount;
                                      m_htMaxTotalMemory = std::max(m_htMaxTotalMemory, m_hashtable.MemoryUsage());
                                      m_htMaxDataMemory = std::max(m_htMaxDataMemory, m_hashtable.DataMemoryUsage());
                                  }

                                  m_state = InitReturnAggregated;
                                  break;
                }
                case InitReturnAggregated:
                {
                                             m_aggIterator = m_hashtable.Begin();

                                             m_state = ReturnAggregated;
                                             break;
                }
                case ReturnAggregated:
                {
                                         if (m_aggIterator != m_hashtable.End())
                                         {
                                             Policy::GetOutput(m_aggIterator->first, *(m_aggIterator->second), output);
                                             stats.IncreaseRowCount(1);
                                             ++m_aggIterator;

                                             return true;
                                         }
                                         else
                                             m_state = Aggregate;

                                         break;
                }
                default:
                {
                           SCOPE_ASSERT(!"Invalid state for local hash aggregate");
                           return false;
                }
                };
            }
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_input->Close();

            // destroy iterator before cleaning up the memory
            m_aggIterator = HashtableIterator();
            //release all the memory used by the hashtable
            m_hashtable.Clear();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("LocalHashAggregate");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_input->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            node.AddAttribute(RuntimeStats::AvgHashtableResetCount(), m_htResetCount);
            node.AddAttribute(RuntimeStats::MaxHashtableResetCount(), m_htResetCount);
            node.AddAttribute(RuntimeStats::MaxHashtableTotalMemory(), m_htMaxTotalMemory);
            node.AddAttribute(RuntimeStats::AvgHashtableTotalMemory(), m_htMaxTotalMemory);
            node.AddAttribute(RuntimeStats::AvgHashtableMaxDataMemory(), m_htMaxDataMemory);
            node.AddAttribute(RuntimeStats::MaxHashtableMaxDataMemory(), m_htMaxDataMemory);
            node.AddAttribute(RuntimeStats::AvgHashtableInsertCount(), m_htInsertCount);
            node.AddAttribute(RuntimeStats::MaxHashtableInsertCount(), m_htInsertCount);
            node.AddAttribute(RuntimeStats::AvgHashtableUpdateCount(), m_htUpdateCount);
            node.AddAttribute(RuntimeStats::MaxHashtableUpdateCount(), m_htUpdateCount);

            m_input->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements(
                OperatorRequirementsConstants::LocalHashAggregator__Size_MinMemory + OperatorRequirementsConstants::RowBuffer_HashNoSpill__Size_MinMemory,
                OperatorRequirementsConstants::RowBuffer_HashNoSpill__Size_OptimalMemory);
        }
    };

    template<typename InputOperator, typename InputSchema, typename OutputSchema, int UID = -1>
    class LocalHashAggregatorV2 : public Operator<LocalHashAggregatorV2<InputOperator, InputSchema, OutputSchema, UID>, OutputSchema, UID>
    {
        template<typename, typename, typename, int> friend struct LocalHashAggregatorV2_TestHelper;

    private:
        typedef typename HashAggregationPolicyV2<InputSchema, OutputSchema, UID> P;

        static_assert(P::s_memoryQuota >= COMMIT_BLOCK_SIZE, "Memory quota is less than the required mimimum");
        static_assert(P::s_initialSize && !(P::s_initialSize & (P::s_initialSize - 1)), "Hashtable initial size is not a power of two");
        static_assert(P::s_initialSize <= (P::s_directorySize * (1 << P::s_segmentSizeExponent)), "Hashtable initial size exceeds bucket array max capacity");

        template <typename Item, typename CharAllocator>
        struct HashtablePolicy : public SlimHashtableLayoutPolicy<Item, CharAllocator>::Type
        {
            struct BucketArrayTraits
            {
                static const size_t s_directorySize{ P::s_directorySize };
                static const size_t s_segmentSizeExponent{ P::s_segmentSizeExponent };
            };
        };

        typedef          STLIncrementalAllocator<char>         CharAllocator;
        typedef typename P::KeySchema                          Key;
        typedef typename P::ValueSchema                        Value;
        typedef          SlimHashMap<Key,
            Value,
            HashtablePolicy,
            typename P::Hash,
            typename P::EqualTo>      Hashtable;
        typedef typename Hashtable::ConstIterator              HIterator;

    private:
        enum State
        {
            UnInit,
            Aggregate,
            InitOutput,
            Output
        };

    private:
        InputOperator *      m_input;

        IncrementalAllocator m_alloc;
        CharAllocator        m_calloc{ &m_alloc }; // a wrapper with STL allocator interface

        std::unique_ptr<Hashtable> m_hashtable;
        HIterator                  m_it;
        State                      m_state{ UnInit };
        bool                       m_leftoverRow{ false };
        bool                       m_moreRowsToRead{ true };

#pragma region runtime_stats
        size_t m_htResetCnt{ 0 };
        size_t m_htMaxTotalMemory{ 0 };
        size_t m_htInsertCount{ 0 };
        size_t m_htUpdateCount{ 0 };
#pragma endregion runtime_stats

    public:
        LocalHashAggregatorV2(InputOperator * input, int operatorId) :
            Operator(operatorId),
            m_input(input),
            m_alloc(P::s_memoryQuota, "LocalHashAggregatorV2")
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_input->Init();

            SCOPE_LOG_FMT_INFO("HashAggregator", "Init opid=%d, hashtable_type='%s', collision_list_type='%s'",
                GetOperatorId(),
                typeid(Hashtable).name(),
                typeid(typename Hashtable::ItemListType).name());
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_input->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            for (;;)
            {
                switch (m_state)
                {
                case UnInit:
                {
                               m_alloc.Reset();
                               m_hashtable.reset(new Hashtable(m_calloc, P::s_initialSize));
                               m_leftoverRow = false;
                               m_state = Aggregate;

                               break;
                }
                case Aggregate:
                {
                                  // check if the last time the Aggregate state read the input to the end
                                  if (!m_moreRowsToRead)
                                  {
                                      return false;
                                  }

                                  OutputSchema row;
                                  Key key;
                                  Value value;
                                  while (m_input->GetNextRow(row))
                                  {
                                      try
                                      {
                                          if (P::s_valueSchemaHasDeepData) //compile time check
                                          {
                                              INT64 hash = P::RowHashF()(row);
                                              auto it = m_hashtable->FindWithPrecomputedHash(hash, row, P::RowPredF());

                                              if (it == m_hashtable->End())
                                              {
                                                  P::DeepCopyRowToKey(row, key, &m_alloc);
                                                  P::DeepCopyRowToValue(row, value, &m_alloc);
                                                  m_hashtable->InsertWithPrecomputedHash(hash, { key, value });

                                                  ++m_htInsertCount;
                                              }
                                              else
                                              {
                                                  // devnote: two-phase update is executed as follows:
                                                  // 1) make a shallow copy of stored state;
                                                  // 2) update copy -- if the allocator runs out of memory
                                                  //    in the middle of that operation the hashtable internal state
                                                  //    is not corrupted;
                                                  // 3) save a shallow copy of the updated value into the hashtable,
                                                  //    all the deep data is copied to the allocator at that point.
                                                  Value copy = it->second;
                                                  P::UpdateValue(copy, row, &m_alloc);
                                                  it->second = copy;

                                                  ++m_htUpdateCount;
                                              }
                                          }
                                          else
                                          {
                                              // devnote: bucket traversal is the most expensive operation
                                              // and should be avoided if possible. That is why for the case
                                              // of shallow value schema making a maybe unnecessary copy of
                                              // the key is preferable to an extra hashtable lookup.
                                              //
                                              // That can potentially waste memory if the key has strings/binary
                                              // fields greater than 11 bytes (do not fit into FixedSizedArray
                                              // inline field). FixedArrayType copy optimization would not work here,
                                              // as every row has different instance of FixedArrayType even if
                                              // the content is the same. Nevertheless, wasting memory in those
                                              // cases is still preferable to making two lookups instead of one.
                                              P::DeepCopyRowToKey(row, key, &m_alloc);
                                              P::ShallowCopyRowToValue(row, value);
                                              auto it = m_hashtable->Insert({ key, value });
                                              if (it.second) // inserted
                                              {
                                                  ++m_htInsertCount;
                                              }
                                              else
                                              {
                                                  P::UpdateValue(it.first->second, row, &m_alloc); // shallow update
                                                  ++m_htUpdateCount;
                                              }
                                          }
                                      }
                                      catch (RuntimeMemoryException&)
                                      {
                                          // out of memory
                                          m_leftoverRow = true;
                                          ++m_htResetCnt;
                                          break;
                                      }
                                  }

                                  m_moreRowsToRead = m_leftoverRow;
                                  m_htMaxTotalMemory = std::max(m_htMaxTotalMemory, m_alloc.GetSize());
                                  m_state = InitOutput;

                                  if (m_leftoverRow)
                                  {
                                      m_leftoverRow = false;
                                      P::ShallowCopyRowToKey(row, key);
                                      P::ShallowCopyRowToValue(row, value);
                                      P::GetOutput(key, value, output);

                                      return true;
                                  }

                                  // check if the input was empty
                                  if (!m_hashtable->Size())
                                  {
                                      return false;
                                  }

                                  break;
                }
                case InitOutput:
                {
                                   m_it = m_hashtable->Begin();
                                   m_state = Output;

                                   SCOPE_LOG_FMT_INFO("HashAggregator", "Outputting result, hashtable size=%I64u", m_hashtable->Size());

                                   break;
                }
                case Output:
                {
                               if (m_it != m_hashtable->End())
                               {
                                   P::GetOutput(m_it->first, m_it->second, output);
                                   stats.IncreaseRowCount(1);
                                   ++m_it;

                                   return true;
                               }

                               m_state = UnInit;
                               break;
                }
                default:
                {
                           SCOPE_ASSERT(!"Invalid state for local hash aggregate v2");
                           return false;
                }
                };
            }
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_input->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("LocalHashAggregatorV2");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_input->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            node.AddAttribute(RuntimeStats::AvgHashtableResetCount(), m_htResetCnt);
            node.AddAttribute(RuntimeStats::MaxHashtableResetCount(), m_htResetCnt);
            node.AddAttribute(RuntimeStats::MaxHashtableTotalMemory(), m_htMaxTotalMemory);
            node.AddAttribute(RuntimeStats::AvgHashtableTotalMemory(), m_htMaxTotalMemory);
            node.AddAttribute(RuntimeStats::AvgHashtableInsertCount(), m_htInsertCount);
            node.AddAttribute(RuntimeStats::MaxHashtableInsertCount(), m_htInsertCount);
            node.AddAttribute(RuntimeStats::AvgHashtableUpdateCount(), m_htUpdateCount);
            node.AddAttribute(RuntimeStats::MaxHashtableUpdateCount(), m_htUpdateCount);

            m_input->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements(
                OperatorRequirementsConstants::LocalHashAggregatorV2__Size_MinMemory + OperatorRequirementsConstants::RowBuffer_HashNoSpill__Size_MinMemory,
                OperatorRequirementsConstants::RowBuffer_HashNoSpill__Size_OptimalMemory);
        }
    };

    ///
    /// Stream aggregator operator template.
    ///
    template<typename InputOperator, typename InputSchema, typename OutputSchema, int UID = -1>
    class StreamAggregator : public Operator<StreamAggregator<InputOperator, InputSchema, OutputSchema, UID>, OutputSchema, UID>
    {
    public:
        StreamAggregator(InputOperator * input, bool needsDefault, int operatorId) :
            Operator(operatorId),
            m_child(input),
            m_iter(input),
            m_needsDefault(needsDefault)
        {
        }

        // Initialize child
        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();

            m_iter.ReadFirst();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        // Get row from aggregator
        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            if (!m_iter.HasMoreRows())
            {
                if (m_needsDefault)
                {
                    m_iter.ResetKey();
                    m_aggPolicy.BeginKey(m_iter.GetKey(), &output);
                    m_aggPolicy.Aggregate(&output);

                    stats.IncreaseRowCount(1);

                    m_needsDefault = false;
                    return true;
                }

                return false;
            }

            // start key range scan, deep copy key (to track key change)
            m_iter.ResetKey();

            // shallow copy key to output row
            m_aggPolicy.BeginKey(m_iter.GetKey(), &output);

            // scan key range
            while (!m_iter.End())
            {
                // update policy with input row
                m_aggPolicy.AddRow(m_iter.GetRow());

                m_iter.Increment();
            }

            // write aggregated output row
            m_aggPolicy.Aggregate(&output);

            stats.IncreaseRowCount(1);

            // reset flag to false
            m_needsDefault = false;

            return true;
        }

        // Release all resources of children
        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("StreamAggregate");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_aggPolicy.WriteRuntimeStats(node);
            m_iter.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements(m_aggPolicy.GetOperatorRequirements()).AddMemoryInRows(OperatorRequirementsConstants::StreamAggregator__Row_MinMemory);
        }

    private:
        AggregationPolicy<InputSchema, OutputSchema, UID> m_aggPolicy; // aggregation policy (compiler generated)
        KeyIterator<InputOperator, InputSchema, KeyComparePolicy<InputSchema, UID>> m_iter;    // key range iterator
        InputOperator * m_child;            // child operator
        bool m_needsDefault;                // output default values on empty input
    };

    ///
    /// Stream rollup operator template.
    ///
    template<typename InputOperator, typename InputSchema, typename OutputSchema, int UID = -1>
    class StreamRollup : public Operator<StreamRollup<InputOperator, InputSchema, OutputSchema, UID>, OutputSchema, UID>
    {
    public:
        StreamRollup(InputOperator * input, bool fNeedsDefault /* ignored */, int operatorId) :
            Operator(operatorId),
            m_child(input),
            m_iter(input)
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();

            m_iter.ReadFirst();
            m_keyChanged = true;
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            while (1)
            {
                if (m_rollupPolicy.Outputting())
                {
                    stats.IncreaseRowCount(1);

                    m_rollupPolicy.GetNextRow(m_iter.GetKey(), &output);
                    return true;
                }

                if (!m_iter.HasMoreRows())
                {
                    return false;
                }

                // Ingest
                m_rollupPolicy.AddRow(m_iter.GetRow());

                if (m_keyChanged)
                {
                    // Save the key from the row
                    m_iter.ResetKey();
                    m_keyChanged = false;
                }

                // Look ahead and validate the order
                m_iter.Increment();

                if (m_iter.End())
                {
                    if (m_iter.HasMoreRows())
                    {
                        // Key changed!
                        m_keyChanged = true;

                        // Pass in the match level, the number of prefix columns that match.
                        m_rollupPolicy.Finalize(m_iter.GetMatchLevel(*m_iter.GetRow(), *m_iter.GetKey()));
                    }
                    else
                    {
                        // At the end of the rollup, we need to output all levels.  So we pick a match level that's
                        // sure to be below all levels, which are in [0,keys.Count].
                        m_rollupPolicy.Finalize(-1);
                    }
                }
            }
        }

        // Release all resources of children
        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("StreamRollup");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_rollupPolicy.WriteRuntimeStats(node);
            m_iter.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements(m_rollupPolicy.GetOperatorRequirements()).AddMemoryInRows(OperatorRequirementsConstants::StreamRollup__Row_MinMemory);
        }

    private:
        RollupPolicy<InputSchema, OutputSchema, UID> m_rollupPolicy; // rollup policy (compiler generated)
        KeyIterator<InputOperator, InputSchema, KeyComparePolicy<InputSchema, UID>> m_iter;    // key range iterator
        InputOperator * m_child;            // child operator
        bool m_keyChanged;
    };

    //
    // Template for TOP operator
    //
    template<typename InputOperator, typename OutputSchema, unsigned __int64 top, int UID = -1>
    class Topper : public Operator<Topper<InputOperator, OutputSchema, top, UID>, OutputSchema, UID>
    {
        InputOperator * m_child; // child operator
        size_t          m_rowCount;

    public:
        Topper(InputOperator * input, int operatorId) :
            Operator(operatorId),
            m_child(input),
            m_rowCount(0)
        {
        }

        // Initialize child
        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            if (m_rowCount < top && m_child->GetNextRow(output))
            {
                ++m_rowCount;
                IncreaseRowCount(1);

                return true;
            }

            return false;
        }

        // Release all resources of children
        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("Top");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Topper__Size_MinMemory);
        }

        DEFAULT_IMPLEMENT_SCOPECEP_CHECKPOINT
    };

    //
    // Template for WindowAggregator operator
    //
    template<typename InputOperator, typename InputSchema, typename OutputSchema, int UID = -1>
    class WindowAggregator : public Operator<WindowAggregator<InputOperator, InputSchema, OutputSchema, UID>, OutputSchema, UID>
    {
        typedef enum
        {
            x_wisValid,
            x_wisNoRow,
            x_wisEOS
        } WindowInputState;

    private:
        void FetchInputRow()
        {
            if (!m_hasReadFirst)
            {
                m_iter.ReadFirst();
            }
            else
            {
                m_iter.Increment();
            }

            m_wis = m_iter.HasMoreRows() ? x_wisValid : x_wisEOS;
        }

        void GetNextCurrentRow()
        {
            if (0 == m_lag && x_wisValid == m_wis)
            {
                if (!m_hasReadFirst || 0 != m_iter.Compare(*m_iter.GetRow(), *m_iter.GetKey()))
                {
                    // New group!
                    m_windowPolicy.Reset();

                    // Copy the new key for future comparisons.
                    m_iter.ResetKey();

                    // The first time this happens we have initialized.
                    m_hasReadFirst = true;
                }

                BufferInputRow();
                FetchInputRow();
            }
        }

        void BufferInputRow()
        {
            m_windowPolicy.AddRowMakeRoom(*m_iter.GetRow());
            m_wis = x_wisNoRow;
            m_lag++;
        }

        void GetWindowRows()
        {
            // Break if we've hit the end of stream.
            while (x_wisValid == m_wis)
            {
                // Break if we've hit a new group.
                if (0 != m_iter.Compare(*m_iter.GetRow(), *m_iter.GetKey()))
                {
                    break;
                }

                // Break if we've hit the bottom of the window.
                if (m_lag > m_windowPolicy.Bottom())
                {
                    break;
                }

                BufferInputRow();
                FetchInputRow();
            }
        }

    public:
        WindowAggregator(InputOperator * input, bool, int operatorId) :
            Operator(operatorId),
            m_child(input),
            m_iter(input)
        {
        }

        // Initialize child
        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
            m_hasReadFirst = false;
            m_wis = x_wisNoRow;
            m_lag = 0;
        }

        // Get row from aggregator
        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            if (x_wisNoRow == m_wis)
            {
                FetchInputRow();
            }

            GetNextCurrentRow();

            if (0 < m_lag)
            {
                GetWindowRows();
                --m_lag;
                m_windowPolicy.GetValue(output, m_lag);
                stats.IncreaseRowCount(1);
                return true;
            }

            return false;
        }

        // Release all resources of children
        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("WindowAggregate");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_windowPolicy.WriteRuntimeStats(node);

            m_iter.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements(m_windowPolicy.GetOperatorRequirements()).AddMemoryInRows(OperatorRequirementsConstants::WindowAggregator__Row_MinMemory);
        }

    private:
        bool m_hasReadFirst;
        WindowInputState m_wis;
        WindowPolicy<InputSchema, OutputSchema, UID> m_windowPolicy; // window policy (compiler generated)
        KeyIterator<InputOperator, InputSchema, KeyComparePolicy<InputSchema, UID>> m_iter;    // key range iterator
        InputOperator * m_child;            // child operator
        SIZE_T m_lag;                          // rows buffered beyond those returned as current
    };

    //
    // Template for RANK operator
    //
    template<typename InputOperator, typename OutputSchema, int UID = -1>
    class Ranker : public Operator<Ranker<InputOperator, OutputSchema, UID>, OutputSchema, UID>
    {
        InputOperator * m_child; // child operator

        __int64 m_rank;

    public:
        Ranker(InputOperator * input, int operatorId) :
            Operator(operatorId),
            m_child(input),
            m_rank(0)
        {
        }

        // Initialize child
        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            if (m_child->GetNextRow(output))
            {
                stats.IncreaseRowCount(1);

                RowRankPolicy<OutputSchema, UID>::SetRank(&output, ++m_rank);

                return true;
            }

            return false;
        }

        // Release all resources of children
        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("Rank");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Ranker__Size_MinMemory);
        }
    };

    //
    // Template for SequenceProject operator
    //
    // CONSIDER:  Many sequence functions do not care about isNewGroupOrder.  For those, a simplified version of this
    // class can be used.
    template<typename InputOperator, typename InputSchema, typename OutputSchema, int UID = -1>
    class SequenceProject : public Operator<SequenceProject<InputOperator, InputSchema, OutputSchema, UID>, OutputSchema, UID>
    {
        typedef typename SequenceProjectPolicy<InputSchema, OutputSchema, UID> SequenceProjectPolicyType;
        typedef KeyIterator<InputOperator, InputSchema, typename SequenceProjectPolicyType::GroupKeyPolicy> GroupIteratorType;
        typedef KeyIterator<InputOperator, InputSchema, typename SequenceProjectPolicyType::ValueKeyPolicy> ValueIteratorType;

        bool m_hasReadFirst;
        InputOperator * m_child;                               // child operator
        SequenceProjectPolicyType m_seqprjPolicy;
        GroupIteratorType m_iterGroup;                         // key range iterator for partitionby tuples
        ValueIteratorType m_iterOrder;                         // key range iterator for orderby tuples

    public:
        SequenceProject(InputOperator * input, int operatorId) :
            Operator(operatorId),
            m_child(input),
            m_iterGroup(input),
            m_iterOrder(input)
        {
        }

        void SetVertexID(__int64 vertexID)
        {
            m_seqprjPolicy.SetVertexID(vertexID);
        }

        // Initialize child
        void InitImpl()
        {
            AutoExecStats stats(this);
            m_hasReadFirst = false;

            m_child->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);
            bool isNewGroup;
            bool isNewOrder;

            if (!m_hasReadFirst)
            {
                m_hasReadFirst = true;

                m_iterGroup.ReadFirst();
                if (!m_iterGroup.HasMoreRows())
                {
                    return false;
                }

                m_iterOrder.SetRow(m_iterGroup.GetRow());

                isNewGroup = true;
                isNewOrder = true;
            }
            else
            {
                m_iterGroup.Increment();
                if (!m_iterGroup.HasMoreRows())
                {
                    return false;
                }

                m_iterOrder.SetRow(m_iterGroup.GetRow());

                isNewGroup = 0 != m_iterGroup.Compare(*m_iterGroup.GetRow(), *m_iterGroup.GetKey());
                isNewOrder = 0 != m_iterOrder.Compare(*m_iterGroup.GetRow(), *m_iterOrder.GetKey());
            }

            m_seqprjPolicy.AdvanceAndOutput(isNewGroup, isNewOrder, output, *m_iterGroup.GetRow());

            if (isNewGroup)
            {
                m_iterGroup.ResetKey();
                m_iterOrder.ResetKey();
            }
            else if (isNewOrder)
            {
                m_iterOrder.ResetKey();
            }

            stats.IncreaseRowCount(1);
            return true;
        }

        // Release all resources of children
        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("SequenceProject");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_iterGroup.WriteRuntimeStats(node);
            m_iterOrder.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::SequenceProject__Row_MinMemory);
        }
    };

    //
    // Sequence functions
    //
    class SequenceFunction_ROW_NUMBER
    {
        __int64 rowNumber;

    public:
        SequenceFunction_ROW_NUMBER()
        {
            rowNumber = 0;
        }

        __int64 Step(bool isNewGroup)
        {
            if (isNewGroup)
            {
                rowNumber = 0;
            }

            ++rowNumber;
            return rowNumber;
        }
    };

    class SequenceFunction_RANK
    {
        __int64 prev;
        __int64 rowNumber;

    public:
        SequenceFunction_RANK()
        {
            prev = 0;
            rowNumber = 0;
        }

        __int64 Step(bool isNewGroup, bool isNewValue)
        {
            if (isNewGroup)
            {
                prev = 1;
                rowNumber = 1;
                return prev;
            }
            ++rowNumber;
            if (isNewValue)
            {
                prev = rowNumber;
            }
            return prev;
        }
    };

    class SequenceFunction_DENSE_RANK
    {
        __int64 prev;

    public:
        SequenceFunction_DENSE_RANK()
        {
            prev = 0;
        }

        __int64 Step(bool isNewGroup, bool isNewValue)
        {
            if (isNewGroup)
            {
                prev = 1;
            }
            else if (isNewValue)
            {
                ++prev;
            }
            return prev;
        }
    };


    class SequenceFunction_NTILE
    {
        __int64 rowNumber;
        __int64 rowsPerGroup;
        __int64 groupsWithExtraElement;
        __int64 endRowNumberForGroupsWithExtraElement;
        __int64 resultValue;

    public:
        SequenceFunction_NTILE()
        {
            rowNumber = 0;
        }

        __int64 Step(bool isNewGroup, __int64 count, __int64 intN)
        {
            if (isNewGroup)
            {
                SCOPE_ASSERT(count > 0);
                SCOPE_ASSERT(intN > 0);
                rowsPerGroup = count / intN;
                groupsWithExtraElement = count % intN;
                endRowNumberForGroupsWithExtraElement = (rowsPerGroup + 1)*groupsWithExtraElement;
                rowNumber = 0;
            }

            ++rowNumber;

            if (rowNumber <= endRowNumberForGroupsWithExtraElement)
            {
                resultValue = ((rowNumber - 1) / (rowsPerGroup + 1)) + 1;
            }
            else
            {
                resultValue = (rowNumber - (rowsPerGroup + 1) * groupsWithExtraElement - 1) / rowsPerGroup + groupsWithExtraElement + 1;
            }

            return resultValue;
        }
    };

    class SequenceFunction_ROW_NUMBER_IGNORE_NULLS
    {
        __int64 rowNumber;

    public:
        SequenceFunction_ROW_NUMBER_IGNORE_NULLS()
        {
            rowNumber = 0;
        }
        __int64 Step(bool isNewGroup, bool isNullValue)
        {
            // For null rows, leave the count unchanged.
            if (isNewGroup)
            {
                rowNumber = 0;
            }
            if (!isNullValue)
            {
                ++rowNumber;
            }
            return rowNumber;
        }
    };

    // This sequence function doesn't need any state, because the SequenceProject operator knows the vertex id.
    class SequenceFunction_VERTEX_ID
    {
    };

    //
    // Aggregators
    //

    //
    // FIRST aggregator for non-null and shallow reference types
    //
    template<class T, class AggregateType>
    class AggregateBase_FIRST
    {
    protected:
        AggregateType m_firstValue;
        bool m_firstRow;

    public:
        static void Init(AggregateType & state, AggregateType value)
        {
            state = value;
        }

        static void UpdateState(AggregateType & /*state*/, AggregateType /*value*/)
        {
        }

        static void GetAggregatedValue(AggregateType state, AggregateType * output)
        {
            *output = state;
        }

        AggregateBase_FIRST(const char* /*name*/)
        {
            Reset();
        }

        void Add(const AggregateType & value)
        {
            if (m_firstRow)
            {
                m_firstValue = value;
                m_firstRow = false;
            }
        }

        void Aggregate(AggregateType * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(AggregateType * output)
        {
            *output = m_firstValue;
        }

        void Reset()
        {
            m_firstRow = true;
            m_firstValue = AggregateType();
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // FIRST/ANY_VALUE aggregator for non-null value types.
    // This is a base class for shared logic between FIRST and ANY_VALUE.
    // The only functional difference between them is that ANY_VALUE skips nulls, and FIRST doesn't.
    //
    template<typename T, bool fSkipNulls>
    class Aggregate_FIRST_ANY : public AggregateBase_FIRST<T, T>
    {
    public:
        Aggregate_FIRST_ANY(const char* name) : AggregateBase_FIRST(name)
        {
        }
    };

    //
    // FIRST/ANY_VALUE aggregator for nullable types
    //
    template<typename T, bool fSkipNulls>
    class Aggregate_FIRST_ANY<class NativeNullable<T>, fSkipNulls>
        : public AggregateBase_FIRST<T, NativeNullable<T>>
    {
    public:
        Aggregate_FIRST_ANY(const char* name) : AggregateBase_FIRST(name)
        {
        }

        void Add(const NativeNullable<T> & value)
        {
            if (m_firstRow)
            {
                if (fSkipNulls)
                {
                    if (value.IsNull())
                    {
                        return;
                    }
                }

                m_firstValue = value;
                m_firstRow = false;
            }
        }
    };

    //
    // FIRST/ANY_VALUE aggregator for sql nullable types.
    //
    template<typename T, bool fSkipNulls>
    class Aggregate_FIRST_ANY<class ScopeSqlType::SqlNativeNullable<T>, fSkipNulls>
        : public Aggregate_FIRST_ANY<class NativeNullable<T>, fSkipNulls>
    {
    public:
        Aggregate_FIRST_ANY(const char* name) : Aggregate_FIRST_ANY<class NativeNullable<T>, fSkipNulls>(name)
        {
        }
    };

    //
    // FIRST/ANY_VALUE aggregator for guid
    //
    template<bool fSkipNulls>
    class Aggregate_FIRST_ANY<class ScopeGuid, fSkipNulls>
        :  public AggregateBase_FIRST<ScopeGuid, ScopeGuid>
    {
    public:
        Aggregate_FIRST_ANY(const char* name) : AggregateBase_FIRST(name)
        {
        }
    };

    template <typename A>
    struct AggregateBaseCopier
    {
        std::string             m_name;
        RowEntityAllocator      m_allocator;

        AggregateBaseCopier(string name) : m_name(name), m_allocator(RowEntityAllocator::ColumnContent)
        {
            m_allocator.Init(Configuration::GetGlobal().GetMaxVariableColumnSize(), m_name);
        }

        void Copy(A & dest, const A & src)
        {
            m_allocator.Reset();
            new ((char*)&dest) A(src, &m_allocator);
        }
    };

    template <typename A>
    struct AggregateBaseCopierShell
    {
        IncrementalAllocator*   m_allocator;

        AggregateBaseCopierShell(IncrementalAllocator* allocator) : m_allocator(allocator)
        {
        }

        void Copy(A & dest, const A & src) const
        {
            new ((char*)&dest) A(src, m_allocator);
        }
    };

    //
    // AggregateFixedArrayCopier is a helper class for
    // FixedArrayType aggregates.
    //
    template <typename T>
    struct AggregateFixedArrayCopier : public AggregateBaseCopier<FixedArrayType<T> >
    {
        AggregateFixedArrayCopier(string name) : AggregateBaseCopier(name) {}
    };

    template <typename K, typename V>
    struct AggregateMapCopier : public AggregateBaseCopier<ScopeMapNative<K, V> >
    {
        AggregateMapCopier(string name) : AggregateBaseCopier(name) {}
    };

    template <typename T>
    struct AggregateArrayCopier : public AggregateBaseCopier<ScopeArrayNative<T> >
    {
        AggregateArrayCopier(string name) : AggregateBaseCopier(name) {}
    };

    //
    // AggregateFixedArrayCopierShell is a helper class for
    // FixedArrayType and ScopeMap/ScopeArray aggregates.
    //

    template <typename T>
    struct AggregateFixedArrayCopierShell : public AggregateBaseCopierShell<FixedArrayType<T> >
    {
        AggregateFixedArrayCopierShell(IncrementalAllocator* allocator) : AggregateBaseCopierShell(allocator) {}
    };

    template <typename K, typename V>
    struct AggregateMapCopierShell : public AggregateBaseCopierShell<ScopeMapNative<K, V> >
    {
        AggregateMapCopierShell(IncrementalAllocator* allocator) : AggregateBaseCopierShell(allocator) {}
    };

    template <typename T>
    struct AggregateArrayCopierShell : public AggregateBaseCopierShell<ScopeArrayNative<T> >
    {
        AggregateArrayCopierShell(IncrementalAllocator* allocator) : AggregateBaseCopierShell(allocator) {}
    };

    //
    // FIRST/ANY_VALUE aggregator for fixed deep types
    //
    template<typename T, bool fSkipNulls>
    class AggregateFixedArray_FIRST_ANY
    {
    private:
        AggregateFixedArrayCopier<T>    m_copier;
        FixedArrayType<T>               m_firstValue;
        bool                            m_firstRow;

    public:
        static void Init(FixedArrayType<T> & state, const FixedArrayType<T> & value)
        {
            // do a shallow copy
            state = value;
        }

        template <typename Copier>
        static void UpdateState(FixedArrayType<T> & /*state*/, const FixedArrayType<T> & /*value*/, Copier & /*copier*/)
        {
        }

        static void GetAggregatedValue(const FixedArrayType<T> & state, FixedArrayType<T> * output)
        {
            // do a shallow copy
            *output = state;
        }

        AggregateFixedArray_FIRST_ANY(const char* name) :
            m_copier(name),
            m_firstRow(true)
        {
            Reset();
        }

        void Add(FixedArrayType<T> & value)
        {
            if (m_firstRow)
            {
                if (fSkipNulls)
                {
                    if (value.IsNull())
                    {
                        return;
                    }
                }

                m_copier.Copy(m_firstValue, value);
                m_firstRow = false;
            }
        }

        void Aggregate(FixedArrayType<T> * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(FixedArrayType<T> * output)
        {
            GetAggregatedValue(m_firstValue, output);
        }

        void Reset()
        {
            m_firstRow = true;
            m_firstValue.SetNull();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_copier.m_name);
            m_copier.m_allocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_FIRST_VariableLength__Column_MinMemory);
        }
    };

    //
    // FIRST/ANY_VALUE aggregator for FString
    //
    template<bool fSkipNulls>
    class Aggregate_FIRST_ANY<FString, fSkipNulls>
        : public AggregateFixedArray_FIRST_ANY<char, fSkipNulls>
    {
    public:
        Aggregate_FIRST_ANY(const char* name) : AggregateFixedArray_FIRST_ANY(name)
        {
        }
    };

    //
    // FIRST/ANY_VALUE aggregator for FBinary
    //
    template<bool fSkipNulls>
    class Aggregate_FIRST_ANY<FBinary, fSkipNulls>
        : public AggregateFixedArray_FIRST_ANY<unsigned char, fSkipNulls>
    {
    public:
        Aggregate_FIRST_ANY(const char* name) : AggregateFixedArray_FIRST_ANY(name)
        {
        }
    };

    //
    // FIRST/ANY_VALUE aggregator for complex type
    //
    template<typename A, typename Copier, bool fSkipNulls>
    class AggregateComplex_FIRST_ANY
    {
    private:

        Copier                  m_copier;
        A                       m_firstValue;
        bool                    m_firstRow;

    public:

        static void Init(A & state, const A & value)
        {
            // shallow copy
            state = value;
        }

        template <typename Copier>
        static void UpdateState(A & /*state*/, const A & /*value*/, Copier & /*copier*/)
        {
        }

        static void GetAggregatedValue(const A & state, A * output)
        {
            // do a shallow copy
            *output = state;
        }

        AggregateComplex_FIRST_ANY(const char* name)
            : m_copier(name)
        {
            Reset();
        }

        void Add(A & value)
        {
            if (m_firstRow)
            {
                if (fSkipNulls)
                {
                    if (value.IsNull())
                    {
                        return;
                    }
                }

                m_copier.Copy(m_firstValue, value);
                m_firstRow = false;
            }
        }

        void Aggregate(A* output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(A * output)
        {
            GetAggregatedValue(m_firstValue, output);
        }

        void Reset()
        {
            m_firstRow = true;
            m_firstValue.SetNull();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_copier.m_name);
            m_copier.m_allocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_FIRST_VariableLength__Column_MinMemory);
        }
    };

    //
    // FIRST/ANY_VALUE aggregator for MAP type
    //
    template<typename K, typename V, bool fSkipNulls>
    class Aggregate_FIRST_ANY<ScopeMapNative<K, V>, fSkipNulls> : public AggregateComplex_FIRST_ANY<ScopeMapNative<K, V>, AggregateMapCopier<K, V>, fSkipNulls>
    {
    public:
        Aggregate_FIRST_ANY(const char* name) : AggregateComplex_FIRST_ANY(name) {}
    };

    //
    // FIRST/ANY_VALUE aggregator for ARRAY type
    //
    template<typename T, bool fSkipNulls>
    class Aggregate_FIRST_ANY<ScopeArrayNative<T>, fSkipNulls> : public AggregateComplex_FIRST_ANY<ScopeArrayNative<T>, AggregateArrayCopier<T>, fSkipNulls>
    {
    public:
        Aggregate_FIRST_ANY(const char* name) : AggregateComplex_FIRST_ANY(name) {}
    };

#if !defined(SCOPE_NO_UDT)
    //
    // FIRST/ANY_VALUE aggregator for UDT type
    //
    template<int UserDefinedTypeId, template<int> class UserDefinedType, bool fSkipNulls>
    class Aggregate_FIRST_ANY<class UserDefinedType<UserDefinedTypeId>, fSkipNulls>
    {
        UserDefinedType<UserDefinedTypeId> m_firstValue;
        bool m_firstRow;

    public:
        Aggregate_FIRST_ANY(const char* /*name*/) : m_firstRow(true)
        {
        }

        void Add(UserDefinedType<UserDefinedTypeId> & value)
        {
            if (m_firstRow)
            {
                if (fSkipNulls)
                {
                    if (value.IsNull())
                    {
                        return;
                    }
                }

                m_firstValue = value;
                m_firstRow = false;
            }
        }

        void Aggregate(UserDefinedType<UserDefinedTypeId> * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(UserDefinedType<UserDefinedTypeId> * output)
        {
            *output = m_firstValue;
        }

        void Reset()
        {
            m_firstRow = true;
            m_firstValue.Reset();
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };
#endif // SCOPE_NO_UDT

    //
    // FIRST aggregator - doesn't skip nulls
    //
    template<typename T>
    class Aggregate_FIRST : public Aggregate_FIRST_ANY<T, false>
    {
    public:
        Aggregate_FIRST(const char* name) : Aggregate_FIRST_ANY(name)
        {
        }
    };

    //
    // ANY_VALUE aggregator - skips nulls
    //
    template<typename T>
    class Aggregate_ANY_VALUE : public Aggregate_FIRST_ANY<T, true>
    {
    public:
        Aggregate_ANY_VALUE(const char* name) : Aggregate_FIRST_ANY(name)
        {
        }
    };

    //
    // LAST aggregator for non-null or shallow reference types
    //
    template<typename T, typename AggregateType>
    class AggregateBase_LAST
    {
        AggregateType m_lastValue;

    public:
        static void Init(AggregateType & state, AggregateType value)
        {
            state = value;
        }

        static void UpdateState(AggregateType & state, AggregateType value)
        {
            state = value;
        }

        static void GetAggregatedValue(AggregateType state, AggregateType * output)
        {
            *output = state;
        }

        AggregateBase_LAST(const char* /*name*/)
        {
            Reset();
        }

        // Pass-by-value since for most numeric T => sizeof(T) < sizeof(T&)
        void Add(const AggregateType & value)
        {
            UpdateState(m_lastValue, value);
        }

        void Aggregate(AggregateType * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(AggregateType * output)
        {
            GetAggregatedValue(m_lastValue, output);
        }

        void Reset()
        {
            Init(m_lastValue, AggregateType());
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // LAST aggregator for non-null value types
    //
    template<typename T>
    class Aggregate_LAST : public AggregateBase_LAST<T, T>
    {
    public:
        Aggregate_LAST(const char* name) : AggregateBase_LAST(name)
        {
        }
    };

    //
    // LAST aggregator for nullable types
    //
    template<typename T>
    class Aggregate_LAST<class NativeNullable<T> >
        : public AggregateBase_LAST<T, NativeNullable<T>>
    {
    public:
        Aggregate_LAST(const char* name) : AggregateBase_LAST(name)
        {
        }
    };

    //
    // LAST aggregator for sql nullable types.
    //
    template<typename T>
    class Aggregate_LAST<class ScopeSqlType::SqlNativeNullable<T> >
        : public Aggregate_LAST<NativeNullable<T>>
    {
    public:
        Aggregate_LAST(const char* name) : Aggregate_LAST<NativeNullable<T>>(name)
        {
        }
    };

    //
    // LAST aggregator for guid
    //
    template<>
    class Aggregate_LAST<class ScopeGuid>
        : public AggregateBase_LAST<ScopeGuid, ScopeGuid>
    {
    public:
        Aggregate_LAST(const char* name) : AggregateBase_LAST(name)
        {
        }
    };

    //
    // LAST aggregator for string/binary type
    //
    template<typename T>
    class AggregateFixedArray_LAST
    {
    private:
        AggregateFixedArrayCopier<T>    m_copier;
        FixedArrayType<T>               m_lastValue;

    public:
        static void Init(FixedArrayType<T> & state, const FixedArrayType<T> & value)
        {
            // do a shallow copy
            state = value;
        }

        template <typename Copier>
        static void UpdateState(FixedArrayType<T> & state, const FixedArrayType<T> & value, Copier & copier)
        {
            copier.Copy(state, value);
        }

        static void GetAggregatedValue(const FixedArrayType<T> & state, FixedArrayType<T> * output)
        {
            // do a shallow copy
            *output = state;
        }

        AggregateFixedArray_LAST(const char* name) : m_copier(name)
        {
            Reset();
        }

        void Add(FixedArrayType<T> & value)
        {
            UpdateState(m_lastValue, value, m_copier);
        }

        void Aggregate(FixedArrayType<T> * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(FixedArrayType<T> * output)
        {
            GetAggregatedValue(m_lastValue, output);
        }

        void Reset()
        {
            m_lastValue.SetNull();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_copier.m_name);
            m_copier.m_allocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_LAST_VariableLength__Column_MinMemory);
        }
    };

    //
    // LAST aggregator for FString
    //
    template<>
    class Aggregate_LAST<FString>
        : public AggregateFixedArray_LAST<char>
    {
    public:
        Aggregate_LAST(const char* name) : AggregateFixedArray_LAST(name)
        {
        }
    };

    //
    // LAST aggregator for FBinary
    //
    template<>
    class Aggregate_LAST<FBinary>
        : public AggregateFixedArray_LAST<unsigned char>
    {
    public:
        Aggregate_LAST(const char* name) : AggregateFixedArray_LAST(name)
        {
        }
    };

    //
    // LAST aggregator for complex type
    //
    template<typename A, typename Copier>
    class AggregateComplex_LAST
    {
    private:

        Copier                  m_copier;
        A                       m_lastValue;

    public:

        static void Init(A & state, const A & value)
        {
            // do a shallow copy
            state = value;
        }

        template <typename Copier>
        static void UpdateState(A & state, const A & value, Copier & copier)
        {
            copier.Copy(state, value);
        }

        static void GetAggregatedValue(const A & state, A * output)
        {
            // do a shallow copy
            *output = state;
        }

        AggregateComplex_LAST(const char* name)
            : m_copier(name)
        {
            Reset();
        }

        void Add(A & value)
        {
            UpdateState(m_lastValue, value, m_copier);
        }

        void Aggregate(A * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(A * output)
        {
            GetAggregatedValue(m_lastValue, output);
        }

        void Reset()
        {
            m_lastValue.SetNull();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_copier.m_name);
            m_copier.m_allocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_LAST_VariableLength__Column_MinMemory);
        }
    };

    //
    // LAST aggregator for map type
    //
    template<typename K, typename V>
    class Aggregate_LAST<ScopeMapNative<K, V>> : public AggregateComplex_LAST<ScopeMapNative<K, V>, AggregateMapCopier<K, V> >
    {
    public:
        Aggregate_LAST(const char* name) : AggregateComplex_LAST(name) {}
    };

    //
    // LAST aggregator for array type
    //
    template<typename T>
    class Aggregate_LAST<ScopeArrayNative<T>> : public AggregateComplex_LAST<ScopeArrayNative<T>, AggregateArrayCopier<T> >
    {
    public:
        Aggregate_LAST(const char* name) : AggregateComplex_LAST(name) {}
    };

#if !defined(SCOPE_NO_UDT)
    //
    // LAST aggregator for UDT type
    //
    template<int UserDefinedTypeId, template<int> class UserDefinedType>
    class Aggregate_LAST<UserDefinedType<UserDefinedTypeId>>
    {
        UserDefinedType<UserDefinedTypeId> m_lastValue;

    public:
        Aggregate_LAST(const char* /*name*/)
        {
        }

        void Add(UserDefinedType<UserDefinedTypeId> & value)
        {
            m_lastValue = value;
        }

        void Aggregate(UserDefinedType<UserDefinedTypeId> * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(UserDefinedType<UserDefinedTypeId> * output)
        {
            *output = m_lastValue;
        }

        void Reset()
        {
            m_lastValue.Reset();
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };
#endif // SCOPE_NO_UDT

    //
    // MAX aggregator for non-null value types
    //
    template<typename T>
    class Aggregate_MAX
    {
        T m_negativeMin;
        T m_maxValue;

        template<class T2>
        struct Limit
        {
            static INLINE T2 min()
            {
                return numeric_limits<T2>::min();
            }
        };

        template<>
        struct Limit<float>
        {
            static INLINE float min()
            {
                return -numeric_limits<float>::max();
            }
        };

        template<>
        struct Limit<double>
        {
            static INLINE double min()
            {
                return -numeric_limits<double>::max();
            }
        };

    public:
        static void Init(T & state, T value)
        {
            state = value;
        }

        static bool UpdateState(T & state, T value)
        {
            if (state < value)
            {
                state = value;
                return true;
            }

            return false;
        }

        static void GetAggregatedValue(T state, T * output)
        {
            *output = state;
        }

        Aggregate_MAX(const char* /*name*/)
        {
            m_negativeMin = Limit<T>::min();
            Reset();
        }

        // Pass-by-value since for most numeric T => sizeof(T) < sizeof(T&)
        // I need "bool" for ARGMAX, if there is new max value we return true,
        // Otherwise, return false
        bool Add(T value)
        {
            return UpdateState(m_maxValue, value);
        }

        void Aggregate(T * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(T * output)
        {
            GetAggregatedValue(m_maxValue, output);
        }

        // needed for ARGMAX
        T & GetMax()
        {
            return m_maxValue;
        }

        void Reset()
        {
            Init(m_maxValue, m_negativeMin);
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // MAX aggregator for nullable value types
    //
    template<typename T>
    class Aggregate_MAX<class NativeNullable<T> >
    {
        NativeNullable<T> m_maxValue;

    public:
        static void Init(NativeNullable<T> & state, NativeNullable<T> value)
        {
            state = value;
        }

        static bool UpdateState(NativeNullable<T> & state, NativeNullable<T> value)
        {
            if (!value.IsNull() && (state.IsNull() || state < value))
            {
                state = value;
                return true;
            }
            else
            {
                return false;
            }
        }

        static void GetAggregatedValue(NativeNullable<T> state, NativeNullable<T> * output)
        {
            *output = state;
        }

        Aggregate_MAX(const char* /*name*/)
        {
        }

        // Pass-by-value since for most numeric T => sizeof(T) < sizeof(T&)
        // I need "bool" for ARGMAX
        bool Add(NativeNullable<T> & value)
        {
            return UpdateState(m_maxValue, value);
        }

        void Aggregate(NativeNullable<T> * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(NativeNullable<T> * output)
        {
            GetAggregatedValue(m_maxValue, output);
        }

        // needed for ARGMAX
        NativeNullable<T> & GetMax()
        {
            return m_maxValue;
        }

        void Reset()
        {
            // reset max value to null
            m_maxValue.reset();
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // MAX aggregator for nullable value types
    //
    template<typename T>
    class Aggregate_MAX<class ScopeSqlType::SqlNativeNullable<T> > : public Aggregate_MAX<class NativeNullable<T> >
    {
    public:
        Aggregate_MAX(const char* name) : Aggregate_MAX<class NativeNullable<T>>(name)
        {
        }
    };

    //
    // MAX aggregator for string/binary type
    //
    template<typename T>
    class AggregateFixedArray_MAX
    {
    private:
        AggregateFixedArrayCopier<T>    m_copier;
        FixedArrayType<T>               m_maxValue;

    public:
        static void Init(FixedArrayType<T> & state, const FixedArrayType<T> & value)
        {
            // do a shallow copy
            state = value;
        }

        template <typename Copier>
        static bool UpdateState(FixedArrayType<T> & state, const FixedArrayType<T> & value, Copier & copier)
        {
            if (value.IsNull())
            {
                return false;
            }

            int cmp;
            if (state.IsNull())
            {
                cmp = -1;
            }
            else
            {
                cmp = state.Compare(value);
            }

            if (cmp < 0)
            {
                copier.Copy(state, value);
            }

            return cmp < 0;
        }

        static void GetAggregatedValue(const FixedArrayType<T> & state, FixedArrayType<T> * output)
        {
            // do a shallow copy
            *output = state;
        }

        AggregateFixedArray_MAX(const char* name) : m_copier(name)
        {
            Reset();
        }

        // I need "bool" for ARGMAX
        bool Add(FixedArrayType<T> & value)
        {
            return UpdateState(m_maxValue, value, m_copier);
        }

        void Aggregate(FixedArrayType<T> * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(FixedArrayType<T> * output)
        {
            GetAggregatedValue(m_maxValue, output);
        }

        // needed for ARGMAX
        FixedArrayType<T> & GetMax() //TODO
        {
            return m_maxValue;
        }

        void Reset()
        {
            m_maxValue.SetNull();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_copier.m_name);
            m_copier.m_allocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_MAX_VariableLength__Column_MinMemory);
        }
    };

    //
    // MAX aggregator for FString
    //
    template<>
    class Aggregate_MAX<FString>
        : public AggregateFixedArray_MAX<char>
    {
    public:
        Aggregate_MAX(const char* name) : AggregateFixedArray_MAX(name)
        {
        }
    };

    //
    // MAX aggregator for FBinary
    //
    template<>
    class Aggregate_MAX<FBinary>
        : public AggregateFixedArray_MAX<unsigned char>
    {
    public:
        Aggregate_MAX(const char* name) : AggregateFixedArray_MAX(name)
        {
        }
    };

    //
    // MIN aggregator for non-null value types
    //
    template<typename T>
    class Aggregate_MIN
    {
        T m_minValue;

    public:
        static void Init(T & state, T value)
        {
            state = value;
        }

        static void UpdateState(T & state, T value)
        {
            if (value < state)
            {
                state = value;
            }
        }

        static void GetAggregatedValue(T state, T * output)
        {
            *output = state;
        }

        Aggregate_MIN(const char* /*name*/)
        {
            Reset();
        }

        // Pass-by-value since for most numeric T => sizeof(T) < sizeof(T&)
        void Add(T value)
        {
            UpdateState(m_minValue, value);
        }

        void Aggregate(T * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(T * output)
        {
            GetAggregatedValue(m_minValue, output);
        }

        void Reset()
        {
            Init(m_minValue, numeric_limits<T>::max());
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // MIN aggregator for null value types
    //
    template<typename T>
    class Aggregate_MIN<class NativeNullable<T> >
    {
        NativeNullable<T> m_minValue;

    public:
        static void Init(NativeNullable<T> & state, NativeNullable<T> value)
        {
            state = value;
        }

        static void UpdateState(NativeNullable<T> & state, NativeNullable<T> value)
        {
            if (!value.IsNull() && (state.IsNull() || value < state))
            {
                state = value;
            }
        }

        static void GetAggregatedValue(NativeNullable<T> state, NativeNullable<T> * output)
        {
            *output = state;
        }

        Aggregate_MIN(const char* /*name*/)
        {
        }

        void Add(NativeNullable<T> & value)
        {
            UpdateState(m_minValue, value);
        }

        void Aggregate(NativeNullable<T> * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(NativeNullable<T> * output)
        {
            GetAggregatedValue(m_minValue, output);
        }

        void Reset()
        {
            m_minValue.reset();
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // MIN aggregator for sql null value types.
    //
    template<typename T>
    class Aggregate_MIN<class ScopeSqlType::SqlNativeNullable<T> > : public Aggregate_MIN<class NativeNullable<T> >
    {
    public:
        Aggregate_MIN(const char* name) : Aggregate_MIN<class NativeNullable<T>>(name)
        {
        }
    };

    //
    // MIN aggregator for string/binary type
    //
    template<typename T>
    class AggregateFixedArray_MIN
    {
    private:
        AggregateFixedArrayCopier<T>    m_copier;
        FixedArrayType<T>               m_minValue;

    public:
        static void Init(FixedArrayType<T> & state, const FixedArrayType<T> & value)
        {
            // do a shallow copy
            state = value;
        }

        template <typename Copier>
        static void UpdateState(FixedArrayType<T> & state, const FixedArrayType<T> & value, Copier & copier)
        {
            if (!value.IsNull() && (state.IsNull() || value.Compare(state) < 0))
            {
                copier.Copy(state, value);
            }
        }

        static void GetAggregatedValue(const FixedArrayType<T> & state, FixedArrayType<T> * output)
        {
            // do a shallow copy
            *output = state;
        }

        AggregateFixedArray_MIN(const char* name) : m_copier(name)
        {
            Reset();
        }

        void Add(FixedArrayType<T> & value)
        {
            UpdateState(m_minValue, value, m_copier);
        }

        void Aggregate(FixedArrayType<T> * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(FixedArrayType<T> * output)
        {
            GetAggregatedValue(m_minValue, output);
        }

        void Reset()
        {
            m_minValue.SetNull();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_copier.m_name);
            m_copier.m_allocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_MIN_VariableLength__Column_MinMemory);
        }
    };

    //
    // MIN aggregator for FString
    //
    template<>
    class Aggregate_MIN<FString>
        : public AggregateFixedArray_MIN<char>
    {
    public:
        Aggregate_MIN(const char* name) : AggregateFixedArray_MIN(name)
        {
        }
    };

    //
    // MIN aggregator for FBinary
    //
    template<>
    class Aggregate_MIN<FBinary>
        : public AggregateFixedArray_MIN<unsigned char>
    {
    public:
        Aggregate_MIN(const char* name) : AggregateFixedArray_MIN(name)
        {
        }
    };

    //
    // Base template for the Accumulate operation (it's later specialized for signed, unsigned and floating point types)
    //
    template <class T, class Enable>
    class Accumulate
    {
    public:
        INLINE static T Add(T x, T y)
        {
            static_assert(false, "Accumulate() is not defined for this type");
        }
    };

    //
    // Checked "+" operation (for "signed" types)
    //
    template <class T>
    class Accumulate<T, typename enable_if<is_signed<T>::value && is_integral<T>::value, T>::type>
    {
    public:
        INLINE static T Add(T x, T y)
        {
            T result = x + y;

    // Check arithmetic overflow
    // I use XOR since "x ^ y < 0" means that 'x' and 'y' have different signs (0 needs special treating)
    // Whenever x == 0 || y == 0 no overflow may happen so non trivial cases are those when both 'x' and 'y' are non-zero
    // "result ^ x < 0" may be OK if we just pass through zero and NOK if we pass through T boundary so I also need to check sign of "x ^ y"
    // Necessary condition to pass through zero is to have "x ^ y < 0", so >= 0 is bad sign
    if ((x ^ result) < 0 &&
        (x ^ y) >= 0 &&
        x != 0 && y != 0)
        {
            throw RuntimeException(E_USER_AGGREGATE_OVERFLOW, "Integer", "SUM", typeid(T).name());
        }

            return result;
        }
    };

    //
    // Checked "+" operation (for "unsigned" types)
    //
    template <class T>
    class Accumulate<T, typename enable_if<is_unsigned<T>::value, T>::type>
    {
    public:
        INLINE static T Add(T x, T y)
        {
            T result = x + y;

            if (result < x || result < y)
            {
                throw RuntimeException(E_USER_AGGREGATE_OVERFLOW, "Integer", "SUM", typeid(T).name());
            }

            return result;
        }
    };

    //
    // Unchecked "+" operation (for "float/double/long double" types)
    //
    template <class T>
    class Accumulate<T, typename enable_if<is_floating_point<T>::value, T>::type>
    {
    public:
        INLINE static T Add(T x, T y)
        {
            return x + y;
        }
    };

    //
    // Checked "+" operation (for "decimal" types), ScopeDecimal throws exception on overflow
    //
    template <>
    class Accumulate<ScopeDecimal, ScopeDecimal>
    {
    public:
        INLINE static ScopeDecimal Add(const ScopeDecimal & x, const ScopeDecimal & y)
        {
            return x + y;
        }
    };

    //
    // Unchecked "+" operation (for "float/double/long double" types)
    //
    template <class T>
    class Accumulate<T, typename enable_if<ScopeSqlType::is_sql_type<T>::value, T>::type>
    {
    public:
        INLINE static T Add(T x, T y)
        {
            return x + y;
        }
    };

    //
    // SUM aggregator (T must be a numeric type)
    //
    template<typename Tx, typename Ty>
    class Aggregate_SUM
    {
        Ty m_sumValue;

    public:
        static void Init(Ty & state, Tx value)
        {
            state = 0;
            UpdateState(state, value);
        }

        static void UpdateState(Ty & state, Tx value)
        {
            // overflow checked only for integral types (bool, char, int, long, ...)
            state = Accumulate<Ty, Ty>::Add(state, (Ty)value);
        }

        static void GetAggregatedValue(Ty state, Ty * output)
        {
            *output = state;
        }

        Aggregate_SUM(const char* /*name*/)
        {
            // TODO we need static assert here to ensure that values of Tx can be converted to Ty
            Reset();
        }

        // Pass-by-value since for most numeric T => sizeof(T) < sizeof(T&)
        void Add(Tx value)
        {
            UpdateState(m_sumValue, value);
        }

        void Aggregate(Ty * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(Ty * output)
        {
            GetAggregatedValue(m_sumValue, output);
        }

        void Reset()
        {
            Init(m_sumValue, 0);
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // Specialization for nullable argument, non-nullable result.
    // This is a very rare case generated when there are multiple DISTINCT aggregates and coalesce
    // generation for the argument is skipped.
    //
    template<typename Tx, typename Ty>
    class Aggregate_SUM<typename NativeNullable<Tx>, typename Ty>
    {
        Ty m_sumValue;

    public:
        static void Init(Ty & state, NativeNullable<Tx> value)
        {
            state = 0;
            UpdateState(state, value);
        }

        static void UpdateState(Ty & state, NativeNullable<Tx> value)
        {
            if (!value.IsNull())
            {
                // overflow checked only for integral types (bool, char, int, long, ...)
                state = Accumulate<Ty, Ty>::Add(state, (Ty)value.get());
            }
        }

        static void GetAggregatedValue(Ty state, Ty * output)
        {
            *output = state;
        }

        Aggregate_SUM(const char* /*name*/)
        {
            // TODO we need static assert here to ensure that values of Tx can be converted to Ty
            Reset();
        }

        // Pass-by-value since for most numeric T => sizeof(T) < sizeof(T&)
        void Add(NativeNullable<Tx> value)
        {
            UpdateState(m_sumValue, value);
        }

        void Aggregate(Ty * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(Ty * output)
        {
            GetAggregatedValue(m_sumValue, output);
        }

        void Reset()
        {
            Init(m_sumValue, 0);
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // SUM aggregator for sql nullable argument and non-nullable result.
    //
    template<typename Tx, typename Ty>
    class Aggregate_SUM<typename ScopeSqlType::SqlNativeNullable<Tx>, typename Ty > : public Aggregate_SUM<typename NativeNullable<Tx>, typename Ty >
    {
    public:
        Aggregate_SUM(const char* name) : Aggregate_SUM<typename NativeNullable<Tx>, typename Ty>(name)
        {
        }
    };

    //
    // Specialization for nullable argument and result.
    // SQLIP flavor uses this specialization exclusively.
    //
    template<typename Tx, typename Ty>
    class Aggregate_SUM<typename NativeNullable<Tx>, typename NativeNullable<Ty>>
    {
        bool m_fEmpty;
        Ty m_sumValue;

    public:
        static void Init(Ty & stateValue, bool & stateEmpty, NativeNullable<Tx> value)
        {
            stateValue = 0;
            stateEmpty = true;
            UpdateState(stateValue, stateEmpty, value);
        }

        static void UpdateState(Ty & stateValue, bool & stateEmpty, NativeNullable<Tx> value)
        {
            if (!value.IsNull())
            {
                stateEmpty = false;

                stateValue = Accumulate<Ty, Ty>::Add(stateValue, (Ty)value.get());
            }
        }

        static void GetAggregatedValue(Ty stateValue, bool stateEmpty, NativeNullable<Ty> * output)
        {
            *output = stateEmpty ? NativeNullable<Ty>() : stateValue;
        }

        Aggregate_SUM(const char* /*name*/)
        {
            Reset();
        }

        // Pass-by-value since for most numeric T => sizeof(T) < sizeof(T&)
        void Add(NativeNullable<Tx> value)
        {
            UpdateState(m_sumValue, m_fEmpty, value);
        }

        void Aggregate(NativeNullable<Ty> * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(NativeNullable<Ty> * output)
        {
            GetAggregatedValue(m_sumValue, m_fEmpty, output);
        }

        void Reset()
        {
            m_fEmpty = true;
            m_sumValue = 0;
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // MIN aggregator for sql nullable argument and result.
    //
    template<typename Tx, typename Ty>
    class Aggregate_SUM<class ScopeSqlType::SqlNativeNullable<Tx>, class ScopeSqlType::SqlNativeNullable<Ty> > : public Aggregate_SUM<class NativeNullable<Tx>, class NativeNullable<Ty> >
    {
    public:
        Aggregate_SUM(const char* name) : Aggregate_SUM<class NativeNullable<Tx>, class NativeNullable<Ty>>(name)
        {
        }
    };

    //
    // SUM(x*x) aggregator for STDEV and VAR
    //
    template<typename Tx, typename Ty>
    class Aggregate_UNCHECKED_SUMx2
    {
        Ty m_sumValue;

    public:
        Aggregate_UNCHECKED_SUMx2(const char* /*name*/)
        {
            m_sumValue = 0;
        }

        void Add(Tx & value)
        {
            Ty castedValue = (Ty)value;
            m_sumValue += castedValue * castedValue;
        }

        void Aggregate(Ty * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(Ty * output)
        {
            *output = m_sumValue;
        }

        void Reset()
        {
            m_sumValue = 0;
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };


    //
    // UNCHECKED_SUM aggregator
    //
    template<typename Tx, typename Ty>
    class Aggregate_UNCHECKED_SUM
    {
        Ty m_sumValue;

    public:
        static void Init(Ty & state, Tx value)
        {
            state = (Ty)value;
        }

        static void UpdateState(Ty & state, Tx value)
        {
            state += (Ty)value;
        }

        static void GetAggregatedValue(Ty state, Ty * output)
        {
            *output = state;
        }


        Aggregate_UNCHECKED_SUM(const char* /*name*/)
        {
            Reset();
        }

        void Add(Tx & value)
        {
            UpdateState(m_sumValue, value);
        }

        void Aggregate(Ty * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(Ty * output)
        {
            GetAggregatedValue(m_sumValue, output);
        }

        void Reset()
        {
            // TODO we need static assert here to ensure that values of Tx can be converted to Ty
            Init(m_sumValue, 0);
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // UNCHECKED_SUM aggregator (nullable types)
    //
    template<typename Tx, typename Ty>
    class Aggregate_UNCHECKED_SUM<typename NativeNullable<Tx>, Ty>
    {
        Ty m_sumValue;

    public:
        Aggregate_UNCHECKED_SUM(const char* /*name*/)
        {
            m_sumValue = 0;
        }

        void Add(NativeNullable<Tx> & value)
        {
            if (!value.IsNull())
            {
                m_sumValue += (Ty)value.get();
            }
        }

        void Aggregate(Ty * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(Ty * output)
        {
            *output = m_sumValue;
        }

        void Reset()
        {
            m_sumValue = 0;
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // UNCHECKED_SUM aggregator (sql nullable argument).
    //
    template<typename Tx, typename Ty>
    class Aggregate_UNCHECKED_SUM<typename ScopeSqlType::SqlNativeNullable<Tx>, typename Ty >
        : public Aggregate_UNCHECKED_SUM<typename NativeNullable<Tx>, typename Ty >
    {
    public:
        Aggregate_UNCHECKED_SUM(const char* name) : Aggregate_UNCHECKED_SUM<typename NativeNullable<Tx>, typename Ty>(name)
        {
        }
    };

    //
    // SUM(x*x) aggregator for STDEV and VAR (nullable types)
    //
    template<typename Tx, typename Ty>
    class Aggregate_UNCHECKED_SUMx2<typename NativeNullable<Tx>, Ty>
    {
        Ty m_sumValue;

    public:
        Aggregate_UNCHECKED_SUMx2(const char* /*name*/)
        {
            m_sumValue = 0;
        }

        void Add(NativeNullable<Tx> & value)
        {
            if (!value.IsNull())
            {
                Ty castedValue = (Ty)value.get();
                m_sumValue += castedValue * castedValue;
            }
        }

        void Aggregate(Ty * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(Ty * output)
        {
            *output = m_sumValue;
        }

        void Reset()
        {
            m_sumValue = 0;
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // SUM(x*x) aggregator for STDEV and VAR (sql nullable types).
    //
    template<typename Tx, typename Ty>
    class Aggregate_UNCHECKED_SUMx2<typename ScopeSqlType::SqlNativeNullable<Tx>, typename Ty >
        : public Aggregate_UNCHECKED_SUMx2<typename NativeNullable<Tx>, typename Ty >
    {
    public:
        Aggregate_UNCHECKED_SUMx2(const char* name) : Aggregate_UNCHECKED_SUMx2<typename NativeNullable<Tx>, typename Ty>(name)
        {
        }
    };

    //
    // ARGMAX aggregator for all kinds of types
    //
    template<typename Tx, typename Ty>
    class Aggregate_ARGMAX
    {
        Aggregate_MAX<Tx>    m_maxAggX;
        Aggregate_MAX<Ty> m_maxAggY;
        string m_name;

    public:
        Aggregate_ARGMAX(const char* name) : m_name(name), m_maxAggX(name), m_maxAggY(name)
        {
        }

        bool Add(Tx & valueX, Ty & valueY)
        {
            if (m_maxAggX.Add(valueX))
            {
                m_maxAggY.Reset();
                return m_maxAggY.Add(valueY);
            }
            else if (m_maxAggX.GetMax() == valueX)
            {
                return m_maxAggY.Add(valueY);
            }
            else
            {
                return false;
            }
        }

        void Aggregate(Ty * output)
        {
            m_maxAggX.Reset(); // reset aggregator

            m_maxAggY.Aggregate(output);
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_name);
            m_maxAggX.WriteRuntimeStats(node);
            m_maxAggY.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements(m_maxAggX.GetOperatorRequirements()).Add(m_maxAggY.GetOperatorRequirements());
        }
    };

    template<typename T>
    class DistinctHashTable : public unordered_set<T, hash<T>, equal_to<T>, STLAllocator<T> >
    {
        typedef unordered_set<T, hash<T>, equal_to<T>, STLAllocator<T> > BaseType;

        static const int    c_defaultBucketSize = 256;

        // Load factor change start from 10M rows. Each time we will double the limit as well as the load factor.
        static const int    c_loadFactorChangeLimit = 10000000;

        IncrementalAllocator & m_alloc;
        int                  m_loadFactorChangeThreshold;

    public:
        DistinctHashTable(IncrementalAllocator & alloc) : BaseType(c_defaultBucketSize, hash<T>(), equal_to<T>(), STLAllocator<T>(alloc)),
            m_alloc(alloc),
            m_loadFactorChangeThreshold(c_loadFactorChangeLimit)
        {
        }

        DistinctHashTable& operator=(const DistinctHashTable &other) = delete;

        _Pairib insert(const T & val)
        {
            // If hash table size grow beyond current threshold, we need to change load factor.
            // For now we just keep the heuristic simple. Our T size range from 4 bytes to 32 bytes.
            // The hash memory consumption is proportion to T size and number of element. This heuristics is based on average value.
            // In the future if we need to consider memory consumption, we can easily extend it. However, we need to have global memory allocator
            // to track large memory allocation for hash table to do that.
            if (size() > m_loadFactorChangeThreshold)
            {
                // Multiple threshold by 2 and set new load factor to current value times 2.
                // The heuristics basically will double load factor at 10M, 20M, 40M, 80M ....
                // The larger the load factor the less likely the hash table will resize. This
                // will prevent hash table to take too much memory when it expand. Currently,
                // each time hash table expands it will multiple 8 times.
                m_loadFactorChangeThreshold *= 2;
                max_load_factor(max_load_factor()*(float)2.0);
            }

            return BaseType::insert(val);
        }
    };

    //
    // COUNT aggregator for all kinds of types
    //
    template<typename T>
    class Aggregate_COUNT
    {
        T m_countValue;

    public:
        Aggregate_COUNT(const char* /*name*/)
        {
            m_countValue = 0;
        }

        // Pass-by-value since for most numeric T => sizeof(T) < sizeof(T&)
        void Add()
        {
            m_countValue++;
        }

        void Aggregate(T * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(T * output)
        {
            *output = m_countValue;
        }

        void Reset()
        {
            m_countValue = 0;
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // COUNTIF aggregator for all kinds of types
    //
    template<typename Tx, typename Ty>
    class Aggregate_COUNTIF
    {
        Aggregate_COUNT<Ty> m_aggCount;
        string m_name;

    public:
        Aggregate_COUNTIF(const char* name) : m_name(name), m_aggCount(name)
        {
        }

        // Pass-by-value since for most numeric T => sizeof(T) < sizeof(T&)
        void Add(Tx input)
        {
            if (input)
            {
                m_aggCount.Add();
            }
        }

        void Aggregate(Ty * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(Ty * output)
        {
            m_aggCount.GetValue(output);
        }

        void Reset()
        {
            m_aggCount.Reset();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_name);
            m_aggCount.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // DISTINCT aggregator for numeric types
    //
    template<typename T>
    class Aggregate_DISTINCT
    {
        IncrementalAllocator m_containerAllocator;

        // TODO: Should we override TR1 hash? In C# we have dedicated hash for bool, int and long
        typedef DistinctHashTable<T> THashTable;
        std::unique_ptr<THashTable> m_hashTable;
        string m_name;

        // don't control the memory, if it go over 6G it will fail in managed runtime as well.
        static const SIZE_T c_defaultAllocatorSize = 6 * 1024ULL * 1024ULL * 1024ULL;

    public:
        Aggregate_DISTINCT(const char* name) : m_name(name)
        {
            m_containerAllocator.Init(c_defaultAllocatorSize, m_name);
            m_hashTable.reset(new THashTable(m_containerAllocator));
        }

        // Pass-by-value since for most numeric T => sizeof(T) < sizeof(T&)
        bool Add(T value)
        {
            THashTable::iterator it = m_hashTable->find(value);
            if (it != m_hashTable->end())
            {
                return false;
            }

            return m_hashTable->insert(value).second;
        }

        void Reset()
        {
            m_containerAllocator.Reset();
            m_hashTable.reset(new THashTable(m_containerAllocator));
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_name);
            m_containerAllocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_DISTINCT_FixedLength__Size_MinMemory);
        }
    };

    //
    // DISTINCT aggregator for numeric types
    //
    template<typename T>
    class Aggregate_DISTINCT<class NativeNullable<T> >
    {
        IncrementalAllocator m_containerAllocator;

        // TODO: Should we override TR1 hash? In C# we have dedicated hash for bool, int and long
        typedef DistinctHashTable<NativeNullable<T>> THashTable;
        std::unique_ptr<THashTable> m_hashTable;
        string m_name;

        // don't control the memory, if it go over 6G it will fail in managed runtime as well.
        static const SIZE_T c_defaultAllocatorSize = 6 * 1024ULL * 1024ULL * 1024ULL;
        static const int    c_defaultBucketSize = 256;

    public:
        Aggregate_DISTINCT(const char* name) : m_name(name)
        {
            m_containerAllocator.Init(c_defaultAllocatorSize, m_name);
            m_hashTable.reset(new THashTable(m_containerAllocator));
        }

        // Pass-by-value since for most numeric T => sizeof(T) < sizeof(T&)
        bool Add(NativeNullable<T> & value)
        {
            if (!value.IsNull())
            {
                THashTable::iterator it = m_hashTable->find(value);
                if (it != m_hashTable->end())
                {
                    return false;
                }

                return m_hashTable->insert(value).second;
            }
            else
                return false;
        }

        void Reset()
        {
            m_containerAllocator.Reset();
            m_hashTable.reset(new THashTable(m_containerAllocator));
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_name);
            m_containerAllocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_DISTINCT_FixedLength__Size_MinMemory);
        }
    };

    //
    // DISTINCT aggregator for sql null value types.
    //
    template<typename T>
    class Aggregate_DISTINCT<class ScopeSqlType::SqlNativeNullable<T> > : public Aggregate_DISTINCT<class NativeNullable<T> >
    {
    public:
        Aggregate_DISTINCT(const char* name) : Aggregate_DISTINCT<class NativeNullable<T>>(name)
        {
        }
    };

    //
    // DISTINCT aggregator for string type
    //
    template<typename T>
    class AggregateFixedArray_DISTINCT
    {
        IncrementalAllocator m_allocator;
        IncrementalAllocator m_containerAllocator;

        typedef DistinctHashTable<FixedArrayType<T>> THashTable;
        std::unique_ptr<THashTable> m_hashTable;
        string m_name;

        // don't control the memory, if it go over 6G it will fail in managed runtime as well.
        static const SIZE_T c_defaultAllocatorSize = 6 * 1024ULL * 1024ULL * 1024ULL;
        static const int    c_defaultBucketSize = 256;

    public:
        AggregateFixedArray_DISTINCT(const char* name) : m_name(name)
        {
            m_allocator.Init(c_defaultAllocatorSize, m_name + "_DeepCopy");
            m_containerAllocator.Init(c_defaultAllocatorSize, m_name);
            m_hashTable.reset(new THashTable(m_containerAllocator));
        }

        bool Add(FixedArrayType<T> & value)
        {
            // Null is not counted.
            if (value.IsNull())
            {
                return false;
            }

            THashTable::iterator it = m_hashTable->find(value);
            if (it != m_hashTable->end())
            {
                return false;
            }

            // make deep copy
            FixedArrayType<T> str(value, &m_allocator);

            return m_hashTable->insert(str).second;
        }

        void Reset()
        {
            m_allocator.Reset();
            m_containerAllocator.Reset();
            m_hashTable.reset(new THashTable(m_containerAllocator));
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_name);
            m_allocator.WriteRuntimeStats(node);
            m_containerAllocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_DISTINCT_VariableLength__Size_MinMemory);
        }
    };

    //
    // DISTINCT aggregator for FString
    //
    template<>
    class Aggregate_DISTINCT<FString>
        : public AggregateFixedArray_DISTINCT<char>
    {
    public:
        Aggregate_DISTINCT(const char* name) : AggregateFixedArray_DISTINCT(name)
        {
        }
    };

    //
    // DISTINCT aggregator for FBinary
    //
    template<>
    class Aggregate_DISTINCT<FBinary>
        : public AggregateFixedArray_DISTINCT<unsigned char>
    {
    public:
        Aggregate_DISTINCT(const char* name) : AggregateFixedArray_DISTINCT(name)
        {
        }
    };

    //
    // MAP_AGG aggregator
    //
    template<typename K, typename V>
    class Aggregate_MAP_AGG
    {
        string m_name;
        RowEntityAllocator m_allocator;
        std::shared_ptr<ScopeMapNative<K, V>> m_accum;
        bool m_start;  // indicate a new aggregation iteration starts

    public:
        Aggregate_MAP_AGG(const char* name)
            : m_allocator(RowEntityAllocator::ColumnContent), m_name(name), m_start(false)
        {
            m_allocator.Init(Configuration::GetGlobal().GetMaxVariableColumnSize(), m_name);
            m_accum.reset(new ScopeMapNative<K, V>(&m_allocator));
        }

        bool Add(K & key, V & value)
        {
            if (!m_start)
            {
                Reset();
                m_start = true;
            }

            m_accum->Add(key, value);
            return true;
        }

        void Aggregate(ScopeMapNative<K, V> * output)
        {
            *output = *m_accum;
            m_start = false;
        }

        void Reset()
        {
            m_allocator.Reset();
            m_accum->Reset(&m_allocator);
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_name);
            m_allocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_MAP_AGG__Column_MinMemory);
        }
    };

    //
    // ARRAY_AGG aggregator
    //
    template<typename T>
    class Aggregate_ARRAY_AGG
    {
        string m_name;
        RowEntityAllocator m_allocator;
        std::shared_ptr<ScopeArrayNative<T>> m_accum;
        bool m_start;

    public:
        Aggregate_ARRAY_AGG(const char* name)
            : m_allocator(RowEntityAllocator::ColumnContent), m_name(name), m_start(false)
        {
            m_allocator.Init(Configuration::GetGlobal().GetMaxVariableColumnSize(), m_name);
            m_accum.reset(new ScopeArrayNative<T>(&m_allocator));
        }

        bool Add(T & v)
        {
            if (!m_start)
            {
                Reset();
                m_start = true;
            }

            m_accum->Append(v);
            return true;
        }

        void Aggregate(ScopeArrayNative<T> * output)
        {
            *output = *m_accum;
            m_start = false;
        }

        void Reset()
        {
            m_allocator.Reset();
            m_accum->Reset(&m_allocator);
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_name);
            m_allocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_ARRAY_AGG__Column_MinMemory);
        }
    };

#pragma region PluginTypeSystem
    template<typename K, typename V>
    class Aggregate_MAP_AGG_PTS : public Aggregate_MAP_AGG<K, V>
    {
    public:
        Aggregate_MAP_AGG_PTS(const char* name)
            : Aggregate_MAP_AGG<K, V>(name)
        {
            }
    };

    template<typename T>
    class Aggregate_ARRAY_AGG_PTS : public Aggregate_ARRAY_AGG<T>
    {
    public:
        Aggregate_ARRAY_AGG_PTS(const char* name)
            : Aggregate_ARRAY_AGG<T>(name)
        {
            }
    };

    template<typename T>
    class Aggregate_FIRST_PTS : public Aggregate_FIRST<T>
    {
    public:
        Aggregate_FIRST_PTS(const char* name) : Aggregate_FIRST(name)
        {
        }
    };

    //
    // COUNT aggregator for all kinds of types
    //
    template<typename T>
    class Aggregate_COUNT_PTS : public Aggregate_COUNT<T>
    {
    public:
        Aggregate_COUNT_PTS(const char* name) : Aggregate_COUNT<T>(name)
        {
        }
    };

    //
    // COUNTIF aggregator for all kinds of types
    //
    template<typename Tx, typename Ty>
    class Aggregate_COUNTIF_PTS
    {
        Aggregate_COUNT_PTS<Ty> m_aggCount;
        string m_name;

    public:
        Aggregate_COUNTIF_PTS(const char* name) : m_name(name), m_aggCount(name)
        {
        }

        // Pass-by-value since for most numeric T => sizeof(T) < sizeof(T&)
        void Add(Tx input)
        {
            if (input)
            {
                m_aggCount.Add();
            }
        }

        void Aggregate(Ty * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(Ty * output)
        {
            m_aggCount.GetValue(output);
        }

        void Reset()
        {
            m_aggCount.Reset();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_name);
            m_aggCount.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    template<typename T>
    class DistinctHashTable_PTS : public unordered_set<T, hash_pts<T>, equal_to_pts<T>, STLAllocator<T> >
    {
        typedef unordered_set<T, hash_pts<T>, equal_to_pts<T>, STLAllocator<T> > BaseType;

        static const int    c_defaultBucketSize = 256;

        // Load factor change start from 10M rows. Each time we will double the limit as well as the load factor.
        static const int    c_loadFactorChangeLimit = 10000000;

        IncrementalAllocator & m_alloc;
        int                  m_loadFactorChangeThreshold;

    public:
        DistinctHashTable_PTS(IncrementalAllocator & alloc) : BaseType(c_defaultBucketSize, hash_pts<T>(), equal_to_pts<T>(), STLAllocator<T>(alloc)),
            m_alloc(alloc),
            m_loadFactorChangeThreshold(c_loadFactorChangeLimit)
        {
        }

        DistinctHashTable_PTS& operator=(const DistinctHashTable_PTS &other) = delete;

        _Pairib insert(const T & val)
        {
            // If hash table size grow beyond current threshold, we need to change load factor.
            // For now we just keep the heuristic simple. Our T size range from 4 bytes to 32 bytes.
            // The hash memory consumption is proportion to T size and number of element. This heuristics is based on average value.
            // In the future if we need to consider memory consumption, we can easily extend it. However, we need to have global memory allocator
            // to track large memory allocation for hash table to do that.
            if (size() > m_loadFactorChangeThreshold)
            {
                // Multiple threshold by 2 and set new load factor to current value times 2.
                // The heuristics basically will double load factor at 10M, 20M, 40M, 80M ....
                // The larger the load factor the less likely the hash table will resize. This
                // will prevent hash table to take too much memory when it expand. Currently,
                // each time hash table expands it will multiple 8 times.
                m_loadFactorChangeThreshold *= 2;
                max_load_factor(max_load_factor()*(float)2.0);
            }

            return BaseType::insert(val);
        }
    };

    //
    // FIRST aggregator for all types
    //
    template<class AggregateType>
    class AggregateBase_FIRST_PTS
    {
    private:
        AggregateBaseCopier<AggregateType>      m_copier;
        AggregateType                           m_firstValue;
        bool                                    m_firstRow;

    public:
        static void Init(AggregateType & state, const AggregateType & value)
        {
            // do a shallow copy
            ShallowCopier_PTS<AggregateType>::Copy(value, state);
        }

        template <typename Copier>
        static void UpdateState(AggregateType & /*state*/, const AggregateType & /*value*/, Copier & /*copier*/)
        {
        }

        static void GetAggregatedValue(const AggregateType & state, AggregateType * output)
        {
            // do a shallow copy
            ShallowCopier_PTS<AggregateType>::Copy(state, *output);
        }

        AggregateBase_FIRST_PTS(const char* name) :
            m_copier(name),
            m_firstRow(true)
        {
            Reset();
        }

        void Add(AggregateType & value)
        {
            if (m_firstRow)
            {
                DeepCopier_PTS<AggregateType, AggregateBaseCopier<AggregateType> >::Copy(value, m_firstValue, m_copier);
                m_firstRow = false;
            }
        }

        void Aggregate(AggregateType * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(AggregateType * output)
        {
            GetAggregatedValue(m_firstValue, output);
        }

        void Reset()
        {
            m_firstRow = true;
            NullHelper_PTS<AggregateType>::SetNull(m_firstValue);
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_copier.m_name);
            m_copier.m_allocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_FIRST_VariableLength__Column_MinMemory);
        }
    };


    template<typename T, bool fSkipNulls, bool CHECK_NULL = IsNullablePrimaryTypeTraits_PTS<T>::val && !fSkipNulls>
    class Aggregate_FIRST_ANY_PTS : public AggregateBase_FIRST_PTS<T>
    {
    public:
        Aggregate_FIRST_ANY_PTS(const char* name) : AggregateBase_FIRST_PTS(name)
        {
        }
    };

    template<typename T, bool fSkipNulls>
    class Aggregate_FIRST_ANY_PTS<T, fSkipNulls, true> : public AggregateBase_FIRST_PTS<T>
    {
    public:
        Aggregate_FIRST_ANY_PTS(const char* name) : AggregateBase_FIRST_PTS(name)
        {
        }

        void Add(const T & value)
        {
            if (m_firstRow)
            {
                if (NullHelper_PTS<AggregateType>::IsNull(value))
                {
                    return;
                }

                DeepCopier_PTS<AggregateType, AggregateBaseCopier<T> >::Copy(value, m_firstValue, m_copier);
                m_firstRow = false;
            }
        }
    };

    //
    // ANY_VALUE aggregator - skips nulls
    //
    template<typename T>
    class Aggregate_ANY_VALUE_PTS : public Aggregate_FIRST_ANY_PTS<T, true>
    {
    public:
        Aggregate_ANY_VALUE_PTS(const char* name) : Aggregate_FIRST_ANY_PTS(name)
        {
        }
    };

    //
    // LAST aggregator
    //
    template<typename AggregateType>
    class Aggregate_LAST_PTS
    {
    private:
        AggregateBaseCopier<AggregateType>    m_copier;
        AggregateType                          m_lastValue;

    public:
        static void Init(AggregateType & state, const AggregateType & value)
        {
            // do a shallow copy
            ShallowCopier_PTS<AggregateType>::Copy(value, state);
        }

        template <typename Copier>
        static void UpdateState(AggregateType & state, const AggregateType & value, Copier & copier)
        {
            DeepCopier_PTS<AggregateType, AggregateBaseCopier<AggregateType> >::Copy(value, state, copier);
        }

        static void UpdateState(AggregateType & state, const AggregateType & value)
        {
            DeepCopier_PTS<AggregateType, AggregateBaseCopier<AggregateType> >::Copy(value, state);
        }

        static void GetAggregatedValue(const AggregateType & state, AggregateType * output)
        {
            // do a shallow copy
            ShallowCopier_PTS<AggregateType>::Copy(state, *output);
        }

        Aggregate_LAST_PTS(const char* name) : m_copier(name)
        {
            Reset();
        }

        void Add(AggregateType & value)
        {
            UpdateState(m_lastValue, value, m_copier);
        }

        void Aggregate(AggregateType * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(AggregateType * output)
        {
            GetAggregatedValue(m_lastValue, output);
        }

        void Reset()
        {
            NullHelper_PTS<AggregateType>::SetNull(m_lastValue);
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_copier.m_name);
            m_copier.m_allocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_LAST_VariableLength__Column_MinMemory);
        }
    };

    //
    // MAX aggregator
    //
    template<typename T>
    class Aggregate_MAX_PTS
    {
        template<typename T2, bool NULLABLE = IsNullablePrimaryTypeTraits_PTS<T>::val>
        class ResetHelper
        {
        public:
            static INLINE void Reset(T2 & value, T2 const & minValue)
            {
                ShallowCopier_PTS<T2>::Copy(minValue, value);
            }
        };

        template<typename T2>
        class ResetHelper<T2, true>
        {
        public:
            static INLINE void Reset(T2 & value, T2 const & minValue)
            {
                value.SetNull();
            }
        };

    private:
        AggregateBaseCopier<T>    m_copier;
        T                          m_maxValue;
        T                          m_negativeMin;

    public:
        static void Init(T & state, const T & value)
        {
            // do a shallow copy
            ShallowCopier_PTS<T>::Copy(value, state);
        }

        template <typename Copier>
        static bool UpdateState(T & state, const T & value, Copier & copier)
        {
            if (NullHelper_PTS<T>::UpdateToLargerValue(state, value))
            {
                DeepCopier_PTS<T, Copier>::Copy(value, state, copier);
                return true;
            }

            return false;
        }

        static bool UpdateState(T & state, const T & value)
        {
            if (NullHelper_PTS<T>::UpdateToLargerValue(state, value))
            {
                DeepCopier_PTS<T, AggregateBaseCopier<T>>::Copy(value, state);
                return true;
            }

            return false;
        }

        static void GetAggregatedValue(const T & state, T * output)
        {
            // do a shallow copy
            ShallowCopier_PTS<T>::Copy(state, *output);
        }

        Aggregate_MAX_PTS(const char* name) : m_copier(name)
        {
            T min = numeric_limits_pts<T>::min();
            ShallowCopier_PTS<T>::Copy(min, m_negativeMin);
            Reset();
        }

        // I need "bool" for ARGMAX
        bool Add(T & value)
        {
            return UpdateState<AggregateBaseCopier<T> >(m_maxValue, value, m_copier);
        }

        void Aggregate(T * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(T * output)
        {
            GetAggregatedValue(m_maxValue, output);
        }

        // needed for ARGMAX
        T & GetMax() //TODO
        {
            return m_maxValue;
        }

        void Reset()
        {
            ResetHelper<T>::Reset(m_maxValue, m_negativeMin);
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_copier.m_name);
            m_copier.m_allocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_MAX_VariableLength__Column_MinMemory);
        }
    };

    //
    // MIN aggregator
    //
    template<typename T>
    class Aggregate_MIN_PTS
    {
        template<class T2, bool NULLABLE = IsNullablePrimaryTypeTraits_PTS<T>::val>
        struct ResetHelper
        {
            static INLINE void Reset(T2 & value, T2 const & maxValue)
            {
                ShallowCopier_PTS<T2>::Copy(maxValue, value);
            }
        };

        template<class T2>
        struct ResetHelper<T2, true>
        {
            static INLINE void Reset(T2 & value, T2 const & maxValue)
            {
                value.SetNull();
            }
        };

        AggregateBaseCopier<T>    m_copier;
        T                          m_minValue;

    public:
        static void Init(T & state, T value)
        {
            ShallowCopier_PTS<T>::Copy(value, state);
        }

        template <typename Copier>
        static void UpdateState(T & state, T value, Copier & copier)
        {
            if (value < state)
            {
                DeepCopier_PTS<T, AggregateBaseCopier<T> >::Copy(value, state, copier);
            }
        }

        static void UpdateState(T & state, T value)
        {
            if (value < state)
            {
                DeepCopier_PTS<T, AggregateBaseCopier<T> >::Copy(value, state);
            }
        }

        static void GetAggregatedValue(T state, T * output)
        {
            ShallowCopier_PTS<T>::Copy(state, *output);
        }

        Aggregate_MIN_PTS(const char* name) : m_copier(name)
        {
            Reset();
        }

        // Pass-by-value since for most numeric T => sizeof(T) < sizeof(T&)
        void Add(T value)
        {
            UpdateState<AggregateBaseCopier<T> >(m_minValue, value, m_copier);
        }

        void Aggregate(T * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(T * output)
        {
            GetAggregatedValue(m_minValue, output);
        }

        void Reset()
        {
            ResetHelper<T>::Reset(m_minValue, numeric_limits_pts<T>::max());
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_copier.m_name);
            m_copier.m_allocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_MIN_VariableLength__Column_MinMemory);
        }
    };

    //
    // ARGMAX aggregator for all kinds of types
    //
    template<typename Tx, typename Ty>
    class Aggregate_ARGMAX_PTS
    {
        Aggregate_MAX_PTS<Tx>    m_maxAggX;
        Aggregate_MAX_PTS<Ty> m_maxAggY;
        string m_name;

    public:
        Aggregate_ARGMAX_PTS(const char* name) : m_name(name), m_maxAggX(name), m_maxAggY(name)
        {
        }

        bool Add(Tx & valueX, Ty & valueY)
        {
            if (m_maxAggX.Add(valueX))
            {
                m_maxAggY.Reset();
                return m_maxAggY.Add(valueY);
            }
            else if (m_maxAggX.GetMax() == valueX)
            {
                return m_maxAggY.Add(valueY);
            }
            else
            {
                return false;
            }
        }

        void Aggregate(Ty * output)
        {
            m_maxAggX.Reset(); // reset aggregator

            m_maxAggY.Aggregate(output);
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_name);
            m_maxAggX.WriteRuntimeStats(node);
            m_maxAggY.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements(m_maxAggX.GetOperatorRequirements()).Add(m_maxAggY.GetOperatorRequirements());
        }
    };

    //
    // Base template for the Accumulate_PTS operation (it's later specialized for signed, unsigned and floating point types)
    //
    template <class T, class Enable, bool ATOMIC = is_atomic_pts<T>::value>
    class Accumulate_PTS
    {
    public:
        INLINE static T Add(T x, T y)
        {
            static_assert(false, "Accumulate_PTS() is not defined for this type");
        }
    };

    //
    // Checked "+" operation (for "signed" types)
    //
    template <class T>
    class Accumulate_PTS<T, typename enable_if<is_signed<T>::value && is_integral<T>::value, T>::type, true>
    {
    public:
        INLINE static T Add(T x, T y)
        {
            T result = x + y;

            // Check arithmetic overflow
            // I use XOR since "x ^ y < 0" means that 'x' and 'y' have different signs (0 needs special treating)
            // Whenever x == 0 || y == 0 no overflow may happen so non trivial cases are those when both 'x' and 'y' are non-zero
            // "result ^ x < 0" may be OK if we just pass through zero and NOK if we pass through T boundary so I also need to check sign of "x ^ y"
            // Necessary condition to pass through zero is to have "x ^ y < 0", so >= 0 is bad sign
            if ((x ^ result) < 0 &&
            (x ^ y) >= 0 &&
            x != 0 && y != 0)
            {
                throw RuntimeException(E_USER_AGGREGATE_OVERFLOW, "Integer", "SUM", typeid(T).name());
            }
    
            return result;
        }
    };

    //
    // Checked "+" operation (for "unsigned" types)
    //
    template <class T>
    class Accumulate_PTS<T, typename enable_if<is_unsigned<T>::value, T>::type, true>
    {
    public:
        INLINE static T Add(T x, T y)
        {
            T result = x + y;

            if (result < x || result < y)
            {
                throw RuntimeException(E_USER_AGGREGATE_OVERFLOW, "Integer", "SUM", typeid(T).name());
            }

            return result;
        }
    };

    //
    // Unchecked "+" operation (for "float/double/long double" types)
    //
    template <class T>
    class Accumulate_PTS<T, typename enable_if<is_floating_point<T>::value, T>::type, true>
    {
    public:
        INLINE static T Add(T x, T y)
        {
            return x + y;
        }
    };

    //
    // Unchecked "+" operation (for "float/double/long double" types)
    //
    template <class T>
    class Accumulate_PTS<T, typename enable_if<is_floating_point_pts<T>::value || is_signed_pts<T>::value && is_integral_pts<T>::value || is_unsigned_pts<T>::value, T>::type, false>
    {
    public:
        INLINE static T Add(T x, T y)
        {
            return x.AddByChecked(y);
        }
    };

    //
    // SUM aggregator (T must be a numeric type)
    //
    template <typename Tx, typename Ty, bool BOTH_NULLABLE = IsNullablePrimaryTypeTraits_PTS<Tx>::val && IsNullablePrimaryTypeTraits_PTS<Ty>::val>
    class Aggregate_SUM_PTS
    {
        Ty m_sumValue;
        bool m_fEmpty;

    public:
        static void Init(Ty & state, Tx value)
        {
            NumericOpHelper_PTS<Ty>::SetZero(state);
            UpdateState(state, value);
        }

        template <typename Ty_v>
        static void Init(Ty_v & state, bool & stateEmpty, Tx value)
        {
            NumericOpHelper_PTS<Ty_v>::SetZero(state);
            stateEmpty = true;
            UpdateState(state, stateEmpty, value);
        }

        static void UpdateState(Ty & state, Tx value)
        {
            if (!NullHelper_PTS<Tx>::IsNull(value))
            {
                // overflow checked only for integral types (bool, char, int, long, ...)
                ShallowCopier_PTS<Ty>::Copy(Accumulate_PTS<Ty, Ty>::Add(state, ScopeCast<Tx, Ty>::get(value)), state);
            }
        }

        template <typename Ty_v>
        static void UpdateState(Ty_v & state, bool & stateEmpty, Tx value)
        {
            if (!NullHelper_PTS<Tx>::IsNull(value))
            {
                stateEmpty = false;
                ShallowCopier_PTS<Ty_v>::Copy(Accumulate_PTS<Ty_v, Ty_v>::Add(state, ScopeCast<Ty_v, Tx>::get(value)), state);
            }
        }

        static void UpdateState(Ty & state, bool & stateEmpty, Tx value)
        {
            if (!NullHelper_PTS<Tx>::IsNull(value))
            {
                stateEmpty = false;
                ShallowCopier_PTS<Ty>::Copy(Accumulate_PTS<Ty, Ty>::Add(state, value), state);
            }
        }

        static void GetAggregatedValue(Ty state, Ty * output)
        {
            ShallowCopier_PTS<Ty>::Copy(state, *output);
        }

        template <typename Ty_v>
        static void GetAggregatedValue(Ty_v state, bool stateEmpty, Ty * output)
        {
            ShallowCopier_PTS<Ty>::Copy(stateEmpty ? Ty() : ScopeCast<Ty, Ty_v>::get(state), *output);
        }

        Aggregate_SUM_PTS(const char* /*name*/)
        {
            // TODO we need static assert here to ensure that values of Tx can be converted to Ty
            Reset();
        }

        // Pass-by-value since for most numeric T => sizeof(T) < sizeof(T&)
        void Add(Tx value)
        {
            if (BOTH_NULLABLE)
            {
                UpdateState(m_sumValue, m_fEmpty, value);
            }
            else
            {
                UpdateState(m_sumValue, value);
            }
        }

        void Add(NativeNullable<Tx> value)
        {
            UpdateState(m_sumValue, m_fEmpty, value);
        }

        void Aggregate(Ty * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(Ty * output)
        {
            if (BOTH_NULLABLE)
            {
                GetAggregatedValue(m_sumValue, m_fEmpty, output);
            }
            else
            {
                GetAggregatedValue(m_sumValue, output);
            }
        }

        void Reset()
        {
            m_fEmpty = true;
            Tx v;
            NumericOpHelper_PTS<Tx>::SetZero(v);
            Init(m_sumValue, v);
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // SUM(x*x) aggregator for STDEV and VAR
    //
    template<typename Tx, typename Ty>
    class Aggregate_UNCHECKED_SUMx2_PTS
    {
        Ty m_sumValue;

    public:
        Aggregate_UNCHECKED_SUMx2_PTS(const char* /*name*/)
        {
            NumericOpHelper_PTS<Ty>::SetZero(m_sumValue);
        }

        void Add(Tx & value)
        {
            if (!NullHelper_PTS<Tx>::IsNull(value))
            {
                Ty castedValue = ScopeCast<Tx, Ty>::get(value);
                Ty mult = NumericOpHelper_PTS<Ty>::Multiply(castedValue, castedValue);
                NumericOpHelper_PTS<Ty>::AddBy(m_sumValue, mult);
            }
        }

        void Aggregate(Ty * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(Ty * output)
        {
            ShallowCopier_PTS<Ty>::Copy(m_sumValue, *output);
        }

        void Reset()
        {
            NumericOpHelper_PTS<Ty>::SetZero(m_sumValue);
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // UNCHECKED_SUM aggregator (nullable types)
    //
    template<typename Tx, typename Ty>
    class Aggregate_UNCHECKED_SUM_PTS
    {
        Ty m_sumValue;

    public:
        Aggregate_UNCHECKED_SUM_PTS(const char* /*name*/)
        {
            NumericOpHelper_PTS<Ty>::SetZero(m_sumValue);
        }

        void Add(Tx & value)
        {
            if (!NullHelper_PTS<Tx>::IsNull(value))
            {
                NumericOpHelper_PTS<Ty>::AddBy(m_sumValue, ScopeCast<Tx, Ty>::get(value));
            }
        }

        void Aggregate(Ty * output)
        {
            GetValue(output);
            Reset();
        }

        void GetValue(Ty * output)
        {
            ShallowCopier_PTS<Ty>::Copy(m_sumValue, *output);
        }

        void Reset()
        {
            NumericOpHelper_PTS<Ty>::SetZero(m_sumValue);
        }

        void WriteRuntimeStats(TreeNode &)
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    //
    // DISTINCT aggregator, common template will NEED_DEEP_COPY==false
    //
    template<typename T, bool NEED_DEEP_COPY = need_deep_copy_traits_pts<T>::val>
    class Aggregate_DISTINCT_PTS
    {
        IncrementalAllocator m_containerAllocator;

        // TODO: Should we override TR1 hash? In C# we have dedicated hash for bool, int and long
        typedef DistinctHashTable_PTS<T> THashTable;
        std::unique_ptr<THashTable> m_hashTable;
        string m_name;

        // don't control the memory, if it go over 6G it will fail in managed runtime as well.
        static const SIZE_T c_defaultAllocatorSize = 6 * 1024ULL * 1024ULL * 1024ULL;

    public:
        Aggregate_DISTINCT_PTS(const char* name) : m_name(name)
        {
            m_containerAllocator.Init(c_defaultAllocatorSize, m_name);
            m_hashTable.reset(new THashTable(m_containerAllocator));
        }

        // Pass-by-value since for most numeric T => sizeof(T) < sizeof(T&)
        bool Add(T value)
        {
            // Null is not counted.
            if (NullHelper_PTS<T>::IsNull(value))
            {
                return false;
            }

            THashTable::iterator it = m_hashTable->find(value);
            if (it != m_hashTable->end())
            {
                return false;
            }

            return m_hashTable->insert(value).second;
        }

        void Reset()
        {
            m_containerAllocator.Reset();
            m_hashTable.reset(new THashTable(m_containerAllocator));
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_name);
            m_containerAllocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_DISTINCT_FixedLength__Size_MinMemory);
        }
    };

    //
    // DISTINCT aggregator, NEED_DEEP_COPY==true
    //
    template<typename T>
    class Aggregate_DISTINCT_PTS<T, true>
    {
        IncrementalAllocator m_allocator;
        IncrementalAllocator m_containerAllocator;

        typedef DistinctHashTable<T> THashTable;
        std::unique_ptr<THashTable> m_hashTable;
        string m_name;

        // don't control the memory, if it go over 6G it will fail in managed runtime as well.
        static const SIZE_T c_defaultAllocatorSize = 6 * 1024ULL * 1024ULL * 1024ULL;
        static const int    c_defaultBucketSize = 256;

    public:
        Aggregate_DISTINCT_PTS(const char* name) : m_name(name)
        {
            m_allocator.Init(c_defaultAllocatorSize, m_name + "_DeepCopy");
            m_containerAllocator.Init(c_defaultAllocatorSize, m_name);
            m_hashTable.reset(new THashTable(m_containerAllocator));
        }

        bool Add(T & value)
        {
            // Null is not counted.
            if (NullHelper_PTS<T>::IsNull(value))
            {
                return false;
            }

            THashTable::iterator it = m_hashTable->find(value);
            if (it != m_hashTable->end())
            {
                return false;
            }

            // make deep copy
            T str(value, &m_allocator);

            return m_hashTable->insert(str).second;
        }

        void Reset()
        {
            m_allocator.Reset();
            m_containerAllocator.Reset();
            m_hashTable.reset(new THashTable(m_containerAllocator));
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(m_name);
            m_allocator.WriteRuntimeStats(node);
            m_containerAllocator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInColumns(OperatorRequirementsConstants::Aggregate_DISTINCT_VariableLength__Size_MinMemory);
        }
    };
#pragma endregion PluginTypeSystem

    ///
    /// parallel data loader template.
    ///
    template<typename InputOperator, typename InputSchema>
    class ParallelLoader : public ExecutionStats
    {
        static const char* const sm_className;

        ConcurrentBatchQueue<InputSchema>    * m_outputQueue;
        InputOperator                        * m_child;
        volatile ULONG                         m_cancelled; // indicate iteration is canceled. Use volatile and write barrier to avoid synchronization.
        bool                                   m_autoOpenClose;
        volatile bool                          m_isInitialized;
        SIZE_T                                 m_peakMemorySize;

        static void LoaderCallback(PVOID Param)
        {
            ParallelLoader<InputOperator, InputSchema> * curOp = (ParallelLoader<InputOperator, InputSchema> *)(Param);

            bool requireQueueFinish = true;

            __try
            {
                if (curOp->m_autoOpenClose)
                {
                    curOp->Init();
                }
                requireQueueFinish = curOp->LoadingData();
                if (curOp->m_autoOpenClose)
                {
                    curOp->Close();
                }
            }
            __finally
            {
                // finish current queue if it is required.
                if (requireQueueFinish)
                {
                    curOp->m_outputQueue->Finish();
                }
            }
        }

        // Loading Data from child. Return true if we need to call finish producer.
        bool LoadingData()
        {
            AutoExecStats stats(this);

            InputSchema row;

            typedef AutoRowArray<InputSchema> BatchType;

            // we use small autorowarray as batch to manage memory
            std::unique_ptr<BatchType> curBatch;

            // if we could not get a free batch, create one.
            if (!m_outputQueue->GetFreeBatch(curBatch))
            {
                curBatch.reset(new BatchType(sm_className, BatchType::ExtraSmall, BatchType::MediumMem));
            }

            while (m_child->GetNextRow(row))
            {
                stats.IncreaseRowCount(1);

                // check if operation is cancelled.
                if (m_cancelled)
                {
                    return false;
                }

                SIZE_T bucketSize = curBatch->MemorySize();

                // If current batch is full, push to the output queue
                if (!curBatch->AddRow(row))
                {
                    // We only push non empty batch.
                    if (curBatch->Size() > 0)
                    {
                        m_peakMemorySize = max<SIZE_T>(m_peakMemorySize, curBatch->MemorySize());
                        m_outputQueue->PushBatch(curBatch);

                        // if we could not get a free batch, create one.
                        if (!m_outputQueue->GetFreeBatch(curBatch))
                        {
                            curBatch.reset(new BatchType(sm_className, BatchType::ExtraSmall, BatchType::MediumMem));
                        }

                        // reset bucket size.
                        bucketSize = curBatch->MemorySize();
                    }

                    // If it is keep failing we need to raise a runtime exception
                    if (!curBatch->AddRow(row))
                    {
                        throw RuntimeException(E_USER_OUT_OF_MEMORY, "Out of memory in parallel loader callback. Row is too large to hold in memory.");
                    }
                }

                // if we encounter large row(more than 1G) we should push the batch
                if ((curBatch->MemorySize() - bucketSize) > MemoryManager::x_maxMemSize / 2 || curBatch->FFull())
                {
                    // We only push non empty batch.
                    if (curBatch->Size() > 0)
                    {
                        m_peakMemorySize = max<SIZE_T>(m_peakMemorySize, curBatch->MemorySize());
                        m_outputQueue->PushBatch(curBatch);

                        // if we could not get a free batch, create one.
                        if (!m_outputQueue->GetFreeBatch(curBatch))
                        {
                            curBatch.reset(new BatchType(sm_className, BatchType::ExtraSmall, BatchType::MediumMem));
                        }
                    }
                }
            }

            // push remaining rows if there is any.
            if (curBatch->Size() > 0)
            {
                m_peakMemorySize = max<SIZE_T>(m_peakMemorySize, curBatch->MemorySize());
                m_outputQueue->PushBatch(curBatch);
            }

            return true;
        }

    public:
        ParallelLoader(InputOperator * input, ConcurrentBatchQueue<InputSchema> * outputQueue, bool autoOpenClose) :
            m_child(input),
            m_outputQueue(outputQueue),
            m_cancelled(false),
            m_autoOpenClose(autoOpenClose),
            m_isInitialized(!autoOpenClose),
            m_peakMemorySize(0)
        {
        }

        ~ParallelLoader()
        {
            CancelLoading();
        }

        void QueueWorkItem(PrivateThreadPool * threadPool)
        {
            threadPool->QueueUserWorkItem(LoaderCallback, (PVOID)this);
        }

        // Initialize children and merger
        void Init()
        {
            AutoExecStats stats(this);

            m_child->Init();
            m_isInitialized = true;
        }

        PartitionMetadata * GetMetadata()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        // Release all resources of children
        void Close()
        {
            AutoExecStats stats(this);

            m_child->Close();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            // Init is called in threadpool thread
            // in some case, WriteRuntimeStats could be called before Init
            // e.g, the aggregator (parallel union all) is structured stream generator.
            // some vertices won't generate any data - PARTITION_NOT_EXIST case
            // that is, GetNextRow will return false immediately.
            // then main thread will call WriteRuntimeStats.
            // however, Init could not be call at that time.
            // so, it needs to check if it's initialized before calling functions on m_child.

            auto & node = root.AddElement(sm_className);

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_isInitialized ? m_child->GetInclusiveTimeMillisecond() : 0);
            RuntimeStats::WriteRowCount(node, GetRowCount());
            node.AddAttribute(RuntimeStats::MaxPeakInMemorySize(), m_peakMemorySize);
            node.AddAttribute(RuntimeStats::AvgPeakInMemorySize(), m_peakMemorySize);

            if (m_isInitialized)
            {
                m_child->WriteRuntimeStats(node);
            }
        }

        void CancelLoading()
        {
            if (m_cancelled)
            {
                return;
            }

            m_cancelled = TRUE;

            // This is x64 only. use _mm_sfence for x86
            __faststorefence();

            // Make sure we cancel our producer
            m_outputQueue->CancelProducer();
        }
    };

    template<typename InputOperator, typename InputSchema>
    const char* const ParallelLoader<InputOperator, InputSchema>::sm_className = "ParallelLoader";

    ///
    /// Parallel UnionAll operator
    ///
    template<typename InputOperator, typename InputSchema, bool nativeOnly>
    class ParallelUnionAll : public Operator<ParallelUnionAll<InputOperator, InputSchema, nativeOnly>, InputSchema, -1>
    {
        typedef InputSchema OutputSchema;

        typedef ParallelLoader<InputOperator, InputSchema> LoaderType;

        std::vector<InputOperator *>            m_childOp;  // optimize of serial execution
        std::vector<std::shared_ptr<LoaderType>>     m_children; // Array of child operator
        ConcurrentBatchQueue<InputSchema>  m_inputQueue; // concurrentBatchQueue from child.
        PrivateThreadPool                  m_threadPool; // Private thread pool for parallel loading
        std::unique_ptr<AutoRowArray<InputSchema>>    m_outputBatch;    // current output batch
        ULONG                                    m_count;     // number of child operator
        int                                      m_curOutputItem;
        ULONG                                    m_index; // current output child for serial execution.
        bool                                     m_optForSerialExec; // optimized for serial execution.
        bool                                     m_closed;

        void Initialize(InputOperator ** inputs, ULONG count, bool fSerially)
        {
            SCOPE_ASSERT(count > 0);
            m_optForSerialExec = (count < 2) || fSerially;

            if (m_optForSerialExec)
            {
                for (ULONG i = 0; i < count; i++)
                {
                    m_childOp.push_back(inputs[i]);
                }
            }
            else
            {
                for (ULONG i = 0; i < count; i++)
                {
                    m_children.push_back(std::shared_ptr<LoaderType>(new LoaderType(inputs[i], &m_inputQueue, i != 0)));
                }
            }
        }

        void CancelAsyncOperations()
        {
            if (!m_optForSerialExec)
            {
                // stop all loading task first.
                for (ULONG i = 0; i< m_count; i++)
                {
                    m_children[i]->CancelLoading();
                }

                // Cancel any outstanding background threads
                // - this is needed for early bail out scenarios where we have enough rows but the parallel
                //   threads may still be active
                // - we cancel here instead of waiting as we don't want to wait for the threads
                // - we believe we are done, any error in the parallel threads is of no interest now
                m_threadPool.CancelAllCallbacks();
            }
        }

    public:
        ParallelUnionAll(InputOperator ** inputs, ULONG count, bool fSerially, int operatorId) :
            Operator(operatorId),
            m_inputQueue(count),
            m_count(count),
            m_curOutputItem(0),
            m_index(0),
            m_threadPool(nativeOnly),
            m_closed(false)
        {
            Initialize(inputs, count, fSerially);
        }

        ParallelUnionAll(InputOperator ** inputs, ULONG count, int operatorId) :
            Operator(operatorId),
            m_inputQueue(count),
            m_count(count),
            m_curOutputItem(0),
            m_index(0),
            m_threadPool(nativeOnly),
            m_closed(false)
        {
            Initialize(inputs, count, false);
        }

        ~ParallelUnionAll()
        {
            // Stop all loading tasks, otherwise threadpool destructor may hang in early bail out scenario.
            // This happens when the consumer (ParallelUnionAll) is already destroyed,
            // while the producter (ParallelLoader) wait on it forever.
            CancelAsyncOperations();
        }

        // Initialize children and merger
        void InitImpl()
        {
            AutoExecStats stats(this);

            if (m_optForSerialExec)
            {
                // We only initialize the first child now.
                // Others will be init'ed one by one, after the previous child was used.
                SCOPE_ASSERT(m_index == 0);
                m_childOp[0]->Init();
            }
            else
            {
                m_threadPool.SetThreadpoolMax(16);
                m_threadPool.SetThreadpoolMin(4);

                // Open the 0th child so GetMetadata call is valid
                m_children[0]->Init();

                for (ULONG i = 0; i< m_count; i++)
                    m_children[i]->QueueWorkItem(&m_threadPool);

            }
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            if (m_optForSerialExec)
            {
                // Return metadata from the currently open child.
                SCOPE_ASSERT(m_index < m_count);
                return PartitionMetadata::UnionMetadata(m_childOp[m_index]);
            }
            else
            {
                return PartitionMetadata::UnionMetadata(m_children);
            }
        }

        /// Get row from merger
        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);


            if (m_optForSerialExec)
            {
                for (;;) // Loop over children until we find one that has data or run out of children
                {
                    if (m_childOp[m_index]->GetNextRow(output))
                    {
                        stats.IncreaseRowCount(1);
                        return true;
                    }



                    SCOPE_ASSERT(m_index < m_count);
                    if (m_index == m_count - 1) // If it is the last child
                    {                           // postpone closing it in case we will need metadata.
                        return false;           // It will be closed in our CloseImpl().
                    }

                    m_childOp[m_index]->Close();
                    ++m_index;
                    m_childOp[m_index]->Init();
                }
            }
            else
            {
                for(;;)
                {
                    // outputBatch is empty, get one.
                    if (!m_outputBatch)
                    {
                        m_inputQueue.Pop(m_outputBatch);
                        m_curOutputItem = 0;


                        // we reach the end of the stream
                        if (m_outputBatch->Size() == 0)
                        {
                            return false;
                        }
                    }

                    if (m_curOutputItem < m_outputBatch->Size())
                    {
                        output = (*m_outputBatch)[m_curOutputItem++];
                        stats.IncreaseRowCount(1);
                        return true;
                    }
                    else
                    {
                        // reuse the free batch
                        m_outputBatch->Reset<IncrementalAllocator::DontReclaimMemoryPolicy>();
                        m_inputQueue.PutFreeBatch(m_outputBatch);
                    }
                }
            }
        }

        // Release all resources of children
        void CloseImpl()
        {
            if (m_closed)
            {
                return;
            }

            AutoExecStats stats(this);

            if (!m_optForSerialExec)
            {
                CancelAsyncOperations();


                // Manually close 0th child as it is special due to the GetMetadata call
                m_children[0]->Close();
            }
            else
            {
                SCOPE_ASSERT(m_index < m_count);
                m_childOp[m_index]->Close();
            }


            m_closed = true;
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("ParallelUnionAll");

            LONGLONG childrenInclusiveTime = 0;
            node.AddAttribute(RuntimeStats::MaxInputCount(), m_count);
            node.AddAttribute(RuntimeStats::AvgInputCount(), m_count);
            if (m_optForSerialExec)
            {
                for (SIZE_T i = 0; i < m_count; i++)
                {
                    // Calculate SUM
                    childrenInclusiveTime += m_childOp[i]->GetInclusiveTimeMillisecond();
                    m_childOp[i]->WriteRuntimeStats(node);
                }
            }
            else
            {
                for (SIZE_T i = 0; i < m_count; i++)
                {
                    // Calculate MAX
                    childrenInclusiveTime = std::max<LONGLONG>(childrenInclusiveTime, m_children[i]->GetInclusiveTimeMillisecond());
                    m_children[i]->WriteRuntimeStats(node);
                }
            }

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - childrenInclusiveTime);
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }
        }

        void DoScopeCEPCheckpointImpl(BinaryOutputStream & output)
        {
            SCOPE_ASSERT(m_count == 1);
            m_childOp[0]->DoScopeCEPCheckpoint(output);
        }

        void LoadScopeCEPCheckpointImpl(BinaryInputStream & input)
        {
            SCOPE_ASSERT(m_count == 1);
            m_childOp[0]->LoadScopeCEPCheckpoint(input);
        }
    };

    ///
    /// Spill Bucket operator. This operator will try to cache up to a predefine number of rows or reach its memory quota.
    /// If the memory pressure detected by the caller, the remaining rows will be spilled to disk using binary outputer.
    /// After spill, the rows can be read again using binary extractor.
    template<typename InputOperator, typename InputSchema, typename RowIteratorType>
    class SpillBucket : public ExecutionStats
    {
    protected:
        typedef InputSchema OutputSchema;

        LONGLONG         m_spillTime;  // track total time spend in spilling
        SIZE_T           m_rowOutput;
        SIZE_T           m_size;
        SIZE_T           m_maxRowSize;

        AutoRowArray<OutputSchema> m_rowCache; // auto grow array to cache rows

        std::unique_ptr<Extractor<OutputSchema, BinaryExtractPolicy<OutputSchema>, BinaryInputStream>> m_spillInput; // input stream for spilled bucket
        std::unique_ptr<BinaryOutputStream>   m_spillOutput; // output stream for spilled bucket
        string                           m_spillStreamName;
        IncrementalAllocator::Statistics m_spilledStat;

        static const SIZE_T x_ioBufferSize = 4 * 1 << 20; //4Mb
        static const SIZE_T x_ioBufferCount = 2; // minimum amount of buffers

        SIZE_T GetIOMemorySize() const
        {
            return x_ioBufferSize * x_ioBufferCount;
        }

        void InitSpilled()
        {
            stringstream ss;
            ss << "INPUT_" << m_spillStreamName;
            string node = ss.str();

            IOManager::GetGlobal()->AddInputStream(node, m_spillStreamName);

            SCOPE_ASSERT(!m_spillInput);
            m_spillInput.reset(new Extractor<OutputSchema, BinaryExtractPolicy<OutputSchema>, BinaryInputStream>(InputFileInfo(node), false, x_ioBufferSize, x_ioBufferCount, Configuration::GetGlobal().GetMaxOnDiskRowSize(), 0));
            m_spillInput->Init();
        }

        void SetupSpillStream()
        {
            m_spillStreamName = IOManager::GetTempStreamName();

            // spill bucket to disk
            stringstream ss;
            ss << "OUTPUT_" << m_spillStreamName;
            string node = ss.str();

            IOManager::GetGlobal()->AddOutputStream(node, m_spillStreamName);

            SCOPE_ASSERT(!m_spillOutput);
            m_spillOutput.reset(new BinaryOutputStream(node, x_ioBufferSize, x_ioBufferCount));
            m_spillOutput->Init();
        }

        void CloseSpillStream()
        {
            // flush all remaining bytes from buffer.
            m_spillOutput->Finish();
            m_spillOutput->Close();

            m_spillTime += m_spillOutput->GetTotalIoWaitTime();

            // release output stream
            m_spillOutput.reset();
        }

        void ResetState()
        {
            m_rowCache.Reset();
            m_rowOutput = 0;

            if (m_spillInput)
            {
                m_spillInput->Close();

                m_spillInput->AggregateToOuterMemoryStatistics(m_spilledStat);
                m_spillTime += m_spillInput->GetTotalIoWaitTime();

                m_spillInput.reset();
            }
        }

        static const SIZE_T x_maxRowsPerBucket = 1000000; // 1 million
        static const SIZE_T x_maxMemoryPerBucket = 2ULL << 30; // 2 Gb

    public:
        SpillBucket() :
            m_rowCache("SpillBucket", x_maxRowsPerBucket, x_maxMemoryPerBucket),
            m_size(0),
            m_spillTime(0),
            m_rowOutput(0),
            m_maxRowSize(0)
        {
        }

        //
        // Amount of rows in the bucket
        //
        SIZE_T Size() const
        {
            return m_size;
        }

        // load rows into bucket until it all done or it hits capacity.
        // return false if there no more rows in input.
        // return true if there is more rows in input left.
        bool LoadingPhase(RowIteratorType * it, SIZE_T maxBucketSize = MemoryManager::x_maxMemSize)
        {
            AutoExecStats stats(this);

            SIZE_T bucketSize = 0;
            while(!m_rowCache.FFull() && bucketSize < maxBucketSize && (!it->End()))
            {
                SIZE_T tmp = bucketSize;

                // If cache is full, we will return without move the iterator.
                // The caller of LoadingPhase will handle not enough memory case correctly.
                if (!m_rowCache.AddRow(*(it->GetRow())))
                {
                    break;
                }

                bucketSize = m_rowCache.MemorySize();
                // just an upper estimation of row size, actual value may be much smaller
                m_maxRowSize = max<SIZE_T>(m_maxRowSize, bucketSize - tmp);

                it->Increment();
            }

            m_size = m_rowCache.Size();
            stats.IncreaseRowCount(m_rowCache.Size());

            return !it->End();
        }

        // Spill rest of rows in the iterator to disk
        void SpillingRestPhase(RowIteratorType * it)
        {
            AutoExecStats stats(this);

            SetupSpillStream();

            while (!it->End())
            {
                BinaryOutputPolicy<OutputSchema>::Serialize(m_spillOutput.get(), *(it->GetRow()));
                it->Increment();
                stats.IncreaseRowCount(1);
                m_size++;
            }

            CloseSpillStream();

            // prepare input stream for later reading
            InitSpilled();
        }

        // Input operator maybe reused, the caller is responsible to initialize it.
        void Init()
        {
        }

        // Rewind the stream to read from beginning
        void ReWind()
        {
            AutoExecStats stats(this);

            m_rowOutput = 0;

            if (m_spillInput)
            {
                m_spillInput->ReWind();
            }
        }

        // Reset the stream so it can be called load again
        void Reset()
        {
            AutoExecStats stats(this);

            ResetState();
        }

        /// Get row from the sorting bucket
        FORCE_INLINE bool GetNextRow(OutputSchema & output)
        {
            AutoExecStats stats(this);

            if (m_rowOutput <  m_size)
            {
                if (m_rowOutput < m_rowCache.Size())
                {
                    // in memory
                    output = m_rowCache[m_rowOutput];
                }
                else
                {
                    bool succeed = m_spillInput->GetNextRow(output);
                    SCOPE_ASSERT(succeed);
                }

                m_rowOutput++;
                return true;
            }
            else
            {
                return false;
            }
        }

        // Input operator maybe reused, the caller is responsible to close it.
        void Close()
        {
            AutoExecStats stats(this);

            ResetState();
        }

        LONGLONG OperatorWaitOnIOTime() const
        {
            return m_spillTime;
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("SpillBucket");

            // SpillBucket may use two streams (input and output) for spilling
            // I report only overall IO time here though complete Scanner::Statistics could be reported too (if needed)
            // The problem however is that one SpillBucket may be used (loaded/spilled/read) multiple times for different data (i.e. data of different size)
            // Even more, once spilled it may be read virtually unlimited amount of times
            // Therefore reporting scanner statistics for each spill/readback operation may explode final statistics
            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_spillTime);
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_spillTime);
            RuntimeStats::WriteRowCount(node, GetRowCount());

            m_rowCache.WriteRuntimeStats(node);
            if (!m_spilledStat.IsEmpty())
            {
                m_spilledStat.WriteRuntimeStats(node, sizeof(OutputSchema));
                node.AddAttribute(RuntimeStats::MaxBufferMemory(), GetIOMemorySize());
            }
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::RowBuffer_RowCacheExtractor__Row_MinMemory)
                .AddMemory(0, OperatorRequirementsConstants::RowBuffer_RowCacheExtractor__Size_OptimalMemory)
                .AddMemoryForOutputUStreams(OperatorRequirementsConstants::RowBuffer_RowCacheExtractor__Count_OutputUStream, x_ioBufferSize, x_ioBufferCount)
                .AddMemoryForInputUStreams(OperatorRequirementsConstants::RowBuffer_RowCacheExtractor__Count_InputUStream, x_ioBufferSize, x_ioBufferCount);
        }
    };

    //
    // Defines whether ordering of left/right side rows must be preserved in the Join output (within a key range)
    //
    enum SortRequirement
    {
        NoOrder,
        KeepLeftOrder,
        KeepRightOrder,
    };

    ///
    /// Base joiner template for INNER/OUTER/CROSS joins
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, template <class, class, class> class IteratorPolicy, typename LeftKeyPolicy, typename RightKeyPolicy, SortRequirement sortReq, int UID = -1>
    class BaseFullOuterJoiner
    {
        // Counts amount of times when flipping left and right sides was successful and not
        LONGLONG m_goodFlips;
        LONGLONG m_badFlips;

        // Keep track of number of rows in left/right input for cross row mode
        LONGLONG m_crossRowsLeftCount;
        LONGLONG m_crossRowsRightCount;
    public:
        typedef IteratorPolicy<InputOperatorLeft, InputSchemaLeft, LeftKeyPolicy> LeftKeyIteratorType;
        typedef IteratorPolicy<InputOperatorRight, RightInputSchema, RightKeyPolicy> RightKeyIteratorType;

        const static unsigned MaxPerKeyCrossSizeLimit = CombinerPolicy<UID>::MaxPerKeyCrossSizeLimit;
        const static int IsCrossJoin = 0;

    protected:
        LeftKeyIteratorType * m_leftKeyIterator;
        RightKeyIteratorType * m_rightKeyIterator;

        SpillBucket<InputOperatorLeft, InputSchemaLeft, LeftKeyIteratorType> m_leftRowCache;
        SpillBucket<InputOperatorRight, RightInputSchema, RightKeyIteratorType> m_rightRowCache;

        bool                            m_firstLeftRowRead;
        bool                            m_firstRightRowRead;

        enum JoinerState{
            UnInit,
            RightCached,
            LeftCached,
            BothCached,
            LeftRow,
            RightRow,
            Finished
        };

        JoinerState     m_state;

        InputSchemaLeft m_leftOut;
        RightInputSchema m_rightOut;

        // memory quota for each size of the join
        const LONGLONG x_joinerMemQuota = CombinerPolicy<UID>::JoinerMemQuota();

    private:
        // Check that joiner will not exceed MaxPerKeyCrossSize limit.
        template<unsigned CrossLimit>
        void CheckMaxCrossSizeLimit()
        {
            if (std::min(m_crossRowsLeftCount, m_crossRowsRightCount) > CrossLimit)
            {
                throw JoinCrossLimitExceededException(UID, CrossLimit);
            }
        }

        template<>
        void CheckMaxCrossSizeLimit<0>()
        {
        }

    protected:
        template<SortRequirement>
        void InitCrossRowModeImpl();

        template<>
        void InitCrossRowModeImpl<NoOrder>()
        {
            // Cross Row Mode require both side of iterator are valid.
            SCOPE_ASSERT((m_rightKeyIterator != nullptr && m_leftKeyIterator != nullptr));

            // Cache right side rows first
            m_rightRowCache.Reset();

            // Load right side into memory
            if (m_rightRowCache.LoadingPhase(m_rightKeyIterator, x_joinerMemQuota))
            {
                // Right side does not fit, behave as in "KeepRightOrder mode"
                m_leftRowCache.Reset();
                if (m_leftRowCache.LoadingPhase(m_leftKeyIterator, x_joinerMemQuota))
                {
                    // Left side also doesn't fit, read the rest of left side and spill to disk
                    if (Configuration::GetGlobal().GetRestrictOperatorMemorySpilling())
                    {
                        throw OperatorOutOfMemoryException(UID);
                    }

                    m_leftRowCache.SpillingRestPhase(m_leftKeyIterator);
                    m_badFlips++;
                }
                else
                {
                    // Ok, left side fits into memory
                    m_goodFlips++;
                }

                m_crossRowsLeftCount = m_leftRowCache.Size();

                // Cache first right side row, m_rightRowCache maybe empty due to memory quota
                if (m_rightRowCache.GetNextRow(m_rightOut))
                {
                    m_state = BothCached;
                }
                else
                {
                    m_state = LeftCached;
                }
            }
            else
            {
                // Right side fully fits into memory, behave as in "KeepLeftOrder" mode
                m_state = RightCached;

                m_crossRowsLeftCount = 1;
            }

            m_crossRowsRightCount = m_rightRowCache.Size();
        }

        template<>
        void InitCrossRowModeImpl<KeepLeftOrder>()
        {
            // Cross Row Mode require both side of iterator are valid.
            SCOPE_ASSERT((m_rightKeyIterator != nullptr && m_leftKeyIterator != nullptr));

            // Cache right side rows first
            m_rightRowCache.Reset();

            // If right side can not fit in memory
            if (m_rightRowCache.LoadingPhase(m_rightKeyIterator, x_joinerMemQuota))
            {
                if (Configuration::GetGlobal().GetRestrictOperatorMemorySpilling())
                {
                    throw OperatorOutOfMemoryException(UID);
                }
                // Read the rest of right side and spill to disk
                m_rightRowCache.SpillingRestPhase(m_rightKeyIterator);

                // Read left side until in-memory bucket is full
                m_leftRowCache.Reset();
                m_leftRowCache.LoadingPhase(m_leftKeyIterator, x_joinerMemQuota);

                m_crossRowsLeftCount = m_leftRowCache.Size();

                // Cache first left side row, m_leftRowCache maybe empty due to memory quota
                if (m_leftRowCache.GetNextRow(m_leftOut))
                {
                    m_state = BothCached;
                }
                else
                {
                    m_state = RightCached;
                }
            }
            else
            {
                m_state = RightCached;

                m_crossRowsLeftCount = 1;
            }

            m_crossRowsRightCount = m_rightRowCache.Size();
        }

        template<>
        void InitCrossRowModeImpl<KeepRightOrder>()
        {
            // Cross Row Mode require both side of iterator are valid.
            SCOPE_ASSERT((m_rightKeyIterator != nullptr && m_leftKeyIterator != nullptr));

            // Cache right side rows first
            m_leftRowCache.Reset();

            // If right side can not fit in memory
            if (m_leftRowCache.LoadingPhase(m_leftKeyIterator, x_joinerMemQuota))
            {
                if (Configuration::GetGlobal().GetRestrictOperatorMemorySpilling())
                {
                    throw OperatorOutOfMemoryException(UID);
                }

                m_leftRowCache.SpillingRestPhase(m_leftKeyIterator);

                // Read right side until in-memory bucket is full
                m_rightRowCache.Reset();
                m_rightRowCache.LoadingPhase(m_rightKeyIterator, x_joinerMemQuota);

                m_crossRowsRightCount = m_rightRowCache.Size();

                // Cache first left side row. m_rightRowCache maybe empty due to memory quota
                if(m_rightRowCache.GetNextRow(m_rightOut))
                {
                    m_state = BothCached;
                }
                else
                {
                    m_state = LeftCached;
                }
            }
            else
            {
                m_state = LeftCached;

                m_crossRowsRightCount = 1;
            }

            m_crossRowsLeftCount = m_leftRowCache.Size();
        }

        template<SortRequirement>
        bool GetCrossRowImpl(OutputSchema & outputRow);

        template<>
        bool GetCrossRowImpl<NoOrder>(OutputSchema & outputRow)
        {
            switch (m_state)
            {
            case RightCached:
                return GetCrossRowImpl<KeepLeftOrder>(outputRow);

            case LeftCached:
            case BothCached:
                return GetCrossRowImpl<KeepRightOrder>(outputRow);

            default:
                SCOPE_ASSERT(!"Invalid join state reached");
                return false;
            }
        }

        template<>
        bool GetCrossRowImpl<KeepLeftOrder>(OutputSchema & outputRow)
        {
            for (;;)
            {
                switch (m_state)
                {
                case RightCached:
                {
                                    if (m_leftKeyIterator->End())
                                    {
                                        m_state = Finished;
                                        return false;
                                    }

                                    RightInputSchema rightOut;

                                    // If right side has finished
                                    while (!m_rightRowCache.GetNextRow(rightOut))
                                    {
                                        // Move left side forward
                                        m_leftKeyIterator->Increment();
                                        ++m_crossRowsLeftCount;
                                        if (m_leftKeyIterator->End())
                                        {
                                            m_state = Finished;
                                            return false;
                                        }

                                        // Rewind right side
                                        m_rightRowCache.ReWind();
                                    }

                                    // Generate row
                                    CheckMaxCrossSizeLimit<MaxPerKeyCrossSizeLimit>();
                                    CombinerPolicy<UID>::CopyRow(m_leftKeyIterator->GetRow(), &rightOut, &outputRow);

                                    return true;
                }

                case BothCached:
                {
                                   RightInputSchema rightOut;
                                   bool             retryNewState = false;

                                   // If right side has finished
                                   while (!m_rightRowCache.GetNextRow(rightOut))
                                   {
                                       // Rewind right side
                                       m_rightRowCache.ReWind();

                                       // Move left side forward
                                       if (!m_leftRowCache.GetNextRow(m_leftOut))
                                       {
                                           retryNewState = true;
                                           m_state = RightCached;
                                           break;
                                       }
                                   }

                                   if (retryNewState)
                                   {
                                       break;
                                   }

                                   // Generate row
                                   CheckMaxCrossSizeLimit<MaxPerKeyCrossSizeLimit>();
                                   CombinerPolicy<UID>::CopyRow(&m_leftOut, &rightOut, &outputRow);

                                   return true;
                }

                default:
                    SCOPE_ASSERT(!"Invalid join state reached");
                    return false;
                }
            }
        }

        template<>
        bool GetCrossRowImpl<KeepRightOrder>(OutputSchema & outputRow)
        {
            for (;;)
            {
                switch (m_state)
                {
                case LeftCached:
                {
                                   if (m_rightKeyIterator->End())
                                   {
                                       m_state = Finished;
                                       return false;
                                   }

                                   InputSchemaLeft leftOut;

                                   // If left side has finished
                                   while (!m_leftRowCache.GetNextRow(leftOut))
                                   {
                                       // Move right side forward
                                       m_rightKeyIterator->Increment();
                                       ++m_crossRowsRightCount;
                                       if (m_rightKeyIterator->End())
                                       {
                                           m_state = Finished;
                                           return false;
                                       }

                                       // Rewind left side
                                       m_leftRowCache.ReWind();
                                   }

                                   // Generate row
                                   CheckMaxCrossSizeLimit<MaxPerKeyCrossSizeLimit>();
                                   CombinerPolicy<UID>::CopyRow(&leftOut, m_rightKeyIterator->GetRow(), &outputRow);

                                   return true;
                }

                case BothCached:
                {
                                   InputSchemaLeft leftOut;
                                   bool             retryNewState = false;

                                   // If left side has finished
                                   while (!m_leftRowCache.GetNextRow(leftOut))
                                   {
                                       // Rewind right side
                                       m_leftRowCache.ReWind();

                                       // Move right side forward
                                       if (!m_rightRowCache.GetNextRow(m_rightOut))
                                       {
                                           retryNewState = true;
                                           m_state = LeftCached;
                                           break;
                                       }
                                   }

                                   if (retryNewState)
                                   {
                                       break;
                                   }

                                   // Generate row
                                   CheckMaxCrossSizeLimit<MaxPerKeyCrossSizeLimit>();
                                   CombinerPolicy<UID>::CopyRow(&leftOut, &m_rightOut, &outputRow);

                                   return true;
                }

                default:
                    SCOPE_ASSERT(!"Invalid join state reached");
                    return false;
                }
            }
        }

    public:
        BaseFullOuterJoiner() :
            m_firstLeftRowRead(true),
            m_firstRightRowRead(true),
            m_goodFlips(0),
            m_badFlips(0),
            m_crossRowsLeftCount(0),
            m_crossRowsRightCount(0)
        {
        }

        void Init(LeftKeyIteratorType * leftIt, RightKeyIteratorType * rightIt)
        {
            SCOPE_ASSERT((leftIt != nullptr && rightIt != nullptr));

            m_leftKeyIterator = leftIt;
            m_rightKeyIterator = rightIt;
        }

        void InitCrossRowMode()
        {
            InitCrossRowModeImpl<sortReq>();
        }

        void InitLeftRowMode()
        {
            m_state = LeftRow;
            m_firstLeftRowRead = true;

        }

        void InitRightRowMode()
        {
            m_state = RightRow;
            m_firstRightRowRead = true;
        }

        bool GetCrossRow(OutputSchema & outputRow)
        {
            return GetCrossRowImpl<sortReq>(outputRow);
        }

        bool GetLeftRow(OutputSchema & outputRow)
        {
            SCOPE_ASSERT(m_state == LeftRow);

            // For the first row it was already read. Otherwise, we need to read one row.
            if (m_firstLeftRowRead)
            {
                m_firstLeftRowRead = false;
            }
            else
            {
                m_leftKeyIterator->Increment();
            }

            if (m_leftKeyIterator->End())
            {
                m_state = Finished;
                return false;
            }

            CombinerPolicy<UID>::CopyLeftRow(m_leftKeyIterator->GetRow(), &outputRow);
            CombinerPolicy<UID>::NullifyRightSide(&outputRow);

            return true;
        }

        bool GetRightRow(OutputSchema & outputRow)
        {
            SCOPE_ASSERT(m_state == RightRow);

            // For the first row it was already read. Otherwise, we need to read one row.
            if (m_firstRightRowRead)
            {
                m_firstRightRowRead = false;
            }
            else
            {
                m_rightKeyIterator->Increment();
            }

            if (m_rightKeyIterator->End())
            {
                m_state = Finished;
                return false;
            }

            CombinerPolicy<UID>::CopyRightRow(m_rightKeyIterator->GetRow(), &outputRow);
            CombinerPolicy<UID>::NullifyLeftSide(&outputRow);

            return true;
        }

        LONGLONG OperatorWaitOnIOTime() const
        {
            // It's not an exact amount of time spent in spilling but a close one
            return m_leftRowCache.OperatorWaitOnIOTime() + m_rightRowCache.OperatorWaitOnIOTime();
        }

        void GetFlipCount(LONGLONG & goodFlips, LONGLONG & badFlips) const
        {
            goodFlips = m_goodFlips;
            badFlips = m_badFlips;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("Joiner");
            m_leftRowCache.WriteRuntimeStatsImpl(node);
            m_rightRowCache.WriteRuntimeStatsImpl(node);
        }

        OperatorRequirements GetBufferRequirements()
        {
            return OperatorRequirements(m_leftRowCache.GetOperatorRequirements()).Add(m_rightRowCache.GetOperatorRequirements());
        }
    };

    ///
    /// FULL OUTER JOIN template
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class FullOuterJoiner : public BaseFullOuterJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, KeyIterator, typename CombinerPolicy<UID>::LeftKeyPolicy, typename CombinerPolicy<UID>::RightKeyPolicy, NoOrder, UID>
    {
    public:
        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::NativeCombinerWrapper_FullOuterJoiner__Row_MinMemory)
                .Add(GetBufferRequirements());
        }
    };

    ///
    /// INNER JOIN template
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename ThirdSchema = None, typename FourthSchema = None>
    class InnerJoiner : public BaseFullOuterJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, KeyIterator, typename CombinerPolicy<UID>::LeftKeyPolicy, typename CombinerPolicy<UID>::RightKeyPolicy, NoOrder, UID>
    {
    public:
        InnerJoiner()
        {
        }

        bool GetLeftRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }

        bool GetRightRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::NativeCombinerWrapper_InnerJoiner__Row_MinMemory)
                .Add(GetBufferRequirements());
        }
    };

    ///
    /// LEFT OUTER JOIN template
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class LeftOuterJoiner : public BaseFullOuterJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, KeyIterator, typename CombinerPolicy<UID>::LeftKeyPolicy, typename CombinerPolicy<UID>::RightKeyPolicy, KeepLeftOrder, UID>
    {
    public:
        LeftOuterJoiner()
        {
        }

        bool GetRightRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::NativeCombinerWrapper_LeftOuterJoiner__Row_MinMemory)
                .Add(GetBufferRequirements());
        }
    };

    ///
    /// RIGHT OUTER JOIN template
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class RightOuterJoiner : public BaseFullOuterJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, KeyIterator, typename CombinerPolicy<UID>::LeftKeyPolicy, typename CombinerPolicy<UID>::RightKeyPolicy, KeepRightOrder, UID>
    {
    public:
        RightOuterJoiner()
        {
        }

        bool GetLeftRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::NativeCombinerWrapper_RightOuterJoiner__Row_MinMemory)
                .Add(GetBufferRequirements());
        }
    };

    ///
    /// CROSS JOIN template
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class CrossLoopJoiner : public BaseFullOuterJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, CrossJoinKeyIterator, EmptyKeyPolicy<InputSchemaLeft, UID>, EmptyKeyPolicy<RightInputSchema, UID>, NoOrder, UID>
    {
    public:
        const static int IsCrossJoin = 1;

        bool GetLeftRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }

        bool GetRightRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::NativeCombinerWrapper_CrossLoopJoiner__Row_MinMemory)
                .Add(GetBufferRequirements());
        }
    };

    ///
    /// CROSS JOIN template (preserve sort property on the left side)
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class CrossLoopJoinerDrvSortFromLeft : public BaseFullOuterJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, CrossJoinKeyIterator, EmptyKeyPolicy<InputSchemaLeft, UID>, EmptyKeyPolicy<RightInputSchema, UID>, KeepLeftOrder, UID>
    {
    public:
        const static int IsCrossJoin = 1;

        bool GetLeftRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }

        bool GetRightRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::NativeCombinerWrapper_CrossLoopJoinerDrvSortFromLeft__Row_MinMemory)
                .Add(GetBufferRequirements());

        }
    };

    ///
    /// CROSS JOIN template (preserve sort property on the right side)
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class CrossLoopJoinerDrvSortFromRight : public BaseFullOuterJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, CrossJoinKeyIterator, EmptyKeyPolicy<InputSchemaLeft, UID>, EmptyKeyPolicy<RightInputSchema, UID>, KeepRightOrder, UID>
    {
    public:
        const static int IsCrossJoin = 1;

        bool GetLeftRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }

        bool GetRightRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::NativeCombinerWrapper_CrossLoopJoinerDrvSortFromRight__Row_MinMemory)
                .Add(GetBufferRequirements());
        }
    };

    ///
    /// SEMIJOIN template
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID, bool leftSemiJoin>
    class SemiJoiner
    {
    public:
        typedef KeyIterator<InputOperatorLeft, InputSchemaLeft, typename CombinerPolicy<UID>::LeftKeyPolicy> LeftKeyIteratorType;
        typedef KeyIterator<InputOperatorRight, RightInputSchema, typename CombinerPolicy<UID>::RightKeyPolicy> RightKeyIteratorType;
        const static int IsCrossJoin = 0;

    protected:
        LeftKeyIteratorType * m_leftKeyIterator;
        RightKeyIteratorType * m_rightKeyIterator;

        bool            m_firstRead;

    public:
        SemiJoiner() :
            m_firstRead(true)
        {
        }

        void Init(LeftKeyIteratorType * leftIt, RightKeyIteratorType * rightIt)
        {
            SCOPE_ASSERT((leftIt != nullptr && rightIt != nullptr));

            m_leftKeyIterator = leftIt;
            m_rightKeyIterator = rightIt;
        }

        void InitCrossRowMode()
        {
            // Cross Row Mode require both side of iterator are valid.
            SCOPE_ASSERT((m_rightKeyIterator != nullptr && m_leftKeyIterator != nullptr));

            m_firstRead = true;
        }

        void InitLeftRowMode()
        {
        }

        void InitRightRowMode()
        {
        }

        bool GetCrossRow(OutputSchema & outputRow)
        {
            if (leftSemiJoin)
            {
                if (m_firstRead)
                {
                    m_firstRead = false;
                }
                else
                {
                    m_leftKeyIterator->Increment();
                }

                if (m_leftKeyIterator->End())
                {
                    return false;
                }

                //generator row
                CombinerPolicy<UID>::CopyRow(m_leftKeyIterator->GetRow(), nullptr, &outputRow);
            }
            else
            {
                if (m_firstRead)
                {
                    m_firstRead = false;
                }
                else
                {
                    m_rightKeyIterator->Increment();
                }

                if (m_rightKeyIterator->End())
                {
                    return false;
                }

                //generator row
                CombinerPolicy<UID>::CopyRow(nullptr, m_rightKeyIterator->GetRow(), &outputRow);
            }

            return true;
        }

        bool GetLeftRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }

        bool GetRightRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }

        LONGLONG OperatorWaitOnIOTime() const
        {
            return 0;
        }

        void GetFlipCount(LONGLONG & goodFlips, LONGLONG & badFlips) const
        {
            goodFlips = 0;
            badFlips = 0;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
        }
    };

    ///
    /// LEFT SEMIJOIN template
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class LeftSemiJoiner : public SemiJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, UID, true>
    {
    public:
        LeftSemiJoiner()
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements(OperatorRequirementsConstants::NativeCombinerWrapper_LeftSemiJoiner__Row_MinMemory);
        }
    };

    ///
    /// RIGHT SEMIJOIN template
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class RightSemiJoiner : public SemiJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, UID, false>
    {
    public:
        RightSemiJoiner()
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements(OperatorRequirementsConstants::NativeCombinerWrapper_RightSemiJoiner__Row_MinMemory);
        }
    };

    ///
    /// ANTI SEMIJOIN template
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID, bool leftAntiSemiJoin>
    class AntiSemiJoiner
    {
    public:
        typedef KeyIterator<InputOperatorLeft, InputSchemaLeft, typename CombinerPolicy<UID>::LeftKeyPolicy> LeftKeyIteratorType;
        typedef KeyIterator<InputOperatorRight, RightInputSchema, typename CombinerPolicy<UID>::RightKeyPolicy> RightKeyIteratorType;
        const static int IsCrossJoin = 0;

    protected:
        LeftKeyIteratorType * m_leftKeyIterator;
        RightKeyIteratorType * m_rightKeyIterator;

        bool            m_firstRead;

    public:
        AntiSemiJoiner() :
            m_firstRead(true)
        {
        }

        void Init(LeftKeyIteratorType * leftIt, RightKeyIteratorType * rightIt)
        {
            SCOPE_ASSERT((leftIt != nullptr && rightIt != nullptr));

            m_leftKeyIterator = leftIt;
            m_rightKeyIterator = rightIt;
        }

        void InitCrossRowMode()
        {
        }

        void InitLeftRowMode()
        {
            m_firstRead = leftAntiSemiJoin;
        }

        void InitRightRowMode()
        {
            m_firstRead = !leftAntiSemiJoin;
        }

        bool GetCrossRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }

        bool GetLeftRow(OutputSchema & outputRow)
        {
            if (!leftAntiSemiJoin)
            {
                return false;
            }

            if (m_firstRead)
            {
                m_firstRead = false;
            }
            else
            {
                m_leftKeyIterator->Increment();
            }

            if (m_leftKeyIterator->End())
            {
                return false;
            }

            //generator row
            CombinerPolicy<UID>::CopyRow(m_leftKeyIterator->GetRow(), nullptr, &outputRow);

            return true;
        }

        bool GetRightRow(OutputSchema & outputRow)
        {
            if (leftAntiSemiJoin)
            {
                return false;
            }

            if (m_firstRead)
            {
                m_firstRead = false;
            }
            else
            {
                m_rightKeyIterator->Increment();
            }

            if (m_rightKeyIterator->End())
            {
                return false;
            }

            //generator row
            CombinerPolicy<UID>::CopyRow(nullptr, m_rightKeyIterator->GetRow(), &outputRow);

            return true;
        }

        LONGLONG OperatorWaitOnIOTime() const
        {
            return 0;
        }

        void GetFlipCount(LONGLONG & goodFlips, LONGLONG & badFlips) const
        {
            goodFlips = 0;
            badFlips = 0;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
        }
    };

    ///
    /// LEFT ANTI-SEMIJOIN template
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class LeftAntiSemiJoiner : public AntiSemiJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, UID, true>
    {
    public:
        LeftAntiSemiJoiner()
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::NativeCombinerWrapper_LeftAntiSemiJoiner__Row_MinMemory);
        }
    };

    ///
    /// RIGHT ANTI-SEMIJOIN template
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class RightAntiSemiJoiner : public AntiSemiJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, UID, false>
    {
    public:
        RightAntiSemiJoiner()
        {
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::NativeCombinerWrapper_RightAntiSemiJoiner__Row_MinMemory);
        }
    };

    ///
    /// set operation joiner template.
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID>
    class SetOperationJoiner
    {
    public:
        typedef KeyIterator<InputOperatorLeft, InputSchemaLeft, typename CombinerPolicy<UID>::LeftKeyPolicy> LeftKeyIteratorType;
        typedef KeyIterator<InputOperatorRight, RightInputSchema, typename CombinerPolicy<UID>::RightKeyPolicy> RightKeyIteratorType;
        const static int IsCrossJoin = 0;

    protected:
        LeftKeyIteratorType * m_leftKeyIterator;
        RightKeyIteratorType * m_rightKeyIterator;

        bool            m_firstLeftRead;
        bool            m_firstRightRead;

    public:
        SetOperationJoiner() :
            m_firstLeftRead(true),
            m_firstRightRead(true)
        {
        }

        void Init(LeftKeyIteratorType * leftIt, RightKeyIteratorType * rightIt)
        {
            SCOPE_ASSERT((leftIt != nullptr && rightIt != nullptr));

            m_leftKeyIterator = leftIt;
            m_rightKeyIterator = rightIt;
        }

        void InitCrossRowMode()
        {
            // Cross Row Mode require both side of iterator are valid.
            SCOPE_ASSERT((m_rightKeyIterator != nullptr && m_leftKeyIterator != nullptr));

            m_firstLeftRead = true;
            m_firstRightRead = true;
        }

        void InitLeftRowMode()
        {
            SCOPE_ASSERT(m_leftKeyIterator != nullptr);
            m_firstLeftRead = true;
        }

        void InitRightRowMode()
        {
            SCOPE_ASSERT(m_rightKeyIterator != nullptr);
            m_firstRightRead = true;
        }

        LONGLONG OperatorWaitOnIOTime() const
        {
            return 0;
        }

        void GetFlipCount(LONGLONG & goodFlips, LONGLONG & badFlips) const
        {
            goodFlips = 0;
            badFlips = 0;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
        }
    };

    ///
    /// union all operation joiner template.
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID, bool IsDistinct>
    class UnionOperationJoiner : public SetOperationJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, UID>
    {
    public:
        bool GetCrossRow(OutputSchema & outputRow)
        {
            // For the first row it was already read. Otherwise, we need to read one row.
            if (m_firstLeftRead)
            {
                m_firstLeftRead = false;
            }
            else
            {
                if (IsDistinct)
                {
                    return false;
                }

                m_leftKeyIterator->Increment();

                // Done with left side rows, we will need to read right side rows.
                if (m_leftKeyIterator->End())
                {
                    // For the first row it was already read. Otherwise, we need to read one row.
                    if (m_firstRightRead)
                    {
                        m_firstRightRead = false;
                    }
                    else
                    {
                        m_rightKeyIterator->Increment();

                        if (m_rightKeyIterator->End())
                        {
                            return false;
                        }
                    }

                    CombinerPolicy<UID>::CopyRightRow(m_rightKeyIterator->GetRow(), &outputRow);

                    return true;
                }
            }

            CombinerPolicy<UID>::CopyLeftRow(m_leftKeyIterator->GetRow(), &outputRow);

            return true;

        }

        bool GetLeftRow(OutputSchema & outputRow)
        {
            // For the first row it was already read. Otherwise, we need to read one row.
            if (m_firstLeftRead)
            {
                m_firstLeftRead = false;
            }
            else
            {
                if (IsDistinct)
                {
                    return false;
                }

                m_leftKeyIterator->Increment();
                if (m_leftKeyIterator->End())
                {
                    return false;
                }
            }

            CombinerPolicy<UID>::CopyLeftRow(m_leftKeyIterator->GetRow(), &outputRow);

            return true;
        }

        bool GetRightRow(OutputSchema & outputRow)
        {
            // For the first row it was already read. Otherwise, we need to read one row.
            if (m_firstRightRead)
            {
                m_firstRightRead = false;
            }
            else
            {
                if (IsDistinct)
                {
                    return false;
                }

                m_rightKeyIterator->Increment();
                if (m_rightKeyIterator->End())
                {
                    return false;
                }
            }

            CombinerPolicy<UID>::CopyRightRow(m_rightKeyIterator->GetRow(), &outputRow);

            return true;
        }
    };

    ///
    /// set operation joiner template.
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class UnionJoiner : public UnionOperationJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, UID, true>
    {
    public:
        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::NativeCombinerWrapper_UnionJoiner__Row_MinMemory);
        }
    };

    ///
    /// set operation joiner template.
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class UnionAllJoiner : public UnionOperationJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, UID, false>
    {
    public:
        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::NativeCombinerWrapper_UnionAllJoiner__Row_MinMemory);
        }
    };

    ///
    /// Except operation joiner template.
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID, bool IsDistinct>
    class ExceptOperationJoiner : public UnionOperationJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, UID, IsDistinct>
    {
    public:
        bool GetCrossRow(OutputSchema & outputRow)
        {
            if (IsDistinct)
            {
                return false;
            }

            // For the first read, set up the iterator properly for except operation.
            if (m_firstLeftRead && m_firstRightRead)
            {
                m_firstLeftRead = false;
                m_firstRightRead = false;

                // move both side iterator at same time, until one end.
                while (!m_rightKeyIterator->End() && !m_leftKeyIterator->End())
                {
                    m_leftKeyIterator->Increment();
                    m_rightKeyIterator->Increment();
                }

                // If left side has less rows, then there is no return rows.
                if (m_leftKeyIterator->End())
                {
                    return false;
                }
            }
            else
            {
                m_leftKeyIterator->Increment();
                if (m_leftKeyIterator->End())
                {
                    return false;
                }
            }

            CombinerPolicy<UID>::CopyLeftRow(m_leftKeyIterator->GetRow(), &outputRow);

            return true;
        }

        bool GetRightRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }
    };

    ///
    /// except operation joiner template.
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class ExceptJoiner : public ExceptOperationJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, UID, true>
    {
    public:
        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::NativeCombinerWrapper_ExceptJoiner__Row_MinMemory);
        }
    };

    ///
    /// except all operation joiner template.
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class ExceptAllJoiner : public ExceptOperationJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, UID, false>
    {
    public:
        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::NativeCombinerWrapper_ExceptAllJoiner__Row_MinMemory);
        }
    };

    ///
    /// Except operation joiner template.
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID, bool IsDistinct>
    class IntersectOperationJoiner : public UnionOperationJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, UID, IsDistinct>
    {
    public:
        bool GetCrossRow(OutputSchema & outputRow)
        {
            // For the first read, set up the iterator properly for except operation.
            if (m_firstLeftRead && m_firstRightRead)
            {
                m_firstLeftRead = false;
                m_firstRightRead = false;
            }
            else
            {
                // For distinct intersect, it is done after we read first row from both side.
                if (IsDistinct)
                {
                    return false;
                }

                m_leftKeyIterator->Increment();
                m_rightKeyIterator->Increment();

                // if one side has no row, we are done.
                if (m_leftKeyIterator->End() || m_rightKeyIterator->End())
                {
                    return false;
                }
            }

            CombinerPolicy<UID>::CopyLeftRow(m_leftKeyIterator->GetRow(), &outputRow);

            return true;
        }

        bool GetLeftRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }

        bool GetRightRow(OutputSchema & outputRow)
        {
            UNREFERENCED_PARAMETER(outputRow);

            return false;
        }
    };

    ///
    /// except operation joiner template.
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class IntersectJoiner : public IntersectOperationJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, UID, true>
    {
    public:
        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::NativeCombinerWrapper_IntersectJoiner__Row_MinMemory);
        }
    };

    ///
    /// except all operation joiner template.
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID = -1, typename = None, typename = None> // The last two are for broadcast schemas
    class IntersectAllJoiner : public IntersectOperationJoiner<InputOperatorLeft, InputSchemaLeft, InputOperatorRight, RightInputSchema, OutputSchema, UID, false>
    {
    public:
        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::NativeCombinerWrapper_IntersectAllJoiner__Row_MinMemory);
        }
    };

    ///
    /// Wrapper UDO ICombiner in SQLIP mode
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID, typename = None, typename = None> // The last two are for broadcast schemas
    class SqlIpUdoJoiner
    {
        // Key policy type
        typedef typename CombinerPolicy<UID>::LeftKeyPolicy KeyPolicyLeft;
        typedef typename CombinerPolicy<UID>::RightKeyPolicy KeyPolicyRight;

    public:

        // KeyIterator type
        typedef KeyIterator<InputOperatorLeft, InputSchemaLeft, KeyPolicyLeft> LeftKeyIteratorType;
        typedef KeyIterator<InputOperatorRight, RightInputSchema, KeyPolicyRight> RightKeyIteratorType;
        const static int IsCrossJoin = 0;

    protected:

        // UDO combiner
        typedef SqlIpCombinerManaged<InputSchemaLeft, RightInputSchema, OutputSchema, KeyPolicyLeft, KeyPolicyRight> CombinerType;
        std::shared_ptr<CombinerType> m_managedCombiner;

        // Key iterator
        LeftKeyIteratorType  *m_leftKeyIterator;
        RightKeyIteratorType *m_rightKeyIterator;

    public:

        SqlIpUdoJoiner()
        {
        }

        SqlIpUdoJoiner(SqlIpCombinerManaged<InputSchemaLeft, RightInputSchema, OutputSchema, KeyPolicyLeft, KeyPolicyRight> * managedCombiner)
        {
            m_managedCombiner.reset(managedCombiner);
        }

        void Init(LeftKeyIteratorType * leftIt, RightKeyIteratorType * rightIt)
        {
            SCOPE_ASSERT((leftIt != nullptr && rightIt != nullptr));

            m_leftKeyIterator = leftIt;
            m_rightKeyIterator = rightIt;
            m_managedCombiner->Init();
        }

        void InitCrossRowMode()
        {
            InitLeftRowMode();
            InitRightRowMode();
        }

        void InitLeftRowMode()
        {
            m_managedCombiner->SetLeftKeyIterator(m_leftKeyIterator);
        }

        void InitRightRowMode()
        {
            m_managedCombiner->SetRightKeyIterator(m_rightKeyIterator);
        }

        bool GetCrossRow(OutputSchema & outputRow)
        {
            return m_managedCombiner->GetNextRow(outputRow, CombinerType::SIDETYPE::BOTH);
        }

        bool GetLeftRow(OutputSchema & outputRow)
        {
            return m_managedCombiner->GetNextRow(outputRow, CombinerType::SIDETYPE::LEFT);
        }

        bool GetRightRow(OutputSchema & outputRow)
        {
            return m_managedCombiner->GetNextRow(outputRow, CombinerType::SIDETYPE::RIGHT);
        }

        LONGLONG OperatorWaitOnIOTime() const
        {
            return 0;
        }

        void GetFlipCount(LONGLONG & goodFlips, LONGLONG & badFlips) const
        {
            goodFlips = 0;
            badFlips = 0;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("SqlIpUdoJoiner");

            m_managedCombiner->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return m_managedCombiner->GetOperatorRequirements();
        }
    };

    ///
    /// Wrapper UDO ICombiner in SQLIP mode
    ///
    template<typename InputOperatorLeft, typename InputSchemaLeft, typename InputOperatorRight, typename RightInputSchema, typename OutputSchema, int UID, typename ThirdInputSchema = None, typename FourthInputSchema = None>
    class SqlIpUdoMultiwayCombiner
    {
        // Key policy type
        typedef typename CombinerPolicy<UID>::LeftKeyPolicy KeyPolicyLeft;
        typedef typename CombinerPolicy<UID>::RightKeyPolicy KeyPolicyRight;

    public:

        // KeyIterator type
        typedef KeyIterator<InputOperatorLeft, InputSchemaLeft, KeyPolicyLeft> LeftKeyIteratorType;
        typedef KeyIterator<InputOperatorRight, RightInputSchema, KeyPolicyRight> RightKeyIteratorType;
        const static int IsCrossJoin = 0;

    protected:

        // UDO combiner
        typedef SqlIpMultiwayCombinerManaged<InputSchemaLeft, RightInputSchema, OutputSchema, KeyPolicyLeft, KeyPolicyRight, ThirdInputSchema, FourthInputSchema> CombinerType;
        std::shared_ptr<CombinerType> m_managedCombiner;

        // Key iterator
        LeftKeyIteratorType  *m_leftKeyIterator;
        RightKeyIteratorType *m_rightKeyIterator;

    public:

        SqlIpUdoMultiwayCombiner()
        {
        }

        SqlIpUdoMultiwayCombiner(SqlIpMultiwayCombinerManaged<InputSchemaLeft, RightInputSchema, OutputSchema, KeyPolicyLeft, KeyPolicyRight, ThirdInputSchema, FourthInputSchema> * managedCombiner)
        {
            m_managedCombiner.reset(managedCombiner);
        }

        void Init(LeftKeyIteratorType * leftIt, RightKeyIteratorType * rightIt)
        {
            SCOPE_ASSERT((leftIt != nullptr && rightIt != nullptr));

            m_leftKeyIterator = leftIt;
            m_rightKeyIterator = rightIt;
            m_managedCombiner->Init();
        }

        void InitCrossRowMode()
        {
            InitLeftRowMode();
            InitRightRowMode();
        }

        void InitLeftRowMode()
        {
            m_managedCombiner->SetLeftKeyIterator(m_leftKeyIterator);
        }

        void InitRightRowMode()
        {
            m_managedCombiner->SetRightKeyIterator(m_rightKeyIterator);
        }

        bool GetCrossRow(OutputSchema & outputRow)
        {
            return m_managedCombiner->GetNextRow(outputRow, CombinerType::SIDETYPE::BOTH);
        }

        bool GetLeftRow(OutputSchema & outputRow)
        {
            return m_managedCombiner->GetNextRow(outputRow, CombinerType::SIDETYPE::LEFT);
        }

        bool GetRightRow(OutputSchema & outputRow)
        {
            return m_managedCombiner->GetNextRow(outputRow, CombinerType::SIDETYPE::RIGHT);
        }

        LONGLONG OperatorWaitOnIOTime() const
        {
            return 0;
        }

        void GetFlipCount(LONGLONG & goodFlips, LONGLONG & badFlips) const
        {
            goodFlips = 0;
            badFlips = 0;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("SqlIpUdoJoiner");

            m_managedCombiner->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return m_managedCombiner->GetOperatorRequirements();
        }
    };

    ///
    /// native Combiner operator template.
    ///
    template<typename LeftOperator, typename LeftInputSchema, typename RightOperator, typename RightInputSchema, typename OutputSchema, template<class, class, class, class, class, int, class = None, class = None> class JoinerPolicy, int UID = -1, typename ThirdInputSchema = None, typename FourthInputSchema = None>
    class NativeCombinerWrapper : public Operator<NativeCombinerWrapper<LeftOperator, LeftInputSchema, RightOperator, RightInputSchema, OutputSchema, JoinerPolicy, UID, ThirdInputSchema, FourthInputSchema>, OutputSchema, UID>
    {
        LeftOperator  *  m_leftChild;  // left child operator
        RightOperator *  m_rightChild; // right child operator
        OperatorDelegate<ThirdInputSchema> *  m_thirdChild; // third child operator
        OperatorDelegate<FourthInputSchema> *  m_fourthChild; // fourth child operator

        typedef JoinerPolicy<LeftOperator, LeftInputSchema, RightOperator, RightInputSchema, OutputSchema, UID, ThirdInputSchema, FourthInputSchema> Joiner;

        typedef typename JoinerPolicy<LeftOperator, LeftInputSchema, RightOperator, RightInputSchema, OutputSchema, UID, ThirdInputSchema, FourthInputSchema>::LeftKeyIteratorType LeftKeyIteratorType;
        typedef typename JoinerPolicy<LeftOperator, LeftInputSchema, RightOperator, RightInputSchema, OutputSchema, UID, ThirdInputSchema, FourthInputSchema>::RightKeyIteratorType RightKeyIteratorType;

        int m_payloadSrcIndex;

        enum GetRowState{
            UnInit,
            Start,
            SameKey,
            LeftLarger,
            RightLarger,
            LeftRemain,
            RightRemain,
            Finished,
        };

        GetRowState              m_state;
        std::shared_ptr<Joiner>  m_joiner;

        LeftKeyIteratorType      m_leftKeyIterator;
        RightKeyIteratorType     m_rightKeyIterator;

        LONGLONG m_keyCount;
        bool     m_keyRangeStart;
        bool     m_isBulk;

        void Reset(GetRowState state)
        {
            m_state = state;
            m_keyRangeStart = true;
        }

        void ProcessStats(AutoExecStats & stats)
        {
            stats.IncreaseRowCount(1);

            if (m_keyRangeStart)
            {
                m_keyRangeStart = false;
                m_keyCount++;
            }
        }

    public:
        NativeCombinerWrapper(LeftOperator * left, RightOperator * right, int operatorId, int payloadSrcIndex, bool isBulk, std::shared_ptr<Joiner> joiner, OperatorDelegate<ThirdInputSchema> * third = nullptr, OperatorDelegate<FourthInputSchema> * fourth = nullptr) :
            Operator(operatorId),
            m_state(UnInit),
            m_leftChild(left),
            m_rightChild(right),
            m_thirdChild(third),
            m_fourthChild(fourth),
            m_leftKeyIterator(left),
            m_rightKeyIterator(right),
            m_payloadSrcIndex(payloadSrcIndex),
            m_keyCount(0),
            m_isBulk(isBulk),
            m_keyRangeStart(false)
        {
            if (joiner == nullptr)
            {
                m_joiner.reset(new Joiner());
            }
            else
            {
                m_joiner = joiner;
            }
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_leftChild->Init();
            m_rightChild->Init();
            if (m_thirdChild)
                m_thirdChild->Init();
            if (m_fourthChild)
                m_fourthChild->Init();

            m_joiner->Init(&m_leftKeyIterator, &m_rightKeyIterator);
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            if (0 == m_payloadSrcIndex)
            {
                return m_leftChild->GetMetadata();
            }

            return m_rightChild->GetMetadata();
        }

        /// GetRow implementation for combiner. The combiner expected the rows get from left and right child are sorted on joining key.
        /// Get row from merger
        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            for (;;)
            {
                switch (m_state)
                {
                case UnInit:
                {
                               //read first row from both side
                               m_leftKeyIterator.ReadFirst();
                               m_rightKeyIterator.ReadFirst();

                               Reset(Start);
                               break;
                }

                case Start:
                {
                              // reset key change flag for both side
                              m_leftKeyIterator.ResetKey();
                              m_rightKeyIterator.ResetKey();

                              // If left side has no row, we will only work on right side
                              if (m_leftKeyIterator.End())
                              {
                                  if (m_rightKeyIterator.End())
                                  {
                                      // If right side is also ended, then we finished with the combine operation.
                                      Reset(Finished);
                                      return false;
                                  }
                                  else
                                  {
                                      Reset(RightRemain);
                                      m_joiner->InitRightRowMode();
                                  }
                                  break;
                              }

                              // If right side has no row, we will only work on left side.
                              if (m_rightKeyIterator.End())
                              {
                                  Reset(LeftRemain);
                                  m_joiner->InitLeftRowMode();
                                  break;
                              }

                              // If this is a cross join or bulk, there is no need to compare key.
                              if (JoinerPolicy<LeftOperator, LeftInputSchema, RightOperator, RightInputSchema, OutputSchema, UID>::IsCrossJoin || m_isBulk)
                              {
                                  Reset(SameKey);
                                  m_joiner->InitCrossRowMode();
                                  break;
                              }
                              else
                              {
                                  // Compare key from left and right side
                                  int cmpResult = CombinerPolicy<UID>::Compare(m_leftKeyIterator.GetRow(), m_rightKeyIterator.GetRow());
                                  if (cmpResult == 0)
                                  {
                                      Reset(SameKey);
                                      m_joiner->InitCrossRowMode();
                                      break;
                                  }
                                  else if (cmpResult > 0)
                                  {
                                      Reset(LeftLarger);
                                      m_joiner->InitRightRowMode();
                                      break;
                                  }
                                  else
                                  {
                                      Reset(RightLarger);
                                      m_joiner->InitLeftRowMode();
                                      break;
                                  }
                              }
                }

                case SameKey:
                {
                                // get cross product of left and right rows which has same key
                                if (m_joiner->GetCrossRow(output))
                                {
                                    ProcessStats(stats);
                                    return true;
                                }

                                // both iterator has done with current key, need to reestablish the state.
                                Reset(Start);

                                // Drain current key from both side before move on
                                m_leftKeyIterator.Drain();
                                m_rightKeyIterator.Drain();
                                break;
                }

                case LeftLarger:
                {
                                   if (m_joiner->GetRightRow(output))
                                   {
                                       ProcessStats(stats);
                                       return true;
                                   }

                                   Reset(Start);

                                   // Drain current key from right side before move on
                                   m_rightKeyIterator.Drain();
                                   break;
                }

                case RightLarger:
                {
                                    if (m_joiner->GetLeftRow(output))
                                    {
                                        ProcessStats(stats);
                                        return true;
                                    }

                                    Reset(Start);

                                    // Drain current key from left side before move on
                                    m_leftKeyIterator.Drain();
                                    break;
                }

                case LeftRemain:
                {
                                   if (m_joiner->GetLeftRow(output))
                                   {
                                       ProcessStats(stats);
                                       return true;
                                   }

                                   // Drain current key from left side.
                                   m_leftKeyIterator.Drain();

                                   // Reset key if key has changed
                                   m_leftKeyIterator.ResetKey();

                                   // If there is no more rows, finish the operator
                                   if (m_leftKeyIterator.End())
                                   {
                                       Reset(Finished);
                                       return false;
                                   }
                                   else
                                   {
                                       Reset(LeftRemain);
                                       m_joiner->InitLeftRowMode();
                                       break;
                                   }
                }

                case RightRemain:
                {
                                    if (m_joiner->GetRightRow(output))
                                    {
                                        ProcessStats(stats);
                                        return true;
                                    }

                                    // Drain current key from right side.
                                    m_rightKeyIterator.Drain();

                                    // Reset key if key has changed
                                    m_rightKeyIterator.ResetKey();

                                    // If there is no more rows, finish the operator
                                    if (m_rightKeyIterator.End())
                                    {
                                        Reset(Finished);
                                        return false;
                                    }
                                    else
                                    {
                                        Reset(RightRemain);
                                        m_joiner->InitRightRowMode();
                                        break;
                                    }
                }

                default:
                    SCOPE_ASSERT(!"invalid default state for combiner");
                    return false;
                }
            }
        }

        // Release all resources of children
        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_leftChild->Close();
            m_rightChild->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("MergeJoin");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_leftChild->GetInclusiveTimeMillisecond() - m_rightChild->GetInclusiveTimeMillisecond() - m_joiner->OperatorWaitOnIOTime());
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_joiner->OperatorWaitOnIOTime());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            RuntimeStats::WriteKeyCount(node, m_keyCount);
            node.AddAttribute(RuntimeStats::MaxAvgJoinProduct(), m_keyCount > 0 ? GetRowCount() / m_keyCount : 0);

            // Add stats regarding left/right flipping for INNER/FULL OUTER joins (this is to measure benefit of the "flipping" code)
            LONGLONG goodFlips, badFlips;
            m_joiner->GetFlipCount(goodFlips, badFlips);
            node.AddAttribute("goodFlips", goodFlips);
            node.AddAttribute("badFlips", badFlips);

            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_joiner->WriteRuntimeStats(node);
            m_leftKeyIterator.WriteRuntimeStats(node);
            m_rightKeyIterator.WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return m_joiner->GetOperatorRequirements();
        }
    };

    //
    // Hash Combiner Join Policies
    // ProbeSchema -- probe row type; BuildContainer -- build rows container type.
    //
    template <typename ProbeSchema, typename BuildContainer, typename OutputSchema, typename Policy, int UID>
    class HashOuterJoiner
    {
    private:
        typedef typename BuildContainer::Values::const_iterator Iterator;

    private:
        const ProbeSchema *    m_probeRow;
        Iterator               m_it;
        Iterator               m_end;
        bool                   m_first;
    protected:
        bool                   m_buildExists;

    private:
        template<unsigned CrossLimit>
        void CheckMaxCrossSizeLimit(LONGLONG buildSize, LONGLONG probeSize)
        {
            if (std::min(buildSize, probeSize) > CrossLimit)
            {
                throw JoinCrossLimitExceededException(UID, CrossLimit);
            }
        }

        template<>
        void CheckMaxCrossSizeLimit<0>(LONGLONG buildSize, LONGLONG probeSize)
        {
        }

        template<typename Container>
        void UpdateProbeStats(const Container * container)
        {
        }

        template<typename KeySchema, typename ValueSchema, typename Allocator>
        void UpdateProbeStats(const ListOfValuesContainerWithStats<KeySchema, ValueSchema, Allocator> * container)
        {
            container->Stats().IncProbeSize();

            CheckMaxCrossSizeLimit<Policy::m_maxPerKeyCrossSizeLimit>(
                container->Stats().BuildSize(),
                container->Stats().ProbeSize());
        }

    protected:
        bool GetProbeRow(OutputSchema & output)
        {
            if (m_first)
            {
                Policy::CopyProbeAndNullifyBuild(*m_probeRow, output);
                m_first = false;
                return true;
            }

            return false;
        }

        bool GetBoth(OutputSchema & output)
        {
            SCOPE_ASSERT(m_buildExists);  // Call GetProbeRow, not GetBoth.

            if (m_it != m_end)
            {
                Policy::CopyBoth(*m_probeRow, *(*m_it), output);
                ++m_it;

                return true;
            }
            return false;
        }

    public:
        HashOuterJoiner() :
            m_probeRow(nullptr),
            m_first(false),
            m_buildExists(false)
        {
        }

        static bool EmptyBuildImpliesEmptyOutput()
        {
            return false;
        }

        void Init(const ProbeSchema & probeRow, const BuildContainer * buildContainer)
        {
            m_probeRow = &probeRow;
            m_first = true;

            if (buildContainer)
            {
                const BuildContainer::Values &buildValues = buildContainer->Both().second;
                m_it = buildValues.begin();
                m_end = buildValues.end();
                m_buildExists = true;

                SCOPE_ASSERT(m_it != m_end);

                UpdateProbeStats(buildContainer);
            }
            else
            {
                m_buildExists = false;
                // No build values exist - don't probe for them.  If you accidentally do, you may hit an assert from STL
                // on debug builds because you cannot compare the iterators safely with one another if they aren't properly initialized.
            }
        }

        bool GetNextRow(OutputSchema & output)
        {
            if (!m_buildExists)
            {
                return GetProbeRow(output);
            }
            else
            {
                return GetBoth(output);
            }
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    template <typename ProbeSchema, typename BuildContainer, typename OutputSchema, typename Policy, int UID>
    class HashLeftOuterJoiner : public HashOuterJoiner<ProbeSchema, BuildContainer, OutputSchema, Policy, UID>
    {};

    template <typename ProbeSchema, typename BuildContainer, typename OutputSchema, typename Policy, int UID>
    class HashRightOuterJoiner : public HashOuterJoiner<ProbeSchema, BuildContainer, OutputSchema, Policy, UID>
    {};

    template <typename ProbeSchema, typename BuiltIterator, typename OutputSchema, typename Policy, int UID>
    class HashInnerJoiner : public HashOuterJoiner<ProbeSchema, BuiltIterator, OutputSchema, Policy, UID>
    {
    public:
        static bool EmptyBuildImpliesEmptyOutput()
        {
            return true;
        }

        bool GetNextRow(OutputSchema & output)
        {
            if (m_buildExists)
            {
                return GetBoth(output);
            }
            else
            {
                return false;
            }
        }
    };

    template <typename ProbeSchema, typename BuildContainer, typename OutputSchema, typename Policy, int UID>
    class HashSemiJoiner : public HashOuterJoiner<ProbeSchema, BuildContainer, OutputSchema, Policy, UID>
    {
    public:
        static bool EmptyBuildImpliesEmptyOutput()
        {
            return true;
        }

        bool GetNextRow(OutputSchema & output)
        {
            if (m_buildExists)
            {
                return GetProbeRow(output);
            }
            else
            {
                return false;
            }
        }
    };

    template <typename ProbeSchema, typename BuildContainer, typename OutputSchema, typename Policy, int UID>
    class HashLeftSemiJoiner : public HashSemiJoiner<ProbeSchema, BuildContainer, OutputSchema, Policy, UID>
    {};

    template <typename ProbeSchema, typename BuildContainer, typename OutputSchema, typename Policy, int UID>
    class HashRightSemiJoiner : public HashSemiJoiner<ProbeSchema, BuildContainer, OutputSchema, Policy, UID>
    {};

    template <typename ProbeSchema, typename BuildContainer, typename OutputSchema, typename Policy, int UID>
    class HashAntiSemiJoiner : public HashOuterJoiner<ProbeSchema, BuildContainer, OutputSchema, Policy, UID>
    {
    public:
        static bool EmptyBuildImpliesEmptyOutput()
        {
            return false;
        }

        bool GetNextRow(OutputSchema & output)
        {
            if (!m_buildExists)
            {
                return GetProbeRow(output);
            }
            else
            {
                return false;
            }
        }
    };

    template <typename ProbeSchema, typename BuildContainer, typename OutputSchema, typename Policy, int UID>
    class HashLeftAntiSemiJoiner : public HashAntiSemiJoiner<ProbeSchema, BuildContainer, OutputSchema, Policy, UID>
    {};

    template <typename ProbeSchema, typename BuildContainer, typename OutputSchema, typename Policy, int UID>
    class HashRightAntiSemiJoiner : public HashAntiSemiJoiner<ProbeSchema, BuildContainer, OutputSchema, Policy, UID>
    {};

    //
    // Hash Combiner
    //
    template<typename ProbeOperator,
        typename ProbeSchema,
        typename BuildOperator,
        typename BuildSchema,
        typename OutputSchema,
        template <typename, typename, typename, typename, int> class Joiner,
        int UID = -1>
    class HashCombiner : public Operator<HashCombiner<ProbeOperator, ProbeSchema, BuildOperator, BuildSchema, OutputSchema, Joiner, UID>, OutputSchema, UID>
    {
    private:
        typedef          HashCombinerPolicy<ProbeSchema,
            BuildSchema,
            OutputSchema,
            UID>                               Policy;

        typedef typename Policy::KeySchema                                     KeySchema;
        typedef typename Policy::ValueSchema                                   ValueSchema;

    private:
        typedef typename Policy::Hashtable                                     Hashtable;

        typedef          std::pair<typename Hashtable::ConstIterator,
            typename Hashtable::EResult>                HashtableIterator;

        typedef typename Hashtable::Container::KeyValue                        BuildKeyValue;

        typedef          Joiner<ProbeSchema,
            typename Hashtable::Container,
            OutputSchema,
            Policy,
            UID>                                           Joiner;

        typedef          RowIterator<ProbeOperator, ProbeSchema>               ProbeIterator;
        typedef          RowIterator<BuildOperator, BuildSchema>               BuildIterator;

        typedef          RowIteratorAdapter<ProbeIterator,
            KeySchema,
            ProbeSchema,
            typename Policy::ProbeKeyValue>    ProbeInputIterator;

        typedef          RowIteratorAdapter<BuildIterator,
            KeySchema,
            ValueSchema,
            typename Policy::BuildKeyValue>    BuildInputIterator;

        // >>> spilling
        typedef          AuxiliaryStorage<KeySchema,
            ProbeSchema,
            typename Policy::Hash>               ProbeStorage;

        typedef          AuxiliaryStorage<KeySchema,
            ValueSchema,
            typename Policy::Hash>               BuildStorage;

        typedef typename ProbeStorage::PartitionIterator                       ProbePartitionIterator;
        typedef typename BuildStorage::PartitionIterator                       BuildPartitionIterator;
        // <<< spilling

    private:
        struct PartitionIteratorPair
        {
            ProbePartitionIterator    m_probe;
            BuildPartitionIterator    m_build;
            UINT                      m_level;

            PartitionIteratorPair(PartitionIteratorPair&& other)
                : m_probe(std::move(other.m_probe))
                , m_build(std::move(other.m_build))
                , m_level(std::move(other.m_level))
            {
            }

            PartitionIteratorPair(ProbePartitionIterator&& probe, BuildPartitionIterator&& build, UINT level) :
                m_probe(std::move(probe)),
                m_build(std::move(build)),
                m_level(level)
            {
            }
        };

        typedef std::vector<PartitionIteratorPair>  PartitionIteratorPairs;

    private:
        enum SourceState
        {
            ReadFromInput,
            InitNextSpilledPartition,
            ReadFromSpilledPartition
        };

        enum PhaseState
        {
            UnInit,
            Build,
            Probe,
            Output
        };

    private:
        ProbeOperator *                            m_probeChild;
        BuildOperator *                            m_buildChild;

        SourceState                                m_sourceState;
        PhaseState                                 m_phaseState;

        std::unique_ptr<Hashtable>                 m_hashtable;
        HashtableIterator                          m_hashtableIterator;
        Joiner                                     m_joiner;

        ProbeIterator                              m_probeIterator;
        BuildIterator                              m_buildIterator;
        ProbeInputIterator                         m_probeInputIterator;
        BuildInputIterator                         m_buildInputIterator;

        // >>> spilling
        UINT                                       m_level;
        PartitionIteratorPairs                     m_partitionPairs;
        ProbePartitionIterator *                   m_probePartitionIterator;
        BuildPartitionIterator *                   m_buildPartitionIterator;
        std::unique_ptr<ProbeStorage>              m_probeStorage;
        std::unique_ptr<BuildStorage>              m_buildStorage;
        // <<< spilling

        IncrementalAllocator                       m_probeAlloc;
        IncrementalAllocator                       m_buildAlloc;

        int                                        m_payloadSrcIndex;

        // >>> stats
        SIZE_T                                     m_htMaxTotalMemory;
        SIZE_T                                     m_htMaxDataMemory;
        SIZE_T                                     m_htInsertCount;
        SIZE_T                                     m_htLookupCount;
        SIZE_T                                     m_spProbeRowCnt;
        SIZE_T                                     m_spBuildRowCnt;
        SIZE_T                                     m_spMaxLevel;
        // <<< stats

    private:

        void SpillBuild()
        {
            SCOPE_ASSERT(m_level < Policy::Spilling::m_seedCnt); //Hash join exceeded max spill level

            if (!(m_probeStorage && m_buildStorage))
            {
                INT64 seed = Policy::Spilling::SeedBeta()[m_level];
                m_probeStorage.reset(new ProbeStorage(Policy::Spilling::m_partitionCnt,
                    Policy::Spilling::m_bufferSize,
                    Policy::Spilling::m_bufferCnt,
                    Policy::Hash(seed)));
                m_buildStorage.reset(new BuildStorage(Policy::Spilling::m_partitionCnt,
                    Policy::Spilling::m_bufferSize,
                    Policy::Spilling::m_bufferCnt,
                    Policy::Hash(seed)));
            }

            UINT spilledRowCnt = m_hashtable->Spill(*m_buildStorage.get(), Policy::Spilling::FractionOfBucketsToSpill());
            m_spBuildRowCnt += spilledRowCnt;
        }

        template <typename ProbeIterator, typename BuildIterator>
        bool GetNextRowImpl(ProbeIterator & probeIt, BuildIterator & buildIt, OutputSchema & output)
        {
            for (;;)
            {
                switch (m_phaseState)
                {
                case UnInit:
                {
                               probeIt.ReadFirst();
                               buildIt.ReadFirst();

                               SCOPE_ASSERT(m_level < Policy::Spilling::m_seedCnt); //Hash join exceeded max spill level
                               // destroy iterator before cleaning up the memory
                               m_joiner = Joiner();
                               m_hashtableIterator = HashtableIterator();
                               m_hashtable.reset(new Hashtable(Policy::m_memoryQuota,
                                   "HashCombiner",
                                   Policy::m_initialSize,
                                   Policy::MaxLoadFactor(),
                                   Policy::Hash(Policy::Spilling::SeedAlpha()[m_level])));
                               m_phaseState = Build;

                               break;
                }
                case Build:
                {
                              bool probeDone = probeIt.End();
                              bool buildDone = buildIt.End();
                              if (probeDone || (buildDone && m_joiner.EmptyBuildImpliesEmptyOutput()))
                              {
                                  return false;
                              }

                              while (!buildIt.End())
                              {
                                  const KeySchema & key = buildIt.GetKey();
                                  const ValueSchema & value = buildIt.GetValue();

                                  Hashtable::EResult res = m_hashtable->Insert(key, value);
                                  SCOPE_ASSERT(res == Hashtable::OK_INSERT || res == Hashtable::FAILED_OUT_OF_MEMORY || res == Hashtable::FAILED_SPILLED);

                                  if (res == Hashtable::OK_INSERT)
                                  {
                                      m_buildAlloc.Reset();
                                      ++m_htInsertCount;
                                      buildIt.Increment();
                                  }
                                  else if (res == Hashtable::FAILED_SPILLED)
                                  {
                                      SCOPE_ASSERT(m_buildStorage); // hashtable must return FAILED_SPILLED only in case some buckets were already dropped

                                      m_buildStorage->Write(key, value);
                                      m_buildAlloc.Reset();
                                      ++m_spBuildRowCnt;
                                      buildIt.Increment();
                                  }
                                  else // FAILED_OUT_OF_MEMORY
                                  {
                                      if (Configuration::GetGlobal().GetRestrictOperatorMemorySpilling())
                                      {
                                          throw OperatorOutOfMemoryException(GetOperatorId());
                                      }

                                      SpillBuild();
                                  }
                              }

                              m_htMaxTotalMemory = std::max(m_htMaxTotalMemory, m_hashtable->MemoryUsage());
                              m_htMaxDataMemory = std::max(m_htMaxDataMemory, m_hashtable->DataMemoryUsage());

                              m_phaseState = Probe;

                              break;
                }
                case Probe:
                {
                              SCOPE_ASSERT(buildIt.End());

                              if (probeIt.End())
                              {
                                  return false;
                              }

                              const KeySchema & key = probeIt.GetKey();
                              const ProbeSchema & probe = probeIt.GetValue();

                              m_hashtableIterator = m_hashtable->Find(key);
                              ++m_htLookupCount;

                              Hashtable::EResult res = m_hashtableIterator.second;
                              SCOPE_ASSERT(res == Hashtable::OK_FOUND || res == Hashtable::FAILED_NOT_FOUND || res == Hashtable::FAILED_SPILLED);

                              if (res == Hashtable::OK_FOUND || res == Hashtable::FAILED_NOT_FOUND)
                              {
                                  m_joiner.Init(probe, (res == Hashtable::OK_FOUND ? m_hashtableIterator.first.container() : nullptr));
                                  m_phaseState = Output;
                              }
                              else // FAILED_SPILLED
                              {
                                  SCOPE_ASSERT(m_probeStorage);

                                  m_probeStorage->Write(key, probe);
                                  ++m_spProbeRowCnt;
                                  m_probeAlloc.Reset();
                                  probeIt.Increment();
                              }

                              break;
                }
                case Output:
                {
                               if (m_joiner.GetNextRow(output))
                               {
                                   return true;
                               }

                               m_probeAlloc.Reset();
                               probeIt.Increment();
                               m_phaseState = Probe;

                               break;
                }
                default:
                {
                           SCOPE_ASSERT(!"Invalid state for hash joiner");
                           return false;
                }
                }
            }
        }

        void PushSpilledPartitions(UINT level)
        {
            if (!(m_probeStorage && m_buildStorage))
            {
                return;
            }

            while (m_probeStorage->HasMorePartitions() && m_buildStorage->HasMorePartitions())
            {
                m_partitionPairs.emplace_back(m_probeStorage->GetNextPartition(),
                    m_buildStorage->GetNextPartition(),
                    level);

            }

            SCOPE_ASSERT(!m_buildStorage->HasMorePartitions() && !m_buildStorage->HasMorePartitions());

            m_probeStorage.reset();
            m_buildStorage.reset();
        }

        UINT PopSpilledPartition()
        {
            UINT level = m_partitionPairs.back().m_level;

            m_partitionPairs.back().m_probe.Close();
            m_partitionPairs.back().m_build.Close();
            m_partitionPairs.pop_back();

            return level;
        }

        PartitionIteratorPair& InitTopSpilledPartition()
        {
            PartitionIteratorPair& top = m_partitionPairs.back();

            m_probeAlloc.Reset();
            m_buildAlloc.Reset();

            top.m_probe.Init(Policy::Spilling::m_bufferSize, Policy::Spilling::m_bufferCnt, m_probeAlloc);
            top.m_build.Init(Policy::Spilling::m_bufferSize, Policy::Spilling::m_bufferCnt, m_buildAlloc);

            return top;
        }

        bool HasSpilledPartitions() const
        {
                return !m_partitionPairs.empty();
        }

    public:
        HashCombiner(ProbeOperator * probe, BuildOperator * build, int operatorId, int payloadSrcIndex, SIZE_T maxAllocatorSize = 0) :
            Operator(operatorId),
            m_probeChild(probe),
            m_buildChild(build),
            m_sourceState(ReadFromInput),
            m_phaseState(UnInit),
            m_probeIterator(probe),
            m_buildIterator(build),
            m_probeInputIterator(m_probeIterator),
            m_buildInputIterator(m_buildIterator),
            m_level(0),
            m_probePartitionIterator(nullptr),
            m_buildPartitionIterator(nullptr),
            m_probeAlloc(),
            m_buildAlloc(),
            m_payloadSrcIndex(payloadSrcIndex),
            m_htMaxTotalMemory(0),
            m_htMaxDataMemory(0),
            m_htInsertCount(0),
            m_htLookupCount(0),
            m_spProbeRowCnt(0),
            m_spBuildRowCnt(0),
            m_spMaxLevel(0)
        {
            SIZE_T maxSize = (maxAllocatorSize == 0 ? Configuration::GetGlobal().GetMaxInMemoryRowSize() : maxAllocatorSize);
            m_probeAlloc.Init(maxSize, "HashCombinerProbe");
            m_buildAlloc.Init(maxSize, "HashCombinerBuild");
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_probeChild->Init();
            m_buildChild->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            if (0 == m_payloadSrcIndex)
            {
                return m_probeChild->GetMetadata();
            }

            return m_buildChild->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            for (;;)
            {
                switch (m_sourceState)
                {
                case ReadFromInput:
                {
                                      bool hasNextRow = GetNextRowImpl(m_probeInputIterator, m_buildInputIterator, output);

                                      if (hasNextRow)
                                      {
                                          stats.IncreaseRowCount(1);
                                          return true;
                                      }

                                      PushSpilledPartitions(0);

                                      m_sourceState = InitNextSpilledPartition;

                                      break;
                }
                case InitNextSpilledPartition:
                {
                                                 if (!HasSpilledPartitions())
                                                 {
                                                     return false;
                                                 }

                                                 PartitionIteratorPair& top = InitTopSpilledPartition();

                                                 m_probePartitionIterator = &top.m_probe;
                                                 m_buildPartitionIterator = &top.m_build;
                                                 m_level = top.m_level + 1;
                                                 m_spMaxLevel = std::max((UINT)m_spMaxLevel, m_level);

                                                 m_sourceState = ReadFromSpilledPartition;
                                                 m_phaseState = UnInit;

                                                 break;
                }
                case ReadFromSpilledPartition:
                {
                                                 bool hasNextRow = GetNextRowImpl(*m_probePartitionIterator, *m_buildPartitionIterator, output);

                                                 if (hasNextRow)
                                                 {
                                                     stats.IncreaseRowCount(1);
                                                     return true;
                                                 }

                                                 UINT level = PopSpilledPartition();
                                                 PushSpilledPartitions(level + 1);

                                                 m_sourceState = InitNextSpilledPartition;

                                                 break;
                }
                default:
                {
                           SCOPE_ASSERT(!"Invalid source state for hash joiner");
                           return false;
                }
                }
            }
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_probeChild->Close();
            m_buildChild->Close();

            // destroy iterator before cleaning up the memory
            m_joiner = Joiner();
            m_hashtableIterator = HashtableIterator();
            //release all the memory used by the hashtable
            m_hashtable.reset();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("HashJoin");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_probeChild->GetInclusiveTimeMillisecond() - m_buildChild->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            node.AddAttribute(RuntimeStats::AvgHashtableTotalMemory(), m_htMaxTotalMemory);
            node.AddAttribute(RuntimeStats::MaxHashtableTotalMemory(), m_htMaxTotalMemory);
            node.AddAttribute(RuntimeStats::AvgHashtableLookupCount(), m_htLookupCount);
            node.AddAttribute(RuntimeStats::MaxHashtableLookupCount(), m_htLookupCount);
            node.AddAttribute(RuntimeStats::AvgSpillProbeRowCount(), m_spProbeRowCnt);
            node.AddAttribute(RuntimeStats::MaxSpillProbeRowCount(), m_spProbeRowCnt);
            node.AddAttribute(RuntimeStats::AvgSpillBuildRowCount(), m_spBuildRowCnt);
            node.AddAttribute(RuntimeStats::MaxSpillBuildRowCount(), m_spBuildRowCnt);
            node.AddAttribute(RuntimeStats::AvgHashtableInsertCount(), m_htInsertCount);
            node.AddAttribute(RuntimeStats::MaxHashtableInsertCount(), m_htInsertCount);
            node.AddAttribute(RuntimeStats::AvgHashtableMaxDataMemory(), m_htMaxDataMemory);
            node.AddAttribute(RuntimeStats::MaxHashtableMaxDataMemory(), m_htMaxDataMemory);
            node.AddAttribute(RuntimeStats::AvgHashtableSpillLevel(), m_spMaxLevel);
            node.AddAttribute(RuntimeStats::MaxHashtableSpillLevel(), m_spMaxLevel);

            m_probeChild->WriteRuntimeStats(node);
            m_buildChild->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements(m_joiner.GetOperatorRequirements())
                .AddMemory(OperatorRequirementsConstants::RowBuffer_HashWithSpill__Size_MinMemory, OperatorRequirementsConstants::RowBuffer_HashWithSpill__Size_OptimalMemory)
                .AddMemoryForOutputUStreams(1, Policy::Spilling::m_bufferSize, Policy::Spilling::m_bufferCnt);
        }
    };

    //
    // Hash Combiner V2 Join Policies
    // ProbeSchema -- probe row type; Item -- build rows container type.
    //
    enum class HashCombinerValuesPerKeyCountV2
    {
        ZERO,
        ONE,
        MANY,
    };

    template <typename ProbeSchema, typename OutputSchema, typename Policy, int UID>
    class HashOuterJoinerV2
    {
    private:
        typedef typename Policy::Hashtable::ItemType Item;
        typedef typename Policy::Value Value;
        typedef typename Policy::ValueIterator ValueIterator;

        const ProbeSchema * m_probeRow{ nullptr };
        Value *             m_value{ nullptr };
        ValueIterator       m_it;
        ValueIterator       m_end;
        bool                m_first{ false };
    protected:
        bool                m_buildExists{ false };

    private:
        template<unsigned CrossLimit>
        void CheckMaxCrossSizeLimit(size_t buildSize, size_t probeSize)
        {
            if (std::min(buildSize, probeSize) > CrossLimit)
            {
                throw JoinCrossLimitExceededException(UID, CrossLimit);
            }
        }

        template<>
        void CheckMaxCrossSizeLimit<0>(size_t buildSize, size_t probeSize)
        {
        }

        void UpdateProbeStats(Value & value)
        {
            const size_t probeStats = Policy::IncrementValueListStats(value);

            CheckMaxCrossSizeLimit<Policy::s_maxPerKeyCrossSizeLimit>(
                Policy::ValueListSize(value),
                probeStats);
        }

        template <int ValuesPerKeyCount>
        void InitValueImpl(Item * item);

        template <>
        void InitValueImpl<HashCombinerValuesPerKeyCountV2::ZERO>(Item *)
        {
        }

        template <>
        void InitValueImpl<HashCombinerValuesPerKeyCountV2::ONE>(Item * item)
        {
            m_value = &item->second;
        }

        template <>
        void InitValueImpl<HashCombinerValuesPerKeyCountV2::MANY>(Item * item)
        {
            m_it = Policy::ValueListBegin(item->second);
            m_end = Policy::ValueListEnd(item->second);

            UpdateProbeStats(item->second);
        }

        void InitValue(Item * item)
        {
            InitValueImpl<Policy::s_valuesPerKeyCount>(item);
        }

        template <int ValuesPerKeyCount>
        bool GetBothImpl(OutputSchema & output);

        template <>
        bool GetBothImpl<HashCombinerValuesPerKeyCountV2::ZERO>(OutputSchema & output)
        {
            if (m_first)
            {
                Policy::CopyBoth(*m_probeRow, nullptr, output);
                m_first = false;
                return true;
            }

            return false;
        }

        template <>
        bool GetBothImpl<HashCombinerValuesPerKeyCountV2::ONE>(OutputSchema & output)
        {
            if (m_first)
            {
                Policy::CopyBoth(*m_probeRow, m_value, output);
                m_first = false;
                return true;
            }

            return false;
        }

        template <>
        bool GetBothImpl<HashCombinerValuesPerKeyCountV2::MANY>(OutputSchema & output)
        {
            if (m_it != m_end)
            {
                Policy::CopyBoth(*m_probeRow, *m_it, output);
                ++m_it;

                return true;
            }

            return false;
        }

    protected:
        bool GetProbeRow(OutputSchema & output)
        {
            if (m_first)
            {
                Policy::CopyProbeAndNullifyBuild(*m_probeRow, output);
                m_first = false;
                return true;
            }

            return false;
        }

        bool GetBoth(OutputSchema & output)
        {
            return GetBothImpl<Policy::s_valuesPerKeyCount>(output);
        }

    public:
        static bool EmptyBuildImpliesEmptyOutput()
        {
            return false;
        }

        void InitNext(const ProbeSchema * probeRow, Item * item)
        {
            m_probeRow = probeRow;
            m_first = true;
            m_buildExists = false;

            if (item)
            {
                InitValue(item);
                m_buildExists = true;
            }
        }

        bool GetNextRow(OutputSchema & output)
        {
            if (!m_buildExists)
            {
                return GetProbeRow(output);
            }
            else
            {
                return GetBoth(output);
            }
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements();
        }
    };

    template <typename ProbeSchema, typename OutputSchema, typename Policy, int UID>
    class HashLeftOuterJoinerV2 : public HashOuterJoinerV2<ProbeSchema, OutputSchema, Policy, UID>
    {};

    template <typename ProbeSchema, typename OutputSchema, typename Policy, int UID>
    class HashRightOuterJoinerV2 : public HashOuterJoinerV2<ProbeSchema, OutputSchema, Policy, UID>
    {};

    template <typename ProbeSchema, typename OutputSchema, typename Policy, int UID>
    class HashInnerJoinerV2 : public HashOuterJoinerV2<ProbeSchema, OutputSchema, Policy, UID>
    {
    public:
        static bool EmptyBuildImpliesEmptyOutput()
        {
            return true;
        }

        bool GetNextRow(OutputSchema & output)
        {
            if (m_buildExists)
            {
                return GetBoth(output);
            }
            else
            {
                return false;
            }
        }
    };

    template <typename ProbeSchema, typename OutputSchema, typename Policy, int UID>
    class HashSemiJoinerV2 : public HashOuterJoinerV2<ProbeSchema, OutputSchema, Policy, UID>
    {
    public:
        static bool EmptyBuildImpliesEmptyOutput()
        {
            return true;
        }

        bool GetNextRow(OutputSchema & output)
        {
            if (m_buildExists)
            {
                return GetProbeRow(output);
            }
            else
            {
                return false;
            }
        }
    };

    template <typename ProbeSchema, typename OutputSchema, typename Policy, int UID>
    class HashLeftSemiJoinerV2 : public HashSemiJoinerV2<ProbeSchema, OutputSchema, Policy, UID>
    {};

    template <typename ProbeSchema, typename OutputSchema, typename Policy, int UID>
    class HashRightSemiJoinerV2 : public HashSemiJoinerV2<ProbeSchema, OutputSchema, Policy, UID>
    {};

    template <typename ProbeSchema, typename OutputSchema, typename Policy, int UID>
    class HashAntiSemiJoinerV2 : public HashOuterJoinerV2<ProbeSchema, OutputSchema, Policy, UID>
    {
    public:
        static bool EmptyBuildImpliesEmptyOutput()
        {
            return false;
        }

        bool GetNextRow(OutputSchema & output)
        {
            if (!m_buildExists)
            {
                return GetProbeRow(output);
            }
            else
            {
                return false;
            }
        }
    };

    template <typename ProbeSchema, typename OutputSchema, typename Policy, int UID>
    class HashLeftAntiSemiJoinerV2 : public HashAntiSemiJoinerV2<ProbeSchema, OutputSchema, Policy, UID>
    {};

    template <typename ProbeSchema, typename OutputSchema, typename Policy, int UID>
    class HashRightAntiSemiJoinerV2 : public HashAntiSemiJoinerV2<ProbeSchema, OutputSchema, Policy, UID>
    {};

    //
    // Hash Combiner V2
    //
    template<typename ProbeOperator,
        typename ProbeSchema,
        typename BuildOperator,
        typename BuildSchema,
        typename OutputSchema,
        template <typename, typename, typename, int> class Joiner,
        int UID = -1>
    class HashCombinerV2 : public Operator<HashCombinerV2<ProbeOperator, ProbeSchema, BuildOperator, BuildSchema, OutputSchema, Joiner, UID>, OutputSchema, UID>
    {
        template<typename, typename, typename, typename, typename, template <typename, typename, typename, int> class, int> friend struct HashCombinerV2_TestHelper;

    public:
        typedef HashCombinerPolicyV2<ProbeSchema, BuildSchema, OutputSchema, UID> P;

    private:
        static_assert((1 << P::s_hashtableCntExponent) * COMMIT_BLOCK_SIZE <= P::s_memoryQuota, "Memory quota is less than the mimimum required to use the specified number of hashtables");
        static_assert(P::s_initialSize && !(P::s_initialSize & (P::s_initialSize - 1)), "Hashtable initial size is not a power of two");
        static_assert(P::s_initialSize <= (P::s_directorySize * (1 << P::s_segmentSizeExponent)), "Hashtable initial size exceeds bucket array max capacity");

        typedef          STLIncrementalAllocator<char> CharAllocator;
        typedef typename P::KeySchema                  Key;
        typedef typename P::ValueSchema                Value;
        typedef typename P::ProbeEqualTo               ProbePred;
        typedef typename P::Hashtable                  Hashtable;
        typedef typename Hashtable::Iterator           HashtableIterator;
        typedef          Joiner<ProbeSchema,
            OutputSchema,
            P,
            UID>                   Joiner;

    private:
        enum SourceState
        {
            ReadOriginalInput,
            InitSpilledInput,
            ReadSpilledInput
        };

        enum PhaseState
        {
            UnInit,
            Build,
            Probe,
            Output
        };

        struct OutputStream
        {
            std::string m_filename;
            std::unique_ptr<BinaryOutputStream> m_stream;

            bool Initialized() const { return m_stream != nullptr; }

            void Init()
            {
                SCOPE_ASSERT(!Initialized());

                m_filename = IOManager::GetTempStreamName();

                IOManager::GetGlobal()->AddOutputStream(m_filename, m_filename);
                m_stream.reset(new BinaryOutputStream(m_filename, P::Spilling::s_bufferSize, P::Spilling::s_bufferCnt));

                m_stream->Init();
            }

            void Close()
            {
                if (Initialized())
                {
                    m_stream->Finish();
                    m_stream->Close();
                    m_stream.reset();
                }
            }
        };

        struct Partition
        {
            IncrementalAllocator       m_alloc;
            std::unique_ptr<Hashtable> m_ht;
            CharAllocator              m_calloc{ &m_alloc }; // a wrapper with STL allocator interface
            bool                       m_spilled{ false };
        };

    private:
        static const size_t s_hashtableCnt{ 1 << P::s_hashtableCntExponent };

        ProbeOperator * m_probeChild;
        BuildOperator * m_buildChild;

        BuildSchema     m_buildRow;
        ProbeSchema     m_probeRow;
        ProbePred       m_probePred;
        Joiner          m_joiner;

        std::unique_ptr<Partition[]> m_partitions{ new Partition[s_hashtableCnt] };

        SourceState m_sourceState{ ReadOriginalInput };
        PhaseState  m_phaseState{ UnInit };

        size_t m_seedIdx{ 0 };
        size_t m_currentJoinProduct{ 0 };
        size_t m_payloadSrcIndex;
        bool   m_nonEmptyBuild;

#pragma region spilling
        typedef Extractor<BuildSchema, BinaryExtractPolicy<BuildSchema>, BinaryInputStream> BuildExtractor;
        typedef Extractor<ProbeSchema, BinaryExtractPolicy<ProbeSchema>, BinaryInputStream> ProbeExtractor;

        size_t                          m_spilledCnt{ 0 };
        OutputStream                    m_buildOutput;
        OutputStream                    m_probeOutput;
        std::unique_ptr<BuildExtractor> m_buildExtractor;
        std::unique_ptr<ProbeExtractor> m_probeExtractor;
#pragma endregion spilling

#pragma region runtime_stats
        size_t m_htMaxTotalMemory{ 0 };
        size_t m_htUniqueKeyCnt{ 0 };
        size_t m_htLookupCnt{ 0 };
        size_t m_spProbeRowCnt{ 0 };
        size_t m_spBuildRowCnt{ 0 };
        size_t m_spHashtableCnt{ 0 };
        size_t m_htMaxBucketSize{ 0 };
        size_t m_noMatchProbeRowCnt{ 0 };
        size_t m_maxJoinProduct{ 0 };
        size_t m_buildTime{ 0 };
        size_t m_htBucketCnt{ 0 };
        size_t m_htEmptyBucketCnt{ 0 };
        size_t m_spillLevel{ 0 };
#pragma endregion runtime_stats

    private:
        template <typename Extractor>
        static Extractor* Create(const std::string& filename)
        {
            return new Extractor(InputFileInfo(filename), false, P::Spilling::s_bufferSize, P::Spilling::s_bufferCnt, Configuration::GetGlobal().GetMaxOnDiskRowSize(), 0);
        }

        // devnote: crc32 that is used in the code gen for computing row hash values returns a 32-bit value.
        // PartitionHash function masks out upper N bits, where N is the smallest number of bits enough to store
        // the hashtable count.
        static size_t PartitionHash(INT64 hash)
        {
            return hash >> (32 - (1 << (P::s_hashtableCntExponent - 1)));
        }

        void WriteRow(BuildSchema& row) const
        {
            BinaryOutputPolicy<BuildSchema>::Serialize(m_buildOutput.m_stream.get(), row);
        }

        template<int ValuesPerKeyCount>
        size_t SpillItem(HashtableIterator it) const;

        template<>
        size_t SpillItem<HashCombinerValuesPerKeyCountV2::ZERO>(HashtableIterator it) const
        {
            BuildSchema buildRow;
            P::CopyKeyValueToBuildRow(*it, nullptr, buildRow);
            WriteRow(buildRow);

            return 1;
        }

        template<>
        size_t SpillItem<HashCombinerValuesPerKeyCountV2::ONE>(HashtableIterator it) const
        {
            BuildSchema buildRow;
            P::CopyKeyValueToBuildRow(it->first, &it->second, buildRow);
            WriteRow(buildRow);

            return 1;
        }

        template<>
        size_t SpillItem<HashCombinerValuesPerKeyCountV2::MANY>(HashtableIterator it) const
        {
            size_t count = 0;
            BuildSchema buildRow;
            auto vend = P::ValueListEnd(it->second);
            for (auto vit = P::ValueListBegin(it->second); vit != vend; ++vit)
            {
                P::CopyKeyValueToBuildRow(it->first, *vit, buildRow);
                WriteRow(buildRow);
                ++count;
            }

            return count;
        }

        size_t SpillHashtable(size_t idx) const
        {
            size_t spBuildRowCnt = 0;
            auto& partition = m_partitions[idx];
            auto htend = partition.m_ht->End();
            for (auto htit = partition.m_ht->Begin(); htit != htend; ++htit)
            {
                // devnote: that is an extra copy, but it allows to utilize existing code gen
                // for binary extractors/outputters
                spBuildRowCnt += SpillItem<P::s_valuesPerKeyCount>(htit);
            }

            return spBuildRowCnt;
        }

        size_t CurrentMemoryUsage() const
        {
            size_t result = 0;
            for (size_t i = 0; i < s_hashtableCnt; ++i) { result += m_partitions[i].m_alloc.GetSize(); }

            return result;
        }

        void UpdateHashtableStats()
        {
            for (size_t i = 0; i < s_hashtableCnt; ++i)
            {
                auto& partition = m_partitions[i];

                if (!partition.m_spilled)
                {
                    m_htUniqueKeyCnt += partition.m_ht->Size();
                    m_htBucketCnt += partition.m_ht->BucketCount();

                    for (size_t j = 0; j < partition.m_ht->BucketCount(); ++j)
                    {
                        if (partition.m_ht->BucketSize(j) == 0)
                        {
                            ++m_htEmptyBucketCnt;
                        }
                        m_htMaxBucketSize = m_htMaxBucketSize > partition.m_ht->BucketSize(j) ? m_htMaxBucketSize : partition.m_ht->BucketSize(j);
                    }
                }
            }

            m_htMaxTotalMemory = m_htMaxTotalMemory > CurrentMemoryUsage() ? m_htMaxTotalMemory : CurrentMemoryUsage();
            m_spHashtableCnt += m_spilledCnt;
        }

        template <typename ProbeOperator, typename BuildOperator>
        bool GetNextRowImpl(ProbeOperator* probeOp, BuildOperator* buildOp, bool spillWhenOutOfMemory, OutputSchema & output)
        {
            for (;;)
            {
                switch (m_phaseState)
                {
                case UnInit:
                {
                               m_spilledCnt = 0;
                               m_nonEmptyBuild = false;

                               for (size_t i = 0; i < s_hashtableCnt; ++i)
                               {
                                   auto& partition = m_partitions[i];
                                   partition.m_alloc.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
                                   partition.m_ht.reset(new Hashtable(partition.m_calloc, P::s_initialSize, P::Hash(P::Seed()[m_seedIdx])));
                                   partition.m_spilled = false;
                               }

                               m_phaseState = Build;
                               break;
                }
                case Build:
                {
                              // devnote: incoming rows are partitioned into s_hashtableCnt hashtables, each hashtable
                              // is stored in its own allocator.
                              // 
                              // When the overall memory limit is reached, the insert will fail with out of memory exception.
                              // The row that could not be inserted will be spilled to disk along with
                              // the corresponding hashtable. Memory used by that hashtable will be freed and made 
                              // available to use by other partitions.

                              ExecutionStats buildTimer;

                              while (buildOp->GetNextRow(m_buildRow))
                              {
                                  AutoExecStats aBuildTimer(&buildTimer);

                                  bool anyDataInMemory = (m_spilledCnt < s_hashtableCnt);
                                  bool currentHtInMemory = false;
                                  bool insertFailed = false;
                                  if (anyDataInMemory)
                                  {
                                      const INT64 hash = P::BuildHashes()[m_seedIdx](m_buildRow);
                                      const size_t idx = PartitionHash(hash) & (s_hashtableCnt - 1);
                                      auto& partition = m_partitions[idx];

                                      currentHtInMemory = !partition.m_spilled;
                                      if (currentHtInMemory)
                                      {

                                          // devnote: the operator keeps track of overall memory limit between all the allocators
                                          // that allows to utilize the memory efficiently even if the partition sizes
                                          // are skewed.
                                          const size_t allocationQuota = P::s_memoryQuota - CurrentMemoryUsage();
                                          partition.m_alloc.ResetSoftMaxSize(allocationQuota);

                                          try
                                          {
                                              P::InsertOrUpdate(*partition.m_ht, m_buildRow, hash, partition.m_alloc, partition.m_calloc);
                                          }
                                          catch (RuntimeMemoryException&)
                                          {
                                              //out of memory
                                              insertFailed = true;
                                          }

                                          // devnote: the hash join relies on that the internal state of the hashtable stays
                                          // consistent if any of the memory allocation calls fail
                                          if (insertFailed)
                                          {
                                              if (Configuration::GetGlobal().GetRestrictOperatorMemorySpilling())
                                              {
                                                  throw OperatorOutOfMemoryException(GetOperatorId());
                                              }

                                              if (!spillWhenOutOfMemory)
                                              {
                                                  SCOPE_ASSERT(m_probeExtractor);
                                                  SCOPE_ASSERT(m_buildExtractor);

                                                  m_probeExtractor->Close();
                                                  m_buildExtractor->Close();

                                                  m_probeExtractor.reset();
                                                  m_buildExtractor.reset();

                                                  throw RuntimeException(E_RUNTIME_SYSTEM_HASHJOIN_EXCEEDED_SPILL_LIMIT);
                                              }

                                              // update memory stats
                                              m_htMaxTotalMemory = m_htMaxTotalMemory > CurrentMemoryUsage() ? m_htMaxTotalMemory : CurrentMemoryUsage();

                                              // spill the current hashtable
                                              if (!m_buildOutput.Initialized())
                                              {
                                                  m_buildOutput.Init();
                                              }
                                              const size_t spBuildRowCnt = SpillHashtable(idx);

                                              partition.m_ht.reset();
                                              partition.m_alloc.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
                                              partition.m_spilled = true;
                                              ++m_spilledCnt;

                                              m_spBuildRowCnt += spBuildRowCnt;

                                              SCOPE_LOG_FMT_INFO("HashJoin", "Spilled hashtable id=%I64u, spilled rows count=%I64u", idx, spBuildRowCnt);
                                          }
                                      }
                                  }

                                  // spill build row to disk
                                  if (!anyDataInMemory || !currentHtInMemory || insertFailed)
                                  {
                                      SCOPE_ASSERT(spillWhenOutOfMemory);
                                      SCOPE_ASSERT(m_buildOutput.Initialized());

                                      BinaryOutputPolicy<BuildSchema>::Serialize(m_buildOutput.m_stream.get(), m_buildRow);
                                      ++m_spBuildRowCnt;
                                  }

                                  m_nonEmptyBuild = true;
                              }

                              m_buildTime += buildTimer.GetInclusiveTimeMillisecond();
                              m_phaseState = Probe;
                              break;

                              //TODO check if the operator needs managed memory control
                }
                case Probe:
                {
                              if (!probeOp->GetNextRow(m_probeRow) || (!m_nonEmptyBuild && m_joiner.EmptyBuildImpliesEmptyOutput()))
                              {
                                  return false;
                              }

                              // check if there is any data in memory
                              bool probeMemory = (m_spilledCnt < s_hashtableCnt);
                              if (probeMemory)
                              {
                                  const INT64 hash = P::ProbeHashes()[m_seedIdx](m_probeRow);
                                  const size_t idx = PartitionHash(hash) & (s_hashtableCnt - 1);

                                  // check if the target hashtable is in memory
                                  probeMemory = !m_partitions[idx].m_spilled;
                                  if (probeMemory)
                                  {
                                      auto& ht = *(m_partitions[idx].m_ht);

                                      auto it = ht.FindWithPrecomputedHash(hash, m_probeRow, m_probePred);
                                      auto item = (it != ht.End() ? &(*it) : nullptr);

                                      m_joiner.InitNext(&m_probeRow, item);

                                      {
                                          ++m_htLookupCnt;
                                          if (!item)
                                          {
                                              ++m_noMatchProbeRowCnt;
                                          }
                                          m_currentJoinProduct = 0;
                                      }

                                      m_phaseState = Output;
                                  }
                              }

                              // spill probe row to disk
                              if (!probeMemory)
                              {
                                  if (!m_probeOutput.Initialized())
                                  {
                                      m_probeOutput.Init();
                                  }
                                  BinaryOutputPolicy<ProbeSchema>::Serialize(m_probeOutput.m_stream.get(), m_probeRow);

                                  ++m_spProbeRowCnt;
                              }

                              break;
                }
                case Output:
                {
                               if (m_joiner.GetNextRow(output))
                               {
                                   ++m_currentJoinProduct;
                                   return true;
                               }

                               m_maxJoinProduct = m_maxJoinProduct > m_currentJoinProduct ? m_maxJoinProduct : m_currentJoinProduct;

                               m_phaseState = Probe;
                               break;
                }
                default:
                {
                           SCOPE_ASSERT(!"Invalid state for hash joiner v2");
                           return false;
                }
                }
            }
        }

    public:
        HashCombinerV2(ProbeOperator * probe, BuildOperator * build, int operatorId, int payloadSrcIndex) :
            Operator(operatorId),
            m_probeChild(probe),
            m_buildChild(build),
            m_payloadSrcIndex(payloadSrcIndex)
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_probeChild->Init();
            m_buildChild->Init();

            for (size_t i = 0; i < s_hashtableCnt; ++i)
            {
                // devnote: max allocator size for each allocator is set in a way
                // that will allow the operator to execute successfully in case if the data skew
                m_partitions[i].m_alloc.Init(P::s_memoryQuota, "HashCombinerV2");
            }

            SCOPE_LOG_FMT_INFO("HashJoin", "Init opid=%d, hashtable_type='%s', collision_list_type='%s', probe_type='%s', build_type='%s'",
                GetOperatorId(),
                typeid(Hashtable).name(),
                typeid(typename Hashtable::ItemListType).name(),
                typeid(ProbeSchema).name(),
                typeid(BuildSchema).name());
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            if (0 == m_payloadSrcIndex)
            {
                return m_probeChild->GetMetadata();
            }

            return m_buildChild->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            for (;;)
            {
                switch (m_sourceState)
                {
                case ReadOriginalInput:
                {
                                          bool hasNextRow = GetNextRowImpl(m_probeChild, m_buildChild, true, output);

                                          if (hasNextRow)
                                          {
                                              stats.IncreaseRowCount(1);
                                              return true;
                                          }

                                          UpdateHashtableStats();

                                          m_sourceState = InitSpilledInput;
                                          break;
                }
                case InitSpilledInput:
                {
                                         // if probe was spilled build must have been spilled as well
                                         SCOPE_ASSERT(!m_probeOutput.Initialized() || (m_probeOutput.Initialized() && m_buildOutput.Initialized()));

                                         // if no probe rows were spilled there is no more data to process
                                         // TODO revisit when the optimizer enables outer join on the  build size
                                         if (!m_probeOutput.Initialized())
                                         {
                                             return false;
                                         }

                                         m_probeOutput.Close();
                                         m_buildOutput.Close();

                                         size_t estimatedSpilledBuildSize = EstimatedFileSize(SpilledBuildFilename());
                                         SCOPE_LOG_FMT_INFO("HashJoin", "Finished processing original inputs, estimatedSpilledBuildSize=%I64u", estimatedSpilledBuildSize);

                                         // too much data spilled, trigger fallback to sort/merge
                                         if (estimatedSpilledBuildSize > P::s_memoryQuota || m_spilledCnt == s_hashtableCnt)
                                         {
                                             throw RuntimeException(E_RUNTIME_SYSTEM_HASHJOIN_EXCEEDED_SPILL_LIMIT);
                                         }

                                         ++m_spillLevel;

                                         {
                                             IOManager::GetGlobal()->AddInputStream(m_probeOutput.m_filename, m_probeOutput.m_filename);
                                             IOManager::GetGlobal()->AddInputStream(m_buildOutput.m_filename, m_buildOutput.m_filename);

                                             m_buildExtractor.reset(Create<BuildExtractor>(m_buildOutput.m_filename));
                                             m_probeExtractor.reset(Create<ProbeExtractor>(m_probeOutput.m_filename));

                                             m_buildExtractor->Init();
                                             m_probeExtractor->Init();
                                         }

                                         ++m_seedIdx;
                                         SCOPE_ASSERT(m_seedIdx < P::s_seedCnt);

                                         m_phaseState = UnInit;

                                         m_sourceState = ReadSpilledInput;
                                         break;
                }
                case ReadSpilledInput:
                {
                                         bool hasNextRow = GetNextRowImpl(m_probeExtractor.get(), m_buildExtractor.get(), false, output);

                                         if (hasNextRow)
                                         {
                                             stats.IncreaseRowCount(1);
                                             return true;
                                         }

                                         SCOPE_ASSERT(!m_buildOutput.Initialized() && !m_probeOutput.Initialized());

                                         UpdateHashtableStats();

                                         return false;
                }
                default:
                {
                           SCOPE_ASSERT(!"Invalid source state for hash joiner v2");
                           return false;
                }
                }
            }
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_probeChild->Close();
            m_buildChild->Close();

            m_probeOutput.Close();
            m_buildOutput.Close();

            if (m_probeExtractor)
            {
                m_probeExtractor->Close();
            }

            if (m_buildExtractor)
            {
                m_buildExtractor->Close();
            }
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("HashJoinV2");
            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_probeChild->GetInclusiveTimeMillisecond() - m_buildChild->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());

            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            node.AddAttribute(RuntimeStats::AvgHashtableTotalMemory(), m_htMaxTotalMemory);
            node.AddAttribute(RuntimeStats::MaxHashtableTotalMemory(), m_htMaxTotalMemory);

            node.AddAttribute(RuntimeStats::AvgHashtableLookupCount(), m_htLookupCnt);
            node.AddAttribute(RuntimeStats::MaxHashtableLookupCount(), m_htLookupCnt);

            node.AddAttribute(RuntimeStats::AvgSpillProbeRowCount(), m_spProbeRowCnt);
            node.AddAttribute(RuntimeStats::MaxSpillProbeRowCount(), m_spProbeRowCnt);

            node.AddAttribute(RuntimeStats::AvgSpillBuildRowCount(), m_spBuildRowCnt);
            node.AddAttribute(RuntimeStats::MaxSpillBuildRowCount(), m_spBuildRowCnt);

            node.AddAttribute(RuntimeStats::AvgHashtableUniqueKeyCount(), m_htUniqueKeyCnt);
            node.AddAttribute(RuntimeStats::MaxHashtableUniqueKeyCount(), m_htUniqueKeyCnt);

            node.AddAttribute(RuntimeStats::AvgHashtableBucketSize(), m_htMaxBucketSize);
            node.AddAttribute(RuntimeStats::MaxHashtableBucketSize(), m_htMaxBucketSize);

            node.AddAttribute(RuntimeStats::AvgHashtableBucketCount(), m_htBucketCnt);
            node.AddAttribute(RuntimeStats::MaxHashtableBucketCount(), m_htBucketCnt);

            node.AddAttribute(RuntimeStats::AvgHashtableEmptyBucketCount(), m_htEmptyBucketCnt);
            node.AddAttribute(RuntimeStats::MaxHashtableEmptyBucketCount(), m_htEmptyBucketCnt);

            node.AddAttribute(RuntimeStats::AvgHashtableBuildTime(), m_buildTime);
            node.AddAttribute(RuntimeStats::MaxHashtableBuildTime(), m_buildTime);

            node.AddAttribute(RuntimeStats::AvgNoMatchProbeRowCount(), m_noMatchProbeRowCnt);
            node.AddAttribute(RuntimeStats::MaxNoMatchProbeRowCount(), m_noMatchProbeRowCnt);

            node.AddAttribute(RuntimeStats::MaxAvgJoinProduct(), m_maxJoinProduct);

            node.AddAttribute(RuntimeStats::AvgHashtableSpillLevel(), m_spillLevel);
            node.AddAttribute(RuntimeStats::MaxHashtableSpillLevel(), m_spillLevel);

            if (m_buildExtractor)
            {
                m_buildExtractor->WriteRuntimeStats(node);
            }

            if (m_probeExtractor)
            {
                m_probeExtractor->WriteRuntimeStats(node);
            }

            m_probeChild->WriteRuntimeStats(node);
            m_buildChild->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements(m_joiner.GetOperatorRequirements())
                .AddMemory(OperatorRequirementsConstants::RowBuffer_HashWithSpill__Size_MinMemory, OperatorRequirementsConstants::RowBuffer_HashWithSpill__Size_OptimalMemory)
                .AddMemoryForOutputUStreams(s_hashtableCnt, P::Spilling::s_bufferSize, P::Spilling::s_bufferCnt);
        }

        // HashCombinerV2 specific interface
        std::string SpilledBuildFilename() const { return m_buildOutput.m_filename; }
        std::string SpilledProbeFilename() const { return m_probeOutput.m_filename; }

        static size_t EstimatedFileSize(const std::string& filename)
        {
            bool opened;
            CsError errorCode;
            string errorText;
            BlockDeviceStatistics stats;
            bool streamFound = IOManager::GetGlobal()->GetStreamState(filename, opened, errorCode, errorText, &stats);
            SCOPE_ASSERT(streamFound);

            return stats.m_processedBytes;
        }
        //
    };

    //
    // Hash join with fallback to sort/merge
    //
    template<typename ProbeOperator,
        typename ProbeSchema,
        typename BuildOperator,
        typename BuildSchema,
        typename OutputSchema,
        template <typename, typename, typename, int> class HashJoiner,
        template <typename, typename, typename, typename, typename, int, typename = None, typename = None> class MergeJoiner,
        int UID = -1>
    class HashCombinerWithFallback : public Operator<HashCombinerWithFallback<ProbeOperator, ProbeSchema, BuildOperator, BuildSchema, OutputSchema, HashJoiner, MergeJoiner, UID>, OutputSchema, UID>
    {
        template<typename, typename, typename, typename, typename,
        template <typename, typename, typename, int> class,
        template <typename, typename, typename, typename, typename, int, typename, typename> class,
        int> friend struct HashCombinerWithFallback_TestHelper;
    private:
        typedef HashCombinerWithFallbackPolicy<ProbeSchema, BuildSchema, OutputSchema, UID> FallbackPolicy;

        typedef HashCombinerV2<ProbeOperator, ProbeSchema, BuildOperator, BuildSchema, OutputSchema, HashJoiner, UID> HashJoin;
        typedef Extractor<BuildSchema, BinaryExtractPolicy<BuildSchema>, BinaryInputStream> BuildExtractor;
        typedef Extractor<ProbeSchema, BinaryExtractPolicy<ProbeSchema>, BinaryInputStream> ProbeExtractor;
        typedef OperatorDelegate<BuildSchema> BuildExtractorDelegate;
        typedef OperatorDelegate<ProbeSchema> ProbeExtractorDelegate;
        typedef Sorter<BuildSchema> BuildSorter;
        typedef Sorter<ProbeSchema> ProbeSorter;

        typedef NativeCombinerWrapper<ProbeSorter, ProbeSchema, BuildSorter, BuildSchema, OutputSchema, MergeJoiner, UID> MergeJoin;
        typedef typename HashJoin::P HashJoinPolicy;

        enum State
        {
            Hash,
            InitMerge,
            Merge
        };

    private:
        std::unique_ptr<HashJoin>               m_hashJoin;

        std::unique_ptr<BuildExtractor>         m_buildExtractor;
        std::unique_ptr<ProbeExtractor>         m_probeExtractor;
        std::unique_ptr<BuildExtractorDelegate> m_buildExtractorDlg;
        std::unique_ptr<ProbeExtractorDelegate> m_probeExtractorDlg;
        std::unique_ptr<BuildSorter>            m_buildSorter;
        std::unique_ptr<ProbeSorter>            m_probeSorter;
        std::unique_ptr<MergeJoin>              m_mergeJoin;

        State m_state{ Hash };

        size_t m_payloadSrcIndex;

    private:
        template <typename Extractor>
        static std::unique_ptr<Extractor> CreateExtractor(const std::string& filename)
        {
            return std::make_unique<Extractor>(InputFileInfo(filename),
                false,
                (int)HashJoinPolicy::Spilling::s_bufferSize,
                (int)HashJoinPolicy::Spilling::s_bufferCnt,
                Configuration::GetGlobal().GetMaxOnDiskRowSize(),
                0);
        }

        template <typename Input, typename Schema>
        static std::unique_ptr<OperatorDelegate<Schema>> CreateDelegate(Input* input)
        {
            return std::make_unique<OperatorDelegate<Schema>>(OperatorDelegate<Schema>::FromOperator(input));
        }

        template <typename Input, typename Schema, typename SorterAlgorithm>
        static std::unique_ptr<Sorter<Schema>> CreateSorter(Input* input, size_t memoryQuota)
        {
            return std::make_unique<Sorter<Schema>>(input,
                &SorterAlgorithm::Sort<KeyComparePolicy<Schema, UID>, (sizeof(Schema) <= CACHELINE_SIZE)>,
                ScopeLoserTreeDelegate<Schema>::CreateDelegate<UID>(),
                false,
                memoryQuota,
                UID);
        }

        // returns (probe sort quota, build sort quota)
        std::pair<size_t, size_t> SorterMemoryQuotas()
        {
            typedef CombinerPolicy<UID> MergeJoinPolicy;
            SCOPE_ASSERT(HashJoinPolicy::s_memoryQuota >= 2 * MergeJoinPolicy::JoinerMemQuota() + 2 * Configuration::GetGlobal().GetMaxInMemoryRowSize());

            const double totalSortersQuota = (double)(HashJoinPolicy::s_memoryQuota - (2 * MergeJoinPolicy::JoinerMemQuota()));
            const double flexibleSortersQuota = totalSortersQuota - (2 * Configuration::GetGlobal().GetMaxInMemoryRowSize());

            const double probeSpilledSize = (double)HashJoin::EstimatedFileSize(m_hashJoin->SpilledProbeFilename());
            const double buildSpilledSize = (double)HashJoin::EstimatedFileSize(m_hashJoin->SpilledBuildFilename());
            const double totalSpilledSize = probeSpilledSize + buildSpilledSize;

            const double probeFlexiblePortion = probeSpilledSize / totalSpilledSize;
            const double buildFlexiblePortion = 1. - probeFlexiblePortion;

            const size_t probeSorterQuota = (size_t)(Configuration::GetGlobal().GetMaxInMemoryRowSize() + flexibleSortersQuota * probeFlexiblePortion);
            const size_t buildSorterQuota = (size_t)(Configuration::GetGlobal().GetMaxInMemoryRowSize() + flexibleSortersQuota * buildFlexiblePortion);

            return std::make_pair(probeSorterQuota, buildSorterQuota);
        }

    public:
        HashCombinerWithFallback(ProbeOperator * probe, BuildOperator * build, int operatorId, int payloadSrcIndex) :
            Operator(operatorId),
            m_hashJoin(new HashJoin(probe, build, operatorId, payloadSrcIndex)),
            m_payloadSrcIndex(payloadSrcIndex)
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_hashJoin->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_hashJoin->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            for (;;)
            {
                switch (m_state)
                {
                case Hash:
                {
                             try
                             {
                                 if (m_hashJoin->GetNextRow(output))
                                 {
                                     stats.IncreaseRowCount(1);
                                     return true;
                                 }

                                 return false;
                             }
                             catch (RuntimeException& e)
                             {
                                 if (e.GetErrorNumber() != E_RUNTIME_SYSTEM_HASHJOIN_EXCEEDED_SPILL_LIMIT)
                                 {
                                     throw;
                                 }

                                 m_state = InitMerge;
                             }

                             break;
                }
                case InitMerge:
                {
                                  // devnote: builds an operator tree of the following form:
                                  //
                                  //  Extractor(build) Extractor(probe)
                                  //         |              |
                                  //   Op Delegate     Op Delegate
                                  //         |              |
                                  //      Sorter          Sorter
                                  //          \            /
                                  //           \          /
                                  //            Merge Join
                                  //
                                  // That part mirrors what the MainTextTemplate does and should probably be done there,
                                  // but at the point it is to messy to extract that logic.
                                  //
                                  // devnote: the fallback operator tree respects the original memory limit. Merge join gets
                                  // the required minimum (JoinerMemQuota is different for merge joins that are a part 
                                  // of the the fallback tree and is equal to 2x MaxInMemoryRowSize), the sorters get the rest
                                  // split proportional to the build/probe spilled file sizes. 
                                  //
                                  IOManager::GetGlobal()->AddInputStream(m_hashJoin->SpilledBuildFilename(), m_hashJoin->SpilledBuildFilename());
                                  IOManager::GetGlobal()->AddInputStream(m_hashJoin->SpilledProbeFilename(), m_hashJoin->SpilledProbeFilename());

                                  m_buildExtractor = CreateExtractor<BuildExtractor>(m_hashJoin->SpilledBuildFilename());
                                  m_probeExtractor = CreateExtractor<ProbeExtractor>(m_hashJoin->SpilledProbeFilename());

                                  m_buildExtractorDlg = CreateDelegate<BuildExtractor, BuildSchema>(m_buildExtractor.get());
                                  m_probeExtractorDlg = CreateDelegate<ProbeExtractor, ProbeSchema>(m_probeExtractor.get());

                                  auto sorterMemoryQuotas = SorterMemoryQuotas();
                                  SCOPE_LOG_FMT_INFO("HashJoinWithFallback", "probeSorterQuota=%I64u, buildSorterQuota=%I64u", sorterMemoryQuotas.first, sorterMemoryQuotas.second);

                                  m_probeSorter = CreateSorter<ProbeExtractorDelegate, ProbeSchema, FallbackPolicy::ProbeSorterAlgorithm>(m_probeExtractorDlg.get(), sorterMemoryQuotas.first);
                                  m_buildSorter = CreateSorter<BuildExtractorDelegate, BuildSchema, FallbackPolicy::BuildSorterAlgorithm>(m_buildExtractorDlg.get(), sorterMemoryQuotas.second);

                                  m_mergeJoin.reset(new MergeJoin(m_probeSorter.get(), m_buildSorter.get(), (int)UID, (int)m_payloadSrcIndex, false, nullptr));
                                  m_mergeJoin->Init();

                                  m_state = Merge;
                                  break;
                }
                case Merge:
                {
                              if (m_mergeJoin->GetNextRow(output))
                              {
                                  stats.IncreaseRowCount(1);
                                  return true;
                              }

                              return false;
                }
                default:
                {
                           SCOPE_ASSERT(!"Invalid source state for hash join with fallback");
                           return false;
                }
                }
            }

            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_hashJoin->Close();
            if (m_mergeJoin)
            {
                m_mergeJoin->Close();
            }
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("HashJoinWithFallback");
            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            size_t exclusiveTime = GetInclusiveTimeMillisecond() - m_hashJoin->GetInclusiveTimeMillisecond();
            if (m_mergeJoin)
            {
                exclusiveTime -= m_mergeJoin->GetInclusiveTimeMillisecond();
            }
            node.AddAttribute(RuntimeStats::ExclusiveTime(), exclusiveTime);
            RuntimeStats::WriteRowCount(node, GetRowCount());

            m_hashJoin->WriteRuntimeStats(node);
            if (m_mergeJoin)
            {
                m_mergeJoin->WriteRuntimeStats(node);
            }
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            OperatorRequirements result = m_hashJoin->GetOperatorRequirementsImpl();
            if (m_mergeJoin)
            {
                result.Add(m_mergeJoin->GetOperatorRequirementsImpl());
            }

            return result;
        }
    };

    ///
    /// Nested Loop Combiner
    ///
    template<typename LeftOperator, typename LeftInputSchema, typename RightOperator, typename RightInputSchema, typename OutputSchema, typename CorrelatedParametersSchema, int UID = -1>
    class NestedLoopCombiner : public Operator<NestedLoopCombiner<LeftOperator, LeftInputSchema, RightOperator, RightInputSchema, OutputSchema, CorrelatedParametersSchema, UID>, OutputSchema, UID, CorrelatedParametersSchema>
    {
    private:
        typedef RowIterator<LeftOperator, LeftInputSchema> LeftIteratorType;

    private:
        enum State
        {
            AdvanceLeft,
            AdvanceRight
        };

    private:
        LeftOperator  *  m_leftChild;
        RightOperator *  m_rightChild;
        LeftIteratorType  m_leftIterator;
        State m_state;
        CorrelatedParametersSchema m_params;

        int m_payloadSrcIndex;

    public:
        NestedLoopCombiner(LeftOperator * left, RightOperator * right, int operatorId, int payloadSrcIndex) :
            Operator(operatorId),
            m_leftChild(left),
            m_rightChild(right),
            m_leftIterator(left),
            m_state(AdvanceLeft),
            m_payloadSrcIndex(payloadSrcIndex)
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_leftChild->Init();
            m_rightChild->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            if (0 == m_payloadSrcIndex)
            {
                return m_leftChild->GetMetadata();
            }

            return m_rightChild->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            for (;;)
            {
                switch (m_state)
                {
                case AdvanceLeft:
                {
                                    m_leftIterator.Increment();

                                    if (m_leftIterator.End())
                                    {
                                        return false;
                                    }

                                    {
                                        // Init right operator on every left row change
                                        // instead of "init on key change/rewind otherwise" to keep it simple.
                                        // The latter model is practically useless if the left input
                                        // is not sorted.
                                        CorrelatedParametersPolicy<UID>::CopyValues(m_leftIterator.GetRow(), &m_params);
                                        m_rightChild->Init(m_params);
                                    }

                                    m_state = AdvanceRight;

                                    break;
                }
                case AdvanceRight:
                {
                                     RightInputSchema rightRow;
                                     bool hasMoreRows = m_rightChild->GetNextRow(rightRow);
                                     if (hasMoreRows)
                                     {
                                         CombinerPolicy<UID>::CopyRow(m_leftIterator.GetRow(), &rightRow, &output);
                                         stats.IncreaseRowCount(1);
                                         return true;
                                     }

                                     m_state = AdvanceLeft;

                                     break;
                }
                default:
                {
                           SCOPE_ASSERT(!"Invalid state for nested loop joiner");
                           return false;
                }
                }
            }
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_leftChild->Close();
            m_rightChild->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("NestedLoopJoin");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_leftChild->GetInclusiveTimeMillisecond() - m_rightChild->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_leftChild->WriteRuntimeStats(node);
            m_rightChild->WriteRuntimeStats(node);
        }
    };


    template <typename Schema>
    class SpoolBuffer
    {
        typedef AutoRowArray<Schema>          RowsType;

        ULONGLONG                             m_id;

        // note this has nothing to do with a life-time, the ref counting is used to determine
        // an eligibility of spilling (i.e. only buffers not currently referenced may be written to a disk)
        ULONG                                 m_refs;
        ULONG                                 m_consumersCount;

        RowsType                              m_rows;
        bool                                  m_isSpilled;
        string                                m_outputStreamName;

        SIZE_T                                m_ioBufferSize;
        int                                   m_ioBufferCount;

        LONGLONG                              m_ioTime;
        ULONG                                 m_spillCount;
        ULONG                                 m_loadCount;

    public:
        SpoolBuffer(ULONGLONG id,
            ULONG consumersCount,
            SIZE_T maxMemorySize,
            SIZE_T ioBufferSize,
            int ioBufferCount)
            : m_id(id)
            , m_refs(0)
            , m_consumersCount(consumersCount)
            // We don't want to limit maximum number of rows in a buffer
            , m_rows("SpoolBuffer", maxMemorySize / sizeof(Schema), maxMemorySize)
            , m_isSpilled(false)
            , m_outputStreamName()
            , m_ioBufferSize(ioBufferSize)
            , m_ioBufferCount(ioBufferCount)
            , m_ioTime(0)
            , m_spillCount(0)
            , m_loadCount(0)
        {
            SCOPE_ASSERT(m_consumersCount);
            SCOPE_ASSERT(maxMemorySize);
            SCOPE_ASSERT(ioBufferSize);
            SCOPE_ASSERT(ioBufferCount);
        }

        ULONGLONG Id() const
        {
            return m_id;
        }

        void AddRef()
        {
            ++m_refs;
        }

        void DecRef()
        {
            --m_refs;
        }

        ULONG RefCount() const
        {
            return m_refs;
        }

        void DecConsumers()
        {
            SCOPE_ASSERT(m_consumersCount);
            --m_consumersCount;
        }

        ULONG ConsumersCount() const
        {
            return m_consumersCount;
        }

        AutoRowArray<Schema>& Rows()
        {
            SCOPE_ASSERT(!m_isSpilled);
            return m_rows;
        }

        bool IsSpilled() const
        {
            return m_isSpilled;
        }

        bool CanBeSpilled() const
        {
            return !m_isSpilled && m_refs == 0;
        }

        LONGLONG OperatorWaitOnIOTime() const
        {
            return m_ioTime;
        }

        ULONG SpillCount() const
        {
            return m_spillCount;
        }

        ULONG LoadCount() const
        {
            return m_loadCount;
        }

        SIZE_T MemorySize()
        {
            return m_rows.MemorySize();
        }

        void Spill()
        {
            SCOPE_ASSERT(CanBeSpilled());

            if (m_outputStreamName.empty())
            {
                m_outputStreamName = IOManager::GetTempStreamName();
                IOManager::GetGlobal()->AddOutputStream(m_outputStreamName, m_outputStreamName);
                BinaryOutputStream outputStream(m_outputStreamName, m_ioBufferSize, m_ioBufferCount);

                outputStream.Init();

                for (SIZE_T i = 0; i < m_rows.Size(); ++i)
                {
                    BinaryOutputPolicy<Schema>::Serialize(&outputStream, m_rows[i]);
                }

                outputStream.Finish();
                outputStream.Close();

                m_ioTime += outputStream.GetTotalIoWaitTime();
                ++m_spillCount;
            }

            m_rows.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            m_isSpilled = true;
        }

        void Load()
        {
            SCOPE_ASSERT(m_isSpilled);
            SCOPE_ASSERT(!m_outputStreamName.empty());

            IOManager::GetGlobal()->AddInputStream(m_outputStreamName, m_outputStreamName);

            IncrementalAllocator inputAllocator;
            inputAllocator.Init(Configuration::GetGlobal().GetMaxInMemoryRowSize(), "SpoolBuffer");

            BinaryInputStream inputStream(InputFileInfo(m_outputStreamName), &inputAllocator, m_ioBufferSize, m_ioBufferCount);
            inputStream.Init();

            Schema row;
            while (BinaryExtractPolicy<Schema>::Deserialize(&inputStream, row))
            {
                m_rows.AddRow(row);
                inputAllocator.Reset();
            }

            inputStream.Close();

            m_ioTime += inputStream.GetTotalIoWaitTime();
            ++m_loadCount;
            m_isSpilled = false;
        }
    };

    template <typename Schema>
    class SpoolQueue
    {
        template<typename T> friend class SpoolQueue_TestHelper;

        typedef SpoolBuffer<Schema>                     BufferType;
        typedef AutoRowArray<Schema>                    RowsType;
        typedef list<std::unique_ptr<BufferType>>       ContainerType;
        typedef typename ContainerType::iterator        ContainerIteratorType;
        typedef typename ContainerType::const_iterator  ContainerConstIteratorType;
        typedef std::function<PartitionMetadata*()>     MetadataFuncType;

    private:
        CRITICAL_SECTION                      m_cs;
        CONDITION_VARIABLE                    m_consumersRunnable;
        CONDITION_VARIABLE                    m_producerRunnable;
        CONDITION_VARIABLE                    m_consumersClosed;
        CONDITION_VARIABLE                    m_metadataFuncReady;

        std::unique_ptr<BufferType>           m_producerBuffer;
        ContainerType                         m_consumerBuffers;
        ULONG                                 m_countConsumers; // number of consumers (readers)
        ULONG                                 m_activeConsumersCount;

        ULONGLONG                             m_buffersProduced;
        ULONGLONG                             m_buffersConsumed;

        bool                                  m_producerClosed;
        std::vector<ContainerIteratorType>    m_consumers;

        SIZE_T                                m_memoryQuota;
        SIZE_T                                m_maxBufferMemorySize;
        SIZE_T                                m_ioBufferSize;
        int                                   m_ioBufferCount;

        SIZE_T                                m_peakMemoryUsage;
        LONGLONG                              m_ioTime;
        ULONGLONG                             m_buffersSpilled;
        ULONGLONG                             m_buffersLoaded;

        MetadataFuncType                      m_metadataFunc;
        PartitionMetadata*                    m_metadata;
        bool                                  m_metadataReady;

        int                                   m_operatorId;
    public:
        static const SIZE_T DefaultMemoryQuota = 1ULL * 1024 * 1024 * 1024; // 1 GB
        static const ULONG  DefaultBuffersPerConsumer = 3;
        static const SIZE_T DefaultIoBufferSize = 4UL * 1024 * 1024;         // 4 MB
        static const int    DefaultIoBufferCount = 2;

    public:
        SpoolQueue(ULONG consumersCount, SIZE_T memoryQuota, SIZE_T maxBufferMemorySize, SIZE_T ioBufferSize, int ioBufferCount)
        {
            Init(consumersCount, memoryQuota, maxBufferMemorySize, ioBufferSize, ioBufferCount);
        }

        SpoolQueue(ULONG consumersCount, SIZE_T memoryQuota, SIZE_T maxBufferMemorySize)
        {
            Init(consumersCount, memoryQuota, maxBufferMemorySize, DefaultIoBufferSize, DefaultIoBufferCount);
        }

        SpoolQueue(ULONG consumersCount, SIZE_T memoryQuota = DefaultMemoryQuota)
        {
            SIZE_T maxBufferMemorySize = memoryQuota / (consumersCount * DefaultBuffersPerConsumer + 1);

            Init(consumersCount, memoryQuota, maxBufferMemorySize, DefaultIoBufferSize, DefaultIoBufferCount);
        }

        void Init(ULONG consumersCount,
            SIZE_T memoryQuota,
            SIZE_T maxBufferMemorySize,
            SIZE_T ioBufferSize,
            int ioBufferCount)
        {
            m_countConsumers = consumersCount;
            m_activeConsumersCount = consumersCount;
            m_buffersProduced = 0;
            m_buffersConsumed = 0;
            m_producerClosed = false;
            m_memoryQuota = memoryQuota;
            m_maxBufferMemorySize = maxBufferMemorySize;
            m_ioBufferSize = ioBufferSize;
            m_ioBufferCount = ioBufferCount;
            m_peakMemoryUsage = 0;
            m_ioTime = 0;
            m_buffersSpilled = 0;
            m_buffersLoaded = 0;
            m_metadata = nullptr;
            m_metadataReady = false;
            m_operatorId = NO_OPERATOR_ID;

            SCOPE_ASSERT(m_countConsumers);

            SCOPE_ASSERT(maxBufferMemorySize >= sizeof(Schema));
            SCOPE_ASSERT(memoryQuota > 0);
            SCOPE_ASSERT(memoryQuota / maxBufferMemorySize > consumersCount);
            SCOPE_ASSERT(ioBufferSize > 0);
            SCOPE_ASSERT(ioBufferCount > 0);

            InitializeCriticalSection(&m_cs);
            InitializeConditionVariable(&m_consumersRunnable);
            InitializeConditionVariable(&m_producerRunnable);
            InitializeConditionVariable(&m_consumersClosed);
            InitializeConditionVariable(&m_metadataFuncReady);

            m_consumers.resize(m_countConsumers, m_consumerBuffers.end());

            SCOPE_LOG_FMT_INFO("SpoolQueue", "Init finished, consumersCount: %u, memoryQuota: %I64u, maxBufferMemorySize: %I64u", consumersCount, memoryQuota, maxBufferMemorySize);
        }

        ~SpoolQueue()
        {
            DeleteCriticalSection(&m_cs);
        }

        bool IsEmpty()
        {
            AutoCriticalSection aCS(&m_cs);

            return m_consumerBuffers.empty();
        }

        SIZE_T PeakMemoryUsage()
        {
            AutoCriticalSection aCS(&m_cs);

            return m_peakMemoryUsage;
        }

        SIZE_T MaxBufferMemorySize()
        {
            AutoCriticalSection aCS(&m_cs);

            return m_maxBufferMemorySize;
        }

        LONGLONG OperatorWaitOnIOTime()
        {
            AutoCriticalSection aCS(&m_cs);

            return m_ioTime;
        }

        ULONGLONG BuffersProduced()
        {
            AutoCriticalSection aCS(&m_cs);

            return m_buffersProduced;
        }

        ULONGLONG BuffersConsumed()
        {
            AutoCriticalSection aCS(&m_cs);

            return m_buffersConsumed;
        }

        ULONGLONG BuffersSpilled()
        {
            AutoCriticalSection aCS(&m_cs);

            return m_buffersSpilled;
        }

        ULONGLONG BuffersLoaded()
        {
            AutoCriticalSection aCS(&m_cs);

            return m_buffersLoaded;
        }

        SpoolBuffer<Schema>* GetBufferConsumer(ULONG consumerId)
        {
            AutoCriticalSection aCS(&m_cs);
            auto& currentIt = m_consumers[consumerId];

            // see if we have anything to read
            if (currentIt == m_consumerBuffers.end())
            {
                // the first buffer
                while (!m_producerClosed && m_consumerBuffers.empty())
                {
                    SleepConditionVariableCS(&m_consumersRunnable, &m_cs, INFINITE);
                }
                currentIt = m_consumerBuffers.begin();
            }
            else
            {
                SCOPE_ASSERT(m_buffersProduced);
                SCOPE_ASSERT(currentIt != m_consumerBuffers.end());

                // next buffers
                while (!m_producerClosed && (*currentIt)->Id() == m_buffersProduced - 1)
                {
                    SleepConditionVariableCS(&m_consumersRunnable, &m_cs, INFINITE);
                }

                (*currentIt)->DecConsumers();
                ++currentIt;
            }

            // now we have
            SpoolBuffer<Schema>* buffer = nullptr;
            if (currentIt != m_consumerBuffers.end())
            {
                buffer = currentIt->get();

                if (buffer->IsSpilled())
                {
                    FreeMemoryForNewBuffer();
                    buffer->Load();
                }

                buffer->AddRef();


                // wake up a producer if needed
                if (buffer->Id() == m_buffersConsumed)
                {
                    ++m_buffersConsumed;
                    if (m_buffersConsumed == m_buffersProduced)
                    {
                        WakeConditionVariable(&m_producerRunnable);
                    }
                }
            }

            // remove empty front
            if (!m_consumerBuffers.empty() && m_consumerBuffers.front()->ConsumersCount() == 0)
            {
                const BufferType &frontBuffer = *m_consumerBuffers.front();
                SCOPE_ASSERT(frontBuffer.RefCount() == 0);

                m_ioTime += frontBuffer.OperatorWaitOnIOTime();
                m_buffersSpilled += frontBuffer.SpillCount();
                m_buffersLoaded += frontBuffer.LoadCount();

                m_consumerBuffers.pop_front();
            }

            return buffer;
        }

        void ReleaseBufferConsumer(ULONG consumerId, BufferType* buffer)
        {
            AutoCriticalSection aCS(&m_cs);

            SCOPE_ASSERT(m_consumers[consumerId]->get() == buffer);
            buffer->DecRef();
        }

        SpoolBuffer<Schema>* GetBufferProducer()
        {
            AutoCriticalSection aCS(&m_cs);
            SCOPE_ASSERT(!m_producerBuffer);

            // test if at least one consumer (reader) is waiting for us
            // if true(waiting) then create a new buffer else wait
            while (m_buffersConsumed != m_buffersProduced)
            {
                SleepConditionVariableCS(&m_producerRunnable, &m_cs, INFINITE);
            }

            FreeMemoryForNewBuffer();

            m_producerBuffer.reset(new SpoolBuffer<Schema>(
                m_buffersProduced,
                m_countConsumers,
                m_maxBufferMemorySize,
                m_ioBufferSize,
                m_ioBufferCount));
            m_producerBuffer->AddRef();

            return m_producerBuffer.get();
        }

        void ReleaseBufferProducer(BufferType* buffer)
        {
            AutoCriticalSection aCS(&m_cs);

            // make sure that we have the right buffer and the ref count makes sense
            SCOPE_ASSERT(m_producerBuffer.get() == buffer);
            buffer->DecRef();
            SCOPE_ASSERT(buffer->RefCount() == 0);

            m_consumerBuffers.push_back(move(m_producerBuffer));
            ++m_buffersProduced;

            // now we can wake up any waiting consumers
            WakeAllConditionVariable(&m_consumersRunnable);
        }

        void SetMetadataFunc(MetadataFuncType metadataFunc)
        {
            AutoCriticalSection aCS(&m_cs);
            SCOPE_ASSERT(metadataFunc);
            SCOPE_ASSERT(!m_metadataFunc);

            m_metadataFunc = metadataFunc;
            WakeAllConditionVariable(&m_metadataFuncReady);
        }

        PartitionMetadata* GetMetadata()
        {
            SCOPE_LOG_FMT_INFO("SpoolQueue", "Call GetMetadata, consumers count is %d.", m_activeConsumersCount);
            
            AutoCriticalSection aCS(&m_cs);

            // active consumers count can be zero when exception happens and main thread cleans up spool producer and consumers.
            if (m_activeConsumersCount == 0)
            {
                if(m_metadataFunc)
                {
                    return m_metadataFunc();
                }
                else
                {
                    return nullptr;
                }
            }

            // normal case, waiting for producer setting metadataFunc.
            while (!m_metadataFunc)
            {
                // MetadataFunc isn't ready yet, wait for Spool producer to set it.
                SleepConditionVariableCS(&m_metadataFuncReady, &m_cs, INFINITE);
            }

            if (!m_metadataReady)
            {
                m_metadata = m_metadataFunc();
                m_metadataReady = true;
            }

            return m_metadata;
        }

        void CloseProducer()
        {
            AutoCriticalSection aCS(&m_cs);

            m_producerClosed = true;

            // now we can wake up any waiting consumers
            WakeAllConditionVariable(&m_consumersRunnable);

            // Wait here until all Spool consumers will be closed. This is required to be able to provide
            // partition metadata when requested. We need to keep producer operator chain open to be able
            // to call GetMetadata() on it.
            while (m_activeConsumersCount > 0)
            {
                SleepConditionVariableCS(&m_consumersClosed, &m_cs, INFINITE);
            }
        }

        void CloseConsumer()
        {
            AutoCriticalSection aCS(&m_cs);

            //CloseAllConsumers can set this to 0
            if (m_activeConsumersCount > 0)
            {			
                --m_activeConsumersCount;
            }
            if (m_activeConsumersCount == 0)
            {
                WakeConditionVariable(&m_consumersClosed);
            }
        }

        /*
        * devnote: CloseAllConsumers is used to clean up spool when exception happens.
        * In normal case consumer itself should call CloseConsumer.
        */
        void CloseAllConsumers()
        {
            SCOPE_LOG_FMT_INFO("SpoolQueue", "CloseAllConsumers, current consumer count is %d ", m_activeConsumersCount);
            
            AutoCriticalSection aCS(&m_cs);
            m_activeConsumersCount = 0;
            WakeConditionVariable(&m_consumersClosed);
        }

        void SetOperatorId(int operatorId)
        {
            m_operatorId = operatorId;
        }

    private:
        /*
        * Makes sure there is enough memory to allocate new spool buffer in the queue.
        * Spills one of the existing buffers if necessary.
        */
        void FreeMemoryForNewBuffer()
        {
            SIZE_T inMemoryBuffersCount = std::count_if(
                m_consumerBuffers.begin(),
                m_consumerBuffers.end(),
                [](const std::unique_ptr<BufferType>& buffer) { return !buffer->IsSpilled(); });

            if (m_producerBuffer != nullptr)
            {
                ++inMemoryBuffersCount;
            }

            SIZE_T memoryUsage = inMemoryBuffersCount * m_maxBufferMemorySize;
            SCOPE_ASSERT(memoryUsage <= m_memoryQuota);

            // Report actual memory usage
            SIZE_T actualMemoryUsage = std::accumulate(
                m_consumerBuffers.begin(),
                m_consumerBuffers.end(),
                SIZE_T(0),
                [](SIZE_T sum, const std::unique_ptr<BufferType>& buffer) { return sum + buffer->MemorySize(); });

            m_peakMemoryUsage = std::max(m_peakMemoryUsage, actualMemoryUsage);

            if (m_memoryQuota - memoryUsage >= m_maxBufferMemorySize)
            {
                // We have enough memory to allocate one more buffer
                return;
            }

            SCOPE_LOG_FMT_INFO("Spilling SpoolBuffer", "inMemoryBuffersCount: %I64u, totalBuffersCount: %I64u, memoryUsage: %I64u, actualMemoryUsage: %I64u"
                , inMemoryBuffersCount, m_consumerBuffers.size(), memoryUsage, actualMemoryUsage);

            // We have to spill one of the consumer buckets
            if (Configuration::GetGlobal().GetRestrictOperatorMemorySpilling())
            {
                throw OperatorOutOfMemoryException(m_operatorId);
            }

            auto bufferIterator = FindBufferToSpill(m_consumerBuffers);
            SCOPE_ASSERT(bufferIterator != m_consumerBuffers.end());

            BufferType &bufferToSpill = **bufferIterator;
            SCOPE_ASSERT(bufferToSpill.CanBeSpilled());

            bufferToSpill.Spill();
        }

        /*
        * Finds a consumer buffer to spill. We want to increase expected value of number of queue
        * operations before the spilled buffer will be loaded again, so we choose a buffer with the
        * largest distance to the nearest spool consumer.
        */
        static ContainerConstIteratorType FindBufferToSpill(const ContainerType &consumerBuffers)
        {
            SIZE_T resultDistance = 0;
            ContainerConstIteratorType resultIterator = consumerBuffers.end();

            SIZE_T distance = 0;
            for (auto i = consumerBuffers.begin(); i != consumerBuffers.end(); ++i)
            {
                const BufferType &buffer = **i;

                ULONG consumersCurrent = buffer.ConsumersCount();
                ULONG consumersPrevious = i == consumerBuffers.begin() ? 0 : (*std::prev(i))->ConsumersCount();

                SCOPE_ASSERT(consumersCurrent >= consumersPrevious);
                ULONG consumersDiff = consumersCurrent - consumersPrevious;

                distance = consumersDiff == 0 ? distance + 1 : 0;

                if (buffer.CanBeSpilled() && distance >= resultDistance)
                {
                    resultDistance = distance;
                    resultIterator = i;
                }
            }

            return resultIterator;
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements(0, memoryQuota)
                .AddMemoryInRows(OperatorRequirementsConstants::SpoolQueue__Row_MinMemory)
                .AddMemoryForOutputUStreams(OperatorRequirementsConstants::SpoolQueue__Count_OutputUStream, DefaultIoBufferSize, DefaultIoBufferCount)
                .AddMemoryForInputUStreams(OperatorRequirementsConstants::SpoolQueue__Count_InputUStream, DefaultIoBufferSize, DefaultIoBufferCount);
        }
    };

    template<typename InputOperator, typename InputSchema, int UID>
    class SpoolProducer : public Operator<SpoolProducer<InputOperator, InputSchema, UID>, InputSchema, UID>
    {
        typedef SpoolQueue<InputSchema> QueueType;
        typedef SpoolBuffer<InputSchema> BufferType;

        InputOperator*    m_child;
        QueueType*        m_queue;
        BufferType*       m_buffer;
    public:
        SpoolProducer(QueueType* queue, InputOperator* input, int operatorId) :
            Operator(operatorId),
            m_child(input),
            m_queue(queue),
            m_buffer(nullptr)
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();

            m_queue->SetMetadataFunc([this](){ return this->GetMetadataImpl(); });
        }

        PartitionMetadata * GetMetadataImpl()
        {
            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(InputSchema & output)
        {
            AutoExecStats stats(this);

            ULONGLONG count = 0;

            m_buffer = m_queue->GetBufferProducer();

            while (m_child->GetNextRow(output))
            {
                if (m_buffer->Rows().FFull() || !m_buffer->Rows().AddRow(output))
                {
                    m_queue->ReleaseBufferProducer(m_buffer);
                    m_buffer = m_queue->GetBufferProducer();

                    bool added = m_buffer->Rows().AddRow(output);
                    SCOPE_ASSERT(added);
                }
                ++count;
            }

            m_queue->ReleaseBufferProducer(m_buffer);

            m_queue->CloseProducer();

            stats.IncreaseRowCount(count);
            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("SpoolProducer");
            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond() - m_queue->OperatorWaitOnIOTime());
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_queue->OperatorWaitOnIOTime());
            node.AddAttribute(RuntimeStats::MaxPeakInMemorySize(), m_queue->PeakMemoryUsage());
            node.AddAttribute(RuntimeStats::AvgPeakInMemorySize(), m_queue->PeakMemoryUsage());
            node.AddAttribute(RuntimeStats::MaxBuffersProducedCount(), m_queue->BuffersProduced());
            node.AddAttribute(RuntimeStats::AvgBuffersProducedCount(), m_queue->BuffersProduced());
            node.AddAttribute(RuntimeStats::MaxBuffersSpilledCount(), m_queue->BuffersSpilled());
            node.AddAttribute(RuntimeStats::AvgBuffersSpilledCount(), m_queue->BuffersSpilled());
            node.AddAttribute(RuntimeStats::MaxBuffersLoadedCount(), m_queue->BuffersLoaded());
            node.AddAttribute(RuntimeStats::AvgBuffersLoadedCount(), m_queue->BuffersLoaded());
            node.AddAttribute(RuntimeStats::MaxBufferMemory(), m_queue->MaxBufferMemorySize());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements(OperatorRequirementsConstants::SpoolProducer__Size_MinMemory)
                .Add(m_queue->GetOperatorRequirements());
        }
    };

    template<typename OutputSchema, int UID>
    class SpoolConsumer : public Operator<SpoolConsumer<OutputSchema, UID>, typename OutputSchema, UID>
    {
        typedef SpoolQueue<typename OutputSchema> QueueType;
        typedef SpoolBuffer<typename OutputSchema> BufferType;

        QueueType*     m_queue;
        BufferType*    m_buffer;
        ULONG          m_consumerId;
        SIZE_T         m_currentRow;
    public:
        SpoolConsumer(QueueType* queue, ULONG consumerId, int operatorId) :
            Operator(operatorId),
            m_consumerId(consumerId),
            m_queue(queue),
            m_buffer(nullptr),
            m_currentRow(0)
        {
        }
        void InitImpl()
        {
            AutoExecStats stats(this);
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);
            return m_queue->GetMetadata();
        }

        bool GetNextRowImpl(typename OutputSchema & output)
        {
            AutoExecStats stats(this);

            for (;;)
            {
                if (!m_buffer)
                {
                    m_buffer = m_queue->GetBufferConsumer(m_consumerId);

                    // we have hit EOS
                    if (!m_buffer)
                    {
                        return false;
                    }

                    m_currentRow = 0;
                }

                SCOPE_ASSERT(m_buffer);

                if (m_currentRow < m_buffer->Rows().Size())
                {
                    output = m_buffer->Rows()[m_currentRow];

                    ++m_currentRow;
                    stats.IncreaseRowCount(1);

                    return true;
                }
                else
                {
                    // current buffer is empty
                    m_queue->ReleaseBufferConsumer(m_consumerId, m_buffer);
                    m_buffer = nullptr;
                }
            }
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_queue->CloseConsumer();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("SpoolConsumer");
            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements(OperatorRequirementsConstants::SpoolProducer__Size_MinMemory);
        }

    };

    ///
    /// Cross process spool reader for ISCOPE single producer/multiple consumer plans
    ///
    template<typename InputOperator, typename InputSchema, typename OutputSchema, int UID = -1>
    class CrossProcessSpoolReader : public Operator<CrossProcessSpoolReader<InputOperator, InputSchema, OutputSchema, UID>, OutputSchema, UID>
    {
        InputOperator * m_child;
        IncrementalAllocator m_alloc;
        unique_ptr<BinaryInputStream> m_input;
        bool m_firstChance;

    public:
        CrossProcessSpoolReader(InputOperator * input, int operatorId) :
            Operator(operatorId),
            m_child(input),
            m_firstChance(true),
            m_alloc(MemoryManager::x_maxMemSize, "CrossProcessSpoolReader")
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            if (m_firstChance)
            {
                m_firstChance = false;

                InputSchema row;
                bool success = m_child->GetNextRow(row);
                SCOPE_ASSERT(success);

                string inputStreamName(row.m___CosmosPath.buffer(), row.m___CosmosPath.size());
                SCOPE_LOG_FMT_INFO("Spool reader", "Deserializing input from the spool stream %s", inputStreamName);
                IOManager::GetGlobal()->AddInputStream(inputStreamName, inputStreamName);
                m_input.reset(new BinaryInputStream(inputStreamName, &m_alloc, IOManager::x_defaultInputBufSize, IOManager::x_defaultInputBufCount));
                m_input->Init();
            }

            m_alloc.Reset();
            if (m_input && BinaryExtractPolicy<OutputSchema>::Deserialize(m_input.get(), output))
            {
                stats.IncreaseRowCount(1);
                return true;
            }

            m_input->Close();
            m_input.reset();
            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("CrossProcessSpoolReader");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }
        }
    };

    ///
    /// Cross process spool writer for ISCOPE single producer/multiple consumer plans
    ///
    template<typename InputOperator, typename InputSchema, typename OutputSchema, int UID = -1>
    class CrossProcessSpoolWriter : public Operator<CrossProcessSpoolWriter<InputOperator, InputSchema, OutputSchema, UID>, OutputSchema, UID>
    {
        InputOperator * m_child;
        string m_outputStreamName;
        bool m_firstChance;

        ULONGLONG SerializeInput(const string & outputStreamName)
        {
            ULONGLONG rowCount = 0;

            IOManager::GetGlobal()->AddOutputStream(outputStreamName, outputStreamName);
            BinaryOutputStream output(outputStreamName, IOManager::x_defaultOutputBufSize, IOManager::x_defaultOutputBufCount);
            output.Init();

            InputSchema row;
            while (m_child->GetNextRow(row))
            {
                BinaryOutputPolicy<InputSchema>::Serialize(&output, row);
                ++rowCount;
            }

            output.Finish();
            output.Close();

            return rowCount;
        }

    public:
        CrossProcessSpoolWriter(InputOperator * input, int operatorId) :
            Operator(operatorId),
            m_child(input),
            m_firstChance(true)
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema & output)
        {
            AutoExecStats stats(this);

            if (m_firstChance)
            {
                m_firstChance = false;

                // Make m_outputStreamName class member to ensure FString has valid pointer after this method exits
                m_outputStreamName = IOManager::GetTempStreamName();
                SCOPE_LOG_FMT_INFO("Spool writer", "Serializing input into the spool stream %s", m_outputStreamName);
                ULONGLONG rowCount = SerializeInput(m_outputStreamName);
                stats.IncreaseRowCount(rowCount);

                new (&output.m___CosmosPath) FString(m_outputStreamName.c_str(), m_outputStreamName.size());
                return true;
            }

            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("CrossProcessSpoolWriter");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }
        }
    };

#pragma region ManagedOperators
    //
    // Head of split output chain (receives from from the child and passes it down to the split output chain)
    //
    template<typename InputOperator, typename InputSchema, bool needMetadata, int UID = -1>
    class Splitter : public Operator<Splitter<InputOperator, InputSchema, needMetadata, UID>, int, UID>
    {
    protected:
        InputOperator  *  m_child;  // child operator
        std::unique_ptr<SplitPolicy<InputSchema, UID>> m_splitPolicy;

    public:
        Splitter(InputOperator * input, std::string * outputFileNames, SIZE_T outputBufSize, int outputBufCnt, int operatorId) :
            Operator(operatorId),
            m_child(input)
        {
            m_splitPolicy.reset(SplitPolicyFactory<InputSchema, UID>::Create(outputFileNames, outputBufSize, outputBufCnt));
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
            m_splitPolicy->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            return nullptr;
        }

        bool GetNextRowImpl(int & output)
        {
            AutoExecStats stats(this);

            // calling GetMetadata() implies validating metadata which may result in false negatives when metadata is not needed
            if (needMetadata)
            {
                m_splitPolicy->ProcessMetadata(m_child->GetMetadata());
            }

            output = DoOutput();
            stats.IncreaseRowCount((ULONGLONG)output);

            return false;
        }

        virtual int DoOutput()
        {
            int rowCount = 0;

            InputSchema input;
            while (m_child->GetNextRow(input))
            {
                m_splitPolicy->ProcessRow(input);
                rowCount++;
            }

            return rowCount;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_splitPolicy->Close();
            m_child->Close();
        }

        // TODO: provide statistics for every split output
        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("SplitOutput");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond() - m_splitPolicy->GetTotalIoWaitTime());
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_splitPolicy->GetTotalIoWaitTime());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_splitPolicy->WriteRuntimeStats(node);

            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements(OperatorRequirementsConstants::Splitter__Size_MinMemory)
                .Add(m_splitPolicy->GetOperatorRequirements());
        }

        DEFAULT_IMPLEMENT_SCOPECEP_CHECKPOINT_VIRTUAL
    };

    template<typename InputOperator, typename InputSchema, bool needMetadata, int RunScopeCEPMode, bool checkOutput, int UID = -1>
    class StreamingSplitter : public Splitter<InputOperator, InputSchema, needMetadata, UID>
    {
        SlowRowTracker m_slowRowTracker;

    public:
        StreamingSplitter(InputOperator * input, std::string * outputFileNames, SIZE_T outputBufSize, int outputBufCnt, int operatorId) :
            Splitter(input, outputFileNames, outputBufSize, outputBufCnt, operatorId),
            m_slowRowTracker("OUTPUT")
        {
        }

        void Flush()
        {
            m_splitPolicy->FlushOutput();
        }

        virtual int DoOutput()
        {
            int rowCount = 0;
            AutoFlushTimer<StreamingSplitter> autoFlushTimer(this);

            bool fromCheckpoint = false;

            InputSchema input;
            BinaryOutputStream* checkpoint = nullptr;
            if (!g_scopeCEPCheckpointManager->GetStartScopeCEPState().empty())
            {
                ScopeDateTime startTime = g_scopeCEPCheckpointManager->GetStartCTITime();
                input.ResetScopeCEPStatus(startTime, startTime, SCOPECEP_CTI_CHECKPOINT);
                fromCheckpoint = true;
                // the first row we output is a duplicate from the past, decrement it first so later when
                // we increment it again the number is balanced.
                g_scopeCEPCheckpointManager->DecrementSeqNumber();
            }

            while (fromCheckpoint || m_child->GetNextRow(input))
            {
                if (!autoFlushTimer.IsStarted() && !fromCheckpoint && RunScopeCEPMode == SCOPECEP_MODE_REAL)
                {
                    autoFlushTimer.Start();
                }

                m_slowRowTracker.FinishNextRow(input.GetScopeCEPEventStartTime(), input.GetScopeCEPEventType());
                AutoCriticalSection aCs(autoFlushTimer.GetLock());

                if (input.IsScopeCEPCTI())
                {
                    g_scopeCEPCheckpointManager->UpdateLastCTITime(input.GetScopeCEPEventStartTime());
                    g_scopeCEPCheckpointManager->IncrementSeqNumber();
                    m_splitPolicy->ProcessRow(input);

                    if (!fromCheckpoint && input.GetScopeCEPEventType() == (UINT8)SCOPECEP_CTI_CHECKPOINT && g_scopeCEPCheckpointManager->IsWorthyToDoCheckpoint(input.GetScopeCEPEventStartTime()))
                    {
                        m_splitPolicy->FlushOutput(true);
                        if (checkOutput)
                        {
                            // hold the object until streaming splitoutputer processed next row.
                            checkpoint = g_scopeCEPCheckpointManager->InitiateCheckPointChainInternal(this);
                            m_splitPolicy->SetCheckpoint(checkpoint);
                        }
                        else
                        {
                            g_scopeCEPCheckpointManager->InitiateCheckPointChain(this);
                        }
                    }
                }
                else
                {
                    g_scopeCEPCheckpointManager->IncrementSeqNumber();
                    m_splitPolicy->ProcessRow(input);
                }

                fromCheckpoint = false;

                if (!input.IsScopeCEPCTI() && checkpoint != nullptr)
                {
                    checkpoint->Finish();
                    checkpoint->Close();
                    delete checkpoint;
                    checkpoint = nullptr;
                }
                rowCount++;
                m_slowRowTracker.StartNextRow();
            }

            // it could be true that there is no more data after last checkpoint
            if (checkpoint != nullptr)
            {
                checkpoint->Finish();
                checkpoint->Close();
                delete checkpoint;
                checkpoint = nullptr;
            }

            return (int)rowCount;
        }

        virtual void DoScopeCEPCheckpointImpl(BinaryOutputStream & output) override
        {
            m_child->DoScopeCEPCheckpoint(output);
            m_splitPolicy->DoScopeCEPCheckpoint(output);
        }

        virtual void LoadScopeCEPCheckpointImpl(BinaryInputStream & input) override
        {
            m_child->LoadScopeCEPCheckpoint(input);
            m_splitPolicy->LoadScopeCEPCheckpoint(input);
            m_splitPolicy->LoadFirstRowFromCheckpoint(input);
        }
    };


    //TODO move to codegen to enable multiple fusion pipelines in one vertex
    void Fusion_Compute(int getssid, void * dataUnitDescriptor, std::function<void(void *)> outputfuncptr);

    template <typename OutputSchema, int UID = -1>
    class FusionAdapter : public Operator<FusionAdapter<OutputSchema, UID>, OutputSchema, UID>
    {
    private:
        typedef SpoolQueue<OutputSchema>  TQueue;
        typedef SpoolBuffer<OutputSchema> TBuffer;

        TQueue * m_queue;

        int m_getssid;
        SSLibV3::DataUnitDescriptor * m_dataUnitDescriptor;

    public:
        FusionAdapter(TQueue * queue, int getssid, SSLibV3::DataUnitDescriptor * desc, int operatorId)
            : Operator(operatorId)
            , m_queue(queue)
            , m_getssid(getssid)
            , m_dataUnitDescriptor(desc)
        {
        }

        void InitImpl()
        {
            AutoExecStats stats(this);
        }

        PartitionMetadata * GetMetadataImpl() //TODO get metadata from spoolqueue
        {
            AutoExecStats stats(this);

            return nullptr;
        }

        bool GetNextRowImpl(OutputSchema &)
        {
            TBuffer * buffer = m_queue->GetBufferProducer();

            std::function<void(void *)> outputfunc = [this, &buffer](void * row)
            {
                // this code relies on the fusion output schema to be binary
                // identical to OutputSchema
                // TODO: use one schema structure for both runtimes
                OutputSchema * output = reinterpret_cast<OutputSchema *>(row);

                if (buffer->Rows().FFull() || !buffer->Rows().AddRow(*output))
                {
                    m_queue->ReleaseBufferProducer(buffer);
                    buffer = m_queue->GetBufferProducer();

                    bool added = buffer->Rows().AddRow(*output);
                    SCOPE_ASSERT(added);
                }
            };

            Fusion_Compute(m_getssid, m_dataUnitDescriptor, outputfunc);

            m_queue->ReleaseBufferProducer(buffer);
            m_queue->CloseProducer();

            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);
        }

        void WriteRuntimeStatsImpl(TreeNode & /*root*/) //TODO
        {
        }
    };


    //
    // Declarations for operators that have to be implemented in managed code, but they need to be glued in
    // the native code. ScopeManagedHandle hides their managed handles.
    //

    template<typename InputType, typename OutputSchema>
    class ScopeExtractor : public Operator<ScopeExtractor<InputType, OutputSchema>, OutputSchema, -1>
    {
        std::shared_ptr<ScopeExtractorManaged<InputType, OutputSchema>> m_managedExtractor;

        bool            m_hasMoreRows;
        SIZE_T          m_bufSize;
        int             m_bufCount;
        SIZE_T          m_virtualMemSize;
        bool            m_initialized;
        InputFileInfo   m_input;
        volatile long   m_extractorCnt;

    public:
        ScopeExtractor(ScopeExtractorManaged<InputType, OutputSchema> * managedExtractor,
            const InputFileInfo& input,
            bool needMetadata,
            SIZE_T bufSize,
            int bufCount,
            SIZE_T virtualMemSize,
            StreamingInputParams* streamingInputParams, // it's not used, but generated code depends on this parameter - g_scopeCEPCheckpointManager->GetInputParameter(scopeCEPInputIndex++)
            int operatorId) :
            Operator(operatorId),
            m_hasMoreRows(true),
            m_bufSize(bufSize),
            m_bufCount(bufCount),
            m_input(input),
            m_virtualMemSize(virtualMemSize),
            m_initialized(false),
            m_extractorCnt(0)
        {
            if (needMetadata)
            {
                throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "User-defined extractor cannot produce metadata");
            }

            m_managedExtractor.reset(managedExtractor);
            m_managedExtractor->CreateInstance(m_input, m_bufSize, m_bufCount, m_virtualMemSize);
        }

        void InitImpl()
        {
            AutoExecStats stats(this);
            m_managedExtractor->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            return nullptr;
        }

        bool GetNextRowImpl(OutputSchema& output)
        {
            AutoExecStats stats(this);

            if (m_hasMoreRows)
            {
                ScopeGuard guard(&m_extractorCnt);
                m_hasMoreRows = m_managedExtractor->GetNextRow(output);
            }

            if (!m_hasMoreRows)
            {
                return false;
            }

            stats.IncreaseRowCount(1);

            return true;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_managedExtractor->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("ScopeExtract");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_managedExtractor->GetOperatorWaitOnIOTime());
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_managedExtractor->GetOperatorWaitOnIOTime());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_managedExtractor->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return m_managedExtractor->GetOperatorRequirements();
        }

        void DoScopeCEPCheckpointImpl(BinaryOutputStream & output)
        {
            SCOPE_ASSERT(m_extractorCnt == 0);
            m_managedExtractor->DoScopeCEPCheckpoint(output);
        }

        void LoadScopeCEPCheckpointImpl(BinaryInputStream & input)
        {
            m_managedExtractor->LoadScopeCEPCheckpoint(input);
        }

    };

    template<typename OutputSchema, int UID>
    class ScopeSStreamV2Extractor : public Operator<ScopeSStreamV2Extractor<OutputSchema, UID>, OutputSchema, UID>
    {
        std::shared_ptr<ScopeSStreamExtractorManaged<OutputSchema>> m_managedExtractor;
        unique_ptr<PartitionMetadata>                          m_metadata;
        OperatorRequirements m_keyRangeRequirements;

        bool            m_hasMoreRows;
        int             m_getssid;
        SIZE_T          m_bufSize;
        int             m_bufCount;
        SIZE_T          m_virtualMemSize;

    public:
        ScopeSStreamV2Extractor(ScopeSStreamExtractorManaged<OutputSchema> * managedExtractor, int getssid, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize, int operatorId) :
            Operator(operatorId),
            m_keyRangeRequirements(),
            m_hasMoreRows(true),
            m_bufSize(bufSize),
            m_bufCount(bufCount),
            m_getssid(getssid),
            m_virtualMemSize(virtualMemSize)
        {
            m_managedExtractor.reset(managedExtractor);
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_managedExtractor->Init(m_getssid, m_bufSize, m_bufCount, m_virtualMemSize);
        }

        PartitionMetadata * GetMetadataImpl()
        {
            typedef SStreamV2ExtractPolicy<OutputSchema, UID> ExtractPolicy;
            auto partitionDevices = m_managedExtractor->GetPartitionDevices();
            auto processingGroupIds = IOManager::GetGlobal()->GetSStreamProcessingGroupIds(partitionDevices);

            if (ExtractPolicy::m_generateMetadata && processingGroupIds.size() > 0)
            {
                SCOPE_ASSERT(processingGroupIds.size() > 0);
                int partitionId = processingGroupIds[0];
                // make sure that all processing groups belong to the same partition
                for (SIZE_T i = 1; i < processingGroupIds.size(); ++i)
                {
                    SCOPE_ASSERT(processingGroupIds[i] == partitionId);
                }

                auto keyRangeFileName = m_managedExtractor->GetKeyRangeFileName();
                if (!keyRangeFileName.empty())
                {
                    SCOPE_ASSERT(ExtractPolicy::m_partitioning == RangePartition);
                    KeyRangeMetafile<typename ExtractPolicy::PartitionSchema, ExtractPolicy::m_truncatedRangeKey>keyRangeFile(keyRangeFileName);
                    m_keyRangeRequirements = keyRangeFile.GetOperatorRequirements();

                    keyRangeFile.Read();
                    m_metadata.reset(new PartitionPayloadMetadata<typename ExtractPolicy::PartitionSchema, UID>(partitionId, keyRangeFile.Low(partitionId), keyRangeFile.High(partitionId)));
                    return m_metadata.get();
                }
                else
                {
                    if (ExtractPolicy::m_partitioning == HashPartition || ExtractPolicy::m_partitioning == DirectHashPartition || ExtractPolicy::m_partitioning == RandomPartition)
                    {
                        m_metadata.reset(new PartitionPayloadMetadata<typename ExtractPolicy::PartitionSchema, UID>(partitionId));
                        return m_metadata.get();
                    }
                    else if (ExtractPolicy::m_partitioning == RangePartition)
                    {
                        throw MetadataException("RangePartition not yet implemented");
                    }
                    else
                    {
                        throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "Invalid partitioning type");
                    }
                }
            }

            return nullptr;
        }

        bool GetNextRowImpl(OutputSchema& output)
        {
            AutoExecStats stats(this);

            if (m_hasMoreRows)
            {
                m_hasMoreRows = m_managedExtractor->GetNextRow(output);
            }

            if (!m_hasMoreRows)
            {
                return false;
            }

            stats.IncreaseRowCount(1);

            return true;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_managedExtractor->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("ScopeSStreamV2Extract");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_managedExtractor->GetOperatorWaitOnIOTime());
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_managedExtractor->GetOperatorWaitOnIOTime());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            if (m_metadata)
            {
                m_metadata->WriteRuntimeStats(node);
            }
            m_managedExtractor->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            OperatorRequirements result = OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::ScopeSStreamV2Extractor__Row_MinMemory)
                .AddMemoryForInputSStreams(OperatorRequirementsConstants::ScopeSStreamV2Extractor__Count_InputSStream, m_bufCount);

            auto keyRangeFileName = m_managedExtractor->GetKeyRangeFileName();
            if (!keyRangeFileName.empty())
            {
                result.Add(m_keyRangeRequirements);
            }
            if (m_metadata)
            {
                result.Add(m_metadata->GetOperatorRequirements());
            }

            return result;
        }
    };

    // actually, it doesn't care about schema
    template<typename OutputSchema>
    class SStreamMetadataExtractor : public Operator<SStreamMetadataExtractor<OutputSchema>, OutputSchema, -1>
    {
        BlockDevice*                       m_device;
        std::unique_ptr<SStreamMetadata>        m_streamMetadata;
        int                                m_partitionIndex;
        bool                               m_isRandomPartition;
    public:
        SStreamMetadataExtractor(const std::string & fileName, int index, bool isRandomPartition, int operatorId) :
            Operator(operatorId),
            m_partitionIndex(index),
            m_isRandomPartition(isRandomPartition)
        {
            SCOPE_ASSERT(index >= 0);
            m_device = IOManager::GetGlobal()->GetDevice(fileName);
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_streamMetadata.reset(SStreamMetadata::Create(MemoryManager::GetGlobal()));
            m_streamMetadata->Read(m_device);
            if (m_isRandomPartition)
            {
                // In current JM implementation, it can't assign partition index for the vertex when the vertex generating data was scheduled if it's a random partition stream.
                // After all the vertices belonging to data generation stage have done, JM knows all the temporary streams and assume the partition index is the stream index in the whole temporary streams.
                // JM will concatenate the temporary streams into final stream based on order of the stream.
                // therefore, we need to update the partition index based on the input index.
                // details refer to http://bugcheck/bugs/MSNSearchTracking/626325 and CL#969599
                m_streamMetadata->UpdatePartitionIndex(m_partitionIndex);
            }

        }

        PartitionMetadata * GetMetadataImpl()
        {
            return m_streamMetadata.get();
        }

        bool GetNextRowImpl(OutputSchema& output)
        {
            AutoExecStats stats(this);
            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            LONGLONG ioTime = m_streamMetadata ? m_streamMetadata->GetTotalIoWaitTime() : 0;
            auto & node = root.AddElement("SStreamMetadataExtractor");
            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - ioTime);
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), ioTime);
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            if (m_streamMetadata)
            {
                m_streamMetadata->WriteRuntimeStats(node);
            }
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::SStreamMetadataExtractor__Row_MinMemory)
                .AddMemoryForInputUStreams(OperatorRequirementsConstants::SStreamMetadataExtractor__Count_InputUStream, OperatorRequirementsConstants::Input_UStream__Size_FormatterMemory);
        }
    };

    //
    // ScopeProcessor is a managed wrapper of the ScopeRuntime.Processor object.
    // It translates the new Operator interface (i.e. GetNextRow) to the old runtime interface (i.e. IEnumerator based)
    //
    // Input parametes:
    //        RowSet - ScopeRuntime rowset intput to the processor
    //        Args - arguments to the processor
    //        Schema - output schema of the processor
    //        name - a name of the processor
    //
    template<typename InputSchema, typename OutputSchema>
    class ScopeProcessor : public Operator<ScopeProcessor<InputSchema, OutputSchema>, OutputSchema, -1>
    {
        std::shared_ptr<ScopeProcessorManaged<InputSchema, OutputSchema>> m_managedProcessor;

        OperatorDelegate<InputSchema> * m_child;     // Array of child operator

        bool m_hasMoreRows;

    public:
        ScopeProcessor(ScopeProcessorManaged<InputSchema, OutputSchema> * managedProcessor, OperatorDelegate<InputSchema> * child, int operatorId) :
            Operator(operatorId),
            m_child(child),
            m_hasMoreRows(true)
        {
            m_managedProcessor.reset(managedProcessor);
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
            m_managedProcessor->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema& output)
        {
            AutoExecStats stats(this);

            if (m_hasMoreRows)
            {
                m_hasMoreRows = m_managedProcessor->GetNextRow(output);
            }

            if (!m_hasMoreRows)
            {
                return false;
            }

            stats.IncreaseRowCount(1);

            return true;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
            m_managedProcessor->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("ScopeProcess");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_managedProcessor->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return m_managedProcessor->GetOperatorRequirements();
        }

        void DoScopeCEPCheckpointImpl(BinaryOutputStream & output)
        {
            m_managedProcessor->DoScopeCEPCheckpoint(output);
            m_child->DoScopeCEPCheckpoint(output);
        }

        void LoadScopeCEPCheckpointImpl(BinaryInputStream & input)
        {
            m_managedProcessor->LoadScopeCEPCheckpoint(input);
            m_child->LoadScopeCEPCheckpoint(input);
        }

        void AdjustCtiImpl(ScopeDateTime& cti)
        {
            UNREFERENCED_PARAMETER(cti);
            return;
        }
    };

    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema>
    class ScopeCombiner : public Operator<ScopeCombiner<InputSchemaLeft, InputSchemaRight, OutputSchema>, OutputSchema, -1>
    {
        std::shared_ptr<ScopeCombinerManaged<InputSchemaLeft, InputSchemaRight, OutputSchema>> m_managedCombiner;

        OperatorDelegate<InputSchemaLeft> * m_childLeft;
        OperatorDelegate<InputSchemaRight> * m_childRight;

        bool m_hasMoreRows;
        int m_payloadSrcIndex;

    public:
        ScopeCombiner(ScopeCombinerManaged<InputSchemaLeft, InputSchemaRight, OutputSchema> * managedCombiner, OperatorDelegate<InputSchemaLeft> * childLeft, OperatorDelegate<InputSchemaRight> * childRight, int operatorId, int payloadSrcIndex) :
            Operator(operatorId),
            m_childLeft(childLeft),
            m_childRight(childRight),
            m_hasMoreRows(true),
            m_payloadSrcIndex(payloadSrcIndex)
        {
            m_managedCombiner.reset(managedCombiner);
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_childLeft->Init();
            m_childRight->Init();

            m_managedCombiner->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            if (0 == m_payloadSrcIndex)
            {
                return m_childLeft->GetMetadata();
            }

            return m_childRight->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema& output)
        {
            AutoExecStats stats(this);

            if (m_hasMoreRows)
            {
                m_hasMoreRows = m_managedCombiner->GetNextRow(output);
            }

            if (!m_hasMoreRows)
            {
                return false;
            }

            stats.IncreaseRowCount(1);

            return true;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_childLeft->Close();
            m_childRight->Close();

            m_managedCombiner->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("ScopeCombine");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_childLeft->GetInclusiveTimeMillisecond() - m_childRight->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_managedCombiner->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return m_managedCombiner->GetOperatorRequirements();
        }

        void DoScopeCEPCheckpointImpl(BinaryOutputStream & output)
        {
            m_managedCombiner->DoScopeCEPCheckpoint(output);
            m_childLeft->DoScopeCEPCheckpoint(output);
            m_childRight->DoScopeCEPCheckpoint(output);
        }

        void LoadScopeCEPCheckpointImpl(BinaryInputStream & input)
        {
            m_managedCombiner->LoadScopeCEPCheckpoint(input);
            m_childLeft->LoadScopeCEPCheckpoint(input);
            m_childRight->LoadScopeCEPCheckpoint(input);
        }

        void AdjustCtiImpl(ScopeDateTime& cti)
        {
            UNREFERENCED_PARAMETER(cti);
            return;
        }
    };

    template<typename InputOperators, typename OutputSchema, int UID>
    class ScopeMultiProcessor : public Operator<ScopeMultiProcessor<InputOperators, OutputSchema, UID>, OutputSchema, UID>
    {
        std::shared_ptr<ScopeMultiProcessorManaged<InputOperators, OutputSchema, UID>> m_managedProcessor;
        bool m_hasMoreRows;
    public:
        ScopeMultiProcessor(ScopeMultiProcessorManaged<InputOperators, OutputSchema, UID> * managedProcessor, int operatorId) :
            Operator(operatorId),
            m_hasMoreRows(true)
        {
            m_managedProcessor.reset(managedProcessor);
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_managedProcessor->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            return nullptr;
        }

        bool GetNextRowImpl(OutputSchema& output)
        {
            AutoExecStats stats(this);

            if (m_hasMoreRows)
            {
                m_hasMoreRows = m_managedProcessor->GetNextRow(output);
            }

            if (!m_hasMoreRows)
            {
                return false;
            }

            stats.IncreaseRowCount(1);

            return true;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);
            m_managedProcessor->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("ScopeMultiProcessor");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_managedProcessor->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_managedProcessor->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return m_managedProcessor->GetOperatorRequirements();
        }

        void DoScopeCEPCheckpointImpl(BinaryOutputStream & output)
        {
            m_managedProcessor->DoScopeCEPCheckpoint(output);
        }

        void LoadScopeCEPCheckpointImpl(BinaryInputStream & input)
        {
            m_managedProcessor->LoadScopeCEPCheckpoint(input);
        }

        void AdjustCtiImpl(ScopeDateTime& cti)
        {
            UNREFERENCED_PARAMETER(cti);
            return;
        }
    };

    template <typename InputSchema>
    class ScopeCreateContext : public Operator<ScopeCreateContext<InputSchema>, InputSchema, -1>
    {
        std::shared_ptr<ScopeCreateContextManaged<InputSchema>> m_managedCreateContext;

        OperatorDelegate<InputSchema> * m_child;     // Array of child operator
        std::string     m_outputName;
        SIZE_T          m_bufSize;
        int             m_bufCount;

    public:
        ScopeCreateContext(OperatorDelegate<InputSchema> * child, const string& outputName, SIZE_T bufSize, int bufCnt, ScopeCreateContextManaged<InputSchema> * managedCreateContext, int operatorId) :
            Operator(operatorId),
            m_child(child),
            m_outputName(outputName),
            m_bufSize(bufSize),
            m_bufCount(bufCnt)
        {
            m_managedCreateContext.reset(managedCreateContext);
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
            m_managedCreateContext->Init(m_outputName, m_bufSize, m_bufCount);
        }

        PartitionMetadata * GetMetadataImpl()
        {
            return nullptr;
        }

        bool GetNextRowImpl(InputSchema & output)
        {
            AutoExecStats stats(this);

            m_managedCreateContext->Serialize();

            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
            m_managedCreateContext->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("ScopeCreateContext");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond() - m_managedCreateContext->GetOperatorWaitOnIOTime());
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_managedCreateContext->GetOperatorWaitOnIOTime());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            // Writes RowCount and I/O stats
            m_managedCreateContext->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return m_managedCreateContext->GetOperatorRequirements();
        }
    };

    template<typename OutputSchema>
    class ScopeReadContext : public Operator<ScopeReadContext<OutputSchema>, OutputSchema, -1>
    {
        std::shared_ptr<ScopeReadContextManaged<OutputSchema>> m_managedReadContext;

        bool            m_hasMoreRows;
        std::string     m_inputName;
        SIZE_T          m_bufSize;
        int             m_bufCount;
        SIZE_T          m_virtualMemSize;

    public:
        ScopeReadContext(ScopeReadContextManaged<OutputSchema> * managedReadContext, const std::string& inputName, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize, int operatorId) :
            Operator(operatorId),
            m_hasMoreRows(true),
            m_bufSize(bufSize),
            m_bufCount(bufCount),
            m_inputName(inputName),
            m_virtualMemSize(virtualMemSize)
        {
            m_managedReadContext.reset(managedReadContext);
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_managedReadContext->Init(m_inputName, m_bufSize, m_bufCount, m_virtualMemSize);
        }

        PartitionMetadata * GetMetadataImpl()
        {
            return nullptr;
        }

        bool GetNextRowImpl(OutputSchema& output)
        {
            AutoExecStats stats(this);

            if (m_hasMoreRows)
            {
                m_hasMoreRows = m_managedReadContext->GetNextRow(output);
            }

            if (!m_hasMoreRows)
            {
                return false;
            }

            stats.IncreaseRowCount(1);

            return true;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_managedReadContext->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("ScopeReadContext");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_managedReadContext->GetOperatorWaitOnIOTime());
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_managedReadContext->GetOperatorWaitOnIOTime());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_managedReadContext->WriteRuntimeStats(node);
        }
    };

    template <typename InputSchema, bool scopeCEP = false>
    class ScopeOutputer : public Operator<ScopeOutputer<InputSchema, scopeCEP>, InputSchema, -1>
    {
        std::shared_ptr<ScopeOutputerManaged<InputSchema>> m_managedOutputer;

        OperatorDelegate<InputSchema> * m_child;     // Array of child operator
        std::string     m_outputName;
        SIZE_T          m_bufSize;
        int             m_bufCount;

    public:
        ScopeOutputer(OperatorDelegate<InputSchema> * child, const string& outputName, SIZE_T bufSize, int bufCnt, ScopeOutputerManaged<InputSchema> * managedOutputer, int operatorId) :
            Operator(operatorId),
            m_child(child),
            m_outputName(outputName),
            m_bufSize(bufSize),
            m_bufCount(bufCnt)
        {
            m_managedOutputer.reset(managedOutputer);
            m_managedOutputer->CreateStream(m_outputName, m_bufSize, m_bufCount);
        }

        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
            m_managedOutputer->Init();
        }

        PartitionMetadata * GetMetadataImpl()
        {
            return nullptr;
        }

        bool GetNextRowImpl(InputSchema & output)
        {
            AutoExecStats stats(this);

            m_managedOutputer->Output();

            return false;
        }

        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
            m_managedOutputer->Close();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("ScopeOutput");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond() - m_managedOutputer->GetOperatorWaitOnIOTime());
            node.AddAttribute(RuntimeStats::OperatorWaitOnIOTime(), m_managedOutputer->GetOperatorWaitOnIOTime());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            // Writes RowCount and I/O stats
            m_managedOutputer->WriteRuntimeStats(node);
        }

        void DoScopeCEPCheckpointImpl(BinaryOutputStream & output)
        {
            m_managedOutputer->DoScopeCEPCheckpoint(output);
        }

        void LoadScopeCEPCheckpointImpl(BinaryInputStream & input)
        {
            SCOPE_LOG_INFO("CheckpointManager", "ScopeOutputer Load checkpoint");
            m_managedOutputer->LoadScopeCEPCheckpoint(input);
        }

        OperatorRequirements GetOperatorRequirementsImpl()
        {
            return m_managedOutputer->GetOperatorRequirements((SIZE_T)m_bufCount);
        }
    };

    template<typename InputOperator, typename InputSchema, typename OutputSchema, typename SamplerPolicy, typename SchemaToucherPolicy, int UID = -1>
    class SamplerOperator : public Operator<SamplerOperator<InputOperator, InputSchema, OutputSchema, SamplerPolicy, SchemaToucherPolicy, UID>, OutputSchema, UID>
    {
        InputOperator * m_child; // child operator
        SamplerPolicy &m_samplerPolicy;
        IncrementalAllocator m_alloc;

    public:
        SamplerOperator(InputOperator *input, SamplerPolicy &sp) :
            Operator(UID),
            m_child(input),
            m_samplerPolicy(sp)
        {
        }

        // Initialize child
        void InitImpl()
        {
            AutoExecStats stats(this);

            m_child->Init();
            m_samplerPolicy.Init(m_alloc);
        }

        PartitionMetadata * GetMetadataImpl()
        {
            AutoExecStats stats(this);

            return m_child->GetMetadata();
        }

        bool GetNextRowImpl(OutputSchema &output)
        {
            AutoExecStats stats(this);

            InputSchema input;

            while (m_child->GetNextRow(input))
            {
                unsigned __int64 hash_value = SchemaToucherPolicy::GetHash(input);
                double prob = m_samplerPolicy.PickRow(hash_value);
                if (prob > 0.0)
                {
                    SchemaToucherPolicy::OutputRow(input, prob, output);
                    stats.IncreaseRowCount(1);

                    return true;
                }
            }

            // TODO: add other stats
            return false;
        }

        // Release all resources of children
        void CloseImpl()
        {
            AutoExecStats stats(this);

            m_child->Close();
            m_alloc.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
        }

        void WriteRuntimeStatsImpl(TreeNode & root)
        {
            auto & node = root.AddElement("SamplerOperator");

            node.AddAttribute(RuntimeStats::InclusiveTime(), GetInclusiveTimeMillisecond());
            node.AddAttribute(RuntimeStats::ExclusiveTime(), GetInclusiveTimeMillisecond() - m_child->GetInclusiveTimeMillisecond());
            RuntimeStats::WriteRowCount(node, GetRowCount());
            if (HasOperatorId())
            {
                node.AddAttribute(RuntimeStats::OperatorId(), GetOperatorId());
            }

            m_alloc.WriteRuntimeStats(node, sizeof(OutputSchema));
            m_child->WriteRuntimeStats(node);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            // ideally, the memory requirement is about 1KB extra for stats and other stack contents
            return OperatorRequirements().Add(m_samplerPolicy.GetOperatorRequirementsImpl());
        }

        // DEFAULT_IMPLEMENT_SCOPECEP_CHECKPOINT
        // DEFAULT_IMPLEMENT_SCOPECEP_ADJUSTCTI
    };
#pragma endregion ManagedOperators
}
