#pragma once

// Windows api include and debug assert
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <psapi.h>

#include <stdlib.h>
#include <memory>
#include <queue>
#include <map>
#include <intrin.h>
#include <random>
#include "OperatorRequirementsDef.h"

namespace ScopeEngine
{
    //
    // Class to store and track execution stats.
    // This is for per operator tracking as well as tracking I/O timing.
    //
    class SCOPE_ENGINE_API ExecutionStats
    {
#ifdef USE_PERF_COUNTERS
        // Return the frequency of the timestamp
        static LONGLONG GetFrequencyTS()
        {
            LARGE_INTEGER tps;
            QueryPerformanceFrequency(&tps);

            return tps.QuadPart;
        }

        static LONGLONG GetTimestamp()
        {
            LARGE_INTEGER ts;
            QueryPerformanceCounter(&ts);

            return ts.QuadPart;
        }
#else
        // Return the frequency of the timestamp (in counts/second)
        static LONGLONG GetFrequencyTS()
        {
            return 1000;
        }

        static LONGLONG GetTimestamp()
        {
            return GetTickCount64();
        }
#endif

    public:
        ExecutionStats():m_inclusiveTime(0), m_beginTime(0), m_rowCount(0), m_callCount(0)
        {
            m_frequency = GetFrequencyTS();
        }

#if defined(SCOPE_NOSTATS)
        void BeginInclusive()
        {
        }

        void EndInclusive()
        {
        }

        void IncreaseRowCount(ULONGLONG)
        {
        }
#else
        // begin inclusive timer
        void BeginInclusive()
        {
            m_beginTime = GetTimestamp();
        }

        // end inclusive timer
        void EndInclusive()
        {
            m_inclusiveTime += GetTimestamp() - m_beginTime;
            ++m_callCount;
        }

        // increase current operator's produced row count
        void IncreaseRowCount(ULONGLONG count)
        {
            m_rowCount += count;
        }
#endif

        LONGLONG GetRawTicks()
        {
            return m_inclusiveTime;
        }

        // return inclusive time in ms
        LONGLONG GetInclusiveTimeMillisecond()
        {
            // Need to adjust with tick overhead
            // ULONGLONG adjustedTicks = m_inclusiveTime - m_callCount * FTGetTickOverhead();
            return m_inclusiveTime * 1000 / m_frequency;
        }

        ULONGLONG GetRowCount()
        {
            return m_rowCount;
        }

    private:
        LONGLONG m_inclusiveTime;
        LONGLONG m_beginTime;
        ULONGLONG m_rowCount;
        ULONGLONG m_callCount;
        LONGLONG m_frequency;
    };

    //
    // Wrapper to track cpu ticks automatically in a scope.
    //
    class SCOPE_RUNTIME_API AutoExecStats
    {
    public:
        AutoExecStats(ExecutionStats * p)
        {
            m_pStats = p;
            p->BeginInclusive();
        }

        ~AutoExecStats()
        {
            m_pStats->EndInclusive();
        }

        void IncreaseRowCount(ULONGLONG count)
        {
            m_pStats->IncreaseRowCount(count);
        }
    private:
        ExecutionStats * m_pStats;   // stats object
    };

    //
    // RAII for the CRITICAL_SECTION
    //
    class AutoCriticalSection
    {
    public:
        AutoCriticalSection(CRITICAL_SECTION* pCS, bool enterCriticalSection = true) : m_pCS(pCS)
        {
            if (enterCriticalSection)
            {
                EnterCriticalSection(m_pCS);
            }
        }

        ~AutoCriticalSection()
        {
            LeaveCriticalSection(m_pCS);
        }

    private:
        CRITICAL_SECTION* m_pCS;
    };

    //
    // Specialization for the case when AutoRowArray stores objects
    //
    template<class Schema>
    class AutoRowArrayHelper
    {
    public:
        static void DestroyRow(Schema & p)
        {
            // http://msdn.microsoft.com/en-us/library/26kb9fy0.aspx
            // C4100 can also be issued when code calls a destructor on a otherwise unreferenced parameter of primitive type. This is a limitation of the Visual C++ compiler. 
#pragma warning(suppress: 4100)
            (&p)->~Schema();
        }

        static void ConstructRow(Schema & buf, Schema * row, IncrementalAllocator & alloc)
        {
            // call in place new operator to make a deep copy of the row object
            new ((char*)&buf) Schema(*row, &alloc);
        }
    };

    //
    // Specialization for the case when AutoRowArray stores pointers to objects
    //
    template<class Schema>
    class AutoRowArrayHelper<Schema*>
    {
    public:
        static void DestroyRow(Schema * & p)
        {
            // http://msdn.microsoft.com/en-us/library/26kb9fy0.aspx
            // C4100 can also be issued when code calls a destructor on a otherwise unreferenced parameter of primitive type. This is a limitation of the Visual C++ compiler. 
#pragma warning(suppress: 4100)
            p->~Schema();
        }

        static void ConstructRow(Schema * & buf, Schema ** row, IncrementalAllocator & alloc)
        {
            // if Schema is pointer, we need to allocate memory for schema object and do deep copy.
            buf = (Schema *)alloc.Allocate(sizeof(Schema));
            new ((char*)buf) Schema(**row, &alloc);
        }
    };

    //
    // A wrapper class to implement auto grow array for row object.
    // If Schema is a pointer type we deep copy the object that it points to.
    // It relies on exclusive usage of the IncrementalAllocator and the allocator allocating memory continuously.
    //
    template<typename Schema>
    class AutoRowArray
    {
        IncrementalAllocator      m_allocator;// allocator
        IncrementalAllocator      m_blobAllocator;// allocator for blob
        Schema     *   m_buffer;   // buffer start point
        SIZE_T         m_index;    // next write position
        SIZE_T         m_memUsed;  // memory used by the m_buffer.
        SIZE_T         m_size;     // total size of memory that committed
        SIZE_T         m_limit;    // upbound of rows can be stored

        static const SIZE_T x_maxNumOfRow = 1000000;
        static const SIZE_T x_maxMemorySize = 2147483648;  // 2G per buckets
        static const SIZE_T x_allocationUnit = 1 << 16; // 64k

        // call destructor of each Row and reset memory
        void DestroyRows()
        {
            //call destructor of each row.
            for (SIZE_T i=0; i<m_index;i++)
            {
                // AutoRowArray will allocate the whole buffer as char * to improve new performance.
                // Each element in the array will be initialized using in place new operator.
                // When we destroy the array we need to make sure Schema class destructor is called.
                // Otherwise, resource owned by Schema object may not be released.
                AutoRowArrayHelper<Schema>::DestroyRow(m_buffer[i]);
            }

            m_index = 0;
        }

        void Init(const string& ownerName, SIZE_T maxRows, SIZE_T maxMemory)
        {
            m_limit = maxRows;
            m_index = 0;

            // add some extra buffer for the row array because the allocation may not aligned with allocation Unit size
            m_allocator.Init(RoundUp_Size(sizeof(Schema)*maxRows+x_allocationUnit*2), ownerName + "_AutoRowArray");

            SIZE_T blobSize = max<__int64>(maxMemory - m_allocator.GetMaxSize(), 0);
            // if max memory limit is not enough for the schema object, we just allocate bloballocator same as row allocator.
            if (blobSize == 0)
            {
                blobSize = m_allocator.GetMaxSize();
            }
            m_blobAllocator.Init(blobSize, ownerName + "_AutoRowArray_Blob");

            m_buffer = (Schema *)(m_allocator.Buffer());
            m_size = 0;
            m_memUsed = 0;
        }

    public:
        enum MemorySizeCategory
        {
            SmallMem = 100,  // 1%  of the limit
            MediumMem = 5,   // 20% of the limit
            LargeMem = 1     // 100% of the limit
        };

        enum CountSizeCategory
        {
            ExtraSmall = 10000,  // 0.01%  of the limit
            Small = 1000,  // 0.1%  of the limit
            Medium = 20,   // 5% of the limit
            Large = 1     // 100% of the limit
        };

        AutoRowArray(const char* ownerName, SIZE_T maxRows, SIZE_T maxMemory)
        {
            Init(string(ownerName), maxRows, maxMemory);
        }

        AutoRowArray(const char* ownerName, CountSizeCategory sizeCat = Large, MemorySizeCategory memCat = LargeMem)
        {
            Init(string(ownerName), x_maxNumOfRow / sizeCat, x_maxMemorySize / memCat);
        }

        ~AutoRowArray()
        {
            DestroyRows();
        }

        //Reset the AutoRowArray
        template <class ReclaimPolicy>
        FORCE_INLINE void Reset()
        {
            DestroyRows();

            m_allocator.Reset<ReclaimPolicy>();
            m_blobAllocator.Reset<ReclaimPolicy>();

            m_buffer = (Schema *)(m_allocator.Buffer());
            m_size = 0;
            m_memUsed = 0;
        }

        // Reset with default reclaim policy
        FORCE_INLINE void Reset()
        {
            Reset<IncrementalAllocator::AmortizeMemoryAllocationPolicy>();
        }

        // Add a row into the cache. If we run out of memory, return false.
        bool AddRow(Schema & row)
        {
            // if there is not enough space to hold a row
            if (m_memUsed + sizeof(Schema) > m_size)
            {
                // round up allocation to x_allocationUnit block size.
                SIZE_T commitSize = ((m_memUsed + sizeof(Schema) - m_size + x_allocationUnit - 1) >> 16) * x_allocationUnit ;

                // we will not run out of memory here since the limit is precalculated according to schema object size + 2*x_allocationUnit.
                char * tmp = m_allocator.Allocate(commitSize);

                // assert the allocation is continuous
                SCOPE_ASSERT(tmp == (char *)m_buffer+m_size);

                m_size += commitSize;
            }

            bool returnValue = true;

            try
            {
                AutoRowArrayHelper<Schema>::ConstructRow(m_buffer[m_index], &row, m_blobAllocator);
                m_blobAllocator.EndOfRowUpdateStats();

                // bump up index only if we sucessfully copy the row.
                m_index++;
                m_memUsed += sizeof(Schema);
            }
            catch (RuntimeMemoryException & )
            {
                // in case we hit runtime OOM exception
                returnValue = false;
            }

            return returnValue;
        }

        // Amount of occupied memory
        SIZE_T MemorySize() const
        {
            return m_allocator.GetSize() + m_blobAllocator.GetSize();
        }

        // Current size
        SIZE_T Size() const
        {
            return m_index;
        }

        // Current limit
        SIZE_T Limit() const
        {
            return m_limit;
        }

        // array accessor to get to the buffer element
        Schema & operator[] (SIZE_T i) const
        {
            return m_buffer[i];
        }

        Schema * Begin() const
        {
            return m_buffer;
        }

        Schema * End() const
        {
            return &(m_buffer[m_index]);
        }

        // whether autorowarray reach it's capacity
        // TODO, add throttling based on overall memory consumption.
        bool FFull() const
        {
            return m_index >= m_limit;
        }

        void WriteRuntimeStats(TreeNode & root) const
        {
            auto & node = root.AddElement("AutoRowArray");
            m_allocator.WriteRuntimeStats(node);
            m_blobAllocator.WriteRuntimeStats(node, sizeof(Schema));
        }
    };

    //
    // An auto grow array for fixed-count circular queue of rows.
    // The array starts out very lightweight and only commits memory needed.
    // It stores rows inside its buffer, contiguously, up to the limit.
    // It does not own its rows, so it is not responsible for deletion.
    // It relies on the InOrderAllocator for blob storage.
    //
    template<typename Schema>
    class RowRingBufferInternal
    {
    private:
        template<typename Schema> friend class RowRingBuffer;

        IncrementalAllocator      m_allocator;// allocator
        InOrderAllocator      m_blobAllocator;// allocator for blob
        Schema     *  m_buffer;   // buffer start point
        SIZE_T         m_index;    // next write position
        SIZE_T         m_memUsed;  // memory used by the m_buffer.
        SIZE_T         m_size;     // total size of memory that committed
        SIZE_T         m_limit;    // upbound of rows can be stored

        static const SIZE_T x_allocationUnit = 1 << 16; // 64k

        // RowRingBufferInternal will allocate the whole buffer as char * to improve new performance.
        // Each element in the array will be initialized using in place new operator.
        void DestroyRow(SIZE_T i)
        {
            // http://msdn.microsoft.com/en-us/library/26kb9fy0.aspx
            // C4100 can also be issued when code calls a destructor on a otherwise unreferenced parameter of primitive type. This is a limitation of the Visual C++ compiler. 
#pragma warning(suppress: 4100)
            (&m_buffer[i])->~Schema();
        }

        // Single row in place new
        void ConstructRow(SIZE_T i, Schema& row)
        {
            if (0 == i)
            {
                // When maxRows queue insertions have occurred, it is safe to dispose of the previous maxRows rows.
                m_blobAllocator.AdvanceEpoch();
            }

            // Get the right IncrementalAllocator from m_blobAllocator and use it.
            new ((char*)&m_buffer[i]) Schema(row, &m_blobAllocator.CurrentAllocator());
            m_blobAllocator.CurrentAllocator().EndOfRowUpdateStats();
        }

    public:
        RowRingBufferInternal(SIZE_T maxRows, SIZE_T maxMemory, bool isFixedSize, const string& ownerName)
        {
            SCOPE_ASSERT(maxRows > 0);
            m_limit = maxRows;
            m_index = 0;

            // add some extra buffer for the row array because the allocation may not aligned with allocation Unit size
            SIZE_T rowsSize = RoundUp_Size(sizeof(Schema)*maxRows+x_allocationUnit*2);
            SIZE_T blobsSize;

            if (isFixedSize)
            {
                blobsSize = x_allocationUnit*2;
            }
            else
            {
                blobsSize = rowsSize * 2 > maxMemory ? maxMemory /* absurd amount -- cause error (below) */ : maxMemory - rowsSize;
            }

            if (rowsSize + blobsSize > maxMemory)
            {
                throw RuntimeException(E_USER_ROWSET_TOO_BIG, "The set of rows exceeds the window aggregate capacity limit.");
            }

            m_allocator.Init(rowsSize, ownerName);
            m_blobAllocator.Init(blobsSize, ownerName);
            m_buffer = (Schema *)(m_allocator.Allocate(x_allocationUnit));
            m_size = x_allocationUnit;
            m_memUsed = 0;
        }

        void AddRow(Schema & row)
        {
            SCOPE_ASSERT(!FFull());

            // if there is not enough space to hold a row
            if (m_memUsed + sizeof(Schema) > m_size)
            {
                // round up allocation to x_allocationUnit block size.
                SIZE_T commitSize = ((m_memUsed + sizeof(Schema) - m_size + x_allocationUnit - 1) >> 16) * x_allocationUnit ;

                // we will not run out of memory here since the limit is precalculated according to schema object size + 2*x_allocationUnit.
                char * tmp = m_allocator.Allocate(commitSize);

                // assert the allocation is continuous
                SCOPE_ASSERT(tmp == (char *)m_buffer+m_size);

                m_size += commitSize;
            }

            ConstructRow(m_index, row);
            m_index++;
            m_memUsed += sizeof(Schema);
        }

        SIZE_T Size() const
        {
            return m_index;
        }

        SIZE_T Limit() const
        {
            return m_limit;
        }

        Schema & operator[] (SIZE_T i) const
        {
            return m_buffer[i];
        }

        bool FFull() const
        {
            return m_index >= m_limit;
        }

        void WriteRuntimeStats(TreeNode & root) const
        {
            m_allocator.WriteRuntimeStats(root);
            m_blobAllocator.WriteRuntimeStats(root);
        }
    };

    //
    // Ring buffer of rows.  Fixed size, with add and remove operations.
    // Uses an array (RowRingBufferInternal) and controls row lifetime in that array.
    //
    // Not thread-safe.
    //
    template<typename Schema>
    class RowRingBuffer
    {
    private:
        // Dynamic array storage with in-order allocation
        RowRingBufferInternal<Schema> internal;

        // position of front of queue
        // E.g. after first 5 adds to a size 20 ring buffer, the front is at position 0 and the back is at position 5.
        SIZE_T m_front;

        // count of queue
        // Need to track this because it can shrink while RowRingBufferInternal's count does not.
        SIZE_T m_count;

    private:
        SIZE_T Limit() const { return internal.Limit(); }

        SIZE_T Back() const { return RelativePosition(Count()); }

        SIZE_T RelativePosition(SIZE_T i) const
        {
            SCOPE_ASSERT(i <= Count());
            return (i + m_front) % Limit();
        }

    public:
        RowRingBuffer(SIZE_T maxRows, SIZE_T maxBytes, bool isFixedSize, const string& ownerName) : internal
            (
            maxRows,
            maxBytes,
            isFixedSize,
            ownerName + "_RowRingBuffer"
            ), m_front(0), m_count(0)
        {
            SCOPE_ASSERT(maxRows > 0);
        }

        ~RowRingBuffer()
        {
            Reset();
        }

        SIZE_T Count() const { return m_count; }
        SIZE_T FFull() const { return Limit() <= Count(); }
        SIZE_T FValid() const { return Limit() >= Count() && internal.Size() >= Count(); }
        SIZE_T FEmpty() const { return 0 == Count(); }

        // Put a row into an empty slot in the ring buffer.
        void AddRow(Schema & row)
        {
            if (FFull())
            {
                throw RuntimeException(E_SYSTEM_ERROR, "Attempting to add to full ring buffer.");
            }

            if (!internal.FFull())
            {
                // Loading array initially.  The array becomes full
                // after maxRows inserts, regardless of deletes.
                internal.AddRow(row);
            }
            else
            {
                internal.ConstructRow(Back(), row);
            }

            ++m_count;
            SCOPE_ASSERT(FValid());
        }

        void RemoveRow()
        {
            internal.DestroyRow(m_front);
            m_front = RelativePosition(1);
            --m_count;
            SCOPE_ASSERT(m_front < Limit());
        }

        // Access a row, indexing from m_front.
        Schema & operator[](SIZE_T i) const
        {
            SCOPE_ASSERT(i < Count());
            return internal[RelativePosition(i)];
        }

        void Reset()
        {
            while (Count())
            {
                RemoveRow();
            }
        }

        void WriteRuntimeStats(TreeNode & root) const
        {
            auto & node = root.AddElement("RowRingBuffer");
            internal.WriteRuntimeStats(node);
        }
    };

    //
    // Concurrent queue for the ParallelUnionAll
    //
    template<typename Schema>
    class ConcurrentBatchQueue
    {
    private:
        CONDITION_VARIABLE    m_bufFull;   // condition variable used for producer to wait on full buffer
        CONDITION_VARIABLE    m_bufEmpty;   // condition variable used for consumer to wait on empty buffer
        CRITICAL_SECTION    m_lock;      // lock to syncronize queue access
        CRITICAL_SECTION    m_freequeuelock;      // lock to syncronize freequeue access

        // we use small autorowarray as batch to manage memory
        typedef AutoRowArray<Schema> BatchType;

        // queue of produced batches
        queue<unique_ptr<BatchType>>         m_queue;

        // queue of free batches
        queue<unique_ptr<BatchType>>         m_freequeue;

        // number of producer for the queue.
        LONG                                 m_producerCnt;

        bool                                 m_cancelProducer;

        // we can hold at most 12 batch in the queue.
        static const ULONG x_queueLimit = 12;

    public:
        ConcurrentBatchQueue(int producerCnt) : m_producerCnt(producerCnt), m_cancelProducer(false)
        {
            InitializeConditionVariable (&m_bufFull);
            InitializeConditionVariable (&m_bufEmpty);
            InitializeCriticalSection (&m_lock);
            InitializeCriticalSection (&m_freequeuelock);
        }

        ~ConcurrentBatchQueue()
        {
            DeleteCriticalSection (&m_lock);
            DeleteCriticalSection (&m_freequeuelock);
        }

        void PushBatch(unique_ptr<BatchType> & batch)
        {
            AutoCriticalSection lock(&m_lock);

            // Queue is full, we need to wait for consumer to be done with some work first
            while (m_queue.size() >= x_queueLimit && !m_cancelProducer)
            {
                SleepConditionVariableCS(&m_bufFull, &m_lock, INFINITE);
            }

            m_queue.push(move(batch));

            // wake up consumer
            WakeConditionVariable(&m_bufEmpty);
        }

        bool Empty()
        {
            AutoCriticalSection lock(&m_lock);

            return m_queue.empty();
        }

        // Finish a producer.
        void Finish()
        {
            // decrease the producer count
            if (InterlockedDecrement(&m_producerCnt) == 0)
            {
                unique_ptr<BatchType> emptyBatch(new BatchType("ConcurrentBatchQueue_Empty", BatchType::Small, BatchType::MediumMem));

                // push an empty batch to indicate end of work
                PushBatch(emptyBatch);
            }
        }

        // Read one batch from queue.
        void Pop(unique_ptr<BatchType> & out)
        {
            AutoCriticalSection lock(&m_lock);

            // Queue is empty wait infinitely for new batch
            while (m_queue.empty())
            {
                SleepConditionVariableCS(&m_bufEmpty, &m_lock, INFINITE);
            }

            out = move(m_queue.front());

            m_queue.pop();

            // wake up producer
            WakeConditionVariable(&m_bufFull);
        }

        // Get a free batch from freequeue.
        bool GetFreeBatch(unique_ptr<BatchType> & out)
        {
            AutoCriticalSection lock(&m_freequeuelock);

            // Queue is not empty return a free batch
            if (!m_freequeue.empty())
            {
                out = move(m_freequeue.front());
                m_freequeue.pop();
                return true;
            }

            return false;
        }

        // Get a free batch from freequeue.
        void PutFreeBatch(unique_ptr<BatchType> & in)
        {
            AutoCriticalSection lock(&m_freequeuelock);

            // Push a batch to free queue
            m_freequeue.push(move(in));
        }

        // WakeUp producer in case the queue is full
        void CancelProducer()
        {
            AutoCriticalSection lock(&m_lock);

            m_cancelProducer = true;

            // wake up producer
            WakeConditionVariable(&m_bufFull);
        }
    };

    //
    // Class to cache data.
    // the key is std::string
    // this class is not responsible for cache entry memory release
    // therefore, the caller should handle memory clean up if entry type is a pointer.
    template<typename EntryType>
    class ConcurrentCache
    {
        typedef std::map<std::string, typename EntryType> CacheType;
        CacheType m_entryPool;
        CRITICAL_SECTION m_lock;      // lock to syncronize map access
        
    public:
        ConcurrentCache()
        {
            InitializeCriticalSection (&m_lock);
        }

        ~ConcurrentCache()
        {
            DeleteCriticalSection (&m_lock);
        }

        
        bool AddItem(std::string key, EntryType metadata)
        {
            AutoCriticalSection lock(&m_lock);
            return m_entryPool.insert(std::make_pair(key, metadata)).second;
        }
        
        bool RemoveItem(std::string key)
        {
            AutoCriticalSection lock(&m_lock);
            return m_entryPool.erase(key) != 0;
        }

        bool GetItem(std::string key, EntryType& metadata)
        {
            AutoCriticalSection lock(&m_lock);
            CacheType::iterator itr = m_entryPool.find(key);
            if (itr != m_entryPool.end())
            {
                metadata = itr->second;
                return true;
            }
            return false;
        }

        void Clear()
        {
            AutoCriticalSection lock(&m_lock);
            m_entryPool.clear();
        }
        
    };
    
    struct ScannerDeleter
    {
        void operator() (Scanner* scanner)
        {
            Scanner::DeleteScanner(scanner);
        }
    };

    inline double ScopeFmod(double a, double b)
    {
        return fmod(a, b);
    };

    inline double ScopeSqrt(double arg)
    {
        return sqrt(arg);
    };

    inline double ScopeCeiling(double arg)
    {
        return ceil(arg);
    };

    inline double ScopeFloor(double arg)
    {
        return floor(arg);
    };

    inline double ScopeLog(double arg)
    {
        return log(arg);
    };

    inline double ScopeLog10(double arg)
    {
        return log10(arg);
    };

    inline double ScopePower(double a, double b)
    {
        return pow(a, b);
    };

    inline double ScopePI()
    {
        return 3.14159265358979323846;
    };

    inline double ScopeE()
    {
        return 2.71828182845904523536;
    };

    inline float ScopeFmod(float a, float b)
    {
        return fmodf(a, b);
    };

    // C++ methods for floating point modulo operations on nullable types. C# sematic is to return null if one of the
    // operatnds is null.
    inline NativeNullable<double> ScopeFmod(NativeNullable<double> a, NativeNullable<double> b)
    {
        return a.IsNull() ? a : (b.IsNull() ? b : scope_cast<NativeNullable<double>>(fmod(a.get(), b.get())));
    };

    inline NativeNullable<double> ScopeFmod(double a, NativeNullable<double> b)
    {
        return b.IsNull() ? b : scope_cast<NativeNullable<double>>(fmod(a, b.get()));
    };

    inline NativeNullable<double> ScopeFmod(NativeNullable<double> a, double b)
    {
        return a.IsNull() ? a : scope_cast<NativeNullable<double>>(fmod(a.get(), b));
    };

    inline NativeNullable<float> ScopeFmod(NativeNullable<float> a, NativeNullable<float> b)
    {
        return a.IsNull() ? a : (b.IsNull() ? b : scope_cast<NativeNullable<float>>(fmodf(a.get(), b.get())));
    };

    inline NativeNullable<float> ScopeFmod(float a, NativeNullable<float> b)
    {
        return b.IsNull() ? b : scope_cast<NativeNullable<float>>(fmodf(a, b.get()));
    };

    inline NativeNullable<float> ScopeFmod(NativeNullable<float> a, float b)
    {
        return a.IsNull() ? a : scope_cast<NativeNullable<float>>(fmodf(a.get(), b));
    };

    // trim from both ends
    inline std::string &trim(std::string &s)
    {
        if (s.empty())
        {
            return s;
        }
        
        auto firstItr = s.find_first_not_of(' ');
        if (firstItr == std::string::npos)
        {
            // the string only contains space
            s = "";
            return s;
        }
        
        auto lastItr = s.find_last_not_of(' ');
        if (firstItr != 0 || lastItr != s.length() - 1)
        {
            s = s.substr(firstItr, lastItr - firstItr + 1);
        }
        return s;
    }

    inline int GetPartitionIndex(std::string * argv, int argc, int partitionDimension)
    {
        SCOPE_ASSERT(partitionDimension >= 0);
        int partitionIndex = -1;
        vector<int> vertexIndices;
        // get partition index
        for (int i = 0; i < argc - 1; i++)
        {
            // it'll pass vertex index like -vertexIndex [1,2]
            // JM could pass empty array like -vertexIndex [] even it doesn't pass vertex index via this parameter
            if (argv[i].compare("-vertexIndex") == 0)
            {                
                string& str = argv[i+1];
                SCOPE_ASSERT(str.length() >= 2 && str[0] == '[' && str[str.length() - 1] == ']');
                char delimiter = ',';
                string::size_type pos = 1;
                string tmp;
                int idx = 1;
                for (idx = 1; idx < (str.length() - 1); idx++)
                {
                    if (str[idx] == delimiter)
                    {
                        tmp = str.substr(pos, idx - pos);
                        tmp = trim(tmp);
                        if (tmp.empty())
                        {
                            vertexIndices.push_back(-1);
                        }
                        else
                        {
                           int num = atoi(tmp.c_str());
                           SCOPE_ASSERT(num != 0 || tmp[0] == '0');
                           vertexIndices.push_back(num);
                        }
                        pos = idx + 1;
                    }
                }
                
                tmp = str.substr(pos, idx - pos);
                tmp = trim(tmp);
                if (tmp.empty())
                {
                    vertexIndices.push_back(-1);
                }
                else
                {
                   int num = atoi(tmp.c_str());
                   SCOPE_ASSERT(num != 0 || tmp[0] == '0');
                   vertexIndices.push_back(num);
                }
                i++;
            }
            else if (argv[i].compare("-partitionIndex") == 0)
            {
                partitionIndex = atoi(trim(argv[i+1]).c_str());
                SCOPE_ASSERT(partitionIndex != 0 || *(argv[i+1].c_str()) == '0');
                i++;
            }
        }

        for (int i = 0; i < vertexIndices.size(); i++)
        {
            if (vertexIndices[i] < 0)
            {
                vertexIndices[i] = partitionIndex;
            }
        }
        
        SCOPE_ASSERT(partitionDimension < vertexIndices.size());
        return vertexIndices[partitionDimension];
    };

    inline std::string GetChannelName(std::string inputFileName)
    {        
        // file name format is EntryName:ChannelName:input#
        // refer to AddInputChannel which constructs the file name
        string::size_type firstIdx = inputFileName.find_first_of(":");
        SCOPE_ASSERT(firstIdx != string::npos);
        string::size_type secondIdx = inputFileName.find_first_of(":", firstIdx + 1);
        SCOPE_ASSERT(firstIdx != string::npos);
        string channelName = inputFileName.substr(firstIdx + 1, secondIdx - firstIdx - 1);
        if (channelName.compare("__defaultInput") == 0 || channelName.compare("__defaultOutput") == 0)
        {
            return "";
        }

        return channelName;
    }

    class ScopeGuard
    {
    private:
        volatile long* m_pCount;

    public:
        ScopeGuard(volatile long* pCount) : m_pCount(pCount)
        {
            InterlockedIncrement(m_pCount);
        }

        ~ScopeGuard()
        {
            InterlockedDecrement(m_pCount);
        }
    };

    /// ScopeRBTree and related datastructures for sampling v1
    // Represents a heavy-hitter sketch payload, used by the distinct sampler
    // Used as a value type for the red-black tree below
    struct SamplerSketchItem
    {
        unsigned __int64 freq;
        unsigned int bucket;
    };

    //
    // This is Scope's internal implementation of a red-black tree over a memory pool that is allocated
    // from the given IncrementalAllocator
    // We built this because
    // a) we needed to support log(n) time for inserts, deletions and lookups
    // b) using std::map with the incrementalallocator would still not support deletions
    //
    // This is a bounded memory implementation. Inserts return false if allocated memory is used up.
    //
    class SCOPE_ENGINE_API ScopeRBTree
    {
    private:
        // Represents a node in the red-black tree below
        struct ScopeRBNode
        {
            // key
            unsigned __int64 key;
            // payload
            SamplerSketchItem payload;

            // 
            int color; // putting this here to help with memory fragmentation
            //
            // rb tree pointers
            // Indeed these are naked pointers because they all point to locations in the explicitly allocated pool
            union {
                ScopeRBNode *parent; // parent
                ScopeRBNode *next_free; // pointer to next element in free list
            };
            ScopeRBNode *left;
            ScopeRBNode *right;
        };

        void LeftRotate(ScopeRBNode* x);

        void RightRotate(ScopeRBNode* y);

        void RBInsertFixUp(ScopeRBNode* x);

        ScopeRBNode* FindNode(unsigned __int64 key) const;

        // can only be called from within RBErase to fix black heights
        void RBDeleteFixUP(ScopeRBNode* x);

        // behavior is undefined if key >= MaxKey() or rbtree is empty
        // else returns the next largest entry after key
        unsigned __int64 SuccessorKey(unsigned __int64 key) const;

        // cleans nil pointer
        void CleanNil();

        // pull off the free list
        ScopeRBNode* AllocNode();

        // add back to free list
        void ReleaseNode(ScopeRBNode* node);

        // undefined behavior if Count == 0; will get nil.key
        unsigned __int64 MinKey() const;

        // undefined behavior if Count == 0
        unsigned __int64 MaxKey() const;

        // the successor for a given key
        // key need not exist in the tree
        // nil is returned if Count == 0 or key >= MaxKey()
        ScopeRBNode* Successor(unsigned __int64 key) const;

        // internal 
        ScopeRBNode* root;
        ScopeRBNode* nil;
        ScopeRBNode* freeList; // pointer to chain of free nodes
        unsigned int count; // number of entries

        int CheckRBProps(ScopeRBNode* x, string& error) const;
		void InitHelper(IncrementalAllocator &a, unsigned long long numEntries);


#ifdef SCOPE_DEBUG
        // print subtree rooted at curr
        void PrintInternal(ScopeRBNode* curr, int level, string tag) const;
#endif
    public:
		static_assert(
			3 * sizeof(ScopeRBNode) / 2 // 1.5 * sizeof(rbnode)... The size should be 48 but is 56 today due to poor packing. Since this may change with a fancier compiler or pragma flags, we allow some grace.
			>= ::OperatorRequirements::OperatorRequirementsConstants::ScopeRBNode__Size_Entry
			&& 
			::OperatorRequirements::OperatorRequirementsConstants::ScopeRBNode__Size_Entry
			>= sizeof(ScopeRBNode),
			"Constant for RBNode is too far from the actual size of RBNode");

        ScopeRBTree() {}
        void Init(IncrementalAllocator &a, unsigned long long numEntries);
        static size_t MemoryNeeded(unsigned long long numEntries);

        // Destruction is deferred to when the incremental allocator is reset.

        // red black tree implementation
        // returns false if out of memory
        bool Insert(unsigned __int64 key, unsigned __int64 f, unsigned int b);

        // returns false if key not found
        bool Erase(unsigned __int64 key);

        // Takes a function pointer that checks whether a given sketch item should
        // be erased
        template<typename Pred>
        void EraseConditionally(Pred pruner)
        {
			if (Count() == 0)
			{
				return;
			}

			unsigned __int64 k = MinKey();
			while (true)
			{

#ifdef SCOPE_DEBUG
				cout << "\t looping: key = " << k << " minKey= " << MinKey() << " maxkey= " << MaxKey() << endl;
#endif
				ScopeRBNode* n = FindNode(k);
				SCOPE_ASSERT(n != nil);
				if (pruner(&(n->payload)))
				{
#ifdef SCOPE_DEBUG
					cout << "\t\t erasing " << k << endl;
#endif
					Erase(k);
				}

				ScopeRBNode* succ = Successor(k);
				if (succ == nil)
					break;
				k = succ->key;
			}
        }

        // this is the common case operation on this RBTree
        // returns nullptr if key is not found, else returns the payload
        // the 'client' can change the payload 
        SamplerSketchItem* Find(unsigned __int64 key) const;

        // the number of entries
        unsigned int Count() const;

		string CheckRedBlackProperties() const;
#ifdef SCOPE_DEBUG
		void Print() const;
#endif
    };

    class UniformSampler
    {
    private:
        double m_sampleProb;
        unsigned long m_seed;
        std::mt19937 m_gen;
        std::uniform_real_distribution<double> m_unif{ 0.0, 1.0 };

    public:
        UniformSampler(double sampleProb, unsigned long seed) : m_sampleProb(sampleProb), m_seed(seed)
        {
            SCOPE_ASSERT(sampleProb >= 0 && sampleProb <= 1.0);
            
            m_gen.seed(m_seed);			
        }

        double PickRow(unsigned __int64 /*hash_value*/)
        {
            bool picked = m_unif(m_gen) <= m_sampleProb;
            return picked ? m_sampleProb : 0;
        }

		void Init(IncrementalAllocator& /**/){}
		OperatorRequirements GetOperatorRequirementsImpl()
		{ 
			return OperatorRequirements(::OperatorRequirements::OperatorRequirementsConstants::SamplerOperator_UniformSampler__Size_MinMemory); 
		}
    };

    // Logic: Bias probability of picking a row to the 'value' passed
    // Value should be one numerical column
	class ValueBiasedSampler
    {
    private:
        unsigned long m_seed;
        double m_sampleProb;
        unsigned __int64 m_ewma_value{ 0 };
        static const unsigned int c_smoothing_factor=4;
        std::mt19937 m_gen;
        std::uniform_real_distribution<double> m_unif{ 0.0, 1.0 };

    public:
        ValueBiasedSampler(double sampleProb, unsigned long seed) : m_sampleProb(sampleProb), m_seed(seed)
        {
            SCOPE_ASSERT(sampleProb >= 0 && sampleProb <= 1.0);

            m_gen.seed(m_seed);			
        }

        double PickRow(unsigned __int64 value) // todo: make this work with double values
        {
            m_ewma_value -= (m_ewma_value / c_smoothing_factor);
            m_ewma_value += (value / c_smoothing_factor);

            double probToPick = (m_sampleProb * value) / m_ewma_value;

            bool picked = probToPick >= 1.0 || m_unif(m_gen) <= probToPick;
            return picked ? probToPick : 0;
        }

		void Init(IncrementalAllocator& /**/){}
		OperatorRequirements GetOperatorRequirementsImpl()
		{
			return OperatorRequirements(::OperatorRequirements::OperatorRequirementsConstants::SamplerOperator_ValueBiasedSampler__Size_MinMemory);
		}
    };

    // Logic: we want to pick a proportional portion of the value-space at "random"
    // Here, we do so by dividing the hash value with a 'divisor' and examining the bytes.  For example h%3 == 0 is a 1-in-3 sample.
    // Hence in general we would like to set the divisor to 1/p.
    // Three problems: 
    // (1) if p = 0.35, then 1/p is between 2 and 3; we either get a 0.33 or a 0.5 sample... Hence we set divisor to 4/p; 4/p here is between 11 and 12; 
    //     and we give four chances (i.e., h1%div == 0 || h2%div == 0 || h3%div == 0 || h4%div ==0)... Precisely, this lets us mimic p=0.3636.
    // (2) if p is very large then even 4/p is not granular enough; for e.g., p=0.85 => 4/p is between 4 and 5; so we either get an 80% sample or a 100% sample
    //     Fix: capture the first 50% differently and use the divisor only for the remaining...
    //     That is, do something else on the hash value that captures the first 50% and for the remaining 0.35 use trick (1) above.
    // (3) We pick random bits in the hash value by mixing with m_r1 and m_r2
    //
	class UniverseSampler
    {
    private:
        unsigned long m_seed;
        double m_sampleProb;
        unsigned __int64 m_divisor;
        static const unsigned __int64 c_r1 = 0xcc9e2d51;
        static const unsigned __int64 c_r2 = 0x1b873593;
		bool m_useDivisor;

    public:
		UniverseSampler(double prob, unsigned long seed) : m_sampleProb(prob), m_seed(seed), m_useDivisor(true)
        {
            SCOPE_ASSERT(m_sampleProb >= 0 && m_sampleProb <= 1.0);

            if (prob > 0.5)
            {
                // prob -= 0.5;
				prob = 2 * prob - 1;
            }

			m_divisor = (unsigned __int64) floor(2.0 / prob);
			m_useDivisor = (m_divisor <= (1LL << 12)); // Universe Sampler: is residual prob large enough?
        }

        double PickRow(unsigned __int64 hash_value)
        {
            hash_value ^= m_seed;
            unsigned __int64 v1 = hash_value ^ c_r1;
            unsigned __int64 v2 = hash_value ^ c_r2;

            unsigned __int64 sv1 = (v1 >> 7) % m_divisor;
            unsigned __int64 sv2 = v2 % m_divisor;

            unsigned __int64 sv3 = (v1 >> 21) % (m_divisor-1);
            unsigned __int64 sv4 = (v2 >> 5) % (m_divisor-1);
			bool correction = sv3 == 0 && sv4 == 0;

            unsigned __int64 halfv1 = ((v2 >> 17) % 4LL);
            unsigned __int64 halfv2 = ((v1 >> 13) % 4LL);
			bool one_ninth_prob = (v1 % 3) == 0 && (v2 % 3) == 0;

			bool picked =
				m_sampleProb > 0 &&
				(
				m_sampleProb == 1.0 || /* simplest case */
				(m_sampleProb > 0.5 && (halfv1 == 0 || halfv2 == 0 || one_ninth_prob)) || /* the over-half case [sk: we have to give 1-of-two chance; each halfv is uniform over [0,7]] */
				(m_useDivisor && (sv1 == 0 || sv2 == 0 || correction)) /* the residual [sk: we have to give 4-in-divisor chances...]*/
				);

            return picked ? m_sampleProb : 0;
        }

		void Init(IncrementalAllocator& /**/){}
		OperatorRequirements GetOperatorRequirementsImpl()
		{
			return OperatorRequirements(::OperatorRequirements::OperatorRequirementsConstants::SamplerOperator_UniverseSampler__Size_MinMemory);
		}
    };

	class DistinctSampler
    {
    private:
        unsigned long m_seed;
        // configuration arguments: basic distinct sampler
        double m_sampleProb;
        double m_minFreq;
        // to correct for parallel execution
        double m_expectedParallelism;
        // to keep memory footprint small (a hash sketch)
        double m_error;
        unsigned int m_lossyBucketWidth;
        unsigned int m_runningCount{ 0 };
        unsigned int m_currentBucketID{ 1 };

        // hash sketch mem consumption:
        // guarantees that memory usage is no more than (1/m_error) * log (m_error * numRows)
        // numRows = 10^7, error = 10^-3, memuse = 4K * log(10) < 16K entries
        // numRows = 10^9, error = 10^-3, memuse = 6K * log(10) < 24K entries
        // numRows = 10^9, error = 10^-4, memuse = 50K * log(10) < 200K entries
        // numRows = 10^11, error = 10^-4, memuse = 70K * log(10) < 280K entries
        // each entry is 20B
        // we will set it at 200K entries @20B each = 4MB
        //
        // number of entries is defined in ScopeDistinctSampler_PoolEntries
        // and the entry size is defined in ScopeRBNode_EntrySize
        // both are in OperatorRequirements.h

        ScopeRBTree m_rbtree; // the actual sketch
        std::mt19937 m_gen;
        std::uniform_real_distribution<double> m_unif{ 0.0, 1.0 };

        // sketch maintenance that is called when runningcount crosses the window threshold
        // also: when running out of allocated space (more entries than in pool)
        int CleanPayloadBelowThreshold(int bucketThresh)
        {
#ifdef SCOPE_DEBUG
            cout << "CleanPayload begin @Thresh=" << bucketThresh << " entryCount=" << m_rbtree.Count() << endl;
            m_rbtree.Print();
#endif
			int before_count = m_rbtree.Count();

            m_rbtree.EraseConditionally(
                [bucketThresh](SamplerSketchItem *ptr) -> bool
                {
                    return ptr != nullptr && (ptr->freq + ptr->bucket <= bucketThresh); 
                } 
            );

#ifdef SCOPE_DEBUG
            cout << "CleanPayload ends with entryCount=" << m_rbtree.Count() << endl;
#endif
            return before_count - m_rbtree.Count();
        }

    public:
        DistinctSampler(double sampleProb, double minFreq, unsigned long seed, double expParallelism = 50, double freqErr = 0.0001)
            : m_seed(seed),
            m_sampleProb(sampleProb),
            m_minFreq(minFreq),
            m_expectedParallelism(expParallelism),
            m_error(freqErr)
        {
            SCOPE_ASSERT(m_error >= 0.0001);
            SCOPE_ASSERT(m_sampleProb >= 0 && m_sampleProb <= 1.0);

            m_minFreq = 2 * (decltype(m_minFreq)) ceil(m_minFreq / m_expectedParallelism);
            m_lossyBucketWidth = (decltype(m_lossyBucketWidth)) (1.0 / m_error);

            m_gen.seed(m_seed);			
        }

        void Init(IncrementalAllocator& alloc)
        {
			// we init here outside of the RBTree coz this sampler owns this memory
			size_t pool_size = ::OperatorRequirements::OperatorRequirementsConstants::DistinctSampler__Count_PoolEntries;
			size_t memory_needed = ScopeRBTree::MemoryNeeded(pool_size);
            alloc.Init(memory_needed, "SamplerOperator_ScopeDistinctSampler");
			m_rbtree.Init(alloc, pool_size);
        }

		OperatorRequirements GetOperatorRequirementsImpl()
		{
			return OperatorRequirements(ScopeRBTree::MemoryNeeded(::OperatorRequirements::OperatorRequirementsConstants::DistinctSampler__Count_PoolEntries));
		}

        double PickRow(unsigned __int64 hash_value)
        {
            // increment freq count for this value
            SamplerSketchItem* sv = m_rbtree.Find(hash_value);
            if (sv == nullptr)
            {
                bool insertSucceeded =
                    m_rbtree.Insert(hash_value, 0, m_currentBucketID);

                if (!insertSucceeded)
                {
#ifdef SCOPE_DEBUG
                    cout << "rbtree: ins failed, deleting some" << endl;
#endif
					// why this works?
					// All entries in the RBTree have finite freq+bucket
					//  this loop keeps increasing the threshold (EndOfBuck checks for thresh > entry's freq+bucket)
					//  so, we are guaranteed to end
					//  often, the loop will only run once.
                    unsigned int totalDeletedEntries = 0;
					for (unsigned int thresh = m_currentBucketID; totalDeletedEntries == 0; thresh++)
                    {
#ifdef SCOPE_DEBUG
                        cout << "\t deleting with thresh= " << thresh << endl;
#endif
                        totalDeletedEntries += CleanPayloadBelowThreshold(thresh);
                    }

                    insertSucceeded = m_rbtree.Insert(hash_value, 0, m_currentBucketID); // guaranteed to succeed
					SCOPE_ASSERT(insertSucceeded); // just being cautious
                }
                sv = m_rbtree.Find(hash_value);
            }
            sv->freq += 1;

            // decide whether or not to pass this row
            bool picked;
            double prob = 0;
            if (sv->freq <= m_minFreq)
            {
                picked = true;
                prob = 1;
            }
            else
            {
                picked = (m_unif(m_gen) <= m_sampleProb);
                prob = m_sampleProb;
            }
            // TODO: implement reservoir for unbiasing

            // prune low frequency elements intermittently
            if (++m_runningCount >= m_lossyBucketWidth)
            {
                CleanPayloadBelowThreshold(m_currentBucketID);
                m_currentBucketID++;
                m_runningCount = 0;
            }

            return picked ? prob : 0;
        }
    };
} // namespace ScopeEngine
