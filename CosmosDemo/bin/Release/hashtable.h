#pragma once

#include "scopeengine.h"
#include "ScopeIO.h"
#include "ScopeContainers.h"
#include "HashtableStorage.h"
#include <type_traits>

namespace ScopeEngine
{
    /*
     * Hash is an unary function object class that defines
     * the hash function used by the Hashtable.
     */
    template <typename T>
    struct Hash
    {
        INT64 operator () (const T&) const;
    };
    
    /*
     * EqualTo is a binary function object class that defines
     * the equality function used be the Hashtable.
     */
    template <typename T>
    struct EqualTo
    {
        bool operator () (const T& left, const T& right) const;
    };


    /*
     * AuxiliaryStorage is an external storage manager for the Hashtable.
     *
     * The data elements formed by the combination of key and value
     * are partitioned via a hash function and written to disk.
     *
     * The life cycle of the storage has two stages:
     * 1) Accumulating stage: data is partitioned and written to disk;
     * 2) Retrieval stage: data is loaded from disk by one partition
     *    at a time.
     *
     * Internally, the class uses BinaryOutputStream to write data
     * and BinaryInputStream to read it back.
     */
    template <typename KeySchema, typename ValueSchema, typename Hash = Hash<KeySchema>>
    class AuxiliaryStorage
    {
    private:
        typedef std::vector<string>                   Filenames;

        typedef std::unique_ptr<BinaryOutputStream>   Output;
        typedef std::vector<Output>                   Outputs;
        typedef std::unique_ptr<BinaryInputStream>    Input;

        typedef BinaryOutputPolicy<KeySchema>         KeyOutputPolicy;
        typedef BinaryOutputPolicy<ValueSchema>       ValueOutputPolicy;
        typedef BinaryExtractPolicy<KeySchema>        KeyInputPolicy;
        typedef BinaryExtractPolicy<ValueSchema>      ValueInputPolicy;

    private:
        const SIZE_T            m_bufferSize;
        const UINT              m_bufferCount;

        Hash                    m_hash;
        
        Filenames               m_names;
        Outputs                 m_outputs;
        
        bool                    m_sealed;
        UINT                    m_partitionId;

    private:
        void InitOutputs(UINT n)
        {
            for (UINT i = 0; i < n; ++i)
            {
                std::string name = IOManager::GetTempStreamName();

                IOManager::GetGlobal()->AddOutputStream(name, name);

                m_names.push_back(name);
                m_outputs.emplace_back(new BinaryOutputStream(name, m_bufferSize, m_bufferCount));
                m_outputs.back()->Init();
            }
        }

    public:
        class PartitionIterator
        {
        private:
            std::string    m_name;
            Input          m_input;
        
            KeySchema      m_key;
            ValueSchema    m_value;
            bool           m_valid;
 
        private:
            /* Reads the next available key/value pair. */
            bool Read(KeySchema& key, ValueSchema& value)
            {
                return KeyInputPolicy::Deserialize(m_input.get(), key) &&
                       ValueInputPolicy::Deserialize(m_input.get(), value);
            }

        public:
            PartitionIterator(const std::string& name)
                : m_name(name)
                , m_valid(false)
            {
            }

            PartitionIterator(PartitionIterator&& other)
                : m_name(std::move(other.m_name))
                , m_input(std::move(other.m_input))
            {
            }

            void Init(SIZE_T bufferSize, SIZE_T bufferCount, IncrementalAllocator& alloc)
            {
                IOManager::GetGlobal()->AddInputStream(m_name, m_name);
                
                m_input.reset(new BinaryInputStream(InputFileInfo(m_name), &alloc, bufferSize, (int)bufferCount));
                m_input->Init();
            }
        
            bool End()
            {
                return !m_valid;
            }

            void ReadFirst()
            {
                Increment();
            }

            void Increment()
            {
                m_valid = Read(m_key, m_value);
            }

            const KeySchema& GetKey() const
            {
                return m_key;
            }
            
            const ValueSchema& GetValue() const
            {
                return m_value;
            }
            
            void Close()
            {
                if (m_input)
                {
                    m_input->Close();
                    m_input.reset();
                }
            }


            ~PartitionIterator()
            {
                Close();
                IOManager::GetGlobal()->RemoveStream(m_name, m_name);
            }
        };

    public:
        /*
         * Constructs AuxiliaryStorage object, initializes its contents
         * and opens all partitions for writing. 
         *
         * partitionsCount  -- number of partitions;
         * bufferSize       -- size of a single output stream buffer in bytes;
         * bufferCount      -- number of buffers for each output stream;
         * hash             -- hash function used to partition the data. 
         *
         */
        AuxiliaryStorage(UINT partitionCount, SIZE_T bufferSize, UINT bufferCount, Hash hash = Hash())
            : m_bufferSize(bufferSize)
            , m_bufferCount(bufferCount)
            , m_hash(hash)
            , m_sealed(false)
            , m_partitionId(0)
        {
            SCOPE_ASSERT(partitionCount);

            InitOutputs(partitionCount);
        }

        /*
         * Writes the key/value pair to one of the partitions.
         */ 
        void Write(const KeySchema& key, const ValueSchema& value)
        {
            SCOPE_ASSERT(!m_sealed);

            SIZE_T id = m_hash(key) % m_outputs.size();

            KeyOutputPolicy::Serialize(m_outputs[id].get(), const_cast<KeySchema&>(key));
            ValueOutputPolicy::Serialize(m_outputs[id].get(), const_cast<ValueSchema&>(value));
        }

        /*
         * Flushes and closes all the partitions.
         */
        void Seal()
        {
            if (m_sealed)
                return;

            for (auto it = m_outputs.begin(); it != m_outputs.end(); ++it)
            {
                (*it)->Finish();
                (*it)->Close();
            }

            m_sealed = true;
        }

        bool HasMorePartitions() const
        {
            return m_partitionId < m_names.size();
        }

        PartitionIterator GetNextPartition()
        {
            SCOPE_ASSERT(m_partitionId < m_names.size());

            if (!m_sealed)
                Seal();

            return PartitionIterator(m_names[m_partitionId++]);
        }

        // Dtor closes output streams but does not remove them
        // from the GlobalManager, that happens in PartitionIterator dtor.
        ~AuxiliaryStorage()
        {
            Seal();
        }
    };

    template <typename RowIterator, typename KeySchema, typename ValueSchema, typename GetKeyValueFunc>
    class RowIteratorAdapter
    {
    private:
        RowIterator          m_adaptee;
        KeySchema            m_key;
        ValueSchema          m_value;
        GetKeyValueFunc      m_get;
    
    private:
        void Get()
        {
            if (!End())
            {
                m_get(*m_adaptee.GetRow(), m_key, m_value);
            }
        }

    public:
        RowIteratorAdapter(RowIterator& adaptee, GetKeyValueFunc get = GetKeyValueFunc())
            : m_adaptee(adaptee)
            , m_get(get)
        {
        }

        bool End()
        {
            return m_adaptee.End();
        }

        void ReadFirst()
        {
            m_adaptee.ReadFirst();
            Get();
        }

        void Increment()
        {
            m_adaptee.Increment();
            Get();
        }

        const KeySchema& GetKey() const
        {
            return m_key;
        }
        
        const ValueSchema& GetValue() const
        {
            return m_value;
        }
    };

    /*  
     * LFUEvictionStats is a Hashtable bucket statistics
     * accumulator.
     *
     * (LFU = Least Frequently Used) 
     * (we could also implement LRU and FIFO)
     */
    struct LFUEvictionStats
    {
        UINT64 m_value;

        LFUEvictionStats()
            : m_value(0)
        {
        }

        void Update()
        {
            ++m_value;
        }

        bool operator < (const LFUEvictionStats& other) const
        {
            return m_value < other.m_value;
        }
    };

     
    template <typename KeySchema, typename ValueSchema>
    struct DefaultHashtablePolicy
    {
        typedef Hash<KeySchema>                               Hash;
        typedef EqualTo<KeySchema>                            Pred;
        typedef LFUEvictionStats                              EvictionStats;
        typedef DefaultHeapAllocator                          DataAllocator;
        typedef FixedArrayTypeMemoryManager<DataAllocator>    DeepDataMemoryManager;

        static const SIZE_T    m_containerAllocTax = 16; // bytes
    };
    

    /*
     * Hashtable is an associative container that stores elements formed 
     * by the combination of key and mapped value. Given the key, the value
     * can be updated inplace. 
     *
     * Hashtable tracks the memory used by housekeeping and payload data and does not allow
     * it to exceed the given limit.
     *
     * The elements are stored in the Container objects that provide memory usage
     * information and define the way the elements are updated. 
     *
     * Hashtable has two modes of operation: with and without auxiliary storage. 
     * The difference is in the way that Hashtable handles the situation
     * when there is not enough memory to insert or update an element.
     * If auxiliary storage is not used the operation will fail and return 
     * "out of memory" error code. If it is used, the Hashtable will try
     * to free some memory by saving some of the buckets to the auxiliary storage.
     * By default, the most frequently accessed buckets will be kept
     * in the memory as long as possible (this behaviour can be changed by using
     * different EvictionStats policy).
     *
     * Iterator in the container is a forward iterator.
     *
     * Implementation notes:
     * 1. Hashtable uses a private Windows heap to store all its data.
     *
     * 2. Hashtable uses linear hashing algorithm to contol its load factor. 
     *    "Linear hashing increases the address space gradually by splitting buckets
     *    in a predetermined order: 0, 1, ... , N-1. Splitting bucket involves moving
     *    approximately half of records from the bucket to a new bucket at the end
     *    of the table". [Dynamic Hash Tables, P. Larson]
     *
     * 3. Hashtable does not support deletion of individual elements. [There is not need
     *    for that now, but the support can be easily added later].
     *
     */
    template <typename KeySchema,
              typename ValueSchema,
              template <typename KeySchema, typename ValueSchema, typename Allocator> class ContainerType,
              template <typename KeySchema, typename ValueSchema> class Policy = DefaultHashtablePolicy>
    class Hashtable
    {
    public:
        typedef          Policy<KeySchema, ValueSchema>                         Policy;
    
    private:
        typedef          DefaultHeapAllocator                                   Allocator;
        typedef typename Policy::DataAllocator                                  DataAllocator;
    
    public:
        typedef typename ContainerType<KeySchema, ValueSchema, DataAllocator>   Container;

    private:
        typedef typename Policy::Hash                                           Hash;
        typedef typename Policy::Pred                                           Pred;
        typedef typename Policy::EvictionStats                                  EvictionStats;

        typedef typename DataAllocator::template rebind<Container>::other       ContainerAllocator;
        typedef typename DataAllocator::template rebind<ValueSchema>::other     ValueAllocator;
        typedef typename DataAllocator::template rebind<char>::other            CharAllocator;
        
        typedef          std::forward_list<Container*, Allocator>               Containers;

        typedef typename Container::KeyValue                                    KeyValue;

        typedef typename Containers::iterator                                   ContainersIterator;
        typedef typename Containers::const_iterator                             ConstContainersIterator;
        typedef          AuxiliaryStorage<KeySchema, ValueSchema, Hash>         AuxiliaryStorage;
    
    public:
        typedef typename Policy::DeepDataMemoryManager                          DeepDataMemoryManager;


    public:     
        /*
         * Bucket is the hashtable bucket. 
         * 
         * Key/value pairs are packed into Container objects and stored 
         * in a single-linked list. 
         *
         * [ankorsun] Single linked list is not the fastest, but the most memory efficient
         * data structure. But I do not think that has significant effect
         * on performance because the linear hash keeps the load factor low (1.5 by default)
         * and hash skew is not very likely.
         */ 
        struct Bucket
        {
            typedef typename EvictionStats Stats;
        
            enum EStatus
            {
                ACTIVE,
                SPILLED
            };

            Containers        m_containers;
            EStatus           m_status;
            Stats             m_stats;

            Bucket(Allocator& alloc, EStatus status = ACTIVE, Stats stats = Stats())
                : m_containers(alloc)
                , m_status(status)
                , m_stats(stats)
            {
            }

            Bucket(Bucket&& other)
                : m_containers(std::move(other.m_containers))
                , m_status(other.m_status)
                , m_stats(other.m_stats)
            {
            }

            Bucket(const Bucket&) = delete;
            Bucket& operator = (const Bucket&) = delete;            


            ConstContainersIterator Begin() const
            {
                return m_containers.begin();
            }

            ConstContainersIterator End() const
            {
                return m_containers.end();
            }
            
            ContainersIterator Begin()
            {
                return m_containers.begin();
            }

            ContainersIterator End()
            {
                return m_containers.end();
            }

            // Returns the number of containers.
            SIZE_T Size() const
            {
                return std::distance(Begin(), End());
            }

            void Add(Container* container)
            {
                m_containers.push_front(container);
            }

            // Updates bucket usage statistics.
            // The method is called on successfull insert or update
            // operations.
            void UpdateUsageStats()
            {
                m_stats.Update();
            }

            void DeleteValue(ValueSchema* v, MemoryManager* memoryManager)
            {
                // delete deep data from the value
                v->Delete(memoryManager->m_deepDataMemoryManager);
                
                // delete the value itself
                memoryManager->DeleteValue(v);
            }

            template <template <typename, typename, typename> class Cnt>
            void DeleteValues(typename Cnt<KeySchema, ValueSchema, Allocator>::KeyValue& kv, MemoryManager* memoryManager);
            
            template <>
            void DeleteValues<MutableValueContainer>(typename MutableValueContainer<KeySchema, ValueSchema, Allocator>::KeyValue& kv, MemoryManager* memoryManager)
            {
                ValueSchema* v = const_cast<ValueSchema*>(kv.second);
                DeleteValue(v, memoryManager);
            }
            
            template <>
            void DeleteValues<ListOfValuesContainer>(typename ListOfValuesContainer<KeySchema, ValueSchema, Allocator>::KeyValue& kv, MemoryManager* memoryManager)
            {
                for (auto it = kv.second.begin(); it != kv.second.end(); ++it)
                {
                    ValueSchema* v = const_cast<ValueSchema*>(*it);
                    DeleteValue(v, memoryManager);
                } 
            }

            template <>
            void DeleteValues<ListOfValuesContainerWithStats>(typename ListOfValuesContainer<KeySchema, ValueSchema, Allocator>::KeyValue& kv, MemoryManager* memoryManager)
            {
                DeleteValues<ListOfValuesContainer>(kv, memoryManager);
            }

            template <>
            void DeleteValues<SingletonContainer>(typename SingletonContainer<KeySchema, ValueSchema, Allocator>::KeyValue& kv, MemoryManager* memoryManager)
            {
                for (auto it = kv.second.begin(); it != kv.second.end(); ++it)
                {
                    ValueSchema* v = const_cast<ValueSchema*>(*it);
                    DeleteValue(v, memoryManager);
                } 
            }
          
            template <template <typename, typename, typename> class Cnt>
            void Clear(MemoryManager* memoryManager)
            { 
                for (auto cit = Begin(); cit != End(); ++cit)
                {
                    KeyValue& kv = const_cast<KeyValue&>((*cit)->Both());
                    KeySchema& k = const_cast<KeySchema&>(kv.first);

                    // delete deep data from the key
                    k.Delete(memoryManager->m_deepDataMemoryManager);
                    
                    DeleteValues<Cnt>(kv, memoryManager);
                }
                
                memoryManager->m_deepDataMemoryManager->Commit();

                // delete containers
                std::for_each(Begin(), End(), [memoryManager] (Container* c) { memoryManager->DeleteContainer(c); });

                Containers(m_containers.get_allocator()).swap(m_containers);
                
                memoryManager->m_deepDataMemoryManager->Reset();
            }

            bool IsActive() const
            {
                return m_status == ACTIVE;
            }
            
            void SetSpilled()
            {
                SCOPE_ASSERT(Size() == 0);
                m_status = SPILLED;
            }
        };
        
        // a supplementary data structure used to store pointers to the buckets
        // that can be spilled to the auxiliary storage
        typedef          std::vector<Bucket*, Allocator>   NonEmptyBuckets;

    private:
        typedef          std::vector<Bucket, Allocator>    Buckets;
        typedef typename Buckets::iterator                 BucketsIterator;
        typedef typename Buckets::const_iterator           ConstBucketsIterator;

        
        class MemoryManager
        {
        private:
            DataAllocator             m_alloc;
            ContainerAllocator        m_calloc;
            ValueAllocator            m_valloc;
            MemoryTracker*            m_memoryTracker;

        public:
            DeepDataMemoryManager*    m_deepDataMemoryManager;

        private:
            template <typename Allocator>
            void Rollback() { m_deepDataMemoryManager->Rollback(); }
            
            template <>
            void Rollback<DefaultSTLIncrementalAllocator>() { /* do not do anything */}
            
            template <typename Allocator>
            void DeleteValue(ValueSchema* value) { DeleteValue(value); }
            
            template <>
            void DeleteValue<DefaultSTLIncrementalAllocator>(ValueSchema*) { /* do not do anything */ }

            template <typename Allocator>
            void DeleteContainer(Container* container) { DeleteContainer(container); }
            
            template <>
            void DeleteContainer<DefaultSTLIncrementalAllocator>(Container*) { /* do not do anything */ }

        public:
            MemoryManager(DataAllocator& alloc, MemoryTracker* memoryTracker, DeepDataMemoryManager* deepDataMemoryManager)
                : m_alloc(alloc)
                , m_calloc(alloc)
                , m_valloc(alloc)
                , m_memoryTracker(memoryTracker)
                , m_deepDataMemoryManager(deepDataMemoryManager)
            {
            }

            void DeleteContainer(Container* container)
            {
                m_calloc.deallocate(container, 0/*unused*/);
                m_memoryTracker->UpdateTaxablePayload(-(int)(sizeof(Container)));
            }
            
            void DeleteValue(ValueSchema* value)
            {
                m_valloc.deallocate(value, 0/*unused*/);
                m_memoryTracker->UpdatePayload(-(int)(sizeof(ValueSchema)));
            }

            Container* CreateContainer(const KeySchema& key, const ValueSchema& value)
            {
                if (!m_memoryTracker->AvailableTaxable(sizeof(Container) + sizeof(ValueSchema)))
                    return nullptr;
                
                m_memoryTracker->UpdateTaxablePayload(sizeof(Container) + sizeof(ValueSchema));
                
                ValueSchema* valueCopy = new (m_valloc.allocate(1)) ValueSchema(value, m_deepDataMemoryManager);
                
                if (!m_deepDataMemoryManager->Valid())
                {
                    // free allocated memory and update used memory record
                    //m_deepDataMemoryManager->Rollback();
                    Rollback<DataAllocator>();
                    m_deepDataMemoryManager->Reset();
                    DeleteValue<DataAllocator>(valueCopy);
                    m_memoryTracker->UpdateTaxablePayload(-(int)(sizeof(Container)));
                    
                    return nullptr;
                }
                
                Container* container = new (m_calloc.allocate(1)) Container(key, valueCopy, *m_deepDataMemoryManager, m_alloc);
                Container* result = nullptr;
                if (!m_deepDataMemoryManager->Valid())
                {
                    // free allocated memory and update used memory recond
                    //m_deepDataMemoryManager->Rollback();
                    Rollback<DataAllocator>();
                    DeleteValue<DataAllocator>(valueCopy);
                    DeleteContainer<DataAllocator>(container);
                }
                else
                {
                    m_deepDataMemoryManager->Commit();
                    result = container;   
                }
                
                m_deepDataMemoryManager->Reset();

                return result;
            }

            ValueSchema* CreateShallowCopy(const ValueSchema& value)
            {
                ValueSchema* shallowCopy = new (m_valloc.allocate(1)) ValueSchema(value);
                m_memoryTracker->UpdatePayload(sizeof(ValueSchema));

                return shallowCopy;
            }

            ValueSchema* CreateDeepCopy(const ValueSchema& value)
            {
                if (!m_memoryTracker->Available(sizeof(ValueSchema)))
                    return nullptr;
                
                m_memoryTracker->UpdatePayload((INT)sizeof(ValueSchema));
                ValueSchema* deepCopy = new (m_valloc.allocate(1)) ValueSchema(value, m_deepDataMemoryManager);
                
                ValueSchema* result = nullptr;
                if (!m_deepDataMemoryManager->Valid())
                {
                    // free allocated memory and update used memory record
                    //m_deepDataMemoryManager->Rollback();
                    Rollback<DataAllocator>();
                    DeleteValue<DataAllocator>(deepCopy);
                }
                else
                {
                    m_deepDataMemoryManager->Commit();
                    result = deepCopy;
                }
                
                m_deepDataMemoryManager->Reset();

                return result;
            }
        };

   public:
        enum EResult
        {
            OK_INSERT,
            OK_UPDATE,
            OK_FOUND,
            FAILED_SPILLED,
            FAILED_NOT_FOUND,
            FAILED_DUPLICATE_KEY,
            FAILED_OUT_OF_MEMORY,
            READY_FOR_UPDATE
        };

   private:
        /*
         * IteratorBase is a base class template for hashtable iterators. 
         *
         * BIt -- bucket iterator type;
         * CIt -- container iterator type;
         * T   -- key/value pair type.
         *
         */
        template <typename BIt, typename CIt, typename T>
        class IteratorBase : public iterator<forward_iterator_tag, T>
        {
        public:
            typedef typename CIt::value_type Container;

        private:
            const Buckets*  m_bs;
            BIt             m_bit;
            CIt             m_cit;

        private:
            // skip bucket iterator to the next
            // non-empty bucket and if such exist
            // initialize the container iterator
            void SkipToNext()
            {
                while (m_bit != m_bs->end())
                {
                    if (m_bit->Begin() != m_bit->End())
                    { 
                        m_cit = m_bit->Begin();
                        break;
                    }

                    ++m_bit;
                }
            }

        public:
            IteratorBase()
                : m_bs(nullptr)
            {
            }

            IteratorBase(const Buckets* bs, BIt bit)
                : m_bs(bs)
                , m_bit(bit)
            {
                SkipToNext();
            }
            
            IteratorBase(const Buckets* bs, BIt bit, CIt cit)
                : m_bs(bs)
                , m_bit(bit)
                , m_cit(cit)
            {
            }

            IteratorBase& operator ++ ()
            {
                ++m_cit;
                
                if (m_cit == m_bit->End())
                {
                    ++m_bit;
                    SkipToNext();
                }

                return *this;
            }

            IteratorBase operator ++ (int)
            {
                IteratorBase tmp(*this);
                operator ++ ();
                return tmp;
            }

            T& operator * () const
            {
                return m_cit->Both();
            }
            
            T* operator -> () const
            {
                return &((*m_cit)->Both());
            }

            const Container container() const
            {
                return (*m_cit);
            }

            Container container()
            {
                return (*m_cit);
            }

            /*
             * Compares two iterators.
             *
             * Method returns true if either one of the following
             * conditions is met:
             * 1) Bucket iterators point to the same position and the position
             *    is not the end of buckets sequence, container iterators
             *    point to the same position;
             * 2) Bucket iterators point to the same position and the position
             *    is exactly the end of bucket sequence.
             *
             */
            bool operator == (const IteratorBase& other) const
            {
                if (m_bs != other.m_bs)
                    return false;
                
                if (m_bit != other.m_bit)
                    return false;

                if (m_bit == m_bs->end())
                    return true;
                
                return m_cit == other.m_cit;
            }

            bool operator != (const IteratorBase& other) const
            {
                return !(operator == (other)); 
            }
        };

        typedef IteratorBase<BucketsIterator, ContainersIterator, KeyValue>                   Iterator;

    public:
        typedef IteratorBase<ConstBucketsIterator, ConstContainersIterator, const KeyValue>   ConstIterator;
        typedef std::pair<ConstIterator, EResult>                                             FindResult;

    public:
        static const SIZE_T    SIZEOF_BUCKET = sizeof(Bucket);

    public:
    private:
        static const SIZE_T    DEFAULT_INITIAL_SIZE = 1024;
        static       double    DefaultMaxLoadFactor() { return 1.5; }

        const Hash                               m_hash;
        const Pred                               m_pred;

        // both heap and stack memory allocators need to be
        // owned by the hashtable, so it could just release
        // the memory on Clear() without calling dtors
        std::unique_ptr<Heap>                    m_heap;
        IncrementalAllocator                     m_stack;
        Allocator                                m_alloc;
        DataAllocator                            m_dalloc;
        std::unique_ptr<MemoryManager>           m_memoryManager;

        SIZE_T                                   m_memoryQuota;
        std::unique_ptr<MemoryTracker>           m_memoryTracker;
        std::unique_ptr<DeepDataMemoryManager>   m_deepDataMemoryManager;

        const SIZE_T                             m_initialSize;
        std::unique_ptr<Buckets, NoOpDeleter>    m_buckets;

        // >> two phase update
        Bucket*                                  m_updateBucket;
        Container*                               m_updateContainer;
        ValueSchema                              m_updateValueCopy;
        ValueSchema*                             m_updateValueCopyPtr;
        // << two phase update

        // >> linear hash
        const double                             m_maxLoadFactor;
        SIZE_T                                   m_elementCount;
        SIZE_T                                   m_nextBucketToBeSplitIdx;    // index of the next bucket to be split
        SIZE_T                                   m_maxNextBucketToBeSplitIdx; // upper bound on m_nextBucketToBeSplitIdx during this expansion
        // << linear hash

    private:
        // >> linear hash
        
        /*
         * Returns the bucket index for the key.
         *
         * First, computes hash mod "table size before the expansion" 
         * (m_maxNextBucketToBeSplit).
         * If the bucket index is less than the current value of the index 
         * to the next bucket to be split, the corresponding bucket has already
         * been split, otherwise not. If the bucket has been split,
         * the correct index is given by hash mod "table size after expansion" 
         * (2 * m_maxNextBucketToBeSplit).
         */
        SIZE_T Pos(const KeySchema& key) const
        {
            INT64 hash = m_hash(key);
            SIZE_T result = hash & (m_maxNextBucketToBeSplitIdx - 1); // hash mod m_maxNextBucketToBeSplitIdx

            if (result < m_nextBucketToBeSplitIdx)
                result = hash & ((m_maxNextBucketToBeSplitIdx << 1) - 1); // hash mod (2 * m_maxNextBucketToBeSplitIdx)
            
            return result;
        }
        
        double LoadFactor() const
        {
            return double(m_elementCount)/m_buckets->size();
        }

        bool IsLoadFactorOverMax() const
        {
            return LoadFactor() > m_maxLoadFactor;
        }

        SIZE_T AppendBucket(typename Bucket::EStatus status, typename Bucket::Stats stats)
        {
            m_buckets->emplace_back(m_alloc, status, stats);
            m_memoryTracker->UpdateHousekeeping(sizeof(Bucket));
            
            return m_buckets->size() - 1;
        }

        void UpdateLinearHashStateAfterSplit()
        {
            ++m_nextBucketToBeSplitIdx;
            // if the next to be split bucket index has reached 
            // the table size before expansion, start over from 0
            // and set the maximum to the table size after expansion. 
            if (m_nextBucketToBeSplitIdx == m_maxNextBucketToBeSplitIdx)
            {
                m_maxNextBucketToBeSplitIdx *= 2;
                m_nextBucketToBeSplitIdx = 0;
            }
        }

        // The fuction is used to find the previous element
        // in a single-linked list after erase_after call
        // [pos cannot ever point to the beginning of the list].
        static ContainersIterator FindPrev(Bucket& bucket, ContainersIterator pos)
        {
            SCOPE_ASSERT(pos != bucket.Begin());

            ContainersIterator it = bucket.Begin();
            ContainersIterator previt = it;

            while (it != pos) previt = it++;

            return previt;
        }

        // Move some of the containers from the bucket that
        // is split to the newly created bucket.
        void ReallocateContainers(SIZE_T oldPos, SIZE_T newPos)
        {
            Bucket& oldBucket = (*m_buckets)[oldPos];
            Bucket& newBucket = (*m_buckets)[newPos];
            
            ContainersIterator it = oldBucket.Begin();
            ContainersIterator previt = it;

            while (it != oldBucket.End())
            {
                // if the bucket index for the key is different 
                // in the expanded table then move container 
                // to the new bucket
                if (Pos((*it)->Key()) == newPos)
                {
                    newBucket.Add(*it);

                    if (it == oldBucket.Begin())
                    {
                        oldBucket.m_containers.pop_front();
                        
                        it = oldBucket.Begin();
                        previt = it;
                    }
                    else
                    {
                        it = oldBucket.m_containers.erase_after(previt);
                        previt = FindPrev(oldBucket, it);
                    }
                }
                else
                {
                    previt = it++;
                }
            }
        }

        void SplitNextBucket()
        {
            SIZE_T oldPos = m_nextBucketToBeSplitIdx;
            Bucket& bucket = (*m_buckets)[oldPos];
            // create new bucket at the end of bucket sequence
            SIZE_T newPos = AppendBucket(bucket.m_status, bucket.m_stats);
            UpdateLinearHashStateAfterSplit();
            // Move some of the containers from the bucket that
            // is split to the newly created bucket.
            ReallocateContainers(oldPos, newPos);
        }
        // << linear hash
        
        // >> spilling
        NonEmptyBuckets CollectNonEmptyBuckets()
        {
            NonEmptyBuckets nonEmptyBuckets(m_alloc);
            
            std::for_each(m_buckets->begin(), m_buckets->end(),
                          [&nonEmptyBuckets] (Bucket& b) 
                          { 
                            if (b.Size()) 
                                nonEmptyBuckets.push_back(&b); 
                          });
            
            
            std::sort(nonEmptyBuckets.begin(), nonEmptyBuckets.end(),
                      [] (const Bucket* left, const Bucket* right) { return left->m_stats < right->m_stats; });

            return nonEmptyBuckets;
        }
        // << spilling

        BucketsIterator BucketIt(const KeySchema& key) const
        {
            return m_buckets->begin() + Pos(key);
        }

        ContainersIterator ContainerIt(Bucket& bucket, const KeySchema& key) const
        {
            return std::find_if(bucket.Begin(), bucket.End(),
                               [this, &key] (const Container* c) { return m_pred(key, c->Key()); });
        }


        /*
         * Key and value are inserted to the table, memory usage
         * is updated by the sum of key and value sizes.
         */
        EResult Insert(Bucket& bucket, const KeySchema& key, const ValueSchema& value)
        {
            Container* container = m_memoryManager->CreateContainer(key, value);
            
            if (!container)
                return FAILED_OUT_OF_MEMORY;

            bucket.Add(container);
            bucket.UpdateUsageStats();
            ++m_elementCount;
        
            // expand the table if the load factor has reached the maximum allowed
            if (IsLoadFactorOverMax())
                SplitNextBucket();
            
            
            return OK_INSERT;
        }
        
        
        template <template <typename, typename, typename> class Cnt>
        EResult InsertValueForExistingKey(Bucket& bucket, Container* container, const ValueSchema& value);
        
        template <>
        EResult InsertValueForExistingKey<MutableValueContainer>(Bucket& bucket, Container*, const ValueSchema&)
        {
            return FAILED_DUPLICATE_KEY;
        }
        
        template <>
        EResult InsertValueForExistingKey<ListOfValuesContainer>(Bucket& bucket, Container* container, const ValueSchema& value)
        {
            ValueSchema* valueCopy = m_memoryManager->CreateDeepCopy(value);
            if (valueCopy)
            {
                container->Add(valueCopy);
                bucket.UpdateUsageStats();
                return OK_INSERT;
            }

            return FAILED_OUT_OF_MEMORY;
        }

        template <>
        EResult InsertValueForExistingKey<ListOfValuesContainerWithStats>(Bucket& bucket, Container* container, const ValueSchema& value)
        {
            return InsertValueForExistingKey<ListOfValuesContainer>(bucket, container, value);
        }
       
        template <>
        EResult InsertValueForExistingKey<SingletonContainer>(Bucket& bucket, Container* container, const ValueSchema& value)
        {
            return OK_INSERT;
        }

        EResult Insert(BucketsIterator bit, const KeySchema& key, const ValueSchema& value)
        {
            ContainersIterator cit = ContainerIt(*bit, key);

            if (cit == bit->End())
                return Insert(*bit, key, value);
            else
                return InsertValueForExistingKey<ContainerType>(*bit, *cit, value);
        }

        template <template <typename, typename, typename> class Cnt>
        EResult TryUpdateOrInsertShallow(const KeySchema& key, const ValueSchema& value, ValueSchema*& mutableValue);
        
        /*
         * Inserts or updates elements.
         *
         * NB: this method should only be used to update ValueSchema objects that have 
         * fixed size fields only, deep data fields will not be copied to the internal allocator.
         *
         * If the _key_ is not equivalent to any key that is already in the table,
         * the new element is inserted and the memory usage is updated
         * by the sum of _key_ and _value_ sizes. Returns OK_INSERT or FAILED_OUT_OF_MEMORY.
         *
         * If the _key_ is equivalent to some key that is already in the table,
         * the _mutableValue_ is assigned with a pointer to the _key_'s corresponding value.
         * Returns READY_FOR_UPDATE.
         */
        template <>
        EResult TryUpdateOrInsertShallow<MutableValueContainer>(const KeySchema& key, const ValueSchema& value, ValueSchema*& mutableValue)
        {
            BucketsIterator bit = BucketIt(key);
            ContainersIterator cit = ContainerIt(*bit, key);
            
            if (cit == bit->End())
                return Insert(bit, key, value);

            mutableValue = (*cit)->Both().second;
            bit->UpdateUsageStats();

            return READY_FOR_UPDATE;
        }
        
        // >> two phase update
        template <template <typename, typename, typename> class Cnt>
        EResult TryUpdateOrInsert(const KeySchema& key, const ValueSchema& value, ValueSchema*& mutableValueCopy, DeepDataMemoryManager*& deepDataMemoryManager);
        
        /*
         * Inserts or updates elements.
         *
         * NB: this method should be used to update ValueSchema objects that have deep data fields
         * and has two phases -- the TryUpdateOrInsert() call and the FinalizeUpdate() call.
         *
         * If the _key_ is not equivalent to any key that is already in the table,
         * the new element is inserted, deep data fields are copied to the internal allocator
         * and the memory usage is updated by the sum of _key_ and _value_ sizes
         * and the total size of all deep data fields. Returns OK_INSERT or FAILED_OUT_OF_MEMORY.
         *
         * If the _key_ is equivalent to some key that is already in the table,
         * the _mutableValueCopy_ is asssigned with a pointer to a shallow copy to
         * the _key_'s corresponding value, _deepDataMemoryManager_ is assigned 
         * with a pointer to the internal memory manager. Returns READY_FOR_UPDATE.
         *
         * Deep data must be copied using the _deepDataMemoryManager_.
         *
         * The updated value is saved on the FinalizeUpdate() call.
         *
         */
        template <>
        EResult TryUpdateOrInsert<MutableValueContainer>(const KeySchema& key, const ValueSchema& value, ValueSchema*& mutableValueCopy, DeepDataMemoryManager*& deepDataMemoryManager)
        {
            BucketsIterator bit = BucketIt(key);
            ContainersIterator cit = ContainerIt(*bit, key);
            
            if (cit == bit->End())
                return Insert(bit, key, value);

            m_updateBucket = &(*bit);
            m_updateContainer = *cit;
            
            m_updateValueCopy = *m_updateContainer->Both().second;
            m_updateValueCopyPtr = &m_updateValueCopy;

            mutableValueCopy = m_updateValueCopyPtr;
            deepDataMemoryManager = m_deepDataMemoryManager.get();

            return READY_FOR_UPDATE;
        }


        template <template <typename, typename, typename> class Cnt>
        EResult FinalizeUpdate();
        
        /*
         * The second phase of the two phase update (see TryInsertOrUpdate).
         *
         * If the memory limit was not exceeded during the update saves changes 
         * made to the _mutableValueCopy_ and updates the memory usage. Returns
         * OK_UPDATE.
         *
         * Otherwise, the old value does not change. Returns FAILED_OUT_OF_MEMORY.
         *
         */ 
        template <>
        EResult FinalizeUpdate<MutableValueContainer>()
        {
            SCOPE_ASSERT(m_updateBucket);
            SCOPE_ASSERT(m_updateContainer);
            SCOPE_ASSERT(m_updateValueCopyPtr);

            EResult res = OK_UPDATE;
            if (!m_deepDataMemoryManager->Valid())
            {
                // no need to release deep data memory here,
                // it is handled in m_deepDataMemoryManager::Rollback()
                m_deepDataMemoryManager->Rollback();
                
                res = FAILED_OUT_OF_MEMORY;
            }
            else
            {
                m_deepDataMemoryManager->Commit();
                m_updateContainer->Update(*m_updateValueCopyPtr);
                m_updateBucket->UpdateUsageStats();
            }

            m_updateValueCopyPtr = nullptr;
            m_updateContainer = nullptr;
            m_updateBucket = nullptr;
            
            m_deepDataMemoryManager->Reset();

            return res;
        }
        // << two phase update

        template <typename DataAllocator>
        void ResetAllocators();

        template <>
        void ResetAllocators<DefaultHeapAllocator>()
        {
            m_dalloc = DataAllocator(m_heap->Ptr());
        }

        template <>
        void ResetAllocators<DefaultSTLIncrementalAllocator>()
        {
            m_stack.Reset();
            m_dalloc = DataAllocator(&m_stack);
        }

        void Reset()
        {
            m_heap.reset(new Heap());
            m_alloc = Allocator(m_heap->Ptr());
            ResetAllocators<DataAllocator>();

            //TODO replace with new Buckets(m_initialSize, m_alloc)
            // when compiler supports it
            m_buckets.reset(new Buckets(m_alloc));
            m_buckets->reserve(m_initialSize);
            for (int i = 0; i < m_initialSize; ++i)
            {
                m_buckets->emplace_back(m_alloc);
            }

            m_elementCount = 0;
            m_nextBucketToBeSplitIdx = 0;
            m_maxNextBucketToBeSplitIdx = m_buckets->size();

            m_memoryTracker.reset(new MemoryTracker(m_memoryQuota, Policy::m_containerAllocTax));

            // [ankorsun] Note, that m_buckets->size() provides the lower bound
            // on memory usage and we do not account for real vector
            // capacity. The reason is that the vector capacity 
            // is out of our control and I do not want to bind tests to it.
            m_memoryTracker->UpdateHousekeeping((int)(m_buckets->size() * sizeof(Bucket)));

            m_deepDataMemoryManager.reset(new DeepDataMemoryManager(m_dalloc, m_memoryTracker.get()));
            m_memoryManager.reset(new MemoryManager(m_dalloc, m_memoryTracker.get(), m_deepDataMemoryManager.get()));
        }

    public:
        /*
         * Constructs a Hashtable object and intializes its contents.
         *
         * memoryQuota   -- inclusive upper bound on payload data memory usage;
         * intitialSize  -- initial number of buckets;
         * maxLoadfactor -- inclusive upper bound on the table load factor. 
         *                  When the load factor reaches that limit, the number 
         *                  of buckets is increased.
         */
        Hashtable(SIZE_T memoryQuota,
                  const std::string& operatorName = "Hashtable",
                  SIZE_T initialSize = DEFAULT_INITIAL_SIZE, 
                  double maxLoadFactor = DefaultMaxLoadFactor(), 
                  const Hash hash = Hash(), 
                  const Pred pred = Pred())
            : m_hash(hash)
            , m_pred(pred)
            , m_initialSize(initialSize)
            , m_updateBucket(nullptr)
            , m_updateContainer(nullptr)
            , m_updateValueCopyPtr(nullptr)
            , m_maxLoadFactor(maxLoadFactor)
        {
            SCOPE_ASSERT(m_initialSize != 0 && !(m_initialSize & (m_initialSize - 1))); //initial size is a power of two
            SCOPE_ASSERT(maxLoadFactor > 0);
            
            m_memoryQuota = memoryQuota;
            m_stack.Init(m_memoryQuota, operatorName);
       
            Reset();
        }
        
        SIZE_T Size() const
        {
            return m_elementCount;
        }
        
        bool Empty() const
        {
            return m_elementCount == 0;
        }

        ConstIterator Begin() const
        {
            return ConstIterator(m_buckets.get(), m_buckets->begin());
        }

        ConstIterator End() const
        {
            return ConstIterator(m_buckets.get(), m_buckets->end());
        }
        
        FindResult Find(const KeySchema& key) const
        {
            BucketsIterator bit = BucketIt(key);
            
            if (bit->IsActive())
            {
                ConstContainersIterator cit = ContainerIt(*bit, key);

                if (cit != bit->End())
                    return std::make_pair(ConstIterator(m_buckets.get(), bit, cit), OK_FOUND);
                else 
                    return std::make_pair(End(), FAILED_NOT_FOUND);
            }
            else
            {
                return std::make_pair(End(), FAILED_SPILLED);
            }
        }

        // >> spilling
        UINT Spill(AuxiliaryStorage& storage, double fractionOfBucketsToSpill)
        {
           NonEmptyBuckets buckets = CollectNonEmptyBuckets();
           SCOPE_ASSERT(buckets.size());
           
           // spill at least one bucket
           UINT maxBucketCnt = std::max<UINT>((UINT)(buckets.size() * fractionOfBucketsToSpill), (UINT)1);

           UINT bucketCnt = 0;
           UINT count = 0;
           for (auto bit = buckets.begin(); bit != buckets.end() && bucketCnt++ < maxBucketCnt; ++bit)
           {
               for (auto cit = (*bit)->Begin(); cit != (*bit)->End(); ++cit)
               {
                   const KeyValue& kv = (*cit)->Both();
                   std::for_each(kv.second.begin(), kv.second.end(),
                                 [&storage, &kv, &count] (const ValueSchema * value)
                                 { 
                                     storage.Write(kv.first, *value);
                                     ++count;
                                 });
               }

               (*bit)->Clear<ContainerType>(m_memoryManager.get());
               (*bit)->SetSpilled();
           }

           return count;
        }
        // << spilling

        EResult TryUpdateOrInsertShallow(const KeySchema& key, const ValueSchema& value, ValueSchema*& mutableValue)
        {
            return TryUpdateOrInsertShallow<ContainerType>(key, value, mutableValue);
        }
        
        // >> two phase update
        EResult TryUpdateOrInsert(const KeySchema& key, const ValueSchema& value, ValueSchema*& mutableValueCopy, DeepDataMemoryManager*& deepDataMemoryManager)
        {
            return TryUpdateOrInsert<ContainerType>(key, value, mutableValueCopy, deepDataMemoryManager);
        }

        EResult FinalizeUpdate()
        {
            return FinalizeUpdate<ContainerType>();
        }
        
        // << two phase update

        /*
         * Inserts elements to the table. 
         *
         * If the _key_ is not equivalent to any key that is already in the table,
         * the new element is inserted and the memory usage is updated
         * by the sum of _key_ and _value_ sizes.
         *
         * If the _key_ is equivalent to some key that is already in the table,
         * the function fails with FAILED_DUPLICATE_KEY.
         */
        EResult Insert(const KeySchema& key, const ValueSchema& value)
        {
            BucketsIterator bit = BucketIt(key);
            
            if (bit->IsActive())
                return Insert(bit, key, value);
            else
                return FAILED_SPILLED;
        }
        
        /*
         * Deletes all the elements in the table, resets buckets to the initial state:
         * no destructors are called, the method resets underlying memory heap.
         */
        void Clear()
        {
            Reset();
        }
        
        SIZE_T BucketCount() const
        { 
            return m_buckets->size();
        }

        SIZE_T BucketSize(SIZE_T n) const
        {
            return (*m_buckets)[n].Size();
        }

        /*
         * Returns the number of bytes used by payload data only.
         */ 
        SIZE_T DataMemoryUsage() const
        {
            return m_memoryTracker->Payload();
        }

        /*
         * Returns the number of bytes used by both housekeeping and payload data.
         */ 
        SIZE_T MemoryUsage() const
        {
            return m_memoryTracker->Total();
        }
    };


    /*****************************************************************/

    /*
     * SlimHashtable is an associative container that stores key-value pairs with unique keys.
     *
     * The hashtable does not own memory that it uses.
     *
     * Hashtable uses linear hashing algorithm to contol its load factor. 
     *    "Linear hashing increases the address space gradually by splitting buckets
     *    in a predetermined order: 0, 1, ... , N-1. Splitting bucket involves moving
     *    approximately half of records from the bucket to a new bucket at the end
     *    of the table". [Dynamic Hash Tables, P. Larson]
     *
     * devnote: SlimHashtable is used in HashCombinerV2 and HashAggregatorV2 operators.
     * SlimHashtable and the supporting classes (AutoExpandArray, GranularList, InlineList)
     * are build not as a general purpose containers but rather a set of utilities to support
     * the operators and allow them to process data as fast as possible with the least
     * memory overhead.
     * There are three main ideas behind the design: 
     * (i) Each instance of SlimHashtable is supposed to be used with only one allocator, 
     *     it does not own any memory, the allocator does.
     * (ii) SlimHashtable is a grow only container and does not support deletions. 
     * (iii) SlimHashtable only copies data once -- on the insertion (The only exception
     * is InlineList tail splice, which should not happen often).
     *
     * Those principles allows SlimHashtable to use very efficient IncrementalAllocator,
     * thus save time on allocations and save space on tracking used memory.  
     * 
     */
    template <typename Traits>
    class SlimHashtable
    {
    public:     
        typedef typename Traits::ItemType               ItemType;
        typedef typename Traits::LayoutPolicyType       LayoutPolicyType;
        typedef typename LayoutPolicyType::ItemListType ItemListType;
        
    private:
        typedef typename Traits::KeyType                KeyType;
        typedef typename Traits::HashType               HashType;
        typedef typename Traits::PredType               PredType;
        typedef typename Traits::AllocatorType          AllocatorType;

        typedef typename Traits::CharAllocatorType      CharAllocatorType;

        typedef typename ItemListType::Iterator         ItemListIteratorType;
        typedef typename ItemListType::ConstIterator    ConstItemListIteratorType;

        struct Bucket
        {
            ItemListType m_items;
        };
        
        typedef AutoExpandArray<Bucket, 
                                LayoutPolicyType::BucketArrayTraits::s_directorySize, 
                                LayoutPolicyType::BucketArrayTraits::s_segmentSizeExponent, 
                                AllocatorType> BucketArrayType;
        
        /*
         * Hashtable iterator. 
         * Hashtable operations that invalidate the interator: Insert.
         */
        template <typename BucketArrayType, typename IIt, typename T>
        class IteratorBase : public iterator<forward_iterator_tag, T>
        {
        private:
            BucketArrayType*   m_buckets{ nullptr };
            size_t             m_pos{ 0 };
            IIt                m_iit;

        private:
            // skip bucket iterator to the next
            // non-empty bucket and if such exist
            // initialize the container iterator
            void SkipToNext()
            {
                while (m_pos != m_buckets->Size())
                {
                    if ((*m_buckets)[m_pos].m_items.Begin() != (*m_buckets)[m_pos].m_items.End())
                    { 
                        m_iit = (*m_buckets)[m_pos].m_items.Begin();
                        break;
                    }

                    ++m_pos;
                }
            }

        public:
            IteratorBase() {}

            IteratorBase(BucketArrayType* buckets, size_t pos)
                : m_buckets(buckets)
                , m_pos(pos)
            {
                SkipToNext();
            }
            
            IteratorBase(BucketArrayType* buckets, size_t pos, IIt iit)
                : m_buckets(buckets)
                , m_pos(pos)
                , m_iit(iit)
            {
            }

            template<typename U, typename V, typename W>
            IteratorBase(const IteratorBase<U, V, W>& other)
                : m_buckets((BucketArrayType*)other.m_buckets)
                , m_pos(other.m_pos)
                , m_iit((IIt)other.m_iit)
            {
            }

            IteratorBase& operator ++ ()
            {
                ++m_iit;
                
                if (m_iit == (*m_buckets)[m_pos].m_items.End())
                {
                    ++m_pos;
                    SkipToNext();
                }

                return *this;
            }

            IteratorBase operator ++ (int)
            {
                IteratorBase tmp(*this);
                operator ++ ();
                return tmp;
            }

            T& operator * () const
            {
                return *m_iit;
            }
            
            T* operator -> () const
            {
                return &((*m_iit));
            }

            /*
             * Compares two iterators.
             *
             * Method returns true if either one of the following
             * conditions is met:
             * 1) Bucket iterators point to the same position and the position
             *    is not the end of buckets sequence, container iterators
             *    point to the same position;
             * 2) Bucket iterators point to the same position and the position
             *    is exactly the end of bucket sequence.
             *
             */
            bool operator == (const IteratorBase& other) const
            {
                if (m_buckets != other.m_buckets)
                    return false;
                
                if (m_pos != other.m_pos)
                    return false;

                if (m_pos == m_buckets->Size())
                    return true;
                
                return m_iit == other.m_iit;
            }

            bool operator != (const IteratorBase& other) const
            { 
                return !(operator == (other));
            }
        
            template <typename U, typename V, typename W> friend class IteratorBase;
        };


    public:
        typedef IteratorBase<BucketArrayType, ItemListIteratorType, ItemType>                  Iterator;
        typedef IteratorBase<const BucketArrayType, ConstItemListIteratorType, const ItemType> ConstIterator;
        
    private:
        static const size_t s_defaultInitialSize{ 1 << LayoutPolicyType::BucketArrayTraits::s_segmentSizeExponent };

        bool              m_inited{ false };
        CharAllocatorType m_alloc;

        const HashType    m_hash;
        const PredType    m_pred;

        size_t            m_initialSize{ s_defaultInitialSize };
        
        BucketArrayType   m_buckets;
        size_t            m_elementCnt{ 0 };
        
        // >> linear hash
        double            m_maxLoadFactor{ LayoutPolicyType::MaxLoadFactor() };
        size_t            m_nextBucketToBeSplitIdx{ 0 }; // index of the next bucket to be split
        size_t            m_maxNextBucketToBeSplitIdx{ 0 }; // upper bound on m_nextBucketToBeSplitIdx during this expansion
        // << linear hash

    private:
        // >> linear hash
        
        /*
         * Returns the bucket index for the hash.
         *
         * First, computes hash mod "table size before the expansion" 
         * (m_maxNextBucketToBeSplit).
         * If the bucket index is less than the current value of the index 
         * to the next bucket to be split, the corresponding bucket has already
         * been split, otherwise not. If the bucket has been split,
         * the correct index is given by hash mod "table size after expansion" 
         * (2 * m_maxNextBucketToBeSplit).
         */
        size_t Pos(INT64 hash) const
        {
            size_t result = hash & (m_maxNextBucketToBeSplitIdx - 1); // hash mod m_maxNextBucketToBeSplitIdx

            if (result < m_nextBucketToBeSplitIdx)
                result = hash & ((m_maxNextBucketToBeSplitIdx << 1) - 1); // hash mod (2 * m_maxNextBucketToBeSplitIdx)

            return result;
        }
        
        template <typename HashType>
        size_t Pos(const KeyType& key, HashType& hash) const { return Pos(hash(key)); }
        size_t Pos(const KeyType& key) const { return Pos(key, m_hash); }
        
        void SplitNextBucket()
        {   
            SCOPE_ASSERT(!BucketArrayOutOfCapacity());

            const size_t oldPos = m_nextBucketToBeSplitIdx;
            
            // BucketArrayType::EmplaceBack allocates memory, but if it fails the intenal hashtable state
            // is not corrupted
            m_buckets.EmplaceBack();
            // update linear hash state after split
            {
                ++m_nextBucketToBeSplitIdx;
                // if the next to be split bucket index has reached 
                // the table size before expansion, start over from 0
                // and set the maximum to the table size after expansion. 
                if (m_nextBucketToBeSplitIdx == m_maxNextBucketToBeSplitIdx)
                {
                    m_maxNextBucketToBeSplitIdx *= 2;
                    m_nextBucketToBeSplitIdx = 0;
                }
            }

            // rehash items between the old and the new buckets
            const size_t newPos = m_buckets.Size() - 1;
            LayoutPolicyType::ItemListSplice(m_buckets[oldPos].m_items,
                                             m_buckets[newPos].m_items,
                                             [this, newPos] (const ItemType& item)          // Pred
                                             {
                                                return Pos(Traits::Key(item)) == newPos;
                                             },
                                             [this] (ItemType* dest, const ItemType& src)   // Copy
                                             {
                                                Traits::Copy<LayoutPolicyType>(dest, src, m_alloc);
                                             });

        }
        
        double LoadFactorAfterInsert() const { return double(m_elementCnt + 1)/m_buckets.Size(); }
        bool IsLoadFactorAfterInsertOverMax() const { return LoadFactorAfterInsert() > m_maxLoadFactor; }
        bool BucketArrayOutOfCapacity() const { return m_buckets.Size() == m_buckets.MaxCapacity(); }  
        // >> linear hash
        

        template <typename T, typename It, typename ItemListType, typename PredType>
        It ItemItImpl(ItemListType& items, const T& key, const PredType& pred) const
        {
            return std::find_if(items.Begin(), items.End(),
                               [&pred, &key] (const ItemType& item) { return pred(key, Traits::Key(item)); });
        }

        template <typename T, typename PredType>
        ItemListIteratorType ItemIt(ItemListType& items, const T& key, const PredType& pred) const
        {
            return ItemItImpl<T, ItemListIteratorType, ItemListType, PredType>(items, key, pred);
        }
        
        template <typename T, typename PredType>
        ConstItemListIteratorType ItemIt(const ItemListType& items, const T& key, const PredType& pred) const
        {
            return ItemItImpl<T, ConstItemListIteratorType, const ItemListType, PredType>(items, key, pred);
        }
        
        ItemListIteratorType ItemIt(ItemListType& items, const KeyType& key) const
        {
            return ItemItImpl<KeyType, ItemListIteratorType, ItemListType, PredType>(items, key, m_pred);
        }
        
        ConstItemListIteratorType ItemIt(const ItemListType& items, const KeyType& key) const
        {
            return ItemItImpl<KeyType, ConstItemListIteratorType, const ItemListType, PredType>(items, key, m_pred);
        }
        
        // devnote: the hash join relies on that the internal state of the hashtable stays
        // consistent if any of the memory allocation calls fail
        std::pair<Iterator, bool> Insert(INT64 hash, const ItemType& item)
        {
            bool inserted = false;
            try
            {
                size_t origPos = Pos(hash);
                auto& origItems = m_buckets[origPos].m_items;
                auto origItemIt = ItemIt(origItems, Traits::Key(item));

                if (origItemIt == origItems.End())
                {
                    // expand the table if the load factor has reached the maximum allowed
                    // devnote: SplitNextBucket allocates memory in cases when bucket array expansion
                    // allocates memory
                    const bool expand = IsLoadFactorAfterInsertOverMax() && !BucketArrayOutOfCapacity();
                    if (expand) { SplitNextBucket(); }
                    
                    size_t pos = Pos(hash);
                    auto& items = m_buckets[pos].m_items;
                    
                    // devnote: ItemListType::EmplaceFront allocates memory, but if it fails the intenal hashtable state
                    // is not corrupted
                    items.EmplaceFront(m_alloc, item);
                    ++m_elementCnt;
                    
                    // devnote: this assignement must happen after all potention memory allocation
                    inserted = true; 

                    return std::make_pair(Iterator(&m_buckets, pos, items.Begin()), true);
                }

                return std::make_pair(Iterator(&m_buckets, origPos, origItemIt), false);
            }
            catch (RuntimeMemoryException& e)
            {
                // devnote: element should not be inserted if the allocator run out of memory
                SCOPE_ASSERT(!inserted && "Hashtable internal state is corrupted");
                throw e;
            }
        }
        

        void Init()
        {
            SCOPE_ASSERT(m_initialSize != 0 && !(m_initialSize & (m_initialSize - 1))); //initial size is a power of two 
            SCOPE_ASSERT(m_initialSize <= BucketArrayType::MaxCapacity());

            for (size_t i = 0; i < m_initialSize; ++i) { m_buckets.EmplaceBack(); }

            m_inited = true;
        }

    public:
        SlimHashtable(AllocatorType alloc)
            : m_alloc(alloc)
            , m_buckets(m_alloc, m_initialSize)
            , m_maxNextBucketToBeSplitIdx(m_initialSize)
        {
            Init();
        }
        
        SlimHashtable(AllocatorType alloc, size_t initialSize, const HashType hash = HashType(), const PredType pred = PredType())
            : m_alloc(alloc)
            , m_hash(hash)
            , m_pred(pred)
            , m_initialSize(initialSize)
            , m_buckets(m_alloc, m_initialSize)
            , m_maxNextBucketToBeSplitIdx(m_initialSize)
        {
            Init();
        }

        SlimHashtable(AllocatorType alloc, size_t initialSize, double maxLoadFactor, const HashType hash = HashType(), const PredType pred = PredType())
            : m_alloc(alloc)
            , m_hash(hash)
            , m_pred(pred)
            , m_initialSize(initialSize)
            , m_maxLoadFactor(maxLoadFactor)
            , m_buckets(m_alloc, m_initialSize)
            , m_maxNextBucketToBeSplitIdx(m_initialSize)
        {
            Init();
        }
    
        std::pair<Iterator, bool> InsertWithPrecomputedHash(INT64 hash, const ItemType& item)
        {
            return Insert(hash, item);
        }
        
        std::pair<Iterator, bool> Insert(const ItemType& item)
        {
            return InsertWithPrecomputedHash(m_hash(Traits::Key(item)), item);
        }
        
        /*
         * This function provides the optimization for the hash join.
         * Note, that the _hash_ value must be the same that would be
         * computed be m_hash(key).
         *
         */
        template <typename T, typename PredType>
        Iterator FindWithPrecomputedHash(INT64 hash, const T& key, PredType& pred)
        {
            size_t pos = Pos(hash);
            ItemListType& items = m_buckets[pos].m_items;
            auto it = ItemIt(items, key, pred);

            if (it != items.End())
                return Iterator(&m_buckets, pos, it);
            else
                return End();

        }
        
        /*
         * This function provides the optimization for the hash join.
         * Note, that the _hash_ value must be the same that would be
         * computed be m_hash(key).
         *
         */
        template <typename T, typename PredType>
        ConstIterator FindWithPrecomputedHash(INT64 hash, const T& key, PredType& pred) const
        {
            return ConstIterator(const_cast<SlimHashtable*>(this)->FindWithPrecomputedHash(hash, key, pred));
        }
        
        template <typename T, typename HashType, typename PredType>
        Iterator Find(const T& key, HashType& hash, PredType& pred)
        {
            return FindWithPrecomputedHash(hash(key), key, pred);
        }
        
        template <typename T, typename HashType, typename PredType>
        ConstIterator Find(const T& key, HashType& hash, PredType& pred) const
        {
            return FindWithPrecomputedHash(hash(key), key, pred);
        }

        Iterator Find(const KeyType& key)
        {
            return Find(key, m_hash, m_pred);
        }
        
        ConstIterator Find(const KeyType& key) const
        {
            return Find(key, m_hash, m_pred);
        }

        Iterator Begin() { return Iterator(&m_buckets, 0); }
        Iterator End() { return Iterator(&m_buckets, m_buckets.Size()); }
        
        ConstIterator Begin() const { return ConstIterator(&m_buckets, 0); }
        ConstIterator End() const { return ConstIterator(&m_buckets, m_buckets.Size()); }

        size_t Size() const { return m_elementCnt; }
        bool   Empty() const { return m_elementCnt == 0; }
        size_t BucketCount() const { return m_buckets.Size(); }
        size_t BucketSize(size_t n) const { return m_buckets[n].m_items.Size(); }
    };

    /*
     * Traits required to make SlimHashtable use standart (i.e. suitable for most data types) layout:
     * 1) Use regular singly linked list as collision list, such as the list head contains no 
     *    data but the pointer the first element.
     * 2) Keep load factor <= 2. That gives reasonable trade off between number of allocated bucket
     *    and the average length of the collision list.
     * 3) Such lists have no memory overhead when spliced.
     *
     */
    template <typename Item, typename CharAllocator>
    class DefaultLayoutPolicy
    {
    public:
        typedef GranularList<Item, 0, CharAllocator> ItemListType;
        static_assert(ItemListType::s_segmentSize == 1, "Item list segment size must be 1");
        
        static       double MaxLoadFactor() { return 2.; }
        static const bool   s_needsItemCopy = false;
        
        template <typename Pred, typename Copy>
        static void ItemListSplice(ItemListType& src, ItemListType& dest, Pred pred, Copy/*copy*/)
        {
            src.Splice(pred, dest);
        }
    };
    
    /*
     * Traits required to make SlimHashtable use layout optimized for small sized Item types:
     * 1) Use special kind of singly linked list as a collision list, such as first N (defined equal to 2) elements
     *    are stored in the list head directly. 
     * 2) Keep load factor <= 1. It is very important to avoid the collision lists to grow beyond the number
     *    of inlined elements. 
     * 3) Such lists of size > N have memory overhead when spliced.
     * 4) Recommended for types that are <= 16 bytes.
     *
     */
    template <typename Item, typename CharAllocator>
    class InlineLayoutPolicy
    {
    public:
        typedef InlineList<Item, 2, CharAllocator> ItemListType;
        
        static       double MaxLoadFactor() { return 1.; }
        static const bool   s_needsItemCopy = true;
        
        template <typename Pred, typename Copy>
        static void ItemListSplice(ItemListType& src, ItemListType& dest, Pred pred, Copy copy)
        {
            src.Splice(pred, dest, copy);
        }
    };
    
    template <typename Item, typename CharAllocator>
    class SlimHashtableLayoutPolicy
    {
    public:
// [TFS 7352331] sizeof(Item) <= 12 since 12 is less than size of string/binary structure
// InlineLayoutPolicy has issues in list splicing while copying schema with types that requires deep copies
        static_assert(sizeof(FixedArrayType<char>) > 12, "Inline hashtable policy requires only simple types in the key schema");  
        typedef typename std::conditional_t<sizeof(Item) <= 12,
                                            InlineLayoutPolicy<Item, CharAllocator>,
                                            DefaultLayoutPolicy<Item, CharAllocator>> Type;
    };

    struct SHUtil
    {
        template <typename T, typename Allocator>
        static std::enable_if_t<std::is_trivially_copy_constructible<T>::value, void>
        CopyConstruct(T* dest, const T& src, Allocator&)
        {
            new (dest) T(src);
        }

        template <typename T, typename Allocator>
        static std::enable_if_t<!std::is_trivially_copy_constructible<T>::value, void>
        CopyConstruct(T* dest, const T& src, Allocator& alloc)
        {
            new (dest) T(src, alloc.allocator());
        }
    };

    /* Traits required to make SlimHashtable behave like a map. */
    template <typename Key, 
              typename Value,
              template <typename Item, typename CharAllocator> class LayoutPolicy,
              typename Hash = Hash<Key>,
              typename Pred = EqualTo<Key>,
              typename Allocator = DefaultSTLIncrementalAllocator>
    class SHMapTraits
    {
    public:
        typedef          Allocator                                   AllocatorType;
        typedef typename AllocatorType::template rebind<char>::other CharAllocatorType;
        
        typedef          Key                                         KeyType;
        typedef          Value                                       ValueType;
        typedef          std::pair<Key, Value>                       ItemType;
        typedef typename LayoutPolicy<ItemType, CharAllocatorType>   LayoutPolicyType;
        typedef          Hash                                        HashType;
        typedef          Pred                                        PredType;
        
        static const KeyType& Key(const ItemType& item)
        {
            return item.first;
        }
        
        /* devnote: That function aims to make a shallow copy of the Item
         *          within the same allocator.
         *          It expects the ValueType to be either a GranularList
         *          or a generated schema:
         *          1) GranularList or any other type with a implicit copy
         *          constructor will be copied using that contstructor which
         *          will create a shallow copy.
         *          2) Generated schemas have only one explicit copy contructor that 
         *          takes an IncrementalAllocator as a parameter. That allocator
         *          is only used when the schema has deep data, e.g. FString. Although
         *          it is possible to generate a second copy contructor for schemas
         *          whithout deep data it seems risky. Instead, the function passes
         *          the alloctor to the constructor and relies on the fact that
         *          deep data is not copied within the same IncrementalAllocator
         *          (see SharedPtr<T>::CopySharedPtr()). If, in fact, a deep copy
         *          happens because of a bug, the functionality will still be correct,
         *          it will only add additional memory overhead.
         */
        template <typename T>
        static std::enable_if_t<T::s_needsItemCopy, void>
        Copy(ItemType* dest, const ItemType& src, CharAllocatorType& alloc)
        {
           SHUtil::CopyConstruct(&dest->first, src.first, alloc);
           SHUtil::CopyConstruct(&dest->second, src.second, alloc);
        }
        
        template <typename T>
        static std::enable_if_t<!T::s_needsItemCopy, void>
        Copy(ItemType* dest, const ItemType& src, CharAllocatorType& alloc)
        {
        }
    };
    
    /* Traits required to make SlimHashtable behave like a set. */
    template <typename Key, 
              template <typename Item, typename CharAllocator> class LayoutPolicy,
              typename Hash = Hash<Key>,
              typename Pred = EqualTo<Key>,
              typename Allocator = DefaultSTLIncrementalAllocator>
    class SHSetTraits
    {
    public:
        typedef          Allocator                                   AllocatorType;
        typedef typename AllocatorType::template rebind<char>::other CharAllocatorType;
        
        typedef          Key                                         KeyType;
        typedef          Key                                         ItemType;
        typedef typename LayoutPolicy<ItemType, CharAllocatorType>   LayoutPolicyType;
        typedef          Hash                                        HashType;
        typedef          Pred                                        PredType;
        
        static const KeyType& Key(const ItemType& item)
        {
            return item;
        }
        
        /* 
         * devnote: deep data will not be copied within the same IncrementalAllocator,
         *          see SharedPtr<T>::CopySharedPtr()
         */
        template <typename T>
        static std::enable_if_t<T::s_needsItemCopy, void>
        Copy(ItemType* dest, const ItemType& src, CharAllocatorType& alloc)
        {
           SHUtil::CopyConstruct(dest, src, alloc);
        }
        
        template <typename T>
        static std::enable_if_t<!T::s_needsItemCopy, void>
        Copy(ItemType* dest, const ItemType& src, CharAllocatorType& alloc)
        {
        }
    };
    
    /* SlimHashMap is an associative container that stores key-value pairs with unique keys. */
    template <typename Key, 
              typename Value,
              template <typename Item, typename CharAllocator> class LayoutPolicy,
              typename Hash = Hash<Key>,
              typename Pred = EqualTo<Key>,
              typename Allocator = DefaultSTLIncrementalAllocator>
    class SlimHashMap : public SlimHashtable<SHMapTraits<Key, Value, LayoutPolicy, Hash, Pred, Allocator>>
    {
    private:
        typedef SlimHashtable<SHMapTraits<Key, Value, LayoutPolicy, Hash, Pred, Allocator>> BaseType;

    public:
        SlimHashMap(Allocator alloc) 
            : BaseType(alloc)
        {
        }
        
        SlimHashMap(Allocator alloc, size_t initialSize, const Hash hash = Hash(), const Pred pred = Pred())
            : BaseType(alloc, initialSize, hash, pred)
        {
        }

        SlimHashMap(Allocator alloc, size_t initialSize, double maxLoadFactor, const Hash hash = Hash(), const Pred pred = Pred())
            : BaseType(alloc, initialSize, maxLoadFactor, hash, pred)
        {
        }
    };
    
    /* SlimHashSet is an container that stores unique elements of type Key. */
    template <typename Key, 
              template <typename Item, typename CharAllocator> class LayoutPolicy,
              typename Hash = Hash<Key>,
              typename Pred = EqualTo<Key>,
              typename Allocator = DefaultSTLIncrementalAllocator>
    class SlimHashSet : public SlimHashtable<SHSetTraits<Key, LayoutPolicy, Hash, Pred, Allocator>>
    {
    private:
        typedef SlimHashtable<SHSetTraits<Key, LayoutPolicy, Hash, Pred, Allocator>> BaseType;
    
    public:
        SlimHashSet(Allocator alloc) 
            : BaseType(alloc)
        {
        }
        
        SlimHashSet(Allocator alloc, size_t initialSize, const Hash hash = Hash(), const Pred pred = Pred())
            : BaseType(alloc, initialSize, hash, pred)
        {
        }

        SlimHashSet(Allocator alloc, size_t initialSize, double maxLoadFactor, const Hash hash = Hash(), const Pred pred = Pred())
            : BaseType(alloc, initialSize, maxLoadFactor, hash, pred)
        {
        }
    };
}


