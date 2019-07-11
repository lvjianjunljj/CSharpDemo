#pragma once

#include "scopeengine.h"
#include "scopecontainers.h"
#include <string>
#include <vector>
#include <forward_list>

namespace ScopeEngine
{
    /**** ALLOCATOR ****/

    /*
     * HeapAllocator is STL compatible memory allocator
     * that uses Windows heap to dynamically handle its storage needs.
     */
    template <typename T>
    class HeapAllocator
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
            typedef HeapAllocator<U> other;
        };

    private:
        HANDLE m_heap;

    public:
        HeapAllocator()
            : m_heap(NULL)
        {
        }

        HeapAllocator(HANDLE heap)
            : m_heap(heap)
        {
            SCOPE_ASSERT(m_heap != NULL);
        }

        HeapAllocator(const HeapAllocator& alloc)
            : m_heap(alloc.m_heap)
        {
        }

        template <typename U>
        HeapAllocator(const HeapAllocator<U>& alloc)
            : m_heap(alloc.m_heap)
        {
        }

        pointer allocate(SIZE_T n)
        {
            SIZE_T size = n * sizeof(value_type);
            void* memory = nullptr;

            memory = HeapAlloc(m_heap, 0, size);
            
            if (GetLastError() == ERROR_NOT_ENOUGH_MEMORY)
            {
                cout << MemoryManager::GetProcessMemoryStatistic();                
                std::ostringstream message;
                message << "Could not allocate memory: ";
                message << size;
                throw RuntimeException(E_SYSTEM_HEAP_OUT_OF_MEMORY, message.str().c_str());
            }

            SCOPE_ASSERT(memory);

            return reinterpret_cast<pointer>(memory);
        }

        void deallocate(pointer p, SIZE_T /*n*/)
        {
            BOOL deallocated = HeapFree(m_heap, 0, p);

            SCOPE_ASSERT(deallocated);
        }

        void construct(pointer p, const T& t)
        {
            new ((void*)p) T(t);
        }

        // this cctor enables stl containers to use
        // move instead of copy when possible
        template<class U, class... Args>
		void construct(U* p, Args&&... args)
        {
		    new ((void*)p) U(std::forward<Args>(args)...);
		}

        void destroy(pointer p)
        {
            p->T::~T();
        }

        bool operator == (const HeapAllocator& other) const
        {
            return m_heap == other.m_heap;
        }
        
        bool operator != (const HeapAllocator& other) const
        {
            return !operator == (other);
        }

        template <class U> friend class HeapAllocator;
    };

    // Void specialization of HeapAllocator used for convenience.
    template <>
    class HeapAllocator<void>
    {
    public:
        typedef void value_type;
        typedef void* pointer;
        typedef const void* const_pointer;

        template <typename U>
        struct rebind
        {
            typedef HeapAllocator<U> other;
        };

    private:
        HANDLE m_heap;

    public:
        HeapAllocator()
            : m_heap(NULL)
        {
        }
        
        HeapAllocator(HANDLE heap)
            : m_heap(heap)
        {
            SCOPE_ASSERT(m_heap != NULL);
        }
        
        HeapAllocator(const HeapAllocator& alloc)
            : m_heap(alloc.m_heap)
        {
        }
        
        template <typename U>
        HeapAllocator(const HeapAllocator<U>& alloc)
            : m_heap(alloc.m_heap)
        {
        }
        
        bool operator == (const HeapAllocator& other) const
        {
            return m_heap == other.m_heap;
        }
        
        bool operator != (const HeapAllocator& other) const
        {
            return !operator == (other);
        }
        
        template <class U> friend class HeapAllocator;
    };

    typedef HeapAllocator<void> DefaultHeapAllocator;

    /*
     * Heap is a wrapper around Windows heap that manages
     * its construction and destruction.
     */
    class Heap
    {
    private:    
        HANDLE m_handle;

    public:
        Heap()
            : m_handle(HeapCreate(HEAP_NO_SERIALIZE, 0, 0))
        {
            SCOPE_ASSERT(m_handle != NULL);
        }

        const HANDLE Ptr() const 
        {
            return m_handle;
        }

        ~Heap()
        {
            HeapDestroy(m_handle);
        }
    };

    /*************************************************/
    

    /*
     * CollectDeepDataPtrs stores all FString/FBinary buffer pointers
     * to the provided ptrs array.
     
     * The function should be code gened
     * for schema classes that are used as Hashtable
     * KeySchema and ValueSchema. 
     *
     */
    void CollectDeepDataPtrs(const NullSchema& schema, std::vector<const char*>& ptrs);

    /*
     * MutableValueContainer is Hashtable storage unit. It stores 
     * a key and a value pointer. Supports update operation for the value.
     *
     * The class is not responsible for value memory management.
     * 
     *
     */
    template <typename KeySchema, typename ValueSchema, typename Allocator>
    class MutableValueContainer
    {
    public:
        typedef std::pair<KeySchema, ValueSchema*>                       KeyValue; 

    private:
        KeyValue      m_kv;

    public:
        template <typename Copier>
        MutableValueContainer(const KeySchema& key, ValueSchema* const value, Copier& copier, Allocator&)
            : m_kv(KeySchema(key, &copier), value)
        {
        }

        const KeySchema& Key() const
        {
            return m_kv.first;
        }

        KeyValue& Both()
        {
            return m_kv;
        }

        void Update(const ValueSchema& value)
        {
            *m_kv.second = value;
        }
    };


    /*
     * ListOfValuesContainer is Hashtable storage unit. It stores 
     * a key and a list of value pointer.
     *
     * The class is not responsible for memory management.
     */
    template <typename KeySchema, typename ValueSchema, typename Allocator>
    class ListOfValuesContainer
    {
    public:		
        typedef          std::forward_list<const ValueSchema*, Allocator>   Values;
        typedef          std::pair<const KeySchema, Values>                 KeyValue;

    private:
        KeyValue     m_kv;

    public:
        template <typename Copier>
        ListOfValuesContainer(const KeySchema& key, const ValueSchema* value, Copier& copier, Allocator& alloc)
            : m_kv(KeySchema(key, &copier), Values(alloc))
        {
            Add(value);
        }
        
        const KeySchema& Key() const
        {
            return m_kv.first;
        }
        
        const KeyValue& Both() const
        {
            return m_kv;
        }

        void Add(const ValueSchema* value)
        {
            m_kv.second.push_front(value);
        }
    };

    template <typename KeySchema, typename ValueSchema, typename Allocator>
    class ListOfValuesContainerWithStats : public ListOfValuesContainer<KeySchema, ValueSchema, Allocator>
    {
    public:        
        class Statistics
        {
        private:
            LONGLONG m_buildSize;
            LONGLONG m_probeSize;

        public:
            Statistics(LONGLONG buildSize, LONGLONG probeSize)
            {
                m_buildSize = buildSize;
                m_probeSize = probeSize;
            }

            LONGLONG BuildSize() const { return m_buildSize; }
            LONGLONG ProbeSize() const { return m_probeSize; }

            void IncBuildSize() { ++m_buildSize; }
            void IncProbeSize() { ++m_probeSize; }
        };

    private:
        mutable Statistics m_stats;

    public:
        template <typename Copier>
        ListOfValuesContainerWithStats(const KeySchema& key, const ValueSchema* value, Copier& copier, Allocator& alloc) :
            ListOfValuesContainer(key, value, copier, alloc),
            m_stats(1, 0) // account for value added in ListOfValuesContainer ctor
        {
        }

        void Add(const ValueSchema* value)
        {
            ListOfValuesContainer<KeySchema, ValueSchema, Allocator>::Add(value);
            m_stats.IncBuildSize();
        }

        Statistics& Stats() const { return m_stats; }
    };

    /*
     * SingletonContainer is Hashtable storage unit. It stores only key.
     * The class is not responsible for memory management.
     *
     * TODO Values are not really necessary and can be further optimized
     */
    template <typename KeySchema, typename ValueSchema, typename Allocator>
    class SingletonContainer : public ListOfValuesContainer<KeySchema, ValueSchema, Allocator>
    {
    public:
        template <typename Copier>
        SingletonContainer(const KeySchema& key, const ValueSchema* value, Copier& copier, Allocator& alloc)
            : ListOfValuesContainer(key, value, copier, alloc)
        {}
 
        void Add(const ValueSchema* value)
        {
            static_assert(false, "Not implemented method");
        }
    };

    struct NoOpDeleter
    {
        template <typename T>
        void operator () (T*) const {}
    };

    /*
     * AutoExpandArray is a sequence container that represents dynamically growing array.
     * The array is devided into segments of fixed size. When the array grows the segments
     * are allocated as needed. A directory keeps track of the pointers to segments. 
     * The directory is allocated statically.
     *
     * The array supports fast random access and has an upper limit on the size (directory_size * segment_size).
     *
     * The array does not own the memory it uses.
     *
     * T                   -- type of the elements;
     * DirectorySize       -- maximum number of segments in the array;
     * SegmentSizeExponent -- defines the segment size as 2 ^ s_segmentSizeExponent.
     * Allocator           -- type of the allocator is used to aquire memory to store the elements. 
     *                        It must meet provide rebind interface, allocate and construct methods.
     *
     */
    template <typename T, size_t DirectorySize, size_t SegmentSizeExponent, class Allocator = DefaultSTLIncrementalAllocator>
    class AutoExpandArray
    {
    private:
        typedef typename Allocator::template rebind<char>::other CharAllocator;

        CharAllocator m_alloc;
        std::unique_ptr<T*[], NoOpDeleter> m_directory;

        size_t m_segmentCnt{ 0 };
        size_t m_elementCnt{ 0 };
        
    public:
        static const size_t s_segmentSize = (1 << SegmentSizeExponent);
        static const size_t s_segmentSizeInBytes = s_segmentSize * sizeof(T);

    private:
         void Init()
         {
            m_directory.reset(new (m_alloc.allocate(sizeof(T*) * DirectorySize)) T*[DirectorySize]()); // init with default values
         }

        void AppendSegments(size_t n)
        {
            if (!(m_segmentCnt + n <= DirectorySize)) throw RuntimeInternalErrorException("AutoExpandArray has reached its max capacity");
            
            T* segments = reinterpret_cast<T*>(m_alloc.allocate(sizeof(T) * s_segmentSize * n));
            
            for (size_t i = 0; i < n; ++i)
            { 
                m_directory[m_segmentCnt] = segments + (i * s_segmentSize);
                ++m_segmentCnt;
            }
        }

        T* NextLocation()
        {
            const size_t directoryIdx = m_elementCnt >> SegmentSizeExponent;
            if (m_segmentCnt == 0 || directoryIdx == m_segmentCnt) { AppendSegments(1); }

            return &(m_directory[directoryIdx][m_elementCnt & (s_segmentSize - 1)]);
        }

    public:
        AutoExpandArray(Allocator alloc)
            : m_alloc(alloc)
        {
            Init();
        }
        
        AutoExpandArray(Allocator alloc, size_t initCapacity)
            : m_alloc(alloc)
        {
            SCOPE_ASSERT(initCapacity <= MaxCapacity());
            
            Init();
            size_t n = (initCapacity >> SegmentSizeExponent) + ((initCapacity & (s_segmentSize - 1)) == 0 ? 0 : 1);
            AppendSegments(n);
        }

        void PushBack(const T& value)
        {
            T* ptr = NextLocation();
            *ptr = value;
            ++m_elementCnt;
        }

        // if the allocation fails the internal state is not corrupted 
        template <class... Args>
        void EmplaceBack(Args&&... args)
        {
            T* ptr = NextLocation();
            m_alloc.construct(ptr, std::forward<Args>(args)...);
            ++m_elementCnt;
        }

        const T& operator[](size_t pos) const
        {
            return m_directory[pos >> SegmentSizeExponent][pos & (s_segmentSize - 1)];
        } 
        
        T& operator[](size_t pos)
        {
            return const_cast<T&>(const_cast<const AutoExpandArray*>(this)->operator[](pos));    
        }

        const T& Back() const
        {
            return operator [] (m_elementCnt - 1);
        }
        
        T& Back()
        {
            return const_cast<T&>(const_cast<const AutoExpandArray*>(this)->Back());    
        }

        size_t Size() const { return m_elementCnt; }
        size_t Capacity() const { return m_segmentCnt * s_segmentSize; }
        
        static size_t MaxCapacity() { return DirectorySize * s_segmentSize; }
    };

    /*
     * GranularList is a sequence container that represents grow-only singled linked list of segments.
     * The list is devided to segments of fixed size. When the list grows the segments
     * are allocated as needed. 
     *
     * The list does not support fast random access and has no upper limit on size.
     
     * The list does not own the memory it uses.
     *
     * T                   -- type of the elements;
     * SegmentSizeExponent -- defines the segment size as 2 ^ SegmentSizeExponent.
     * Allocator           -- type of the allocator is used to aquire memory to store the elements. 
     *              It must meet provide rebind interface, allocate and construct methods.
     */
    template <typename T, size_t SegmentSizeExponent, class Allocator>
    class GranularList
    {
        static_assert(sizeof(typename Allocator::value_type) == 1, "Allocator has incorrect value type");
    
    public:
        static const size_t s_segmentSize = (1 << SegmentSizeExponent);
        static const size_t s_segmentSizeInBytes = s_segmentSize * sizeof(T);

    private:
        struct Segment
        {
            T*       m_values;
            Segment* m_next{ nullptr };

            Segment(T* values)
                : m_values(values)
            {
            }
        };


        template <typename T>
        class IteratorBase : public iterator<forward_iterator_tag, T>
        {
        private:
            Segment* m_segment{ nullptr };
            size_t   m_pos{ 0 };

        public:
            IteratorBase() {}
            
            IteratorBase(Segment* segment, size_t pos)
                : m_segment(segment)
                , m_pos(pos)
            {
            }

            IteratorBase(const IteratorBase&) = default;
            IteratorBase& operator = (const IteratorBase&) = default;
            
            template<typename U>
            IteratorBase(const IteratorBase<U>& other)
                : m_segment(other.m_segment)
                , m_pos(other.m_pos)
            {
            }


            IteratorBase& operator ++ ()
            {
                if (m_pos > 0)
                {
                    --m_pos;
                }
                else
                {
                    m_segment = m_segment->m_next;
                    m_pos = s_segmentSize - 1;
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
                return m_segment->m_values[m_pos];
            }
            
            T* operator -> () const
            {
                return &(m_segment->m_values[m_pos]);
            }
            
            bool operator == (const IteratorBase& other) const
            {
                return m_segment == other.m_segment && m_pos == other.m_pos;
            }

            bool operator != (const IteratorBase& other) const
            {
                return !(operator == (other)); 
            }
            
            template <typename U> friend class IteratorBase;
        };

    private:
        Segment* m_firstSegment{ nullptr };
        size_t   m_elementCnt{ 0 };

    private:
        void AppendSegment(Segment* segment)
        {
            Segment* tmp = m_firstSegment;
            m_firstSegment = segment;
            m_firstSegment->m_next = tmp;
        }
        
        void AppendSegment(Allocator& alloc)
        {
            void* values = alloc.allocate(sizeof(T) * s_segmentSize);
            Segment* segment = new (alloc.allocate(sizeof(Segment))) Segment(reinterpret_cast<T*>(values));

            AppendSegment(segment);
        }

        bool FirstSegmentIsFilledUp() const
        {
            return (m_elementCnt & (s_segmentSize - 1)) == 0;
        }

        T* NextLocation(Allocator& alloc)
        {
            if (!m_firstSegment || FirstSegmentIsFilledUp()) { AppendSegment(alloc); }
            return &(m_firstSegment->m_values[m_elementCnt & (s_segmentSize - 1)]);
        }

        size_t FirstPos() const
        {
            if (m_elementCnt) { return (m_elementCnt - 1) & (s_segmentSize - 1); }
            
            return s_segmentSize - 1;
        }

    public:   
        typedef typename IteratorBase<T> Iterator;
        typedef typename IteratorBase<const T> ConstIterator;

    public:
        void PushFront(Allocator& alloc, const T& value)
        {
            T* ptr = NextLocation(alloc);
            *ptr = value;
            ++m_elementCnt;
        }
        
        // if the allocation fails the internal state is not corrupted 
        template <class... Args>
        void EmplaceFront(Allocator& alloc, Args&&... args)
        {
            T* ptr = NextLocation(alloc);
            alloc.construct(ptr, std::forward<Args>(args)...);
            ++m_elementCnt;
        }

        Iterator Begin() { return Iterator(m_firstSegment, FirstPos()); }
        Iterator End() { return Iterator(nullptr, s_segmentSize - 1); }

        ConstIterator Begin() const { return ConstIterator(m_firstSegment, FirstPos()); }
        ConstIterator End() const { return ConstIterator(nullptr, s_segmentSize - 1); }

        /*
         * Splice transfers segments from one list to another if all
         * the elements in the segment satisfy the predicate. No elements
         * are copied or moved, only the internal pointers are redecirected.
         * Does not preserve original order of elements.
         */
        template <typename Pred>
        void Splice(Pred pred, GranularList<T, SegmentSizeExponent, Allocator>& other)
        {
            Segment* next = nullptr;
            Segment* cur = m_firstSegment;
            while (cur)
            {
                const size_t segmentSize = (cur == m_firstSegment ? FirstPos() + 1 : s_segmentSize);
                const bool segmentSatisfiesPred = std::all_of(cur->m_values, cur->m_values + segmentSize, pred);

                if (segmentSatisfiesPred)
                {
                    if (next) { next->m_next = cur->m_next; }
                    if (cur == m_firstSegment) { m_firstSegment = cur->m_next; }
                    
                    Segment* tmp = cur;
                    cur = cur->m_next;
                    tmp->m_next = nullptr;
                    other.AppendSegment(tmp);

                    m_elementCnt -= segmentSize;
                    other.m_elementCnt += segmentSize;
                }
                else
                {
                    next = cur;
                    cur = cur->m_next;
                }
            }
        }

        size_t Size() const { return m_elementCnt; }
    };
    
    /* 
     * GranularList specialization for segments of size one. In this case it
     * is just a simple single linked list which allows to optimizations:
     * 1) segments store element by value rather than by pointer;
     * 2) iterator increment does not need a condition check. 
     */ 
    template <typename T, class Allocator>
    class GranularList<T, 0, Allocator>
    {
        static_assert(sizeof(typename Allocator::value_type) == 1, "Allocator has incorrect value type");
        static_assert(std::is_default_constructible<T>::value, "T does not have a default constructor");
    
    public:
        static const size_t s_segmentSize = 1;
        static const size_t s_segmentSizeInBytes = s_segmentSize * sizeof(T);

    private:
        struct Segment
        {
            T        m_value;
            Segment* m_next{ nullptr };
        };

        template <typename T>
        class IteratorBase : public iterator<forward_iterator_tag, T>
        {
        private:
            Segment* m_segment{ nullptr };

        public:
            IteratorBase() {}
            
            IteratorBase(Segment* segment)
                : m_segment(segment)
            {
            }

            IteratorBase(const IteratorBase&) = default;
            IteratorBase& operator = (const IteratorBase&) = default;

            template<typename U>
            IteratorBase(const IteratorBase<U>& other)
                : m_segment(other.m_segment)
            {
            }

            IteratorBase& operator ++ ()
            {
                m_segment = m_segment->m_next;

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
                return m_segment->m_value;
            }
            
            T* operator -> () const
            {
                return &(m_segment->m_value);
            }
            
            bool operator == (const IteratorBase& other) const
            {
                return m_segment == other.m_segment;
            }

            bool operator != (const IteratorBase& other) const
            {
                return !(operator == (other)); 
            }

            template <typename U> friend class IteratorBase;
        };

    protected:
        Segment* m_firstSegment{ nullptr };
        size_t   m_elementCnt{ 0 };
    private:
        void AppendSegment(Segment* segment)
        {
            Segment* tmp = m_firstSegment;
            m_firstSegment = segment;
            m_firstSegment->m_next = tmp;
        }
        
        void AppendSegment(Allocator& alloc)
        {
            Segment* segment = reinterpret_cast<Segment*>(alloc.allocate(sizeof(Segment)));
            segment->m_next = nullptr;

            AppendSegment(segment);
        }

        T* NextLocation(Allocator& alloc)
        {
            AppendSegment(alloc);
            return &(m_firstSegment->m_value);
        }

    public:   
        typedef typename IteratorBase<T> Iterator;
        typedef typename IteratorBase<const T> ConstIterator;

    public:
        void PushFront(Allocator& alloc, const T& value)
        {
            T* ptr = NextLocation(alloc);
            *ptr = value;
            ++m_elementCnt;
        }
        
        // if the allocation fails the internal state is not corrupted 
        template <class... Args>
        void EmplaceFront(Allocator& alloc, Args&&... args)
        {
            T* ptr = NextLocation(alloc);
            alloc.construct(ptr, std::forward<Args>(args)...);
            ++m_elementCnt;
        }

        Iterator Begin() { return Iterator(m_firstSegment); }
        Iterator End() { return Iterator(nullptr); }

        ConstIterator Begin() const { return ConstIterator(m_firstSegment); }
        ConstIterator End() const { return ConstIterator(nullptr); }
        /*
         * Splice transfers segments from one list to another if all
         * the elements in the segment satisfy the predicate. No elements
         * are copied or moved, only the internal pointers are redecirected.
         * Does not preserve original order of elements.
         */
        template <typename Pred>
        void Splice(Pred pred, GranularList<T, 0, Allocator>& other)
        {
            Segment* next = nullptr;
            Segment* cur = m_firstSegment;
            while (cur)
            {
                const bool segmentSatisfiesPred = pred(cur->m_value);

                if (segmentSatisfiesPred)
                {
                    if (next) { next->m_next = cur->m_next; }
                    if (cur == m_firstSegment) { m_firstSegment = cur->m_next; }
                    
                    Segment* tmp = cur;
                    cur = cur->m_next;
                    tmp->m_next = nullptr;
                    other.AppendSegment(tmp);

                    --m_elementCnt;
                    ++other.m_elementCnt;
                }
                else
                {
                    next = cur;
                    cur = cur->m_next;
                }
            }
        }
        
        size_t Size() const { return m_elementCnt; }
    };

    /*
     * InlineList is a sequence container that represents grow-only singly linked list of segments.
     * First inserted InlineCnt of elements are stored in a compact form in the list head.
     *
     * The list has an upper limit on size of 255.
     * The list does not own the memory used by elements in the tail.
     *
     * T                   -- type of the elements;
     * InlineCnt           -- number of inlined elements.
     * Allocator           -- type of the allocator is used to aquire memory to store the elements. 
     *                        It must provide rebind interface, allocate and construct methods.
     */
    template<typename T, size_t InlineCnt, typename Allocator>
    class InlineList
    {
        static_assert(sizeof(typename Allocator::value_type) == 1, "Allocator has incorrect value type");
        static_assert(InlineCnt && InlineCnt <= 255, "InlineCnt must be in (0, 255]");
        static_assert(std::is_default_constructible<T>::value, "T does not have a default constructor");

     private:
        struct Segment
        {
            T        m_value;
            Segment* m_next{ nullptr };
        };

        static const unsigned char s_inlineEnd = UCHAR_MAX;

        template <typename U>
        class IteratorBase : public iterator<forward_iterator_tag, U>
        {
        private:
            U*            m_inline { nullptr };
            unsigned char m_pos { s_inlineEnd };
            Segment*      m_segment { nullptr };

        public:
            IteratorBase() {}
            
            IteratorBase(U* inl, unsigned char pos, Segment* segment)
                : m_inline(inl)
                , m_pos(pos)
                , m_segment(segment)
            {
            }

            IteratorBase(const IteratorBase&) = default;
            IteratorBase& operator = (const IteratorBase&) = default;

            template <typename V>
            IteratorBase(const IteratorBase<V>& other)
                : m_inline(const_cast<U*>(other.m_inline))
                , m_pos(other.m_pos)
                , m_segment(other.m_segment)
            {
            }

            IteratorBase& operator ++ ()
            {
                if (m_segment)
                {
                    m_segment = m_segment->m_next;
                }
                else
                { 
                    SCOPE_ASSERT(m_pos != s_inlineEnd);
                    m_pos = m_pos ? m_pos - 1 : s_inlineEnd;
                }

                return *this;
            }

            IteratorBase operator ++ (int)
            {
                IteratorBase tmp(*this);
                operator ++ ();
                return tmp;
            }
                
            U& operator * () const
            {
                if (m_segment)
                {
                    return m_segment->m_value;
                }
                else
                {
                    return m_inline[m_pos];
                }
            }
            
            U* operator -> () const
            {
                return &(operator * ());
            }
            
            bool operator == (const IteratorBase& other) const
            {
                return m_pos == other.m_pos &&
                       m_segment == other.m_segment &&
                       m_inline == other.m_inline;
            }

            bool operator != (const IteratorBase& other) const
            {
                return !(operator == (other)); 
            }

            template <typename V> friend class IteratorBase;
        };

    private:
        static const size_t s_ptrMask = 0x0000FFFFFFFFFFFFULL;
        static const size_t s_tagMask = 0xFFFF000000000000ULL;
        static const unsigned char s_maxLength = UCHAR_MAX;
        
        union
        {
            Segment* m_firstSegment{ nullptr };
            struct
            {
                unsigned char m_pad[7];
                unsigned char m_elementCnt;
            } m_head;
        };

        T m_inline[InlineCnt];

    private:
        static size_t Get(Segment* ptr, size_t mask)
        {
            return reinterpret_cast<size_t>(ptr) & mask;
        }
        
        Segment* GetFirstSegmentPtr() const
        {
            return reinterpret_cast<Segment*>(Get(m_firstSegment, s_ptrMask));
        }
        
        void SetFirstSegmentPtr(Segment* ptr) 
        {
            m_firstSegment = reinterpret_cast<Segment*>(Get(ptr, s_ptrMask) | Get(m_firstSegment, s_tagMask));
        }

        unsigned char GetInlinePos() const
        {
            return m_head.m_elementCnt ? ((m_head.m_elementCnt > InlineCnt ? InlineCnt : m_head.m_elementCnt) - 1) 
                                       : s_inlineEnd;
        }

        void AppendSegment(Segment* segment)
        {
            Segment* tmp = GetFirstSegmentPtr();
            segment->m_next = tmp;
            SetFirstSegmentPtr(segment);
        }
        
        void AppendSegment(Allocator& alloc)
        {
            Segment* segment = reinterpret_cast<Segment*>(alloc.allocate(sizeof(Segment)));
            segment->m_next = nullptr;

            AppendSegment(segment);
        }

        T* NextInlineLocation()
        {
            return &m_inline[m_head.m_elementCnt];
        }

        T* NextLocation(Allocator& alloc)
        {
            if (m_head.m_elementCnt < InlineCnt)
            {
                return NextInlineLocation();
            }
            else
            {
                AppendSegment(alloc);
                return &(GetFirstSegmentPtr()->m_value);
            }
        }

        void CheckCapacity() const
        {
            if (!(m_head.m_elementCnt < s_maxLength)) { throw RuntimeInternalErrorException("InlineList has reached its max length"); }
        }

    public:   
        typedef typename IteratorBase<T> Iterator;
        typedef typename IteratorBase<const T> ConstIterator;

    public:
        // if the allocation fails the internal state is not corrupted 
        template <class... Args>
        void EmplaceFront(Allocator& alloc, Args&&... args)
        {
            CheckCapacity();
            T* ptr = NextLocation(alloc);
            alloc.construct(ptr, std::forward<Args>(args)...);
            ++m_head.m_elementCnt;
        }

        Iterator Begin() { return Iterator(m_inline, GetInlinePos(), GetFirstSegmentPtr()); }
        Iterator End() { return Iterator(m_inline, s_inlineEnd, nullptr); }

        ConstIterator Begin() const { return ConstIterator(m_inline, GetInlinePos(), GetFirstSegmentPtr()); }
        ConstIterator End() const { return ConstIterator(m_inline, s_inlineEnd, nullptr); }

        /*
         * Splice transfers elements satisfying the predicate to the other list.
         * Inline elements are copied. Elements stored in the tail
         * are not copied, only the internal pointers are redecirected.
         * Splice does not preserve orginal order of elements.
         *
         * devnote: the function does not deallocate memory used by
         * tail segments that were inlined.
         */
        template <typename Pred, typename Copy>
        void Splice(Pred pred, InlineList& other, Copy copy)
        {
            if (other.Size()) { throw RuntimeInternalErrorException("Destination list must be empty"); }
            
            // 1. Copy qualifying inlined elements and compact the rest.
            unsigned char pos = 0;
            unsigned char validpos = 0;
            while (pos < m_head.m_elementCnt && pos < InlineCnt)
            {
                if (pred(m_inline[pos]))
                {
                    T* ptr = other.NextInlineLocation();
                    copy(ptr, m_inline[pos]);
                    ++other.m_head.m_elementCnt;
                } 
                else
                {
                    if (pos != validpos)
                    {
                        copy(&m_inline[validpos], m_inline[pos]);
                    }

                    ++validpos;
                }
                
                ++pos;
            }

            // 2. Move tail elements.
            Segment* prev = nullptr;
            Segment* cur = GetFirstSegmentPtr();
            while (cur)
            {
                if (pred(cur->m_value))
                {
                    if (prev) { prev->m_next = cur->m_next; }
                    if (cur == GetFirstSegmentPtr()) { SetFirstSegmentPtr(cur->m_next); }
                    
                    if (other.m_head.m_elementCnt < InlineCnt)
                    {
                        T* ptr = other.NextInlineLocation();
                        copy(ptr, cur->m_value);
                        cur = cur->m_next;
                    }
                    else
                    {
                        Segment* tmp = cur;
                        cur = cur->m_next;
                        tmp->m_next = nullptr;
                        other.AppendSegment(tmp);
                    }

                    ++other.m_head.m_elementCnt;
                }
                else
                {
                    prev = cur;
                    cur = cur->m_next;
                }
            }

            // 3. Compact original list.
            // Fill remaining inlined positions with valid elements from the tail.
            cur = GetFirstSegmentPtr();
            while ((validpos < InlineCnt) && cur)
            {
                copy(&m_inline[validpos], cur->m_value);
                
                ++validpos;
                cur = cur->m_next;
            }
            
            // 4. Attach tail.
            SetFirstSegmentPtr(cur);
            
            // 5. Calculate and store new element count.
            while (cur) 
            {
                ++validpos;
                cur = cur->m_next;
            }
            
            m_head.m_elementCnt = validpos;
        }

        size_t Size() const { return m_head.m_elementCnt; }
    };
    
    // PlaceboGranularList is used to optimize memory usage for joins with empty value schema.
    template <typename T, typename Allocator>
    struct PlaceboGranularList
    {
        unsigned int m_size{ 0 };

        struct Iterator
        {
            T      m_placeholder{ nullptr };
            size_t m_pos{ 0 };

            Iterator() {}

            Iterator(size_t pos)
                : m_pos(pos)
            {
            }

            Iterator& operator ++ ()
            { 
                ++m_pos;
                return *this; 
            }

            Iterator operator ++ (int)
            { 
                Iterator tmp(*this);
                ++m_pos;
                return tmp;
            }
            
            const T& operator * () const
            {
                return m_placeholder;
            }
            
            T* operator -> () const
            {
                return &m_placeholder; 
            }
            
            bool operator == (const Iterator& other) const { return m_pos == other.m_pos; }
            bool operator != (const Iterator& other) const { return !(operator == (other)); }
        };

        Iterator Begin() const { return Iterator(0); }
        Iterator End() const { return Iterator(m_size); }

        template <class... Args>
        void EmplaceFront(Allocator&, Args&&...) { ++m_size; }

        unsigned int Size() const { return m_size; }
    };
}


