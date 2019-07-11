#pragma once

// Enable this code once generated C++ files are split
#ifndef __cplusplus_cli
#error "Managed scope operators must be compiled with /clr"
#endif

#include "OperatorRequirementsDef.h"
#include "Managed.h"

using namespace OperatorRequirements;
using namespace ScopeEngineManaged;

namespace ScopeEngine
{
    extern INLINE void CheckSqlIpUdoYieldRow(IRow^ row, System::Object^ explicitCookie)
    {
        if (row == nullptr ||
            row->GetType() != SqlIpRow::typeid ||
            (((SqlIpRow^)row)->Cookie() != SqlIpRow::s_default_cookie && ((SqlIpRow^)row)->Cookie() != explicitCookie)
            )
        {
            throw gcnew InvalidOperationException("Yielded IRow object is not the instance from output.AsReadOnly() or original IRow");
        }
    }

    extern INLINE void GetReadOnlyColumnOrdinal(ISchema^ schema,
        cli::array<String^>^ readOnlyColumns,
        cli::array<int>^ %ordinal // out
        )
    {
        SCOPE_ASSERT(readOnlyColumns);
        SCOPE_ASSERT(schema);

        ordinal = gcnew cli::array<int>(readOnlyColumns->Length);

        for (int idx = 0; idx < readOnlyColumns->Length; ++idx)
        {
            // TODO: @mireed: this doesn't work for quoted identifiers, since the readOnlyColumns don't match
            // TODO: @mireed: the schema columns.
            int i = schema->IndexOf(readOnlyColumns[idx]);
            SCOPE_ASSERT(i >= 0);
            ordinal[idx] = i;
        }
    }

    ref class SqlIpRowset abstract : public IRowset, public Generic::IEnumerable<IRow^>
    {

    protected:

        ref class SqlIpRowsetEnumerator : public Generic::IEnumerator<IRow^>
        {
            bool                      m_hasRow;
            SqlIpRowset^              m_inputRowset;
            unsigned __int64          m_rowCount;

        public:

            SqlIpRowsetEnumerator(SqlIpRowset^ inputRowset) :
                m_inputRowset(inputRowset),
                m_hasRow(false),
                m_rowCount(0)
            {
            }

            ~SqlIpRowsetEnumerator()
            {
            }

            property unsigned __int64 RowCount
            {
                unsigned __int64 get()
                {
                    return m_rowCount;
                }
            }

            property IRow^ Current
            {
                virtual IRow^ get()
                {
                    return m_hasRow ? m_inputRowset->m_currentRow : nullptr;
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

        SqlIpRowsetEnumerator^ m_enumeratorOutstanding;
        SqlIpRow^ m_currentRow;
        SqlIpSchema^ m_schema;
        cli::array<ColumnOffsetId>^ m_columnOffset;

        //
        // SqlIpInputKeySet which is used for reducer needs distinct enuemrator for each key range
        //
        void ResetEnumerator()
        {
            m_enumeratorOutstanding = nullptr;
        }

        //
        // Clear data (cache + native ptr) for current row.
        //
        void InvalidateCurrentRow()
        {
            if (m_currentRow != nullptr)
            {
                m_currentRow->ResetData(nullptr);
            }
        }

    public:

        SqlIpRowset(SqlIpSchema^ schema, cli::array<ColumnOffsetId>^ offsetId)
            : m_schema(schema),
            m_columnOffset(offsetId),
            m_enumeratorOutstanding(nullptr)
        {
        }

        virtual property Generic::IEnumerable<IRow^>^ Rows
        {
            Generic::IEnumerable<IRow^>^ get() override
            {
                return this;
            }
        }

        virtual property ISchema^ Schema
        {
            ISchema^ get() override
            {
                return m_schema;
            }
        }

        //
        // IEnumerable<T> part
        //
        virtual Generic::IEnumerator<IRow^>^ GetEnumerator() = Generic::IEnumerable<IRow^>::GetEnumerator
        {
            if (m_enumeratorOutstanding != nullptr)
            {
                throw gcnew InvalidOperationException("User Error: Multiple instances of enumerators are not supported on input RowSet.Rows. The input Rowset.Rows may be enumerated only once. If user code needs to enumerate it multiple times, then all Rows must be cached during the first pass and use cached Rows later.");
            }

            m_enumeratorOutstanding = gcnew SqlIpRowsetEnumerator(this);

            // Do not return "this" to avoid object distruction by Dispose()
            return m_enumeratorOutstanding;
        }

        //
        // IEnumerable part
        //
        virtual IEnumerator^ GetEnumeratorIEnumerable() sealed = IEnumerable::GetEnumerator
        {
            return GetEnumerator();
        }

        virtual bool MoveNext() abstract;

        property IRow^ CurrentRow
        {
            IRow^ get()
            {
                return m_currentRow;
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

    }; // end of SqlIpRowset

    // Empty SQLIP rowset
    ref class EmptySqlIpRowset : public SqlIpRowset
    {
        SqlIpRowsetEnumerator^ enumrator;

    public:

        EmptySqlIpRowset(SqlIpSchema^ schema) : SqlIpRowset(schema, nullptr)
        {
            enumrator = gcnew SqlIpRowsetEnumerator(this);
        }

        virtual bool MoveNext() override
        {
            return false;
        }

        virtual Generic::IEnumerator<IRow^>^ GetEnumerator() override
        {
            return enumrator;
        }

    };

    template<typename InputSchema>
    ref class SqlIpInputRowset : public SqlIpRowset
    {
        InputSchema                   &m_nativeRow;
        OperatorDelegate<InputSchema> *m_child;
        InteropAllocator              *m_allocator;

    public:

        SqlIpInputRowset(OperatorDelegate<InputSchema> * child,
            SqlIpSchema^ schema,
            cli::array<ColumnOffsetId>^ offsetId)
            : SqlIpRowset(schema, offsetId),
            m_child(child),
            m_nativeRow(*child->CurrentRowPtr())
        {
            m_child->ReloadCurrentRow();
        }

        InputSchema& GetCurrentNativeRow()
        {
            return m_nativeRow;
        }

        virtual bool MoveNext() override
        {
            if (!m_child->End())
            {
                bool hasRow = m_child->MoveNext();
                if (hasRow)
                {
                    InvalidateCurrentRow();

                    //create a new row.
                    m_currentRow = gcnew SqlIpRow(m_schema, m_columnOffset, (unsigned char*)(&m_nativeRow));
                }

                return hasRow;
            }

            return false;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("SqlIpInputRowset");

            m_child->WriteRuntimeStats(node);
        }

    }; // end of SqlIpInputRowset

    //
    // Enumerate rowset honoring key ranges
    //
    template <typename InputSchema, typename KeyPolicy>
    ref class SqlIpInputKeyset : public SqlIpRowset
    {

    public:

        typedef KeyIterator<OperatorDelegate<InputSchema>, InputSchema, KeyPolicy> KeyIteratorType;

        SqlIpInputKeyset(KeyIteratorType * keyIterator,
            SqlIpSchema^ schema,
            cli::array<ColumnOffsetId>^ offsetId)
            : SqlIpRowset(schema, offsetId),
            m_state(State::eINITIAL)
        {
            SCOPE_ASSERT(keyIterator != nullptr);

            m_iter = keyIterator;
        }

        ~SqlIpInputKeyset()
        {
            this->!SqlIpInputKeyset();
        }

        !SqlIpInputKeyset()
        {
            m_iter = nullptr;
        }

        bool Init()
        {
            SCOPE_ASSERT(m_state == State::eINITIAL);

            m_iter->ReadFirst();
            m_iter->ResetKey();

            m_state = m_iter->End() ? State::eEOF : State::eSTART;

            InitStartRow();

            return m_state == State::eSTART;
        }

        //
        // End current key range and start next one
        //
        bool NextKey()
        {
            SCOPE_ASSERT(m_state != State::eINITIAL);

            switch (m_state)
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

            InitStartRow();
            return m_state == State::eSTART;
        }

        void SetIterator(KeyIteratorType *iter)
        {
            m_iter = iter;

            if (m_state != State::eSTART && m_state != State::eRANGE)
            {
                m_state = m_iter->End() ? State::eEND : State::eSTART;
                InitStartRow();
            }
        }

        virtual bool MoveNext() override
        {
            SCOPE_ASSERT(m_state != State::eINITIAL);

            switch (m_state)
            {
            case State::eSTART:
                // Row was already read
                m_state = State::eRANGE;
                break;

            case State::eRANGE:
                m_iter->Increment();
                m_state = m_iter->End() ? State::eEND : State::eRANGE;
                InitRangeRow();
                break;
            }

            return m_state == State::eRANGE;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("SqlIpInputKeyset");

            if (m_iter != nullptr)
            {
                m_iter->WriteRuntimeStats(node);
            }
        }

    private:

        void InitStartRow()
        {
            if (m_state == State::eSTART)
            {
                InvalidateCurrentRow();
                m_currentRow = gcnew SqlIpRow(m_schema, m_columnOffset, (unsigned char*)(m_iter->GetRow()));
            }
        }

        void InitRangeRow()
        {
            if (m_state == State::eRANGE)
            {
                InvalidateCurrentRow();
                m_currentRow = gcnew SqlIpRow(m_schema, m_columnOffset, (unsigned char*)(m_iter->GetRow()));
            }
        }

    private:

        enum class State { eINITIAL, eSTART, eRANGE, eEND, eEOF };
        State m_state;

        KeyIteratorType * m_iter; // does not own key iterator

    }; //end of SqlIpInputKeyset

    template<>
    struct ManagedRow<ScopeEngine::None>
    {
        cli::array<ScopeEngineManaged::ColumnOffsetId>^ ColumnOffsets()
        {
            cli::array<ScopeEngineManaged::ColumnOffsetId>^ offset = gcnew cli::array<ScopeEngineManaged::ColumnOffsetId>(0);
            return offset;
        }

        cli::array<ScopeEngineManaged::SqlIpColumn^>^ Columns(cli::array<String^>^ readOnlyColumns)
        {
            cli::array<ScopeEngineManaged::SqlIpColumn^>^ c = gcnew cli::array<ScopeEngineManaged::SqlIpColumn^>(0);
            return c;
        }

        static System::Object^ ComplexColumnGetter(int index, BYTE* address)
        {
            System::Object^ r = nullptr;
            return r;
        }

        static System::Object^ UDTColumnGetter(int index, BYTE* address)
        {
            System::Object^ r = nullptr;
            return r;
        }

        ManagedRow()
        {
        }

        IRow^ get()
        {
            return m_row.get();
        }

    private:

        ScopeTypedManagedHandle<IRow^> m_row;
        ManagedRow & operator=(ManagedRow &);
    };

    template class ManagedRowFactory<ScopeEngine::None>;

    // SqlIpStreamProxy - user proxy around our internal stream
    //  This allows us to disable exposed functionality (read on output, write on input, closing/flushing of our stream, etc)
    template<typename T>
    ref class SqlIpStreamProxy abstract : public System::IO::Stream
    {

    protected:

        T       m_stream;
        bool    m_closed;

    public:

        SqlIpStreamProxy(T baseStream)
            : m_stream(baseStream),
            m_closed(false)
        {
        }

        virtual property bool       CanSeek
        {
            bool get() override
            {
                return false;
            }
        }

        virtual property LONGLONG   Length
        {
            LONGLONG get() override
            {
                throw gcnew NotSupportedException();
            }
        }

        virtual property LONGLONG   Position
        {
            LONGLONG get() override
            {
                VerifyOpen();
                return m_stream->Position;
            }

            void set(LONGLONG pos) override
            {
                UNREFERENCED_PARAMETER(pos);
                throw gcnew NotSupportedException();
            }
        }

        virtual LONGLONG            Seek(LONGLONG pos, System::IO::SeekOrigin origin) override
        {
            UNREFERENCED_PARAMETER(pos);
            UNREFERENCED_PARAMETER(origin);
            throw gcnew NotSupportedException();
        }

        virtual void                SetLength(LONGLONG length) override
        {
            UNREFERENCED_PARAMETER(length);
            throw gcnew NotSupportedException();
        }

        virtual void                Flush() override
        {
            //No-op
        }

        virtual void                Close() override
        {
            //No-op (to underlying stream)
            m_closed = true;
        }

        virtual void                VerifyOpen()
        {
            if (m_closed)
            {
                throw gcnew ObjectDisposedException("Cannot access a closed Stream.");
            }
        }
    };

    // SqlIpInputProxy
    template<typename T>
    ref class SqlIpInputProxy : public SqlIpStreamProxy<T>
    {

    public:

        SqlIpInputProxy(T baseStream)
            : SqlIpStreamProxy<T>(baseStream)
        {
            }

        virtual property bool       CanRead
        {
            bool get() override
            {
                return !m_closed;
            }
        }

        virtual property bool       CanWrite
        {
            bool get() override
            {
                return false;
            }
        }

        virtual int                 Read(cli::array<System::Byte>^ buffer, int offset, int count) override
        {
            VerifyOpen();
            return m_stream->Read(buffer, offset, count);
        }

        virtual void                Write(cli::array<System::Byte>^ buffer, int offset, int count) override
        {
            UNREFERENCED_PARAMETER(buffer);
            UNREFERENCED_PARAMETER(offset);
            UNREFERENCED_PARAMETER(count);
            throw gcnew NotSupportedException();
        }
    };

    // SqlIpOutputProxy
    template<typename T>
    ref class SqlIpOutputProxy : public SqlIpStreamProxy<T>
    {

    public:

        SqlIpOutputProxy(T baseStream)
            : SqlIpStreamProxy<T>(baseStream)
        {
            }

        virtual property bool       CanRead
        {
            bool get() override
            {
                return false;
            }
        }

        virtual property bool       CanWrite
        {
            bool get() override
            {
                return !m_closed;
            }
        }

        virtual int                 Read(cli::array<System::Byte>^ buffer, int offset, int count) override
        {
            UNREFERENCED_PARAMETER(buffer);
            UNREFERENCED_PARAMETER(offset);
            UNREFERENCED_PARAMETER(count);
            throw gcnew NotSupportedException();
        }

        virtual void                Write(cli::array<System::Byte>^ buffer, int offset, int count) override
        {
            VerifyOpen();
            m_stream->Write(buffer, offset, count);
        }
    };

    // SqlIp row delimiter
    interface class ISqlIpRowDelimiter
    {
        property int MaxLength
        {
            int get();
        }

        // The delimiter length for current delimiter matched
        property int CurrentLength
        {
            int get();
        }

        // The delimiter length used for calculating split EOF. see comment for NewLineDelimiter.
        property int LengthOnSplitBoundary
        {
            int get();
        }

        // Search delimiter in buffer, return index. -1 means not found. 
        // Search range is (start, end)
        int DelimiterMatch(cli::array<System::Byte>^ buffer, int start, int end);
    };

    ref class FixedLengthDelimiter : public ISqlIpRowDelimiter
    {

    public:

        property int MaxLength
        {
            virtual int get()
            {
                return m_length;
            }
        }

        // for fixed length delimiter, MaxLength == CurrentLength
        property int CurrentLength
        {
            virtual int get()
            {
                return m_length;
            }
        }

        property int LengthOnSplitBoundary
        {
            virtual int get()
            {
                return m_length;
            }
        }

        // KMP pattern match
        // search delimiter in buffer, idx range is (start, end), both start/end exclusive
        virtual int DelimiterMatch(cli::array<System::Byte>^ buffer, int start, int end)
        {
            int i = start;
            int j = -1;
            while (i < end && j < m_raw->Length)
            {
                if (j == -1 || buffer[i] == m_raw[j])
                {
                    i++; j++;
                }
                else
                {
                    j = m_kmp[j];
                }
            }

            if (j == m_length)
            {
                return i - m_length;
            }
            else
            {
                return -1;
            }
        }

        FixedLengthDelimiter(cli::array<System::Byte>^ delimiter)
            : m_raw(delimiter),
            m_length(0),
            m_kmp(nullptr)
        {
            SCOPE_ASSERT(m_raw != nullptr);

            m_length = m_raw->Length;
            SCOPE_ASSERT(m_length > 0);

            m_kmp = CreateKMPTable(m_raw);
        }

        ~FixedLengthDelimiter()
        {
            delete m_kmp;
            m_kmp = nullptr;

            m_raw = nullptr;

            this->!FixedLengthDelimiter();
        }

    private:

        // Create table for KMP algorithm
        static cli::array<int>^ CreateKMPTable(cli::array<System::Byte>^ pattern)
        {
            cli::array<int>^ t = gcnew cli::array<int>(pattern->Length);
            int j = 0;
            int k = -1;

            t[0] = -1;
            while (j < pattern->Length - 1)
            {
                if (k == -1 || pattern[j] == pattern[k])
                {
                    j++;
                    k++;
                    if (pattern[j] != pattern[k])
                    {
                        t[j] = k;
                    }
                    else
                    {
                        t[j] = t[k];
                    }
                }
                else
                {
                    k = t[k];
                }
            }

            return t;
        }

        !FixedLengthDelimiter()
        {
        }

    private:

        // delimiter
        cli::array<System::Byte>^ m_raw;

        // delimiter length
        int m_length;

        // KMP table
        cli::array<int>^ m_kmp;
    }; // end of FixLengthDelimiter

    // Default delimiter used in SqlIpRowStream, is either
    // "0x0d", "0x0a", "0x0d 0x0a", 
    // a.k.a. "\r", "\n", "\r\n" in ASCII and UTF-8 encoding
    ref class NewLineDelimiter : public ISqlIpRowDelimiter
    {

    public:

        property int MaxLength
        {
            virtual int get()
            {
                return 2;
            }
        }

        property int CurrentLength
        {
            virtual int get()
            {
                return m_currentLength;
            }
        }

        // Becuase NewLineDelimiter contains 3 different variation - \r, \n, \r\n
        // It beccomes tricky when split boundary is in between \r\n. e.g. 
        // 1. \r\n is a valid delimiter, but first split end at \r, and second split start at \n. 
        // 2. Second split will skip first uncomplete row until meeting a delimiter, here \n is acutally a valid delimiter
        // 3. So second split treat the row following \n as its responsibility
        // 4. First split always wants to read one more row (pass split boundary and reach another delimiter),
        //    So first split will actually pass \r\n and read one more row. Hence there is duplicaion for that row, with second split.
        //
        // Actually because second split can never see the byte before \n, the solution here is to let 
        // first split stop reading one more row when above scenario happens. 
        // LengthOnSplitBoundary is used to calculate whether EOF happend so first split should stop. 
        // For NewLineDelimiter it's 1 byte. For FixedLengthDelimiter it's alwasy the fixed length.
        property int LengthOnSplitBoundary
        {
            virtual int get()
            {
                return 1;
            }
        }

        // search delimiter in buffer, idx range is (start, end), both start & end are exclusive
        virtual int DelimiterMatch(cli::array<System::Byte>^ buffer, int start, int end)
        {
            int delimiterLength = 0;
            int matchIdx = -1;

            for (int i = start + 1; i < end; ++i)
            {
                // 0x0d
                if (buffer[i] == x_cr)
                {
                    matchIdx = i;
                    delimiterLength++;

                    // 0x0d 0x0a
                    if (i + 1 < end && buffer[i + 1] == x_lf)
                    {
                        delimiterLength++;
                    }

                    break;
                }

                // 0x0a
                if (buffer[i] == x_lf)
                {
                    matchIdx = i;
                    delimiterLength++;
                    break;
                }
            }

            // update delimiter length when found
            if (matchIdx != -1)
            {
                m_currentLength = delimiterLength;
            }

            return matchIdx;
        }

        NewLineDelimiter(cli::array<System::Byte>^ delimiter)
            : m_currentLength(0)
        {
            SCOPE_ASSERT(delimiter == nullptr);
        }

        ~NewLineDelimiter()
        {
        }

    private:

        static const System::Byte x_cr = 0x0d; // \r
        static const System::Byte x_lf = 0x0a; // \n
        int m_currentLength;

    };

    // SqlIpRowStream represents a SQLIP row in byte stream format, to enable extractor udo to process data in stream mode.
    //
    // We return a SqlIpInputProxy rather than the actual SqlIpRowStream stream so if the user gets a reference to it we can safely invalidate it
    // - the proxy contains a reference to a SqlIpRowStream which represents the real stream
    //   to verify that it is still valid
    //
    // SqlIpRowStream is based on another stream (ScopeCosmosInputStream, contains multiple rows) 
    // but it's aware row boundary. SqlIpRowStream::Read interface never move cursor cross row delimiter, 
    // it returns 0 when hiting row delimiter, simulating an EOF. 
    //
    // Idea is to setup a stage buffer, copy content from under layer stream into
    // stage buffer, search row delimiter, and return content when Read method being called. 
    // The stage buffer is used to avoid pushing row delimiter concept into under-layer stream,
    // and avoiding using user buffer for parsing delimiter.
    //
    // KMP algorithm is used for pattern search in stream. 
    //
    template<typename DelimiterType>
    ref class SqlIpRowStream
    {
    public:
        // Get position in row stream
        property LONGLONG Position
        {
            LONGLONG get()
            {
                return m_posInRow;
            }
        }

        int Read(cli::array<System::Byte>^ dst, int offset, int count)
        {
            Validate(dst, offset, count);
            return this->ReadInternal(dst, offset, count, true/*copy bytes to dst*/);
        }

        // ctor    
        SqlIpRowStream(System::IO::Stream^ s,
            bool compressed,
            bool lastSplit,
            INT64 splitLength,
            cli::array<System::Byte>^ delimiterBytes,
            int bufferSize)
            : m_posInRow(0),
            m_stream(s),
            m_compressed(compressed),
            m_lastSplit(lastSplit),
            m_delimiter(nullptr),
            m_rowEnd(false),
            m_cb(0),
            m_cr(0),
            x_bufSize(bufferSize),
            m_searchDelimiter(false)
        {
            m_delimiter = gcnew DelimiterType(delimiterBytes);

            // acutal buffer size is 4MB + extra, extra is delimiter.Length - 1
            m_bufSize = x_bufSize + m_delimiter->MaxLength - 1;

            // create buffer
            m_buf = gcnew cli::array<System::Byte>(m_bufSize);

            // fill buffer
            m_start = 0;
            m_end = m_stream->Read(m_buf, 0, m_bufSize) + m_start;

            // search delimiter
            m_searchDelimiter = true;

            // Compressed stream can not rely on physical length to determine EOF, set it to "max - m_delimiter->MaxLength"
            m_splitLength = m_compressed ? _I64_MAX - m_delimiter->MaxLength : splitLength;
        }

        ~SqlIpRowStream()
        {
            delete m_buf;
            m_buf = nullptr;

            delete m_delimiter;
            m_delimiter = nullptr;

            m_stream = nullptr;

            this->!SqlIpRowStream();
        }

        property bool RowEnd
        {
            bool get()
            {
                return m_rowEnd;
            }
        }

        property bool IsEOF
        {
            bool get()
            {
                // either 
                // 1. reach EOF for under-layer stream
                // 2. cursor passes split end point, and after that passes one complete delimiter
                return (m_start == m_end) || (m_cb >= m_splitLength + m_delimiter->LengthOnSplitBoundary);
            }
        }

        // Start a new row and return a proxy for accessing it
        SqlIpInputProxy<SqlIpRowStream<DelimiterType>^>^ StartNewRow()
        {
            m_rowEnd = false;
            m_posInRow = 0;
            return gcnew SqlIpInputProxy<SqlIpRowStream<DelimiterType>^>(this);
        }

        void SkipCurRow()
        {
            this->ReadInternal(nullptr, 0, x_bufSize, false/*no copy*/);
        }

    protected:

        // read (or advance cursor) until any condition below hit
        // 1) read bytes count as requested
        // 2) end of file
        // 3) end of row
        int ReadInternal(cli::array<System::Byte>^ dst, int offset, int count, bool copy)
        {
            int read = 0;
            int remain = count;

            while (read < count)
            {
                // reach row end, stop reading
                if (m_rowEnd)
                {
                    ++m_cr;
                    break;
                }

                // EOF, stop reading
                if (this->IsEOF)
                {
                    if (!m_lastSplit && (m_start == m_end))
                    {
                        // EOF for under-layer stream is reached for intermediate split
                        throw ScopeStreamExceptionWithEvidence(E_STREAM_SPLIT_UNEXPECTED_EOF, { m_cb, m_cr }, std::string());
                    }

                    break;
                }

                // Search delimiter when needed
                if (m_searchDelimiter)
                {
                    SearchDelimiter();
                }

                int c = 0;
                if (m_delimiterIdx == -1)
                {
                    // there is no delimiter in buffer. copy bytes out, but never cross the x_bufSize boundary
                    c = std::min(remain, std::min(m_end - m_start, x_bufSize - m_start));
                    if (copy) Array::Copy(m_buf, m_start, dst, offset, c);
                }
                else
                {
                    // there is delimiter in buffer
                    c = std::min(remain, m_delimiterIdx - m_start);
                    if (copy) Array::Copy(m_buf, m_start, dst, offset, c);

                    // if m_start reaches delimiter, skip delimiter for preparation next read
                    if (c == m_delimiterIdx - m_start)
                    {
                        int delimiterLength = m_delimiter->CurrentLength;
                        m_start += delimiterLength;
                        m_cb += delimiterLength;

                        m_rowEnd = true;
                        m_searchDelimiter = true;
                    }
                }

                // update all counters
                offset += c;
                read += c;
                m_start += c;
                m_cb += c;
                remain -= c;
                m_posInRow += c;

                // if consumed bytes cross x_bufSize boundary, it's time to refill
                if (m_start >= x_bufSize)
                {
                    // move un-consumed bytes at buffer end, to buffer head. Array::Copy is safe even src/dst overlap           
                    int copyLen = m_end - m_start;
                    Array::Copy(m_buf, m_start, m_buf, 0, copyLen);

                    // refill                    
                    m_end = m_stream->Read(m_buf, copyLen, m_bufSize - copyLen) + copyLen;
                    m_start = 0;
                    m_searchDelimiter = true;
                }
            }

            return read;
        }

        // Search delimiter in m_buf, within range (m_start, m_end).
        // trigger delimiter search by either 1) refill bufer or 2) start to looking for next row
        void SearchDelimiter()
        {
            m_delimiterIdx = m_delimiter->DelimiterMatch(m_buf, m_start - 1, m_end);
            m_searchDelimiter = false;
        }

        void Validate(cli::array<System::Byte>^ buffer, int offset, int count)
        {
            if (buffer == nullptr)
            {
                throw gcnew ArgumentNullException("buffer", "buffer cannot be null");
            }
            if (offset + count > buffer->Length)
            {
                throw gcnew ArgumentException("offset and count were out of bounds for the array");
            }
            else if (offset < 0 || count < 0)
            {
                throw gcnew ArgumentOutOfRangeException("Non-negative number required");
            }
        }

        !SqlIpRowStream()
        {
        }

    private:

        // position in row stream
        INT64 m_posInRow;

        // buffer size default is 4MB
        int x_bufSize;

        // stage buffer
        cli::array<System::Byte>^ m_buf;

        // actual buf size = x_bufSize + delimiter.MaxLength - 1
        int m_bufSize;

        // delimiter
        DelimiterType^ m_delimiter;

        // [start, end) idx guarding valid content in m_buf
        int m_start;
        int m_end;

        // index for matched delimiter in m_buf, -1 means no match
        int m_delimiterIdx;

        // flag indicate row is end, a.k.a. cursor passes row delimiter
        bool m_rowEnd;

        // under layer stream
        System::IO::Stream^ m_stream;

        // under layer stream is compressed or not
        bool m_compressed;

        // true for last split of the stream
        bool m_lastSplit;

        // stream split length
        INT64 m_splitLength;

        // total byte read since SqlIpRowStream created
        INT64 m_cb;

        // total rows read since SqlIpRowStream created
        INT64 m_cr;

        // It's time to search delimiter. flag for searching delimiter in lazy mode
        bool m_searchDelimiter;

    }; // end of SqlIpRowStream

    template<typename DelimiterType>
    ref class SqlIpRowEnumerator : public Generic::IEnumerator<System::IO::Stream^>
    {

    public:

        // IEnumerator interface
        virtual bool MoveNext()
        {
            // first time calling MoveNext
            if (m_row == nullptr)
            {
                m_row = gcnew SqlIpRowStream<DelimiterType>(m_stream, m_compressed, m_lastSplit, m_splitLength, m_delimiter, x_rowLimit);

                if (!m_firstSplit)
                {
                    m_row->SkipCurRow();
                }
            }

            // For non-first row, we should check whether reader consume all bytes for previous row
            // if not, skip the uncompleted bytes before starting new row
            if (!m_row->RowEnd && m_rowCount > 0)
            {
                m_row->SkipCurRow();
            }

            // invalidate any outstanding proxy before exit
            if (m_proxy != nullptr)
            {
                m_proxy->Close();
            }

            if (m_row->IsEOF)
            {
                return false;
            }

            // start a new row
            m_proxy = m_row->StartNewRow();
            m_rowCount++;
            return true;
        }

        // IEnumerator interface
        property System::IO::Stream^ Current
        {
            virtual System::IO::Stream^ get()
            {
                if (m_row == nullptr || m_row->IsEOF)
                {
                    return nullptr;
                }

                // Limit the capability of streams passed to the user
                return m_proxy;
            }
        }

        // IEnumerator interface
        property Object^ CurrentBase
        {
            virtual System::Object^ get() sealed = IEnumerator::Current::get
            {
            return Current;
        }
        }

        // IEnumerator interface
        virtual void Reset()
        {
            throw gcnew NotSupportedException();
        }

        // ctor/dtor
        SqlIpRowEnumerator(System::IO::Stream^ s, bool compressed, bool firstSplit, bool lastSplit, UINT64 splitLength, cli::array<System::Byte>^ delimiter)
            : m_stream(s),
            m_compressed(compressed),
            m_firstSplit(firstSplit),
            m_lastSplit(lastSplit),
            m_splitLength(splitLength),
            m_delimiter(delimiter),
            m_rowCount(0),
            m_row(nullptr),
            m_proxy(nullptr)
        {
            x_rowLimit = (int)Configuration::GetGlobal().GetMaxOnDiskRowSize();
        }

        ~SqlIpRowEnumerator()
        {
            delete m_proxy;
            m_proxy = nullptr;
            delete m_row;
            m_row = nullptr;

            m_delimiter = nullptr;
            m_stream = nullptr;
            this->!SqlIpRowEnumerator();
        }

    protected:

        !SqlIpRowEnumerator()
        {
        }

    private:

        // The underneath split stream
        System::IO::Stream^ m_stream;

        // The underneath split stream is compressed or not        
        bool m_compressed;

        // True for first split in the stream
        bool m_firstSplit;

        // True for last split in the stream
        bool m_lastSplit;

        // Split length
        UINT64 m_splitLength;

        // row delimiter
        cli::array<System::Byte>^ m_delimiter;

        // stream represents a row
        SqlIpRowStream<DelimiterType>^ m_row;

        // Current row proxy - updated on each new row
        SqlIpInputProxy<SqlIpRowStream<DelimiterType>^>^ m_proxy;

        // row size limit 
        int x_rowLimit;

        // how many rows processed
        int m_rowCount;

    }; // end of SqlIpRowEnumerator

    ref class SqlIpRowReader : public Generic::IEnumerable<System::IO::Stream^>
    {
    public:

        // max delimiter length
        static const int x_delimiterMax = 32;

        // IEnumerable interface
        virtual Generic::IEnumerator<System::IO::Stream^>^ GetEnumerator() sealed = Generic::IEnumerable<System::IO::Stream^>::GetEnumerator
        {
            if (m_enumeratorOutstanding)
            {
                throw gcnew InvalidOperationException("User Error: Multiple instances of enumerators are not supported, the input stream may be enumerated only once.");
            }

            m_enumeratorOutstanding = true;

            if (m_delimiter == nullptr || m_delimiter->Length == 0)
            {
                // use default delimiter "0xd", "0xa", or "0xd 0xa"
                return gcnew SqlIpRowEnumerator<NewLineDelimiter>(m_stream, m_compressed, m_firstSplit, m_lastSplit, m_splitLength, nullptr);
            }
            else
            {
                return gcnew SqlIpRowEnumerator<FixedLengthDelimiter>(m_stream, m_compressed, m_firstSplit, m_lastSplit, m_splitLength, m_delimiter);
            }
        }

        // IEnumerable interface
        virtual IEnumerator^ GetEnumeratorBase() sealed = IEnumerable::GetEnumerator
        {
            return GetEnumerator();
        }

        // ctor/dtor
        SqlIpRowReader(System::IO::Stream^ s, bool compressed, bool firstSplit, bool lastSplit, UINT64 splitLength, cli::array<System::Byte>^ delimiter)
            : m_stream(s),
            m_compressed(compressed),
            m_firstSplit(firstSplit),
            m_lastSplit(lastSplit),
            m_splitLength(splitLength),
            m_delimiter(delimiter),
            m_enumeratorOutstanding(false)
        {
        }

        ~SqlIpRowReader()
        {
            m_delimiter = nullptr;
            m_stream = nullptr;

            this->!SqlIpRowReader();
        }

    protected:

        !SqlIpRowReader()
        {
        }

    private:

        // Enumeration singleton to prevent underlying concurrent stream access
        bool m_enumeratorOutstanding;

        // Saved parameters only to pass onto enumerator
        System::IO::Stream^         m_stream;
        bool                        m_compressed; // above m_stream is compressed or not
        bool                        m_firstSplit;
        bool                        m_lastSplit;
        UINT64                      m_splitLength;
        cli::array<System::Byte>^   m_delimiter;

    }; // end of SqlIpRowReader

    template<typename InputType>
    ref class SqlIpStreamReader : public IUnstructuredReader
    {

    public:

        virtual property System::IO::Stream^ BaseStream
        {
            System::IO::Stream^ get() override
            {
                // Limit the capability of streams passed to the user
                return m_proxy;
            }
        }

        virtual property LONGLONG Start
        {
            LONGLONG get() override
            {
                // Return global offset in the stream
                return m_inputGlobalStartOffset + m_inputLocalStartOffset;
            }
        }

        virtual property LONGLONG Length
        {
            LONGLONG get() override
            {
                // Return split length
                return m_inputLocalEndOffset - m_inputLocalStartOffset;
            }
        }

        // delimiter can be null or empty array, means using default row new line as delimiter ("0xd", or "0xa", or "0xd 0xa")
        // other delimiter is a fixed-length delimiter.
        virtual Generic::IEnumerable<System::IO::Stream^>^ Split(cli::array<System::Byte>^ delimiter) override
        {
            // reuse the row reader
            if (m_rowReader != nullptr)
            {
                return m_rowReader;
            }

            SCOPE_ASSERT(m_stream);

            // When generating row reader, the base stream should be clean
            SCOPE_ASSERT(m_inputLocalStartOffset <= LLONG_MAX);
            if (!m_stream->IsCompressed && m_stream->Position != (INT64)m_inputLocalStartOffset)
            {
                throw gcnew InvalidOperationException("BaseStream.Position should be splitStartOffset");
            }

            // Check delimiter length
            if (delimiter != nullptr && delimiter->Length > SqlIpRowReader::x_delimiterMax)
            {
                System::String^ err = System::String::Format("delimiter can not be longer than {0}", SqlIpRowReader::x_delimiterMax);
                throw gcnew ArgumentException(err);
            }

            bool isFirstSplit = this->Start == 0;
            bool isLastSplit = m_inputLocalEndOffset == m_inputLength;
            UINT64 splitLength = m_inputLocalEndOffset - m_inputLocalStartOffset;
            m_rowReader = gcnew SqlIpRowReader(m_stream, m_stream->IsCompressed, isFirstSplit, isLastSplit, splitLength, delimiter);
            return m_rowReader;
        }

        SqlIpStreamReader(ScopeCosmosInputStream<InputType>^ stream, bool blockAligned, UINT64 globalStartOffset, UINT64 localStartOffset,
            UINT64 localEndOffset, UINT64 length) :
            m_stream(stream),
            m_inputBlockAligned(blockAligned),
            m_inputGlobalStartOffset(globalStartOffset),
            m_inputLocalStartOffset(localStartOffset),
            m_inputLocalEndOffset(localEndOffset),
            m_inputLength(length)
        {
            m_stream->Init();

            // Compressed stream does not support Seek.
            if (!m_stream->IsCompressed)
            {
                m_stream->Seek((INT64)m_inputLocalStartOffset, System::IO::SeekOrigin::Begin);
            }
            else
            {
                SCOPE_ASSERT(m_inputLocalStartOffset == 0);
            }

            // JM and runtime contract - JM should slice stream at 4-bytes
            // boundary for extractor handling UTF32 encoding correctly.
            // Split start offset must be multiple of 4.

            // Above contract should be assert at the first place, e.g. in parser.cpp in scopehost, not here
            // The local run which does not include JM component will pass in arbitrary start offset
            // for non-UTF32 encoding input file.

            m_proxy = gcnew SqlIpInputProxy<ScopeCosmosInputStream<InputType>^>(m_stream);
            m_rowReader = nullptr;
        }

        SqlIpStreamReader(const IOManager::StreamData& streamData, SIZE_T bufSize, int bufCount) :
            SqlIpStreamReader(
            gcnew ScopeCosmosInputStream<InputType>(streamData.m_device.get(), bufSize, bufCount),
            streamData.m_channel.m_blockAligned,
            streamData.m_channel.m_globalStartOffset,
            streamData.m_channel.m_localStartOffset,
            streamData.m_channel.m_localEndOffset,
            streamData.m_channel.m_length)
        {
        }

        SqlIpStreamReader(const InputFileInfo& input, SIZE_T bufSize, int bufCount) :
            SqlIpStreamReader(
            gcnew ScopeCosmosInputStream<InputType>(input.inputFileName, bufSize, bufCount),
            input.blockAligned,
            input.globalStartOffset,
            input.localStartOffset,
            input.localEndOffset,
            input.length)
        {
        }

        ~SqlIpStreamReader()
        {
            delete m_stream;
            m_stream = nullptr;

            delete m_proxy;
            m_proxy = nullptr;

            delete m_rowReader;
            m_rowReader = nullptr;

            this->!SqlIpStreamReader();
        }

        void Close()
        {
            m_stream->Close();
        }

        __int64 GetTotalIoWaitTime()
        {
            return m_stream->GetTotalIoWaitTime();
        }

        __int64 GetInclusiveTimeMillisecond()
        {
            return m_stream->GetInclusiveTimeMillisecond();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            m_stream->WriteRuntimeStats(root);
        }

        OperatorRequirements GetOperatorRequirements()
        {
            return m_stream->GetOperatorRequirements();
        }

    protected:

        !SqlIpStreamReader()
        {
        }

    private:
        ScopeCosmosInputStream<InputType>^ m_stream;
        SqlIpInputProxy<ScopeCosmosInputStream<InputType>^>^ m_proxy;
        SqlIpRowReader^ m_rowReader;

        bool   m_inputBlockAligned;
        UINT64 m_inputGlobalStartOffset;
        UINT64 m_inputLocalStartOffset;
        UINT64 m_inputLocalEndOffset;
        UINT64 m_inputLength;
    };

    ref class SqlIpStreamWriter : public IUnstructuredWriter
    {

    public:

        virtual property System::IO::Stream^ BaseStream
        {
            System::IO::Stream^ get() override
            {
                if (m_proxy == nullptr)
                {
                    // Limit the capability of streams passed to the user
                    m_proxy = gcnew SqlIpOutputProxy<ScopeCosmosOutputStream^>(m_stream);
                }

                return m_proxy;
            }
        }

        SqlIpStreamWriter(std::string& name, SIZE_T bufSize, int bufCount)
            : SqlIpStreamWriter(name, bufSize, bufCount, false)
        {
        }

        SqlIpStreamWriter(std::string& name, SIZE_T bufSize, int bufCount, bool disableCompression)
            : m_proxy(nullptr)
        {
            CosmosOutput::SettingFlags cosmosOutputSettingFlags;
            cosmosOutputSettingFlags.DisableCompression = disableCompression;
            cosmosOutputSettingFlags.MaintainBoundaries = true;
            m_stream = gcnew ScopeCosmosOutputStream(name, bufSize, bufCount, cosmosOutputSettingFlags);
            m_stream->Init();
        }

        SqlIpStreamWriter(ScopeCosmosOutputStream^ stream)
            : m_stream(stream),
            m_proxy(gcnew SqlIpOutputProxy<ScopeCosmosOutputStream^>(m_stream))
        {
        }

        ~SqlIpStreamWriter()
        {
            delete m_stream;
            m_stream = nullptr;

            delete m_proxy;
            m_proxy = nullptr;

            this->!SqlIpStreamWriter();
        }

        void Close()
        {
            m_stream->Close();

            // Invalidate any outstanding proxy
            //  Note: We don't null the ptr so the user can't simply just request the ->BaseStream again
            if (m_proxy != nullptr)
                m_proxy->Close();
        }

        void EndOfRow()
        {
            m_stream->Flush();

            // Note: If the user explicitly closed the proxy, we allow them to request a new proxy next time (->BaseStream)
            if (m_proxy != nullptr && !m_proxy->CanWrite)
                m_proxy = nullptr;
        }

        __int64 GetTotalIoWaitTime()
        {
            return m_stream->GetTotalIoWaitTime();
        }

        __int64 GetInclusiveTimeMillisecond()
        {
            return m_stream->GetInclusiveTimeMillisecond();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            m_stream->WriteRuntimeStats(root);
        }

    protected:

        !SqlIpStreamWriter()
        {
        }

    private:

        ScopeCosmosOutputStream^ m_stream;
        SqlIpOutputProxy<ScopeCosmosOutputStream^>^ m_proxy;
    };

    template<typename InputType, typename OutputSchema, int UID, int RunScopeCEPMode>
    INLINE ScopeExtractorManaged<InputType, OutputSchema> * ScopeExtractorManagedFactory::MakeSqlIp(std::string * argv, int argc)
    {
        ManagedUDO<UID> managedUDO(argv, argc);
        ManagedRow<OutputSchema> managedRow;
        SqlIpSchema^ schema = gcnew SqlIpSchema(managedRow.Columns(managedUDO.ReadOnlyColumns()), &ManagedRow<OutputSchema>::ComplexColumnGetter, &ManagedRow<OutputSchema>::UDTColumnGetter);
        SqlIpUpdatableRow^ outputRow = gcnew SqlIpUpdatableRow(schema->Defaults);

        return new SqlIpExtractor<InputType, OutputSchema>(outputRow,
                            managedUDO.get(),
                            managedUDO.args(),
                            managedUDO.StreamIdColumnIndex(),
                            &InteropToNativeRowPolicy<OutputSchema,UID>::Marshal);
    }

    template<typename InputType, typename OutputSchema>
    class SqlIpExtractor : public ScopeExtractorManaged<InputType, OutputSchema>
    {
        friend struct ScopeExtractorManagedFactory;

        static const char* const sm_className;

        // Function pointer type for Marshal call methods.
        typedef void(*MarshalToNativeCallType)(IRow ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<IRow^> ^>              m_enumerator;
        ScopeTypedManagedHandle<IExtractor ^>                               m_extractor;
        ScopeTypedManagedHandle<Generic::List<String^> ^>                   m_args;
        ScopeTypedManagedHandle<SqlIpStreamReader<InputType> ^>             m_inputStreamReader;
        MarshalToNativeCallType                                             m_marshalToNative;
        RowEntityAllocator                                                  m_allocator;
        ScopeTypedManagedHandle<SqlIpUpdatableRow ^>                        m_updatableRow;
        bool                                                                m_nullExtractor;
        IOManager::StreamData                                               m_inputStreamData;
        std::string                                                         m_inputName;
        int                                                                 m_streamIdColumnIdx;  // column idx (for stream Id) in schema, -1 stands for non exist

        SqlIpExtractor(SqlIpUpdatableRow^ outputRow,
            IExtractor ^ extractor,
            Generic::List<String^>^ args,
            int systemIdColumnIdx,
            MarshalToNativeCallType m)
            : m_extractor(extractor),
            m_args(args),
            m_streamIdColumnIdx(systemIdColumnIdx),
            m_marshalToNative(m),
            m_updatableRow(outputRow),
            m_allocator(RowEntityAllocator::RowContent),
            m_nullExtractor(false)
        {
        }

    public:
        virtual void CreateInstance(const InputFileInfo& input, SIZE_T bufSize, int bufCount, SIZE_T virtualMemSize)
        {
            if (input.HasGroupId())
            {
                // In case of stream group get concatenated stream data from IOManager
                m_inputStreamData = IOManager::GetGlobal()->GetInputGroupConcatStream(input.groupId);
                m_inputStreamReader = gcnew SqlIpStreamReader<InputType>(m_inputStreamData, bufSize, bufCount);
            }
            else
            {
                m_inputStreamReader = gcnew SqlIpStreamReader<InputType>(input, bufSize, bufCount);
            }

            m_inputName = input.inputFileName;
            m_allocator.Init(virtualMemSize, sm_className);
            
            // set stream id to output row, with bypassing readonly check.
            if (m_streamIdColumnIdx != -1)
            {
                ManagedRow<OutputSchema> managedRow;
                SqlIpSchema^ tmpSchema = gcnew SqlIpSchema(managedRow.Columns(nullptr), &ManagedRow<OutputSchema>::ComplexColumnGetter, &ManagedRow<OutputSchema>::UDTColumnGetter);
                IUpdatableRow^ tmpRow = tmpSchema->Defaults->AsUpdatable();
                tmpRow->Set(m_streamIdColumnIdx, input.streamId);
                cli::array<int>^ ordinals = gcnew cli::array<int>(1){m_streamIdColumnIdx};
                m_updatableRow->CopyColumns(tmpRow->AsReadOnly(), ordinals, ordinals);
            }
        }

        virtual void Init()
        {
            try
            {
                Generic::IEnumerable<IRow^>^ extractor = m_extractor->Extract(m_inputStreamReader.get(), m_updatableRow);
                if (extractor == nullptr)
                {
                    m_nullExtractor = true;
                }
                else
                {
                    m_enumerator = extractor->GetEnumerator();
                }
            }
            catch (Exception ^ex)
            {
                ScopeEngineManaged::UserExceptionHelper::WrapUserException(m_extractor->GetType()->FullName, "Extract", ex);
                throw;
            }
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            // IExtractor.Extract method may return null, treat as empty IEnumerable.  
            if (m_nullExtractor)
            {
                // EOF
                return false;
            }

            Generic::IEnumerator<IRow^> ^ enumerator = m_enumerator;

            try
            {
                if (enumerator->MoveNext())
                {
                    m_allocator.Reset();
                    CheckSqlIpUdoYieldRow(enumerator->Current, (System::Object^)m_updatableRow.get());
                    (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                    // clean up output row.
                    m_updatableRow->Reset();
                    return true;
                }

                // EOF
                return false;
            }
            catch (ScopeStreamExceptionWithEvidence & e)
            {
                IOManager::GetGlobal()->SetStreamParseErrorState(m_inputName, e.what());
                throw;
            }
            catch (Exception ^ex)
            {
                ScopeEngineManaged::UserExceptionHelper::WrapUserException(m_extractor->GetType()->FullName, "Extract", ex);
                throw;
            }
        }

        virtual void Close()
        {
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();

            // Dispose enumerator
            m_enumerator.reset();
            m_inputStreamReader->Close();
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement(sm_className);
            m_allocator.WriteRuntimeStats(node, sizeof(OutputSchema));
            if (m_inputStreamReader)
            {
                m_inputStreamReader->WriteRuntimeStats(node);
            }
        }

        virtual OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements()
                .AddMemoryInRows(OperatorRequirementsConstants::ScopeExtractor__Row_MinMemory)
                .Add(m_inputStreamReader->GetOperatorRequirements());
        }

        virtual ~SqlIpExtractor()
        {
            // There only to ensure proper destruction when base class destructor is called
            if (m_inputStreamReader)
            {
                try
                {
                    m_inputStreamReader.reset();
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
            if (m_inputStreamReader)
            {
                return m_inputStreamReader->GetTotalIoWaitTime();
            }

            return 0;
        }

        virtual __int64 IoStreamInclusiveTime()
        {
            if (m_inputStreamReader)
            {
                return m_inputStreamReader->GetInclusiveTimeMillisecond();
            }

            return 0;
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke DoScopeCEPCheckpoint for SqlIpExtractor!");
        }

        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke LoadScopeCEPCheckpoint for SqlIpExtractor!");
        }
    };

    template<typename InputType, typename OutputSchema>
    const char* const SqlIpExtractor<InputType, OutputSchema>::sm_className = "SqlIpExtractor";

    template<typename InputSchema, typename OutputSchema, int UID>
    INLINE ScopeProcessorManaged<InputSchema, OutputSchema> * ScopeProcessorManagedFactory::MakeSqlIp(OperatorDelegate<InputSchema> * child)
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        cli::array<System::String^> ^ readOnlyColumns_Input = managedUDO.ReadOnlyColumns_Input();
        cli::array<System::String^> ^ readOnlyColumns_Output = managedUDO.ReadOnlyColumns_Output();
        SCOPE_ASSERT(readOnlyColumns_Input == nullptr || readOnlyColumns_Output == nullptr || readOnlyColumns_Input->Length == readOnlyColumns_Output->Length);

        ManagedRow<InputSchema> managedInputRow;
        SqlIpSchema^ inputSchema = gcnew SqlIpSchema(managedInputRow.Columns(readOnlyColumns_Input), &ManagedRow<InputSchema>::ComplexColumnGetter, &ManagedRow<InputSchema>::UDTColumnGetter);
        SqlIpInputRowset<InputSchema>^ inputRowset = gcnew SqlIpInputRowset<InputSchema>(child, inputSchema, managedInputRow.ColumnOffsets());

        ManagedRow<OutputSchema> managedOuputRow;
        SqlIpSchema^ outputSchema = gcnew SqlIpSchema(managedOuputRow.Columns(readOnlyColumns_Output), &ManagedRow<OutputSchema>::ComplexColumnGetter, &ManagedRow<OutputSchema>::UDTColumnGetter);
        SqlIpUpdatableRow^ outputRow = gcnew SqlIpUpdatableRow(outputSchema->Defaults);

        return new SqlIpProcessor<InputSchema, OutputSchema>(managedUDO.get(),
            inputRowset,
            outputRow,
            &InteropToNativeRowPolicy<OutputSchema, UID>::Marshal,
            &RowTransformPolicy<InputSchema, OutputSchema, UID>::FilterTransformRow,
            readOnlyColumns_Input,
            readOnlyColumns_Output);
    }

    template<typename InputSchema, typename OutputSchema>
    class SqlIpProcessor : public ScopeProcessorManaged<InputSchema, OutputSchema>
    {
        friend struct ScopeProcessorManagedFactory;

        static const char* const sm_className;

        typedef void(*MarshalToNativeCallType)(IRow ^, OutputSchema &, IncrementalAllocator *);
        typedef bool(*TransformRowCallType)(InputSchema &, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<SqlIpInputRowset<InputSchema> ^>        m_inputRowset;
        ScopeTypedManagedHandle<Generic::IEnumerator<IRow^> ^>          m_inputEnumerator;
        ScopeTypedManagedHandle<IProcessor ^>                           m_processor;
        ScopeTypedManagedHandle<SqlIpUpdatableRow ^>                    m_outputRow;
        MarshalToNativeCallType                                         m_marshalToNative;
        TransformRowCallType                                            m_transformRowCall;
        RowEntityAllocator                                              m_allocator;
        ScopeTypedManagedHandle<cli::array<int> ^>                      m_inputReadOnlyColumnOrdinal;
        ScopeTypedManagedHandle<cli::array<int> ^>                      m_outputReadOnlyColumnOrdinal;
        bool                                                            m_checkReadOnly;

        SqlIpProcessor(IProcessor ^ udo,
            SqlIpInputRowset<InputSchema>^ inputRowset,
            SqlIpUpdatableRow^ outputRow,
            MarshalToNativeCallType marshalToNativeCall,
            TransformRowCallType transformRowCall,
            cli::array<System::String^>^ readOnlyColumns_in,
            cli::array<System::String^>^ readOnlyColumns_out)
            : m_processor(udo),
            m_inputRowset(inputRowset),
            m_inputEnumerator(inputRowset->GetEnumerator()),
            m_outputRow(outputRow),
            m_marshalToNative(marshalToNativeCall),
            m_transformRowCall(transformRowCall),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
            m_checkReadOnly(false)
        {
            if (readOnlyColumns_in != nullptr && readOnlyColumns_in->Length > 0)
            {
                m_checkReadOnly = true;

                cli::array<int>^ inputOrdinal = nullptr;
                cli::array<int>^ outputOrdinal = nullptr;
                GetReadOnlyColumnOrdinal(m_inputRowset->Schema, readOnlyColumns_in, inputOrdinal);
                GetReadOnlyColumnOrdinal(m_outputRow->Schema, readOnlyColumns_out, outputOrdinal);

                m_inputReadOnlyColumnOrdinal.reset(inputOrdinal);
                m_outputReadOnlyColumnOrdinal.reset(outputOrdinal);
            }
        }

        void ResetOutput()
        {
            // clear output row before handing over to udo        
            m_outputRow->Reset();

            // some columns are pass-through, copy (from input to output) to be visible in outputrow, inside udo            
            if (m_checkReadOnly)
            {
                m_outputRow->CopyColumns(m_inputEnumerator->Current, m_inputReadOnlyColumnOrdinal, m_outputReadOnlyColumnOrdinal);
            }
        }

    public:

        virtual void Init()
        {
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            while (m_inputEnumerator->MoveNext())
            {
                ResetOutput();

                IRow^ outRow = m_processor->Process(m_inputEnumerator->Current, m_outputRow.get());

                if (outRow != nullptr)
                {
                    m_allocator.Reset();
                    CheckSqlIpUdoYieldRow(outRow, (System::Object^)m_outputRow.get());
                    (*m_marshalToNative)(outRow, output, &m_allocator);
                    (*m_transformRowCall)(m_inputRowset->GetCurrentNativeRow(), output, nullptr);

                    return true;
                }
                else
                {
                    // explicit put "continue" here, indicating that Processor::Process may return null, treat it same as empty rowset
                    continue;
                }
            }

            return false;
        }

        virtual void Close()
        {
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            // Dispose enumerator
            m_inputEnumerator.reset();
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

        virtual ~SqlIpProcessor()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements().AddMemoryInRows(OperatorRequirementsConstants::ScopeProcessor__Row_MinMemory);
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream& output)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke DoScopeCEPCheckpoint for SqlIpProcessor!");
        }

        virtual void LoadScopeCEPCheckpoint(BinaryInputStream& input)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke LoadScopeCEPCheckpoint for SqlIpProcessor!");
        }
    };

    template<typename InputSchema, typename OutputSchema>
    const char* const SqlIpProcessor<InputSchema, OutputSchema>::sm_className = "SqlIpProcessor";

    template<typename InputSchema, typename OutputSchema, int UID>
    INLINE ScopeProcessorManaged<InputSchema, OutputSchema> * ScopeApplierManagedFactory::MakeSqlIp(OperatorDelegate<InputSchema> * child)
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);

        ManagedRow<InputSchema> managedInputRow;
        SqlIpSchema^ inputSchema = gcnew SqlIpSchema(managedInputRow.Columns(nullptr), &ManagedRow<InputSchema>::ComplexColumnGetter, &ManagedRow<InputSchema>::UDTColumnGetter);
        SqlIpInputRowset<InputSchema>^ inputRowset = gcnew SqlIpInputRowset<InputSchema>(child, inputSchema, managedInputRow.ColumnOffsets());

        ManagedRow<OutputSchema> managedOuputRow;
        SqlIpSchema^ outputSchema = gcnew SqlIpSchema(managedOuputRow.Columns(nullptr), &ManagedRow<InputSchema>::ComplexColumnGetter, &ManagedRow<InputSchema>::UDTColumnGetter);
        SqlIpUpdatableRow^ outputRow = gcnew SqlIpUpdatableRow(outputSchema->Defaults);

        return new SqlIpApplier<InputSchema, OutputSchema>(managedUDO.get(),
            inputRowset,
            outputRow,
            &InteropToNativeRowPolicy<OutputSchema, UID>::Marshal,
            &RowTransformPolicy<InputSchema, OutputSchema, UID>::FilterTransformRow);
    }

    template<typename InputSchema, typename OutputSchema>
    class SqlIpApplier : public ScopeProcessorManaged<InputSchema, OutputSchema>
    {
        friend struct ScopeApplierManagedFactory;

        static const char* const sm_className;

        typedef void(*MarshalToNativeCallType)(IRow ^, OutputSchema &, IncrementalAllocator *);
        typedef bool(*TransformRowCallType)(InputSchema &, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<SqlIpInputRowset<InputSchema> ^>        m_inputRowset;
        ScopeTypedManagedHandle<Generic::IEnumerator<IRow^> ^>          m_inputEnumerator;
        ScopeTypedManagedHandle<Generic::IEnumerator<IRow^> ^>          m_outputEnumerator;
        ScopeTypedManagedHandle<IApplier ^>                             m_applier;
        ScopeTypedManagedHandle<SqlIpUpdatableRow ^>                    m_outputRow;
        MarshalToNativeCallType                                         m_marshalToNative;
        TransformRowCallType                                            m_transformRowCall;
        RowEntityAllocator                                              m_allocator;

        SqlIpApplier(IApplier ^ udo,
            SqlIpInputRowset<InputSchema>^ inputRowset,
            SqlIpUpdatableRow^ outputRow,
            MarshalToNativeCallType marshalToNativeCall,
            TransformRowCallType transformRowCall)
            : m_applier(udo),
            m_inputRowset(inputRowset),
            m_inputEnumerator(inputRowset->GetEnumerator()),
            m_outputRow(outputRow),
            m_marshalToNative(marshalToNativeCall),
            m_transformRowCall(transformRowCall),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent)
        {
        }

        void ResetOutput()
        {
            // clear output row before handing over to udo        
            m_outputRow->Reset();
        }

    public:

        virtual void Init()
        {
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            if (m_outputEnumerator.get() != nullptr && m_outputEnumerator->MoveNext())
            {
                m_allocator.Reset();
                CheckSqlIpUdoYieldRow(m_outputEnumerator->Current, (System::Object^)m_outputRow.get());
                (*m_marshalToNative)(m_outputEnumerator->Current, output, &m_allocator);
                (*m_transformRowCall)(m_inputRowset->GetCurrentNativeRow(), output, nullptr);

                return true;
            }

            while (m_inputEnumerator->MoveNext())
            {
                ResetOutput();
                m_outputEnumerator.reset(m_applier->Apply(m_inputEnumerator->Current, m_outputRow.get())->GetEnumerator());

                if (m_outputEnumerator->MoveNext())
                {
                    m_allocator.Reset();
                    CheckSqlIpUdoYieldRow(m_outputEnumerator->Current, (System::Object^)m_outputRow.get());
                    (*m_marshalToNative)(m_outputEnumerator->Current, output, &m_allocator);
                    (*m_transformRowCall)(m_inputRowset->GetCurrentNativeRow(), output, nullptr);

                    return true;
                }
            }

            return false;
        }

        virtual void Close()
        {
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            // Dispose enumerator
            m_inputEnumerator.reset();
            m_outputEnumerator.reset();
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

        virtual ~SqlIpApplier()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke DoScopeCEPCheckpoint for SqlIpApplier!");
        }

        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke LoadScopeCEPCheckpoint for SqlIpApplier!");
        }
    };

    template<typename InputSchema, typename OutputSchema>
    const char* const SqlIpApplier<InputSchema, OutputSchema>::sm_className = "SqlIpApplier";

    template<typename InputSchema, typename OutputSchema, int UID>
    INLINE ScopeProcessorManaged<InputSchema, OutputSchema> * ScopeGrouperManagedFactory::MakeSqlIp(OperatorDelegate<InputSchema> * child)
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);

        ManagedRow<InputSchema> managedInputRow;
        SqlIpSchema^ inputSchema = gcnew SqlIpSchema(managedInputRow.Columns(nullptr), &ManagedRow<InputSchema>::ComplexColumnGetter, &ManagedRow<InputSchema>::UDTColumnGetter);
        SqlIpInputRowset<InputSchema>^ inputRowset = gcnew SqlIpInputRowset<InputSchema>(child, inputSchema, managedInputRow.ColumnOffsets());

        ManagedRow<OutputSchema> managedOuputRow;
        SqlIpSchema^ outputSchema = gcnew SqlIpSchema(managedOuputRow.Columns(nullptr), &ManagedRow<OutputSchema>::ComplexColumnGetter, &ManagedRow<OutputSchema>::UDTColumnGetter);
        SqlIpUpdatableRow^ outputRow = gcnew SqlIpUpdatableRow(outputSchema->Defaults);


        return new SqlIpGrouper<InputSchema, OutputSchema>(managedUDO.get(),
            inputRowset,
            outputRow,
            &InteropToNativeRowPolicy<OutputSchema, UID>::Marshal,
            &RowTransformPolicy<InputSchema, OutputSchema, UID>::FilterTransformRow);
    }

    template<typename InputSchema, typename OutputSchema>
    class SqlIpGrouper : public ScopeProcessorManaged<InputSchema, OutputSchema>
    {
        friend struct ScopeGrouperManagedFactory;

        static const char* const sm_className;

        typedef void(*MarshalToNativeCallType)(IRow ^, OutputSchema &, IncrementalAllocator *);
        typedef bool(*TransformRowCallType)(InputSchema &, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<IRow^> ^>          m_enumerator;
        ScopeTypedManagedHandle<SqlIpInputRowset<InputSchema> ^>        m_inputRowset;
        ScopeTypedManagedHandle<IReducer ^>                             m_grouper;
        ScopeTypedManagedHandle<SqlIpUpdatableRow ^>                    m_outputRow;
        MarshalToNativeCallType                                         m_marshalToNative;
        TransformRowCallType                                            m_transformRowCall;
        RowEntityAllocator                                              m_allocator;
        bool                                                            m_empty;

        SqlIpGrouper(IReducer ^ udo,
            SqlIpInputRowset<InputSchema>^ inputRowset,
            SqlIpUpdatableRow^ outputRow,
            MarshalToNativeCallType marshalToNativeCall,
            TransformRowCallType transformRowCall)
            : m_grouper(udo),
            m_inputRowset(inputRowset),
            m_outputRow(outputRow),
            m_marshalToNative(marshalToNativeCall),
            m_transformRowCall(transformRowCall),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
            m_empty(false)
        {
        }

        void ResetOutput()
        {
            // clear output row before handing over to udo        
            m_outputRow->Reset();
        }

    public:

        virtual void Init()
        {
            Generic::IEnumerable<IRow^>^ reducer = m_grouper->Reduce(m_inputRowset.get(), m_outputRow);
            if (reducer == nullptr)
            {
                m_empty = true;
            }
            else
            {
                m_enumerator = reducer->GetEnumerator();
            }
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            if (m_empty)
            {
                return false;
            }

            Generic::IEnumerator<IRow^> ^ enumerator = m_enumerator;
            ResetOutput();

            if (enumerator->MoveNext())
            {
                CheckSqlIpUdoYieldRow(enumerator->Current, (System::Object^)m_outputRow.get());
                m_allocator.Reset();
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);
                (*m_transformRowCall)(m_inputRowset->GetCurrentNativeRow(), output, nullptr);

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

        virtual ~SqlIpGrouper()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke DoScopeCEPCheckpoint for SqlIpGrouper!");
        }

        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke LoadScopeCEPCheckpoint for SqlIpGrouper!");
        }
    };

    template<typename InputSchema, typename OutputSchema>
    const char* const SqlIpGrouper<InputSchema, OutputSchema>::sm_className = "SqlIpGrouper";


    template<typename InputSchema, int UID, int RunScopeCEPMode>
    ScopeOutputerManaged<InputSchema> * ScopeOutputerManagedFactory::MakeSqlIp(OperatorDelegate<InputSchema> * child)
    {
        return ScopeOutputerManagedFactory::MakeSqlIp(child, false);
    }

    template<typename InputSchema, int UID, int RunScopeCEPMode>
    ScopeOutputerManaged<InputSchema> * ScopeOutputerManagedFactory::MakeSqlIp(OperatorDelegate<InputSchema> * child, bool disableCompression)
    {
        ManagedUDO<UID> managedUDO(nullptr, 0);
        ManagedRow<InputSchema> managedRow;

        SqlIpSchema^ schema = gcnew SqlIpSchema(managedRow.Columns(nullptr), &ManagedRow<InputSchema>::ComplexColumnGetter, &ManagedRow<InputSchema>::UDTColumnGetter);
        SqlIpInputRowset<InputSchema>^ rowset = gcnew SqlIpInputRowset<InputSchema>(child, schema, managedRow.ColumnOffsets());
        return new SqlIpOutputer<InputSchema>(rowset, managedUDO.get(), disableCompression);
    }

    template<typename InputSchema>
    class SqlIpOutputer : public ScopeOutputerManaged<InputSchema>
    {
        friend struct ScopeOutputerManagedFactory;
        static const char* const sm_className;

        ScopeTypedManagedHandle<IOutputter ^>                       m_outputer;
        ScopeTypedManagedHandle<SqlIpInputRowset<InputSchema> ^>    m_inputRowset;
        ScopeTypedManagedHandle<SqlIpStreamWriter ^>                m_outputStreamWriter;
        bool                                                        m_disableCompression;

        SqlIpOutputer(SqlIpInputRowset<InputSchema>^ rowset, IOutputter ^ outputer)
            : SqlIpOutputer( rowset, outputer, false)
        {
        }

        SqlIpOutputer(SqlIpInputRowset<InputSchema>^ rowset, IOutputter ^ outputer, bool disableCompression)
        {
            m_outputer = outputer;
            m_inputRowset = rowset;
            m_disableCompression = disableCompression;
        }

    public:

        virtual ~SqlIpOutputer()
        {
            // There only to ensure proper destruction when base class destructor is called
            if (m_outputStreamWriter)
            {
                try
                {
                    m_outputStreamWriter.reset();
                }
                catch (std::exception&)
                {
                    // ignore any I/O errors as we are destroying the object anyway
                }
            }
        }

        virtual void CreateStream(std::string& outputName, SIZE_T bufSize, int bufCnt)
        {
            m_outputStreamWriter = gcnew SqlIpStreamWriter(outputName, bufSize, bufCnt, m_disableCompression);
        }

        virtual void Init()
        {
        }

        virtual void Output()
        {
            auto writer = m_outputStreamWriter.get();
            for each(auto row in m_inputRowset.get())
            {
                // UDO::Output
                m_outputer->Output(row, writer);

                // EOR (flushes to deal with record boundaries)
                writer->EndOfRow();
            }

            // UDO::Close
            //  Signal the end of rowset to the user
            m_outputer->Close();
        }

        virtual void Close()
        {
            if (m_outputStreamWriter)
            {
                m_outputStreamWriter->Close();
            }
        }

        virtual __int64 GetOperatorWaitOnIOTime()
        {
            if (m_outputStreamWriter)
            {
                return m_outputStreamWriter->GetTotalIoWaitTime();
            }

            return 0;
        }

        virtual __int64 IoStreamInclusiveTime()
        {
            if (m_outputStreamWriter)
            {
                return m_outputStreamWriter->GetInclusiveTimeMillisecond();
            }

            return 0;
        }

        virtual void WriteRuntimeStats(TreeNode & root)
        {
            RuntimeStats::WriteRowCount(root, (LONGLONG)m_inputRowset->RowCount);

            auto & node = root.AddElement(sm_className);

            if (m_inputRowset)
            {
                m_inputRowset->WriteRuntimeStats(node);
            }

            if (m_outputStreamWriter)
            {
                m_outputStreamWriter->WriteRuntimeStats(node);
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

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke DoScopeCEPCheckpoint for SqlIpOutputer!");
        }

        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke LoadScopeCEPCheckpoint for SqlIpOutputer!");
        }
    };// end of SqlIpOutputer

    template<typename InputSchema>
    const char* const SqlIpOutputer<InputSchema>::sm_className = "SqlIpOutputer";

    template<typename InputSchema, typename OutputSchema, int UID>
    INLINE ScopeProcessorManaged<InputSchema, OutputSchema> * ScopeReducerManagedFactory::MakeSqlIp(OperatorDelegate<InputSchema> * child)
    {
        typedef KeyComparePolicy<InputSchema, UID> KeyPolicy;
        ManagedUDO<UID> managedUDO(nullptr, 0);
        cli::array<System::String^> ^ readOnlyColumns_in = managedUDO.ReadOnlyColumns_Input();
        cli::array<System::String^> ^ readOnlyColumns_out = managedUDO.ReadOnlyColumns_Output();

        ManagedRow<InputSchema> inputRow;
        SqlIpSchema^ inputSchema = gcnew SqlIpSchema(inputRow.Columns(readOnlyColumns_in), &ManagedRow<InputSchema>::ComplexColumnGetter, &ManagedRow<InputSchema>::UDTColumnGetter);

        ManagedRow<OutputSchema> outputRow;
        SqlIpSchema^ outputSchema = gcnew SqlIpSchema(outputRow.Columns(readOnlyColumns_out), &ManagedRow<OutputSchema>::ComplexColumnGetter, &ManagedRow<OutputSchema>::UDTColumnGetter);
        SqlIpUpdatableRow^ updatableRow = gcnew SqlIpUpdatableRow(outputSchema->Defaults);

        return new SqlIpReducer<InputSchema, OutputSchema, KeyPolicy>(child,
            inputSchema,
            inputRow.ColumnOffsets(),
            updatableRow,
            managedUDO.get(),
            &InteropToNativeRowPolicy<OutputSchema, UID>::Marshal,
            readOnlyColumns_in,
            readOnlyColumns_out);
    }

    template<typename InputSchema, typename OutputSchema, typename KeyPolicy>
    class SqlIpReducer : public ScopeProcessorManaged<InputSchema, OutputSchema>
    {
        friend struct ScopeReducerManagedFactory;

        static const char* const sm_className;

        InputSchema m_inputRow;

        // Function pointer type for Marshal call methods.
        typedef void(*MarshalToNativeCallType)(IRow ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<IRow^> ^>              m_enumerator;
        ScopeTypedManagedHandle<SqlIpInputKeyset<InputSchema, KeyPolicy> ^>  m_inputKeyset;
        ScopeTypedManagedHandle<SqlIpUpdatableRow ^>                        m_outputRow;
        ScopeTypedManagedHandle<IReducer ^>                                 m_reducer;
        MarshalToNativeCallType                                             m_marshalToNative;
        RowEntityAllocator                                                  m_allocator;
        bool                                                                m_hasMoreRows;
        bool                                                                m_checkReadOnly;
        ScopeTypedManagedHandle<cli::array<int> ^>                          m_inputReadOnlyColumnOrdinal;
        ScopeTypedManagedHandle<cli::array<int> ^>                          m_outputReadOnlyColumnOrdinal;
        bool                                                                m_empty;

        typename SqlIpInputKeyset<InputSchema, KeyPolicy>::KeyIteratorType   m_keyIterator;

        SqlIpReducer(OperatorDelegate<InputSchema> *          child,
            SqlIpSchema^                             inputSchema,
            cli::array<ColumnOffsetId>^              columnOffset,
            SqlIpUpdatableRow ^                      outputRow,
            IReducer ^                               udo,
            MarshalToNativeCallType                  marshalToNativeCall,
            cli::array<String^>^                     readOnlyColumns_in,
            cli::array<String^>^                     readOnlyColumns_out)
            : m_keyIterator(child),
            m_outputRow(outputRow),
            m_reducer(udo),
            m_marshalToNative(marshalToNativeCall),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
            m_hasMoreRows(false),
            m_checkReadOnly(false),
            m_empty(false)
        {
            m_inputKeyset.reset(gcnew SqlIpInputKeyset<InputSchema, KeyPolicy>(&m_keyIterator, inputSchema, columnOffset));

            if (readOnlyColumns_in != nullptr && readOnlyColumns_in->Length > 0)
            {
                m_checkReadOnly = true;

                cli::array<int>^ inputOrdinal = nullptr;
                cli::array<int>^ outputOrdinal = nullptr;
                GetReadOnlyColumnOrdinal(m_inputKeyset->Schema, readOnlyColumns_in, inputOrdinal);
                GetReadOnlyColumnOrdinal(m_outputRow->Schema, readOnlyColumns_out, outputOrdinal);

                m_inputReadOnlyColumnOrdinal.reset(inputOrdinal);
                m_outputReadOnlyColumnOrdinal.reset(outputOrdinal);
            }
        }

        void ResetOutput()
        {
            // clear output row before handing over to udo        
            m_outputRow->Reset();

            // some columns are pass-through, copy (from input to output) to be visible in outputrow, inside udo            
            if (m_checkReadOnly)
            {
                m_outputRow->CopyColumns(m_inputKeyset->CurrentRow, m_inputReadOnlyColumnOrdinal, m_outputReadOnlyColumnOrdinal);
            }
        }

    public:

        virtual void Init()
        {
            m_hasMoreRows = m_inputKeyset->Init();

            if (m_hasMoreRows)
            {
                ResetOutput();

                Generic::IEnumerable<IRow^>^ reducer = m_reducer->Reduce(m_inputKeyset, m_outputRow);

                if (reducer == nullptr)
                {
                    m_empty = true;

                }
                else
                {
                    m_enumerator = reducer->GetEnumerator();
                    m_empty = false;
                }
            }
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            while (m_hasMoreRows)
            {
                // IReducer::Reduce may return null, treat it same as empty IEnumerable
                if (!m_empty)
                {
                    Generic::IEnumerator<IRow^> ^ enumerator = m_enumerator;

                    if (enumerator->MoveNext())
                    {
                        m_allocator.Reset();
                        CheckSqlIpUdoYieldRow(enumerator->Current, (System::Object^)m_outputRow.get());
                        (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                        return true;
                    }
                }

                if (m_inputKeyset->NextKey())
                {
                    ResetOutput();

                    // Proceed to the next key and create enumerator for it

                    Generic::IEnumerable<IRow^>^ reducer = m_reducer->Reduce(m_inputKeyset, m_outputRow);
                    if (reducer == nullptr)
                    {
                        m_empty = true;
                    }
                    else
                    {
                        m_enumerator.reset(reducer->GetEnumerator());
                        m_empty = false;
                    }
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

        virtual ~SqlIpReducer()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke DoScopeCEPCheckpoint for SqlIpReducer!");
        }

        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke LoadScopeCEPCheckpoint for SqlIpReducer!");
        }
    };

    template<typename InputSchema, typename OutputSchema, typename KeyPolicy>
    const char* const SqlIpReducer<InputSchema, OutputSchema, KeyPolicy>::sm_className = "SqlIpReducer";

    // TODO:  POLARIS:  When we have a single outputter operator for columnstore, we should deprecate the MDF reducer
    template<typename InputSchema, typename OutputSchema, int UID>
    INLINE ScopeProcessorManaged<InputSchema, OutputSchema> * ScopeReducerManagedFactory::MakeMDF(OperatorDelegate<InputSchema> * child)
    {
        typedef KeyComparePolicy<InputSchema, UID> KeyPolicy;
        ManagedUDO<UID> managedUDO(nullptr, 0);

        ManagedRow<InputSchema> inputRow;
        SqlIpSchema^ inputSchema = gcnew SqlIpSchema(inputRow.Columns(nullptr));

        ManagedRow<OutputSchema> outputRow;
        SqlIpSchema^ outputSchema = gcnew SqlIpSchema(outputRow.Columns(nullptr));
        SqlIpUpdatableRow^ updatableRow = gcnew SqlIpUpdatableRow(outputSchema->Defaults);

        return new MDFReducer<InputSchema, OutputSchema, KeyPolicy>(child,
            inputSchema,
            inputRow.ColumnOffsets(),
            updatableRow,
            managedUDO.get(),
            &InteropToNativeRowPolicy<OutputSchema, UID>::Marshal);
    }

    template<typename InputSchema, typename OutputSchema, typename KeyPolicy>
    class MDFReducer : public ScopeProcessorManaged<InputSchema, OutputSchema>
    {
        friend struct ScopeReducerManagedFactory;

        static const char* const sm_className;

        InputSchema m_inputRow;

        // Function pointer type for Marshal call methods.
        typedef void(*MarshalToNativeCallType)(IRow ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<IRow^> ^>              m_enumerator;
        ScopeTypedManagedHandle<SqlIpInputKeyset<InputSchema, KeyPolicy> ^>  m_inputKeyset;
        ScopeTypedManagedHandle<SqlIpUpdatableRow ^>                        m_outputRow;
        ScopeTypedManagedHandle<IReducer ^>                                 m_reducer;
        MarshalToNativeCallType                                             m_marshalToNative;
        RowEntityAllocator                                                  m_allocator;
        bool                                                                m_hasMoreRows;
        bool                                                                m_empty;

        typename SqlIpInputKeyset<InputSchema, KeyPolicy>::KeyIteratorType   m_keyIterator;

        MDFReducer(OperatorDelegate<InputSchema> *          child,
            SqlIpSchema^                             inputSchema,
            cli::array<ColumnOffsetId>^              columnOffset,
            SqlIpUpdatableRow ^                      outputRow,
            IReducer ^                               udo,
            MarshalToNativeCallType                  marshalToNativeCall)
            : m_keyIterator(child),
            m_outputRow(outputRow),
            m_reducer(udo),
            m_marshalToNative(marshalToNativeCall),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
            m_hasMoreRows(false),
            m_empty(false)
        {
            m_inputKeyset.reset(gcnew SqlIpInputKeyset<InputSchema, KeyPolicy>(&m_keyIterator, inputSchema, columnOffset));
        }

        void ResetOutput()
        {
            // clear output row before handing over to udo        
            m_outputRow->Reset();
        }

    public:

        virtual void Init()
        {
            // For empty input, we still need to init the reduce method to generate correct mdf, ndf and metadata.
            // For empty output: mdf file has 5 MB overhead data; ndf file has 8 MB overhead data; metadata output rowcount and datasize as 0.
            m_inputKeyset->Init();
            ResetOutput();
            m_hasMoreRows = true;

            Generic::IEnumerable<IRow^>^ reducer = m_reducer->Reduce(m_inputKeyset, m_outputRow);

            if (reducer == nullptr)
            {
                m_empty = true;
            }
            else
            {
                m_enumerator = reducer->GetEnumerator();
                m_empty = false;
            }
        }

        virtual bool GetNextRow(OutputSchema & output)
        {
            while (m_hasMoreRows)
            {
                // IReducer::Reduce may return null, treat it same as empty IEnumerable
                if (!m_empty)
                {
                    Generic::IEnumerator<IRow^> ^ enumerator = m_enumerator;

                    if (enumerator->MoveNext())
                    {
                        m_allocator.Reset();
                        (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                        return true;
                    }
                }

                if (m_inputKeyset->NextKey())
                {
                    ResetOutput();

                    // Proceed to the next key and create enumerator for it

                    Generic::IEnumerable<IRow^>^ reducer = m_reducer->Reduce(m_inputKeyset, m_outputRow);
                    if (reducer == nullptr)
                    {
                        m_empty = true;
                    }
                    else
                    {
                        m_enumerator.reset(reducer->GetEnumerator());
                        m_empty = false;
                    }
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

        virtual ~MDFReducer()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke DoScopeCEPCheckpoint for MDFReducer!");
        }

        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke LoadScopeCEPCheckpoint for MDFReducer!");
        }
    };

    template<typename InputSchema, typename OutputSchema, typename KeyPolicy>
    const char* const MDFReducer<InputSchema, OutputSchema, KeyPolicy>::sm_className = "MDFReducer";

    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, typename LeftKeyPolicy, typename RightKeyPolicy, int UID, typename ThirdInputSchema, typename FourthInputSchema>
    INLINE SqlIpMultiwayCombinerManaged<InputSchemaLeft, InputSchemaRight, OutputSchema, LeftKeyPolicy, RightKeyPolicy, ThirdInputSchema, FourthInputSchema> * SqlIpMultiwayCombinerManagedFactory::MakeSqlIp(
        OperatorDelegate<InputSchemaLeft> * leftChild,
        OperatorDelegate<InputSchemaRight> * rightChild,
        OperatorDelegate<ThirdInputSchema> * thirdChild,
        OperatorDelegate<FourthInputSchema> * fourthChild)

    {
        // udo
        ManagedUDO<UID> managedUDO(nullptr, 0);
        cli::array<System::String^> ^ readOnlyColumns_leftin = managedUDO.ReadOnlyColumns_InputLeft();
        cli::array<System::String^> ^ readOnlyColumns_rightin = managedUDO.ReadOnlyColumns_InputRight();
        cli::array<System::String^> ^ readOnlyColumns_leftout = managedUDO.ReadOnlyColumns_OutputLeft();
        cli::array<System::String^> ^ readOnlyColumns_rightout = managedUDO.ReadOnlyColumns_OutputRight();
        cli::array<System::String^> ^ readOnlyColumns_out = managedUDO.ReadOnlyColumns_Output();

        // output row
        ManagedRow<OutputSchema> outputRow;
        SqlIpSchema^ outputSchema = gcnew SqlIpSchema(outputRow.Columns(readOnlyColumns_out), &ManagedRow<OutputSchema>::ComplexColumnGetter, &ManagedRow<OutputSchema>::UDTColumnGetter);
        SqlIpUpdatableRow^ updatableRow = gcnew SqlIpUpdatableRow(outputSchema->Defaults);

        // Create combiner
        return new SqlIpMultiwayCombiner<InputSchemaLeft, InputSchemaRight, OutputSchema, LeftKeyPolicy, RightKeyPolicy, ThirdInputSchema, FourthInputSchema>(managedUDO.get(),
            leftChild,
            rightChild,
            updatableRow,
            &InteropToNativeRowPolicy<OutputSchema, UID>::Marshal,
            readOnlyColumns_leftin,
            readOnlyColumns_rightin,
            readOnlyColumns_leftout,
            readOnlyColumns_rightout,
            thirdChild,
            fourthChild);

    }
    
    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, typename LeftKeyPolicy, typename RightKeyPolicy, typename ThirdInputSchema = None, typename FourthInputSchema = None>
    class SqlIpMultiwayCombiner : public SqlIpMultiwayCombinerManaged<InputSchemaLeft, InputSchemaRight, OutputSchema, LeftKeyPolicy, RightKeyPolicy, ThirdInputSchema, FourthInputSchema>
    {
        friend struct SqlIpMultiwayCombinerManagedFactory;

        static const char* const sm_className;

        // Function pointer type for Marshal call methods.
        typedef void(*MarshalToNativeCallType)(IRow ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<IRow^> ^>                         m_enumerator;

        ScopeTypedManagedHandle<SqlIpInputKeyset<InputSchemaLeft, LeftKeyPolicy> ^>    m_inputRowsetLeft;
        ScopeTypedManagedHandle<SqlIpInputKeyset<InputSchemaRight, RightKeyPolicy> ^>  m_inputRowsetRight;
        ScopeTypedManagedHandle<SqlIpInputRowset<ThirdInputSchema> ^>                  m_inputRowsetThird;
        ScopeTypedManagedHandle<SqlIpInputRowset<FourthInputSchema> ^>                 m_inputRowsetFourth;

        ScopeTypedManagedHandle<SqlIpUpdatableRow^>                                    m_outputRow;
        ScopeTypedManagedHandle<ICombiner ^>                                           m_combiner;
        MarshalToNativeCallType                                                        m_marshalToNative;
        RowEntityAllocator                                                             m_allocator;

        LeftKeyIteratorType                                                            m_leftKeyIterator;
        RightKeyIteratorType                                                           m_rightKeyIterator;
        OperatorDelegate<ThirdInputSchema>                                             m_thirdOperator;
        OperatorDelegate<FourthInputSchema>                                            m_fourthOperator;

        ScopeTypedManagedHandle<SqlIpSchema^>                                          m_leftSchema;
        ScopeTypedManagedHandle<SqlIpSchema^>                                          m_rightSchema;
        ScopeTypedManagedHandle<SqlIpSchema^>                                          m_thirdSchema;
        ScopeTypedManagedHandle<SqlIpSchema^>                                          m_fourthSchema;

        ScopeTypedManagedHandle<cli::array<ColumnOffsetId>^>                           m_leftColumnOffset;
        ScopeTypedManagedHandle<cli::array<ColumnOffsetId>^>                           m_rightColumnOffset;
        ScopeTypedManagedHandle<cli::array<ColumnOffsetId>^>                           m_thirdColumnOffset;
        ScopeTypedManagedHandle<cli::array<ColumnOffsetId>^>                           m_fourthColumnOffset;

        bool                                                                           m_firstRowInKeySet;
        bool                                                                           m_checkReadOnly;
        ScopeTypedManagedHandle<cli::array<int> ^>                                     m_inputReadOnlyColumnOrdinalLeft;
        ScopeTypedManagedHandle<cli::array<int> ^>                                     m_inputReadOnlyColumnOrdinalRight;

        ScopeTypedManagedHandle<cli::array<int> ^>                                     m_outputReadOnlyColumnOrdinalLeft;
        ScopeTypedManagedHandle<cli::array<int> ^>                                     m_outputReadOnlyColumnOrdinalRight;

        ScopeTypedManagedHandle<EmptySqlIpRowset ^>                                    m_emptyRowsLeft;
        ScopeTypedManagedHandle<EmptySqlIpRowset ^>                                    m_emptyRowsRight;

        SqlIpMultiwayCombiner(ICombiner ^                                         udo,
            OperatorDelegate<InputSchemaLeft> *                 leftChild,
            OperatorDelegate<InputSchemaRight>*                 rightChild,
            SqlIpUpdatableRow^                                  outputRow,
            MarshalToNativeCallType                             marshalToNativeCall,
            cli::array<String^>^                                readOnlyColumns_leftin,
            cli::array<String^>^                                readOnlyColumns_rightin,
            cli::array<String^>^                                readOnlyColumns_leftout,
            cli::array<String^>^                                readOnlyColumns_rightout,
            OperatorDelegate<ThirdInputSchema>*                 thirdChild = nullptr,
            OperatorDelegate<FourthInputSchema>*                fourthChild = nullptr)
            : m_combiner(udo),
            m_leftKeyIterator(leftChild),
            m_rightKeyIterator(rightChild),
            m_thirdOperator(thirdChild),
            m_fourthOperator(fourthChild),
            m_outputRow(outputRow),
            m_marshalToNative(marshalToNativeCall),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
            m_checkReadOnly(false),
            m_firstRowInKeySet(false)
        {
            // left input rowset
            ManagedRow<InputSchemaLeft> leftRow;
            m_leftSchema.reset(gcnew SqlIpSchema(leftRow.Columns(readOnlyColumns_leftin), &ManagedRow<InputSchemaLeft>::ComplexColumnGetter, &ManagedRow<InputSchemaLeft>::UDTColumnGetter));
            m_leftColumnOffset.reset(leftRow.ColumnOffsets());

            // right input rowset
            ManagedRow<InputSchemaRight> rightRow;
            m_rightSchema.reset(gcnew SqlIpSchema(rightRow.Columns(readOnlyColumns_rightin), &ManagedRow<InputSchemaRight>::ComplexColumnGetter, &ManagedRow<InputSchemaRight>::UDTColumnGetter));
            m_rightColumnOffset.reset(rightRow.ColumnOffsets());

            // third input rowset
            if (!m_thirdOperator.IsNull())
            {
                ManagedRow<ThirdInputSchema> thirdRow;
                m_thirdSchema.reset(gcnew SqlIpSchema(thirdRow.Columns(nullptr), &ManagedRow<ThirdInputSchema>::ComplexColumnGetter, &ManagedRow<ThirdInputSchema>::UDTColumnGetter));
                m_thirdColumnOffset.reset(thirdRow.ColumnOffsets());
            }

            // fourth input rowset
            if (!m_fourthOperator.IsNull())
            {
                ManagedRow<FourthInputSchema> fourthRow;
                m_fourthSchema.reset(gcnew SqlIpSchema(fourthRow.Columns(nullptr), &ManagedRow<FourthInputSchema>::ComplexColumnGetter, &ManagedRow<FourthInputSchema>::UDTColumnGetter));
                m_fourthColumnOffset.reset(fourthRow.ColumnOffsets());
            }

            m_emptyRowsLeft = gcnew EmptySqlIpRowset(m_leftSchema);
            m_emptyRowsRight = gcnew EmptySqlIpRowset(m_rightSchema);

            if (readOnlyColumns_leftin != nullptr || readOnlyColumns_rightin != nullptr)
            {
                m_checkReadOnly = true;

                cli::array<int>^ inputOrdinal_left = nullptr;
                cli::array<int>^ inputOrdinal_right = nullptr;
                cli::array<int>^ outputOrdinal_left = nullptr;
                cli::array<int>^ outputOrdinal_right = nullptr;

                GetReadOnlyColumnOrdinal(m_leftSchema, readOnlyColumns_leftin, inputOrdinal_left);
                GetReadOnlyColumnOrdinal(m_outputRow->Schema, readOnlyColumns_leftout, outputOrdinal_left);
                m_inputReadOnlyColumnOrdinalLeft.reset(inputOrdinal_left);
                m_outputReadOnlyColumnOrdinalLeft.reset(outputOrdinal_left);

                GetReadOnlyColumnOrdinal(m_rightSchema, readOnlyColumns_rightin, inputOrdinal_right);
                GetReadOnlyColumnOrdinal(m_outputRow->Schema, readOnlyColumns_rightout, outputOrdinal_right);
                m_inputReadOnlyColumnOrdinalRight.reset(inputOrdinal_right);
                m_outputReadOnlyColumnOrdinalRight.reset(outputOrdinal_right);
            }
        }

        void ResetOutput()
        {
            // clear output row before handing over to udo
            m_outputRow->Reset();

            // some columns are pass-through, copy (from input to output) to be visible in outputrow, inside udo
            if (m_checkReadOnly)
            {
                if (m_inputReadOnlyColumnOrdinalLeft != nullptr)
                {
                    m_outputRow->CopyColumns(m_inputRowsetLeft->CurrentRow, m_inputReadOnlyColumnOrdinalLeft, m_outputReadOnlyColumnOrdinalLeft);
                }

                if (m_inputReadOnlyColumnOrdinalRight != nullptr)
                {
                    m_outputRow->CopyColumns(m_inputRowsetRight->CurrentRow, m_inputReadOnlyColumnOrdinalRight, m_outputReadOnlyColumnOrdinalRight);
                }
            }
        }

    public:

        virtual void Init()
        {
            m_inputRowsetLeft.reset(gcnew SqlIpInputKeyset<InputSchemaLeft, LeftKeyPolicy>(&m_leftKeyIterator, m_leftSchema, m_leftColumnOffset));
            m_inputRowsetRight.reset(gcnew SqlIpInputKeyset<InputSchemaRight, RightKeyPolicy>(&m_rightKeyIterator, m_rightSchema, m_rightColumnOffset));
            if (!m_thirdOperator.IsNull())
            {
                m_inputRowsetThird.reset(gcnew SqlIpInputRowset<ThirdInputSchema>(&m_thirdOperator, m_thirdSchema, m_thirdColumnOffset));
            }
            if (!m_fourthOperator.IsNull())
            {
                m_inputRowsetFourth.reset(gcnew SqlIpInputRowset<FourthInputSchema>(&m_fourthOperator, m_fourthSchema, m_fourthColumnOffset));
            }

            m_enumerator.reset();
        }

        virtual bool GetNextRow(OutputSchema & output, SIDETYPE side)
        {
            // it's a little hacky by moving initialization from Init to GetNextRow
            // in current implemention, m_leftKeyIterator->ReadFirst is called by the caller - NativeCombinerWrapper
            // it works well if UDO is using yield return.
            // It should also be acceptable that UDO creates a List and return it as IEnumerable.
            // in such case, m_combiner->Combine will call m_inputRowsetLeft->MoveNext.
            // But, m_inputRowsetLeft is not inialized at this moment. 
            // if we explicitly call m_inputRowsetLeft->Init(), it'll call m_leftKeyIterator->ReadFirst twice.
            if (m_enumerator.get() == nullptr)
            {
                Generic::IEnumerable<IRow^>^ combiner;

                // If there is more than two inputs we must pass one or more broadcast tables (inputs 3, 4...)
                if (!m_thirdOperator.IsNull())
                {

                    Generic::List<IRowset^>^ inputs = gcnew Generic::List<IRowset^>();
                    inputs->Add(side == SIDETYPE::RIGHT ? (IRowset^)m_emptyRowsLeft : (IRowset^)m_inputRowsetLeft);
                    inputs->Add(side == SIDETYPE::LEFT ? (IRowset^)m_emptyRowsRight : (IRowset^)m_inputRowsetRight);
                    inputs->Add(m_inputRowsetThird);
                    if (!m_fourthOperator.IsNull())
                    {
                        inputs->Add(m_inputRowsetFourth);
                    }

                    // Inputs is a list with at least three tables (the two regular tables plus the broadcast tables)
                    combiner = m_combiner->Combine(inputs, m_outputRow);
                }
                else
                {
                    SCOPE_ASSERT(!m_fourthOperator.IsNull());
                    switch (side)
                    {
                    case SIDETYPE::LEFT:
                        combiner = m_combiner->Combine(m_inputRowsetLeft, m_emptyRowsRight, m_outputRow);
                        break;

                    case SIDETYPE::RIGHT:
                        combiner = m_combiner->Combine(m_emptyRowsLeft, m_inputRowsetRight, m_outputRow);
                        break;

                    case SIDETYPE::BOTH:
                        combiner = m_combiner->Combine(m_inputRowsetLeft, m_inputRowsetRight, m_outputRow);
                        break;

                    default:
                        SCOPE_ASSERT(false);
                    }
                }

                // ICombiner.Combine method may return null, treat as empty IEnumerable. 
                if (combiner == nullptr)
                {
                    m_firstRowInKeySet = false;
                    Init();
                    return false;
                }

                m_enumerator.reset(combiner->GetEnumerator());
            }

            Generic::IEnumerator<IRow^> ^ enumerator = m_enumerator;

            // When calling into Udo first time, call ResetOutput to copy read only columns
            if (!m_firstRowInKeySet)
            {
                ResetOutput();
                m_firstRowInKeySet = true;
            }

            if (enumerator->MoveNext())
            {
                m_allocator.Reset();
                CheckSqlIpUdoYieldRow(enumerator->Current, (System::Object^)m_outputRow.get());
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                return true;
            }
            else
            {
                m_firstRowInKeySet = false;
                Init();
                return false;
            }
        }

        virtual void Close()
        {
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            // Dispose enumerator
            m_enumerator.reset();
        }

        virtual void SetLeftKeyIterator(LeftKeyIteratorType* iter)
        {
            m_inputRowsetLeft->SetIterator(iter);
        }

        virtual void SetRightKeyIterator(RightKeyIteratorType* iter)
        {
            m_inputRowsetRight->SetIterator(iter);
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
            if (m_inputRowsetThird)
            {
                m_inputRowsetThird->WriteRuntimeStats(node);
            }
            if (m_inputRowsetFourth)
            {
                m_inputRowsetFourth->WriteRuntimeStats(node);
            }
        }

        virtual OperatorRequirements GetOperatorRequirements()
        {
            return OperatorRequirements(OperatorRequirementsConstants::NativeCombinerWrapper_SqlIpUdoJoiner__Row_MinMemory);
        }

        virtual ~SqlIpMultiwayCombiner()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke DoScopeCEPCheckpoint for SqlIpMultiwayCombiner!");
        }

        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke LoadScopeCEPCheckpoint for SqlIpMultiwayCombiner!");
        }
    };

    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, typename LeftKeyPolicy, typename RightKeyPolicy, typename ThirdInputSchema, typename FourthInputSchema>
    const char* const SqlIpMultiwayCombiner<InputSchemaLeft, InputSchemaRight, OutputSchema, LeftKeyPolicy, RightKeyPolicy, ThirdInputSchema, FourthInputSchema>::sm_className = "SqlIpMultiwayCombiner";

    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, typename LeftKeyPolicy, typename RightKeyPolicy, int UID>
    INLINE SqlIpCombinerManaged<InputSchemaLeft, InputSchemaRight, OutputSchema, LeftKeyPolicy, RightKeyPolicy> * SqlIpCombinerManagedFactory::MakeSqlIp(OperatorDelegate<InputSchemaLeft> * leftChild,
        OperatorDelegate<InputSchemaRight> * rightChild)
    {
        // udo
        ManagedUDO<UID> managedUDO(nullptr, 0);
        cli::array<System::String^> ^ readOnlyColumns_leftin = managedUDO.ReadOnlyColumns_InputLeft();
        cli::array<System::String^> ^ readOnlyColumns_rightin = managedUDO.ReadOnlyColumns_InputRight();
        cli::array<System::String^> ^ readOnlyColumns_leftout = managedUDO.ReadOnlyColumns_OutputLeft();
        cli::array<System::String^> ^ readOnlyColumns_rightout = managedUDO.ReadOnlyColumns_OutputRight();
        cli::array<System::String^> ^ readOnlyColumns_out = managedUDO.ReadOnlyColumns_Output();

        // output row
        ManagedRow<OutputSchema> outputRow;
        SqlIpSchema^ outputSchema = gcnew SqlIpSchema(outputRow.Columns(readOnlyColumns_out), &ManagedRow<OutputSchema>::ComplexColumnGetter, &ManagedRow<OutputSchema>::UDTColumnGetter);
        SqlIpUpdatableRow^ updatableRow = gcnew SqlIpUpdatableRow(outputSchema->Defaults);

        // Create combiner
        return new SqlIpCombiner<InputSchemaLeft, InputSchemaRight, OutputSchema, LeftKeyPolicy, RightKeyPolicy>(managedUDO.get(),
            leftChild,
            rightChild,
            updatableRow,
            &InteropToNativeRowPolicy<OutputSchema, UID>::Marshal,
            readOnlyColumns_leftin,
            readOnlyColumns_rightin,
            readOnlyColumns_leftout,
            readOnlyColumns_rightout);

    }

    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, typename LeftKeyPolicy, typename RightKeyPolicy>
    class SqlIpCombiner : public SqlIpCombinerManaged<InputSchemaLeft, InputSchemaRight, OutputSchema, LeftKeyPolicy, RightKeyPolicy>
    {
        friend struct SqlIpCombinerManagedFactory;

        static const char* const sm_className;

        // Function pointer type for Marshal call methods.
        typedef void(*MarshalToNativeCallType)(IRow ^, OutputSchema &, IncrementalAllocator *);

        ScopeTypedManagedHandle<Generic::IEnumerator<IRow^> ^>                         m_enumerator;
        ScopeTypedManagedHandle<SqlIpInputKeyset<InputSchemaLeft, LeftKeyPolicy> ^>    m_inputRowsetLeft;
        ScopeTypedManagedHandle<SqlIpInputKeyset<InputSchemaRight, RightKeyPolicy> ^>  m_inputRowsetRight;
        ScopeTypedManagedHandle<SqlIpUpdatableRow^>                                    m_outputRow;
        ScopeTypedManagedHandle<ICombiner ^>                                           m_combiner;
        MarshalToNativeCallType                                                        m_marshalToNative;
        RowEntityAllocator                                                             m_allocator;

        LeftKeyIteratorType                                                            m_leftKeyIterator;
        RightKeyIteratorType                                                           m_rightKeyIterator;
        ScopeTypedManagedHandle<SqlIpSchema^>                                          m_leftSchema;
        ScopeTypedManagedHandle<SqlIpSchema^>                                          m_rightSchema;
        ScopeTypedManagedHandle<cli::array<ColumnOffsetId>^>                           m_leftColumnOffset;
        ScopeTypedManagedHandle<cli::array<ColumnOffsetId>^>                           m_rightColumnOffset;

        bool                                                                           m_firstRowInKeySet;
        bool                                                                           m_checkReadOnly;
        ScopeTypedManagedHandle<cli::array<int> ^>                                     m_inputReadOnlyColumnOrdinalLeft;
        ScopeTypedManagedHandle<cli::array<int> ^>                                     m_inputReadOnlyColumnOrdinalRight;
        ScopeTypedManagedHandle<cli::array<int> ^>                                     m_outputReadOnlyColumnOrdinalLeft;
        ScopeTypedManagedHandle<cli::array<int> ^>                                     m_outputReadOnlyColumnOrdinalRight;

        ScopeTypedManagedHandle<EmptySqlIpRowset ^>                                    m_emptyRowsLeft;
        ScopeTypedManagedHandle<EmptySqlIpRowset ^>                                    m_emptyRowsRight;

        SqlIpCombiner(ICombiner ^                                         udo,
            OperatorDelegate<InputSchemaLeft> *                 leftChild,
            OperatorDelegate<InputSchemaRight>*                 rightChild,
            SqlIpUpdatableRow^                                  outputRow,
            MarshalToNativeCallType                             marshalToNativeCall,
            cli::array<String^>^                                readOnlyColumns_leftin,
            cli::array<String^>^                                readOnlyColumns_rightin,
            cli::array<String^>^                                readOnlyColumns_leftout,
            cli::array<String^>^                                readOnlyColumns_rightout)
            : m_combiner(udo),
            m_leftKeyIterator(leftChild),
            m_rightKeyIterator(rightChild),
            m_outputRow(outputRow),
            m_marshalToNative(marshalToNativeCall),
            m_allocator(Configuration::GetGlobal().GetMaxInMemoryRowSize(), sm_className, RowEntityAllocator::RowContent),
            m_checkReadOnly(false),
            m_firstRowInKeySet(false)
        {
            // left input rowset
            ManagedRow<InputSchemaLeft> leftRow;
            m_leftSchema.reset(gcnew SqlIpSchema(leftRow.Columns(readOnlyColumns_leftin), &ManagedRow<InputSchemaLeft>::ComplexColumnGetter, &ManagedRow<InputSchemaLeft>::UDTColumnGetter));
            m_leftColumnOffset.reset(leftRow.ColumnOffsets());

            // right input rowset
            ManagedRow<InputSchemaRight> rightRow;
            m_rightSchema.reset(gcnew SqlIpSchema(rightRow.Columns(readOnlyColumns_rightin), &ManagedRow<InputSchemaRight>::ComplexColumnGetter, &ManagedRow<InputSchemaRight>::UDTColumnGetter));
            m_rightColumnOffset.reset(rightRow.ColumnOffsets());

            m_emptyRowsLeft = gcnew EmptySqlIpRowset(m_leftSchema);
            m_emptyRowsRight = gcnew EmptySqlIpRowset(m_rightSchema);

            if (readOnlyColumns_leftin != nullptr || readOnlyColumns_rightin != nullptr)
            {
                m_checkReadOnly = true;

                cli::array<int>^ inputOrdinal_left = nullptr;
                cli::array<int>^ inputOrdinal_right = nullptr;
                cli::array<int>^ outputOrdinal_left = nullptr;
                cli::array<int>^ outputOrdinal_right = nullptr;

                GetReadOnlyColumnOrdinal(m_leftSchema, readOnlyColumns_leftin, inputOrdinal_left);
                GetReadOnlyColumnOrdinal(m_outputRow->Schema, readOnlyColumns_leftout, outputOrdinal_left);
                m_inputReadOnlyColumnOrdinalLeft.reset(inputOrdinal_left);
                m_outputReadOnlyColumnOrdinalLeft.reset(outputOrdinal_left);

                GetReadOnlyColumnOrdinal(m_rightSchema, readOnlyColumns_rightin, inputOrdinal_right);
                GetReadOnlyColumnOrdinal(m_outputRow->Schema, readOnlyColumns_rightout, outputOrdinal_right);
                m_inputReadOnlyColumnOrdinalRight.reset(inputOrdinal_right);
                m_outputReadOnlyColumnOrdinalRight.reset(outputOrdinal_right);
            }
        }

        void ResetOutput()
        {
            // clear output row before handing over to udo
            m_outputRow->Reset();

            // some columns are pass-through, copy (from input to output) to be visible in outputrow, inside udo
            if (m_checkReadOnly)
            {
                if (m_inputReadOnlyColumnOrdinalLeft != nullptr)
                {
                    m_outputRow->CopyColumns(m_inputRowsetLeft->CurrentRow, m_inputReadOnlyColumnOrdinalLeft, m_outputReadOnlyColumnOrdinalLeft);
                }

                if (m_inputReadOnlyColumnOrdinalRight != nullptr)
                {
                    m_outputRow->CopyColumns(m_inputRowsetRight->CurrentRow, m_inputReadOnlyColumnOrdinalRight, m_outputReadOnlyColumnOrdinalRight);
                }
            }
        }

    public:

        virtual void Init()
        {
            m_inputRowsetLeft.reset(gcnew SqlIpInputKeyset<InputSchemaLeft, LeftKeyPolicy>(&m_leftKeyIterator, m_leftSchema, m_leftColumnOffset));
            m_inputRowsetRight.reset(gcnew SqlIpInputKeyset<InputSchemaRight, RightKeyPolicy>(&m_rightKeyIterator, m_rightSchema, m_rightColumnOffset));
            m_enumerator.reset();
        }

        virtual bool GetNextRow(OutputSchema & output, SIDETYPE side)
        {
            // it's a little hacky by moving initialization from Init to GetNextRow
            // in current implemention, m_leftKeyIterator->ReadFirst is called by the caller - NativeCombinerWrapper
            // it works well if UDO is using yield return.
            // It should also be acceptable that UDO creates a List and return it as IEnumerable.
            // in such case, m_combiner->Combine will call m_inputRowsetLeft->MoveNext.
            // But, m_inputRowsetLeft is not inialized at this moment. 
            // if we explicitly call m_inputRowsetLeft->Init(), it'll call m_leftKeyIterator->ReadFirst twice.
            if (m_enumerator.get() == nullptr)
            {
                Generic::IEnumerable<IRow^>^ combiner = nullptr;

                switch (side)
                {
                case SIDETYPE::LEFT:
                    combiner = m_combiner->Combine(m_inputRowsetLeft, m_emptyRowsRight, m_outputRow);
                    break;

                case SIDETYPE::RIGHT:
                    combiner = m_combiner->Combine(m_emptyRowsLeft, m_inputRowsetRight, m_outputRow);
                    break;

                case SIDETYPE::BOTH:
                    combiner = m_combiner->Combine(m_inputRowsetLeft, m_inputRowsetRight, m_outputRow);
                    break;

                default:
                    SCOPE_ASSERT(false);
                }

                // ICombiner.Combine method may return null, treat as empty IEnumerable. 
                if (combiner == nullptr)
                {
                    m_firstRowInKeySet = false;
                    Init();
                    return false;
                }

                m_enumerator.reset(combiner->GetEnumerator());
            }

            Generic::IEnumerator<IRow^> ^ enumerator = m_enumerator;

            // When calling into Udo first time, call ResetOutput to copy read only columns
            if (!m_firstRowInKeySet)
            {
                ResetOutput();
                m_firstRowInKeySet = true;
            }

            if (enumerator->MoveNext())
            {
                m_allocator.Reset();
                CheckSqlIpUdoYieldRow(enumerator->Current, (System::Object^)m_outputRow.get());
                (*m_marshalToNative)(enumerator->Current, output, &m_allocator);

                return true;
            }
            else
            {
                m_firstRowInKeySet = false;
                Init();
                return false;
            }
        }

        virtual void Close()
        {
            m_allocator.Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            // Dispose enumerator
            m_enumerator.reset();
        }

        virtual void SetLeftKeyIterator(LeftKeyIteratorType* iter)
        {
            m_inputRowsetLeft->SetIterator(iter);
        }

        virtual void SetRightKeyIterator(RightKeyIteratorType* iter)
        {
            m_inputRowsetRight->SetIterator(iter);
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
            return OperatorRequirements(OperatorRequirementsConstants::NativeCombinerWrapper_SqlIpUdoJoiner__Row_MinMemory);
        }

        virtual ~SqlIpCombiner()
        {
            // There only to ensure proper destruction when base class destructor is called
        }

        virtual void DoScopeCEPCheckpoint(BinaryOutputStream & output)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke DoScopeCEPCheckpoint for SqlIpCombiner!");
        }

        virtual void LoadScopeCEPCheckpoint(BinaryInputStream & input)
        {
            throw RuntimeException(E_SYSTEM_INTERNAL_ERROR, "should not invoke LoadScopeCEPCheckpoint for SqlIpCombiner!");
        }
    };

    template<typename InputSchemaLeft, typename InputSchemaRight, typename OutputSchema, typename LeftKeyPolicy, typename RightKeyPolicy>
    const char* const SqlIpCombiner<InputSchemaLeft, InputSchemaRight, OutputSchema, LeftKeyPolicy, RightKeyPolicy>::sm_className = "SqlIpCombiner";

    // Marshal a managed row to a native row
    template<typename RowSchema, int>
    class InteropToNativeRowPolicy
    {
    public:
        static void Marshal(IRow^ managedRow, RowSchema& nativeRow, IncrementalAllocator* alloc);
    };

    // Wraper for Row object to be used in native code
    template<typename Schema>
    struct ManagedRow
    {
        IRow^ get();
        System::Object^ ComplexColumnGetter(int index, BYTE* address);
        System::Object^ UDTColumnGetter(int index, BYTE* address);
    };

#if !defined(SCOPE_NO_UDT)
#pragma region ScopeUDT

    // TODO: DotNet Binary Serializer is very slow. This is a temporary solution to use the UDT pipeline for serialization until we have native support for SQL types.
    generic<typename T> where T : Microsoft::Analytics::Types::Sql::SqlType
    ref class SqlTypeSerializer : Microsoft::Analytics::Interfaces::IFormatter<T>
    {
    private:
        System::Runtime::Serialization::Formatters::Binary::BinaryFormatter binaryFormatter;

    public:
        virtual void Serialize(T instance, Microsoft::Analytics::Interfaces::IColumnWriter^ writer, Microsoft::Analytics::Interfaces::ISerializationContext^ context)
        {
            this->binaryFormatter.Serialize(writer->BaseStream, instance);
        }

        virtual T Deserialize(Microsoft::Analytics::Interfaces::IColumnReader^ reader, Microsoft::Analytics::Interfaces::ISerializationContext^ context)
        {
            return static_cast<T>(this->binaryFormatter.Deserialize(reader->BaseStream));
        }
    };

    private interface class IUdtContainer
    {
        property System::Object^ Value;
        void Reset();
        bool IsNull();
        void Serialize(System::IO::Stream^ output, Microsoft::Analytics::Interfaces::ISerializationContext^ context);
        void Deserialize(System::IO::Stream^ input, Microsoft::Analytics::Interfaces::ISerializationContext^ context);
    };

    // Note the handle to the UdtContainer instance in SqlUserDefinedType<UserDefinedTypeId> is stored as a ScopeManagedHandle. Since the type parameter TValue
    // is not known to SqlUserDefinedType<UserDefinedTypeId>, the ScopeManagedHandle cannot be casted to UdtContainer<TValue>. We use the internal IUdtContainer 
    // interface to workaround that.
    generic<typename TValue, typename TFormatter> where TFormatter : Microsoft::Analytics::Interfaces::IFormatter<TValue>, gcnew()
    ref class UdtContainer sealed : public IUdtContainer
    {
    private:
        TValue m_value;
        TFormatter m_formatter;

        TFormatter GetFormatter()
        {
            if (this->m_formatter == nullptr)
            {
                this->m_formatter = gcnew TFormatter();
            }

            return this->m_formatter;
        }

    public:
        property System::Object^ Value
        {
            virtual System::Object^ get()
            {
                return this->m_value;
            }

            virtual void set(System::Object^ value)
            {
                this->m_value = static_cast<TValue>(value);
            }
        }

        virtual void Reset()
        {
            this->m_value = TValue();
        }

        virtual bool IsNull()
        {
            return this->m_value == nullptr;
        }

        virtual void Serialize(System::IO::Stream^ output, Microsoft::Analytics::Interfaces::ISerializationContext^ context)
        {
            System::IO::Stream^ outputStream;
            try
            {
                outputStream = gcnew SqlOutputColumnStream(output);
                this->GetFormatter()->Serialize(this->m_value, gcnew ScopeEngineManaged::SqlColumnWriter(outputStream), context);

                // The user code might have already closed the stream but we always close here to gaurantee that Close() is called. The first call to Close()
                // will flush the buffer to the underlying stream. Subsequent calls to Close() will no-op. If the user code didn't write to the stream at all,
                // Close() will write 0 to the block header to make sure that no bytes can be read from the column.
                outputStream->Close();
            }
            catch (System::Exception^ e)
            {

                ScopeEngineManaged::UserExceptionHelper::WrapUserException(this->GetFormatter()->GetType()->FullName, "Serialize", e);
                throw;
            }
        }

        virtual void Deserialize(System::IO::Stream^ input, Microsoft::Analytics::Interfaces::ISerializationContext^ context)
        {
            System::IO::Stream^ inputStream;
            try
            {
                // The SqlInputColumnStream constructor will fill the block buffer and an EndOfStreamException can occur if the underlying stream is at the end
                // of the last row.
                inputStream = gcnew SqlInputColumnStream(input);
                this->m_value = this->GetFormatter()->Deserialize(gcnew ScopeEngineManaged::SqlColumnReader(inputStream), context);

                // The user code might have already closed the stream but we always close here to gaurantee that Close() is called. The first call to Close()
                // throw if there's unread data in the stream and it will read to the end of column so the underlying stream is back to a good valid state.
                // Subsequent calls to Close() will no-op.
                inputStream->Close();
            }
            catch (System::IO::EndOfStreamException^)
            {
                //Translate EndOfStreamException exception to ScopeStreamException.
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
            catch (System::Exception^ e)
            {
                ScopeEngineManaged::UserExceptionHelper::WrapUserException(this->GetFormatter()->GetType()->FullName, "Deserialize", e);
                throw;
            }
        }
    };

    template<int UserDefinedTypeId>
    template<typename Container>
    INLINE Container SqlUserDefinedType<UserDefinedTypeId>::GetUdtContainer() const
    {
        Container container = scope_handle_cast<Container>(this->m_managed);
        SCOPE_ASSERT(container != nullptr);
        return container;
    }

    template<int UserDefinedTypeId>
    SqlUserDefinedType<UserDefinedTypeId>::SqlUserDefinedType()
    {
        this->m_managed = ManagedUDT<UserDefinedTypeId>().get();
    }

    template<int UserDefinedTypeId>
    SqlUserDefinedType<UserDefinedTypeId>::SqlUserDefinedType(const SqlUserDefinedType<UserDefinedTypeId> & c)
    {
        this->m_managed = ManagedUDT<UserDefinedTypeId>().get();
        this->Set(c.Get());
    }

    template<int UserDefinedTypeId>
    void SqlUserDefinedType<UserDefinedTypeId>::Set(const ScopeManagedHandle & value)
    {
        this->GetUdtContainer<IUdtContainer^>()->Value = (System::Object^)value;
    }

    template<int UserDefinedTypeId>
    ScopeManagedHandle SqlUserDefinedType<UserDefinedTypeId>::Get() const
    {
        return this->GetUdtContainer<IUdtContainer^>()->Value;
    }

    template<int UserDefinedTypeId>
    void SqlUserDefinedType<UserDefinedTypeId>::Reset()
    {
        this->GetUdtContainer<IUdtContainer^>()->Reset();
    }

    template<int UserDefinedTypeId>
    bool SqlUserDefinedType<UserDefinedTypeId>::IsNull() const
    {
        return this->GetUdtContainer<IUdtContainer^>()->IsNull();
    }

    template<int UserDefinedTypeId>
    void SqlUserDefinedType<UserDefinedTypeId>::SetNull()
    {
        this->GetUdtContainer<IUdtContainer^>()->Reset();
    }

    template<typename StreamType>
    static INLINE System::IO::Stream^ GetOrCreateStream(StreamType * baseStream)
    {
        // Lazily construct managed wrapper
        if (!baseStream->Wrapper())
        {
            baseStream->Wrapper().reset(new ScopeManagedHandle(gcnew ScopeStreamWrapper<StreamType>(baseStream)));
        }

        return scope_handle_cast<System::IO::Stream^>(*baseStream->Wrapper());
    }

    template<int UserDefinedTypeId>
    void SqlUserDefinedType<UserDefinedTypeId>::BinarySerialize(BinaryOutputStreamBase<CosmosOutput> * stream, const ScopeManagedHandle & serializationContext) const
    {
        System::IO::Stream^ s = GetOrCreateStream<BinaryOutputStreamBase<CosmosOutput>>(stream);
        this->GetUdtContainer<IUdtContainer^>()->Serialize(s, (Microsoft::Analytics::Interfaces::ISerializationContext^)serializationContext);
    }

    template<int UserDefinedTypeId>
    void SqlUserDefinedType<UserDefinedTypeId>::BinarySerialize(BinaryOutputStreamBase<MemoryOutput> * stream, const ScopeManagedHandle & serializationContext) const
    {
        System::IO::Stream^ s = GetOrCreateStream<BinaryOutputStreamBase<MemoryOutput>>(stream);
        this->GetUdtContainer<IUdtContainer^>()->Serialize(s, (Microsoft::Analytics::Interfaces::ISerializationContext^)serializationContext);
    }

    template<int UserDefinedTypeId>
    void SqlUserDefinedType<UserDefinedTypeId>::BinarySerialize(BinaryOutputStreamBase<DummyOutput> * stream, const ScopeManagedHandle & serializationContext) const
    {
        System::IO::Stream^ s = GetOrCreateStream<BinaryOutputStreamBase<DummyOutput>>(stream);
        this->GetUdtContainer<IUdtContainer^>()->Serialize(s, (Microsoft::Analytics::Interfaces::ISerializationContext^)serializationContext);
    }

    template<int UserDefinedTypeId>
    void SqlUserDefinedType<UserDefinedTypeId>::BinarySerialize(SStreamDataOutputStream * stream, const ScopeManagedHandle & serializationContext) const
    {
        throw gcnew NotSupportedException();
    }

    template<int UserDefinedTypeId>
    void SqlUserDefinedType<UserDefinedTypeId>::BinaryDeserialize(BinaryInputStreamBase<CosmosInput> * stream, const ScopeManagedHandle & serializationContext)
    {
        System::IO::Stream^ s = GetOrCreateStream<BinaryInputStreamBase<CosmosInput>>(stream);
        this->GetUdtContainer<IUdtContainer^>()->Deserialize(s, (Microsoft::Analytics::Interfaces::ISerializationContext^)serializationContext);
    }

    template<int UserDefinedTypeId>
    void SqlUserDefinedType<UserDefinedTypeId>::BinaryDeserialize(BinaryInputStreamBase<MemoryInput> * stream, const ScopeManagedHandle & serializationContext)
    {
        System::IO::Stream^ s = GetOrCreateStream<BinaryInputStreamBase<MemoryInput>>(stream);
        this->GetUdtContainer<IUdtContainer^>()->Deserialize(s, (Microsoft::Analytics::Interfaces::ISerializationContext^)serializationContext);
    }

    template<int UserDefinedTypeId>
    void SqlUserDefinedType<UserDefinedTypeId>::TextSerialize(TextOutputStreamBase * stream) const
    {
        throw gcnew NotSupportedException();
    }

    template<int UserDefinedTypeId>
    void SqlUserDefinedType<UserDefinedTypeId>::TextDeserialize(const char * str)
    {
        throw gcnew NotSupportedException();
    }

    template<int UserDefinedTypeId>
    void SqlUserDefinedType<UserDefinedTypeId>::SSLibSerialize(SStreamDataOutputStream * baseStream) const
    {
        throw gcnew NotSupportedException();
    }

    template<int UserDefinedTypeId>
    void SqlUserDefinedType<UserDefinedTypeId>::SSLibDeserialize(BYTE* buffer, int offset, int length, ScopeSStreamSchema& schema)
    {
        throw gcnew NotSupportedException();
    }

#pragma endregion ScopeUDT
#endif // SCOPE_NO_UDT

} // namespace ScopeEngine
