#pragma once

#include <codecvt>
#include <ctime>
#include <memory>
#include <cstring>

#include "ScopeContainers.h"
#include "ScopeSqlType.h"
#include "ScopeUtils.h"
#include "ScopeDateTime.h"
#include "scopecommonerror.h"

#include <iostream>
#include <fstream>

#ifdef PLUGIN_TYPE_SYSTEM_NAMESPACE
namespace PluginType
{
using namespace ScopeEngine;
using namespace PluginType;
#else
namespace ScopeEngine
{
#endif
    static const char x_r = '\r';
    static const char x_n = '\n';
    static const char x_quote = '\"';
    static const char x_hash = '#';
    static const char x_pairquote [] = {'\"', '\"'};    
    static const char x_newline [] = {x_r, x_n};
    static const char x_null [] = {x_hash,'N','U','L','L',x_hash};
    static const char x_error [] = {x_hash,'E','R','R','O','R',x_hash};
    static const char x_tab [] = {x_hash,'T','A','B',x_hash};
    static const char x_escR [] = {x_hash,'R',x_hash};
    static const char x_escN [] = {x_hash,'N',x_hash};
    static const char x_escHash [] = {x_hash,'H','A','S','H',x_hash};
    static const char x_True [] = {'T','r','u','e'};
    static const char x_False [] = {'F','a','l','s','e'};
    static const char x_NaN [] = {'N','a','N'};
    static const char x_PositiveInf [] = {'I','n','f','i','n','i','t','y'};
    static const char x_NegativeInf [] = {'-','I','n','f','i','n','i','t','y'};

    static const char x_UTF7DecoderArray [] = {
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
        52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
        -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
        15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
        -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
    };


    static const bool x_UTF7DirectChars [] = {
        false, false, false, false, false, false, false, false, false, true, true, false, false, true, false, false, 
        false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, 
        true, false, false, false, false, false, false, true, true, true, false, false, true, true, true, true, 
        true, true, true, true, true, true, true, true, true, true, true, false, false, false, false, true, 
        false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, 
        true, true, true, true, true, true, true, true, true, true, true, false, false, false, false, false, 
        false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, 
        true, true, true, true, true, true, true, true, true, true, true, false, false, false, false, false
    };

    static const char x_Base64Chars [] = {
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
        'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd',
        'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
        't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', '+', '/'
    };

    // Returns total number of bytes in UTF8 character assuming that c is its first byte
    // The number of leading ones in the first byte of UTF-8 character equals to the number of bytes in the character
    // except for the single-byte characters. Their leading bit is zero.
    static FORCE_INLINE SIZE_T UTF8ByteCount(unsigned char c)
    {
        // Count total number of bits set in the char 
        // from the left until the first 0
        SIZE_T count = 0;

        // while the first byte is set
        // c & 1000 0000
        while (c & 0x80)
        {
            count++;
            c <<= 1;
        }
        
        return (count != 0) ? count : 1;
    }    

    // Valid non leading byte in utf8 is 10xxxxxx
    static FORCE_INLINE bool IsValidUTF8NonLeadingByte(unsigned char c)
    {
        return (c & 0xC0) == 0x80 ? true : false;
    }

#if defined(SCOPE_RUNTIME_EXPORT_DLL) || defined(SCOPE_RUNTIME_IMPORT_DLL)
    template class SCOPE_RUNTIME_API unique_ptr<Scanner, ScannerDeleter>;
#endif

    // A wrapper for cosmos input device. 
    // It handles the interaction with scopeengin IO system and provide a GetNextPage interface to InputStream
    class SCOPE_RUNTIME_API CosmosInput : public ExecutionStats
    {
        BlockDevice *       m_device;      // device object
        unique_ptr<Scanner, ScannerDeleter> m_scanner;     // scanner class which provide page level read and prefetch

        SIZE_T              m_ioBufSize;
        int                 m_ioBufCount;

        Scanner::Statistics m_statistics;

        bool                m_started;
        bool                m_lowLatency;

        char *  m_buffer; // current reading start point
        SIZE_T  m_numRemain; // number of bytes remaining in the buffer
        SIZE_T  m_position;
        SIZE_T  m_posInBuf;
        SIZE_T  m_posNextBuffer;
        SIZE_T  m_currentBufSize;
        std::string       m_recoverState; // cached state from load checkpoint

        void CloseScanner()
        {
            if (m_scanner->IsOpened())
            {
                if (m_started)
                {
                    m_started = false;
                    m_scanner->Finish();
                }

                m_scanner->Close();
            }
        }

        void ResetBuffer()
        {
            m_posNextBuffer = 0;
            m_currentBufSize = 0;
            m_numRemain = 0;
            m_posInBuf = 0;
            m_buffer = nullptr;
        }
        
    public:
        CosmosInput() : 
            m_device(nullptr), 
            m_ioBufSize(IOManager::x_defaultInputBufSize),
            m_ioBufCount(IOManager::x_defaultInputBufCount),
            m_started(false), m_lowLatency(false)
        {
        }

        CosmosInput(const std::string & filename, SIZE_T bufSize = IOManager::x_defaultInputBufSize, int bufCount = IOManager::x_defaultInputBufCount) :
            m_device(IOManager::GetGlobal()->GetDevice(filename)), 
            m_ioBufSize(bufSize),
            m_ioBufCount(bufCount),
            m_started(false), m_lowLatency(false)
        {
        }

        CosmosInput(BlockDevice* device, SIZE_T bufSize = IOManager::x_defaultInputBufSize, int bufCount = IOManager::x_defaultInputBufCount) :
            m_device(device), 
            m_ioBufSize(bufSize),
            m_ioBufCount(bufCount),
            m_started(false), m_lowLatency(false)
        {
        }

        void SetLowLatency(bool val)
        {
            m_lowLatency = val;
        }

        void Init(bool startReadAhead = true, bool cancelable = false)
        {
            AutoExecStats stats(this);

            if (!m_device)
                throw ScopeStreamException(ScopeStreamException::BadDevice);

            //setup scanner and start scanning. 
            m_scanner.reset(Scanner::CreateScanner(m_device, MemoryManager::GetGlobal(), Scanner::STYPE_ReadOnly, m_ioBufSize, m_ioBufSize, m_ioBufCount));

            if (m_recoverState.size() != 0)
            {
                m_scanner->LoadState(m_recoverState);
                m_recoverState.clear();
            }

            m_scanner->Open(startReadAhead, cancelable);

            m_posNextBuffer = 0;
            m_currentBufSize = 0;
            m_numRemain = 0;
            m_position = 0;
            ResetBuffer();
        }

        void Close()
        {
            AutoExecStats stats(this);

            if (m_scanner)
            {
                CloseScanner();

                // Track statistics before scanner is destroyed.
                m_statistics = m_scanner->GetStatistics();
                m_statistics.ConvertToMilliSeconds();

                m_scanner.reset();
            }

            ResetBuffer();
            m_position = 0;
        }

        // Jump forward specified amount of bytes
        void Skip(SIZE_T size)
        {
            while(m_numRemain < size)
            {
                size -= m_numRemain;
                m_position += m_numRemain;
                m_posInBuf += m_numRemain;
                m_numRemain = 0;

                // No more data to read return failure.
                if (!Refill())
                {
                    throw ScopeStreamException(ScopeStreamException::BadDevice);
                }
            }

            m_numRemain -= size;
            m_posInBuf += size;
            m_position += size;
        }

        SIZE_T GetCurrentPosition() const
        {
            return m_position;
        }

        char* GetCurrentBuffer() 
        {
            return m_buffer;
        }

        SIZE_T RemainBufferSize() const
        {
            return m_numRemain;
        }

        // refill the input buffer.
        // For memory stream, we will simply return false.
        // For cosmos input, we will get next buffer from device.
        FORCE_INLINE bool Refill()
        {
            SIZE_T nRead=0;

            m_buffer = GetNextPage(nRead);
            if(m_buffer == nullptr || nRead == 0)
            {
                m_buffer = nullptr;
                return false;
            }
            m_numRemain = nRead;
            m_posInBuf = 0;
            return true;
        }

        // Read "size" of byte into dest buffer, return how many bytes were read
        unsigned int Read(char* dest, unsigned int size)
        {
            unsigned int bytesRead = 0;

            if (m_numRemain < size)
            {
                // Copy the remaining first, then fetch the next batch
                if (m_numRemain)
                {
                    memcpy(dest, m_buffer + m_posInBuf, m_numRemain);
                }

                bytesRead = static_cast<unsigned int>(m_numRemain);
                m_position += m_numRemain;
                m_posInBuf += m_numRemain;
                m_numRemain = 0;

                // Refill call could be blocked.
                // it returns available data immediately if data freshness matters. e.g., cep jobs
                if (m_lowLatency && bytesRead > 0)
                {
                    return bytesRead;
                }

                if (!Refill())
                {
                    return bytesRead;
                }
                else
                {
                    bytesRead += Read(dest + bytesRead, size - bytesRead);
                    return bytesRead;
                }
            }
            else
            {
                memcpy(dest, m_buffer + m_posInBuf, size);
                m_position += size;
                m_posInBuf += size;
                m_numRemain -= size;
                return size;
            }
        }

        // Reach one page from scanner. The page size is provided by IO system to
        // get best performance.
        char * GetNextPage(SIZE_T & numRead, UINT64 initialOffset = 0)
        {
            AutoExecStats stats(this);

            if (!m_started)
            {
                m_scanner->Start(initialOffset);
                m_started = true;
                m_position = initialOffset;
                m_posNextBuffer = initialOffset;
            }

            const BufferDescriptor* buffer;
            SIZE_T bufferSize;
            bool next = m_scanner->GetNext(buffer, bufferSize);
            if (next && bufferSize > 0)
            {
                numRead = m_numRemain = bufferSize;
                stats.IncreaseRowCount(bufferSize);
                m_buffer = (char *)(buffer->m_buffer);
                m_posNextBuffer += bufferSize;
                m_currentBufSize = bufferSize;
            }
            else
            {
                ResetBuffer();
                numRead = 0;
            }

            m_posInBuf = 0;
            return m_buffer;
        }

        bool TryRecover()
        {
            UINT64 newPosition;
            if (m_scanner->TryRecover(newPosition))
            {
                ResetBuffer();
                m_position = newPosition;
                return true;
            }

            return false;
        }

        SIZE_T Seek(SIZE_T position)
        {
            if (m_position == position)
            {
                return m_position;
            }
            else if (m_buffer != nullptr && position >= m_posNextBuffer - m_currentBufSize && position <= m_posNextBuffer)
            {
                m_position = position;
                m_numRemain = m_posNextBuffer - position;
                m_posInBuf = m_currentBufSize - m_numRemain;
            }
            else
            {
                Close();
                Init(false);
                SIZE_T read;
                GetNextPage(read, position);
            }
            return m_position;
        }

        UINT64 Length()
        {
            AutoExecStats stats(this);

            SCOPE_ASSERT(m_scanner && m_scanner->IsOpened());

            return m_scanner->Length();
        }

        // Return the filename associated with this stream
        std::string StreamName() const
        {
            return m_scanner->GetStreamName();
        }

        // compressed input or not
        bool IsCompressed() const
        {
            return false;
        }
        
        // Restart the input stream. Next GetNextPage will start from position of initialOffset.
        // There is no read ahead for the first page as caller will call GetNextPage immediately. 
        void Restart()
        {
            AutoExecStats stats(this);

            if (m_scanner)
            {
                CloseScanner();

                // open and start from offset 0 again.
                m_scanner->Open(true);
                m_scanner->Start(0);
            }
            
            m_position = 0;
            ResetBuffer();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            RuntimeStats::WriteIOStats(root, m_statistics);
            auto & node = root.AddElement("IOBuffers");
            node.AddAttribute(RuntimeStats::MaxBufferCount(), m_ioBufCount);
            node.AddAttribute(RuntimeStats::MaxBufferSize(), m_ioBufSize);
            node.AddAttribute(RuntimeStats::MaxBufferMemory(), m_statistics.m_memorySize);
            node.AddAttribute(RuntimeStats::AvgBufferMemory(), m_statistics.m_memorySize);
        }

        // return total wait time on all operations
        LONGLONG GetTotalIoWaitTime()
        {
            return m_statistics.GetTotalIoWaitTime();
        }

        ScopeEngine::OperatorRequirements GetOperatorRequirements()
        {
            return ScopeEngine::OperatorRequirements().AddMemoryForIOStreams(1, m_ioBufSize, m_ioBufCount, 0);
        }

        void SaveState(BinaryOutputStream& output, UINT64 position);
        void LoadState(BinaryInputStream& input);
        void ClearState();
    };
    
    class SCOPE_ENGINE_API GZipInput : public ExecutionStats
    {
        BlockDevice *m_device;         // device object
        Scanner     *m_scanner;        // scanner class which provide page level read and prefetch 
        Scanner::Statistics m_statistics;
        const BufferDescriptor *m_outBufDescriptor;
        const BufferDescriptor *m_inBufDescriptor;
        
        SIZE_T  x_outBufferSize;       // buffer size of decompressed data
        SIZE_T  x_inBufferSize;        // buffer size of compressed data
        int     x_inBufferCount;       // in buffer count
        
        char*   m_outBuffer;           // buffer holding decompressed data, owned by this class
        SIZE_T  m_outBufferRemain;     // remain bytes in outBuffer whih are not consumed by application
        SIZE_T  m_outBufferPos;        // start pos inside outbuffer
        
        char *  m_inBuffer;            // buffer holding compressed data
        SIZE_T  m_inBufferRemain;      // remain bytes inBuffer
        SIZE_T  m_InBufferPos;         // pos inside inbuffer
        
        char*   m_zstream;
        bool    m_eof;                 // EOF for compressed data 
        bool    m_started;
        
        bool    Refill();
        void    CloseScanner();
        char*   GetNextCompressedPage(SIZE_T & numRead);
     
    public:
    
        GZipInput();
        GZipInput(const std::string & filename, SIZE_T inputBufferSize, int inputBufferCount, SIZE_T outputBufferSize = IOManager::x_defaultInputBufSize);
        GZipInput(BlockDevice* device, SIZE_T inputBufferSize, int inputBufferCount, SIZE_T outputBufferSize = IOManager::x_defaultInputBufSize);                  
        void         Init();
        void         Close();
        unsigned int Read(char* dest, unsigned int size);
        char*        GetNextPage(SIZE_T & numRead);
        char*        GetCurrentBuffer();
        SIZE_T       RemainBufferSize() const;
        void         WriteRuntimeStats(TreeNode & root);
        LONGLONG     GetTotalIoWaitTime();
        std::string  StreamName() const;
        ScopeEngine::OperatorRequirements GetOperatorRequirements() const;
        bool         IsCompressed() const;

        // Below method is not supported for GZIP input
        SIZE_T       Seek(SIZE_T position);        
        void         SetLowReadLatency(bool val);
        UINT64       Length();
        SIZE_T       GetCurrentPosition() const;
        void         SaveState(BinaryOutputStream& output, UINT64 position);
        void         LoadState(BinaryInputStream& input);        
        
    };

    //
    // Wraps managed Stream to cache UDT (de)serialization
    //
    class SCOPE_RUNTIME_API StreamWithManagedWrapper
    {
#if !defined(SCOPE_NO_UDT)
        unique_ptr<ScopeManagedHandle> m_wrapper; // managed wrapper used to serialize UDT (lazily constructed)

    public:

        // Access to the managed wrapper for UDT Serialize/Deserialize methods
        unique_ptr<ScopeManagedHandle>& Wrapper()
        {
            return m_wrapper;
        }
#endif // SCOPE_NO_UDT
    };

    // A wrapper to provide stream interface on top of cosmos input which provide block reading interface.
    // It hides the page from the reader.
    template<typename InputType>
    class SCOPE_RUNTIME_API BinaryInputStreamBase : public StreamWithManagedWrapper
    {
        InputType  m_asyncInput;       // blocked cosmos input stream 
        IncrementalAllocator  * m_allocator;      // memory allocator pointer for deserialization

    protected:
        IncrementalAllocator* GetAllocator()
        {
            SCOPE_ASSERT(m_allocator != NULL);
            return m_allocator;
        }

    public:
        // constructor - until we get variadic templates
        template<typename Arg1>
        BinaryInputStreamBase(IncrementalAllocator * allocator, const Arg1& arg1) :
            m_asyncInput(arg1), 
            m_allocator(allocator)
        {
        }

        template<typename Arg1, typename Arg2>
        BinaryInputStreamBase(IncrementalAllocator * allocator, const Arg1& arg1, const Arg2& arg2) :
            m_asyncInput(arg1, arg2), 
            m_allocator(allocator)
        {
        }

        template<typename Arg1, typename Arg2, typename Arg3>
        BinaryInputStreamBase(IncrementalAllocator * allocator, const Arg1& arg1, const Arg2& arg2, const Arg3& arg3) :
            m_asyncInput(arg1, arg2, arg3), 
            m_allocator(allocator)
        {
        }

        template<typename Arg1, typename Arg2, typename Arg3, typename Arg4>
        BinaryInputStreamBase(IncrementalAllocator * allocator, const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4) :
            m_asyncInput(arg1, arg2, arg3, arg4),
            m_allocator(allocator)
        {
        }

        template<typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5>
        BinaryInputStreamBase(IncrementalAllocator * allocator, const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5) :
            m_asyncInput(arg1, arg2, arg3, arg4, arg5),
            m_allocator(allocator)
        {
        }

        template<typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5, typename Arg6>
        BinaryInputStreamBase(IncrementalAllocator * allocator, const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6) :
            m_asyncInput(arg1, arg2, arg3, arg4, arg5, arg6),
            m_allocator(allocator)
        {
        }

        unsigned int ReadLen()
        {
            // Decode the variable length encoded string length first
            unsigned int len = 0;
            unsigned int shift = 7;
            char b = 0;
            Read(b);
            len = b & 0x7f;

            while(b & 0x80)
            {
                Read(b);
                len |= (b & 0x7f) << shift;
                shift += 7;
            }

            return len;
        }

        SIZE_T Position() const
        {
            return m_asyncInput.GetCurrentPosition();
        }

        // Init 
        void Init(bool readAhead = true)
        {
            m_asyncInput.Init(readAhead);
        }

        // close stream
        void Close()
        {
            m_asyncInput.Close();
        }

        unsigned int Read(char * dest, unsigned int size)
        {
            return m_asyncInput.Read(dest, size);
        }

#ifdef PLUGIN_TYPE_SYSTEM
        template <typename T>
        void Read(T & t)
        {
            t.Deserialize(this, m_allocator);
        }

        INLINE void Read(bool & s)
        {
            if (Read(reinterpret_cast<char *>(&s), sizeof(bool)) != sizeof(bool))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        INLINE void Read(unsigned char & s)
        {
            if (Read(reinterpret_cast<char *>(&s), sizeof(unsigned char)) != sizeof(unsigned char))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        INLINE void Read(char & s)
        {
            if (Read(reinterpret_cast<char *>(&s), sizeof(char)) != sizeof(char))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        INLINE void Read(short & s)
        {
            if (Read(reinterpret_cast<char *>(&s), sizeof(short)) != sizeof(short))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        INLINE void Read(unsigned short & s)
        {
            if (Read(reinterpret_cast<char *>(&s), sizeof(unsigned short)) != sizeof(unsigned short))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        INLINE void Read(wchar_t & s)
        {
            if (Read(reinterpret_cast<char *>(&s), sizeof(wchar_t)) != sizeof(wchar_t))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        INLINE void Read(int & s)
        {
            if (Read(reinterpret_cast<char *>(&s), sizeof(int)) != sizeof(int))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        INLINE void Read(unsigned int & s)
        {
            if (Read(reinterpret_cast<char *>(&s), sizeof(unsigned int)) != sizeof(unsigned int))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        INLINE void Read(__int64 & s)
        {
            if (Read(reinterpret_cast<char *>(&s), sizeof(__int64)) != sizeof(__int64))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        INLINE void Read(unsigned __int64 & s)
        {
            if (Read(reinterpret_cast<char *>(&s), sizeof(unsigned __int64)) != sizeof(unsigned __int64))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        INLINE void Read(float & s)
        {
            if (Read(reinterpret_cast<char *>(&s), sizeof(float)) != sizeof(float))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        INLINE void Read(double & s)
        {
            if (Read(reinterpret_cast<char *>(&s), sizeof(double)) != sizeof(double))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        INLINE void Read(ULONG & s)
        {
            if (Read(reinterpret_cast<char *>(&s), sizeof(ULONG)) != sizeof(ULONG))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }
#else // PLUGIN_TYPE_SYSTEM
        // deserialize a scope supported type written by BinaryWriter
        template<typename T>
        void Read(T & s)
        {
            if (Read(reinterpret_cast<char *>(&s), sizeof(T)) != sizeof(T))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        // Fill FString/FBinary with size
        template<typename T>
        bool ReadFixedArray(FixedArrayType<T> & s, unsigned int size)
        {
            unsigned int bytesRead = 0;
            if (size == 0)
            {
                s.SetEmpty();
            }
            else
            {
                // @TODO: Initially, due to performance concerns fixed array pointer (size) is not saved for validation
                // We should try to change this in the future and make sure that we never write past size of the buffer.
                char * dest = (char *) s.Reserve(size, m_allocator);
                bytesRead = Read(dest, size);
            }

            return bytesRead == size;
        }

        // deserialize a string object written by BinaryWriter
        void Read(FString & str)
        {
            unsigned int len = ReadLen();

            // Fill the FString
            if (!ReadFixedArray(str, len))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        // deserialize a binary object written by BinaryWriter
        void Read(FBinary & bin)
        {
            unsigned int len = ReadLen();

            // Fill the FBinary
            if (!ReadFixedArray(bin, len))
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        // deserialize a scope map
        template<typename K, typename V>
        void Read(ScopeMapNative<K,V> & m)
        {
            m.Deserialize(this, m_allocator);
        }

        // deserialize a scope array
        template<typename T>
        void Read(ScopeArrayNative<T> & s)
        {
            s.Deserialize(this, m_allocator);
        }

        // deserialize a scope supported type written by BiniaryWriter
        void Read(ScopeDateTime & s)
        {
            __int64 binaryTime;
            Read(binaryTime);
            s = ScopeDateTime::FromBinary(binaryTime);
        }

        // deserialize a scope supported type written by BiniaryWriter
        void Read(ScopeSqlType::SqlDateTime & s)
        {
            __int64 binaryTime;
            Read(binaryTime);
            long binaryDate;
            Read(binaryDate);
            s = ScopeSqlType::SqlDateTime::FromBinary(binaryTime, binaryDate);
        }

        template<class I, typename T>
        void Read(ScopeSqlType::ArithmeticSqlType<I, T> & s)
        {
            T value;
            Read(value);
            s.setValue(value);
        }

        // deserialize a Decimal type written by BiniaryWriter
        void Read(ScopeDecimal & s)
        {
            Read(s.Lo32Bit());
            Read(s.Mid32Bit());
            Read(s.Hi32Bit());
            Read(s.SignScale32Bit());
            SCOPE_ASSERT(s.IsValid());
        }

        // deserialize a scope supported type written by BinaryWriter
        template<typename T>
        void Read(NativeNullable<T> & s)
        {
            Read(s.get());
            s.ClearNull();
        }

        template<typename T>
        void Read(ScopeSqlType::SqlNativeNullable<T> & s)
        {
            Read((NativeNullable<T> &)s);
        }

        template <int N>
        void Read(ScopeSqlType::SqlStringFaceted<N> & str)
        {
            unsigned int len = ReadLen();

            unsigned int bytesRead = 0;
            if (len == 0)
            {
                str.SetEmpty();
            }
            else
            {
                // @TODO: Initially, due to performance concerns fixed array pointer (size) is not saved for validation
                // We should try to change this in the future and make sure that we never write past size of the buffer.
                char * dest = (char *)str.Reserve(len, m_allocator);
                bytesRead = Read(dest, len);
            }

            if (bytesRead != len)
            {
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
        }

        void Read(__int64& i)
        {
            Read<__int64>(i);
        }

        void Read(bool& i)
        {
            Read<bool>(i);
        }

        void Read(char& i)
        {
            Read<char>(i);
        }

        void Read(wchar_t& i)
        {
            Read<wchar_t>(i);
        }

        void Read(int& i)
        {
            Read<int>(i);
        }

#endif //PLUGIN_TYPE_SYSTEM

#if !defined(SCOPE_NO_UDT)

        // deserialize a UDT type written by BinaryWriter
        template<int UserDefinedTypeId, template<int Id> class UserDefinedType>
        void Read(UserDefinedType<UserDefinedTypeId> & s, const ScopeManagedHandle & serializationContext)
        {
            s.BinaryDeserialize(this, serializationContext);
        }

#endif // SCOPE_NO_UDT

        PartitionMetadata * ReadMetadata()
        {
            return nullptr;
        }

        void ReadIndexedPartitionMetadata(std::vector<std::shared_ptr<PartitionMetadata>>& partitionMetadataList)
        {
        }

        void DiscardMetadata()
        {
        }

        void DiscardIndexedPartitionMetadata()
        {
        }

        // Rewind the input stream
        void ReWind()
        {
            //Start to read from beginning again.
            m_asyncInput.Restart();
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            m_asyncInput.WriteRuntimeStats(root);
        }

        ScopeEngine::OperatorRequirements GetOperatorRequirements()
        {
            return m_asyncInput.GetOperatorRequirements();
        }

        // return total wait time on all operations
        LONGLONG GetTotalIoWaitTime()
        {
            return m_asyncInput.GetTotalIoWaitTime();
        }

        // return Inclusive time of input stream
        LONGLONG GetInclusiveTimeMillisecond()
        {
            return m_asyncInput.GetInclusiveTimeMillisecond();
        }

        InputType& GetInputer()
        {
            return m_asyncInput;
        }
    };

    class SCOPE_RUNTIME_API BinaryInputStream : public BinaryInputStreamBase<CosmosInput>
    {
        InputFileInfo   m_input;         // input file info
        
    public:
    
        BinaryInputStream(const InputFileInfo & input, IncrementalAllocator * allocator, SIZE_T bufSize, int bufCount) :
            BinaryInputStreamBase(allocator, input.inputFileName, bufSize, bufCount),
            m_input(input)
        {
            // No support for stream groups and byte-aligned streams
            SCOPE_ASSERT(!m_input.HasGroupId());
            SCOPE_ASSERT(m_input.blockAligned);
        }

        int StreamId() const
        {
            // Binary stream has no stream Id
            throw RuntimeException(E_SYSTEM_NOT_SUPPORTED, "BinaryInputStream::StreamId");
        }
    };

    class MemoryInput : public ExecutionStats
    {
    // TODO: xiaoyuc, protected is just for plugin type system testing. It should be removed when new type system is done
    protected:
        char* m_buffer;
        unsigned int m_size;
        unsigned int  m_position;
        bool m_eos;
    public:
        MemoryInput(char* buffer, SIZE_T size) : m_buffer(buffer), m_size(static_cast<unsigned int>(size)), m_eos(false), m_position(0)
        {
        }

        void Init(bool startReadAhead = true)
        {
            UNREFERENCED_PARAMETER(startReadAhead);

            m_eos = false;
        }

        void Close()
        {
        }

        // Read "size" of byte into dest buffer, return how many bytes were read
        unsigned int Read(char* dest, unsigned int size)
        {
            m_eos = size > m_size - m_position;
            int byteToRead = m_eos ? (m_size - m_position) : size;
            memcpy(dest, m_buffer + m_position, byteToRead);
            m_position += byteToRead;
            return byteToRead; 
        }

        void Restart()
        {
            m_eos = false;
            m_position = 0;
        }

        SIZE_T GetCurrentPosition() const
        {
            return static_cast<SIZE_T>(m_position);
        }

        char* GetCurrentBuffer() 
        {
            return m_buffer;
        }

        SIZE_T RemainBufferSize() const
        {
            return m_size - m_position;
        }


        void WriteRuntimeStats(TreeNode &)
        {
            // No stats
        }

		ScopeEngine::OperatorRequirements GetOperatorRequirements()
        {
            // Memory reader doesn't own buffer
			return ScopeEngine::OperatorRequirements();
        }

        LONGLONG GetTotalIoWaitTime()
        {
            return 0;
        }

        void SaveState(BinaryOutputStream&, UINT64)
        {
        }

        void LoadState(BinaryInputStream&)
        {
        }
    };

    class SCOPE_RUNTIME_API MemoryInputStream : public BinaryInputStreamBase<MemoryInput>
    {
    public:
        MemoryInputStream(IncrementalAllocator * allocator, char* buffer, SIZE_T size) : BinaryInputStreamBase(allocator, buffer, size)
        {
        }
    };

    class ResourceInput : public ExecutionStats
    {
        HANDLE        m_hFile;
        LONGLONG      m_position;
        LONGLONG      m_size;
        char*         m_buf;

    public:
        ResourceInput(const std::string & filename) : m_hFile(NULL), m_position(0), m_size(0), m_buf(NULL)
        {
            AutoExecStats stats(this);

            m_hFile = CreateFileA(
                filename.c_str(),
                GENERIC_READ,
                FILE_SHARE_READ,
                NULL,
                OPEN_EXISTING,
                NULL,
                NULL);

            DWORD errCode = GetLastError();
            if (m_hFile != INVALID_HANDLE_VALUE && errCode != ERROR_FILE_NOT_FOUND && errCode != ERROR_PATH_NOT_FOUND)
            {
                BOOL fRet = GetFileSizeEx(m_hFile, (PLARGE_INTEGER)&m_size);
                if (!fRet)
                {
                    std::stringstream errMsg;
                    errMsg << "Failed to get file length, file name: " << filename.c_str() << " err : " << errCode;
                    throw RuntimeException(E_SYSTEM_RESOURCE_OPEN, errMsg.str());
                }

                return;
            }

            HMODULE hModule = GetModuleHandle(TEXT(CODEGEN_DLL_NAME));
            HRSRC resHandle = FindResourceA(hModule, (LPCSTR)filename.c_str(), (LPCSTR)"EMBEDDEDRESOURCE");
            if (resHandle == NULL)
            {
                std::stringstream errMsg;
                errMsg << "Failed to find resource, resource name: " << filename.c_str() << " err : " << GetLastError();
                throw RuntimeException(E_SYSTEM_RESOURCE_OPEN, errMsg.str());
            }

            HGLOBAL resData = LoadResource(hModule, resHandle);
            if (resData == NULL)
            {
                std::stringstream errMsg;
                errMsg << "Failed to load resource data, resource name: " << filename.c_str() << " err : " << GetLastError();
                throw RuntimeException(E_SYSTEM_RESOURCE_OPEN, errMsg.str());
            }

            m_buf = static_cast<char *>(LockResource(resData));
            if (m_buf == NULL)
            {
                std::stringstream errMsg;
                errMsg << "Failed to load resource buffer, resource name: " << filename.c_str() << " err : " << GetLastError();
                throw RuntimeException(E_SYSTEM_RESOURCE_OPEN, errMsg.str());
            }

            m_size = SizeofResource(hModule, resHandle);
            if (m_size <= 0)
            {
                std::stringstream errMsg;
                errMsg << "Failed to get file length from resource, file name: " << filename.c_str() << " err : " << GetLastError();
                throw RuntimeException(E_SYSTEM_RESOURCE_OPEN, errMsg.str());
            }
        }

        void Init(bool startReadAhead = true)
        {
            UNREFERENCED_PARAMETER(startReadAhead);
        }

        void Close()
        {
            AutoExecStats stats(this);

            if (m_hFile != NULL)
            {
                CloseHandle(m_hFile);
                m_hFile = NULL;
            }
        }

        // Read "size" of byte into dest buffer, return how many bytes were read
        unsigned int Read(char* dest, unsigned int size)
        {
            AutoExecStats stats(this);

            DWORD bytesToRead = size > (m_size - m_position) ? (int)(m_size - m_position) : size;
            DWORD bytesRead = 0;

            if (m_buf != NULL)
            {
                memcpy(dest, m_buf + m_position, bytesToRead);
            }
            else
            {
                BOOL fRet= ReadFile(
                    m_hFile,
                    dest,
                    bytesToRead,
                    &bytesRead,
                    NULL
                    );

                if (!fRet || bytesRead != bytesToRead)
                {
                    std::stringstream errMsg;
                    errMsg << "Failed to read file, err: " << GetLastError();
                    throw RuntimeException(E_SYSTEM_RESOURCE_READ, errMsg.str());
                }
            }

            m_position += bytesToRead;
            return bytesToRead;
        }

        void Restart()
        {
            AutoExecStats stats(this);

            m_position = 0;
            if (m_buf != NULL)
            {
                return;
            }

            BOOL fRet = SetFilePointer(m_hFile, 0, NULL, FILE_BEGIN);
            if (!fRet)
            {
                std::stringstream errMsg;
                errMsg << "Failed to set file pointer, err: " << GetLastError();
                throw RuntimeException(E_SYSTEM_RESOURCE_READ, errMsg.str());
            }
        }

        SIZE_T GetCurrentPosition() const
        {
            return m_position;
        }

        // TODO(weilin) the current class interface design in io layer is terrible, need a big refactroing.
        char* GetCurrentBuffer() 
        {
            SCOPE_ASSERT(false);
        }

        // TODO(weilin) the current class interface design in io layer is terrible, need a big refactroing.
        SIZE_T RemainBufferSize() const
        {
            return m_size - m_position;
        }

        void WriteRuntimeStats(TreeNode &)
        {
            // No stats
        }

        ScopeEngine::OperatorRequirements GetOperatorRequirements()
        {
            // ResourceReder doesn't own buffer
			return ScopeEngine::OperatorRequirements();
        }

        LONGLONG GetTotalIoWaitTime()
        {
            return GetInclusiveTimeMillisecond();
        }

        void SaveState(BinaryOutputStream&, UINT64)
        {
        }

        void LoadState(BinaryInputStream&)
        {
        }
    };

    class SCOPE_RUNTIME_API ResourceInputStream : public BinaryInputStreamBase<ResourceInput>
    {
    public:
        ResourceInputStream(IncrementalAllocator * allocator, const std::string & filename) : BinaryInputStreamBase(allocator, filename)
        {
        }
    };

    //
    // Binary input stream with payload
    //
    template<typename Payload>
    class AugmentedBinaryInputStream : public BinaryInputStream
    {
    public:
        AugmentedBinaryInputStream(const InputFileInfo & input, IncrementalAllocator * allocator, SIZE_T bufSize, int bufCount) :
            BinaryInputStream(input, allocator, bufSize, bufCount)
        {
        }

        PartitionMetadata * ReadMetadata()
        {
            cout << "Reading metadata" << endl;
            return new Payload(this);
        }

        void DiscardMetadata()
        {
            cout << "Discarding metadata" << endl;
            Payload::Discard(this);
        }

        void ReadIndexedPartitionMetadata(std::vector<std::shared_ptr<PartitionMetadata>>& PartitionMetadataList)
        {
            cout << "Reading indexed partition metadata" << endl;
            __int64 count = 0;
            Read(count);
            SCOPE_ASSERT(count > 0);
            PartitionMetadataList.resize(count);
            for(__int64 i = 0; i < count; i++)
            {
                PartitionMetadataList[i].reset(new Payload(this, GetAllocator()));
            }
        }

        void DiscardIndexedPartitionMetadata()
        {
            cout << "Discarding indexed partition metadata" << endl;
            __int64 count = 0;
            Read(count);
            SCOPE_ASSERT(count > 0);
            for(__int64 i = 0; i < count; i++)
            {
                Payload::Discard(this);
            }
        }
    };

    template<typename Schema, typename PolicyType, typename MetadataSchema, int MetadataId>
    class SStreamOutputStream : public ExecutionStats
    {
        static const int PARTITION_NOT_EXIST = -2;

        std::string     m_filename;
        SIZE_T          m_bufSize;
        int             m_bufCount;     // UNDONE: Have the outputer honor this bufCount.

        SSLibV3::DataUnitDescriptor m_dataUnitDesc;
        BlockDevice*    m_device;           // non-owning, owned by IOManager
        MemoryManager*     m_memMgr;

        unique_ptr<Scanner, ScannerDeleter> m_dataScanner;
        unique_ptr<SSLibV3::DataUnitWriter> m_dataUnit_ptr;

        GUID                m_affinityId;
        UINT64              m_partitionIndex;
        int                 m_columngroupIndex;

        std::shared_ptr<SSLibV3::SStreamStatistics> m_statistics;

        std::string     m_partitionKeyRangeSerialized;

        std::string     m_dataSchemaString;
        std::string     m_indexSchemaString;
        bool            m_preferSSD;
        bool            m_enableBloomFilter;

    protected:

    public:
        SStreamOutputStream(std::string& filename, int columngroupIndex, SIZE_T bufSize, int bufCnt, bool preferSSD, bool enableBloomFilter) :
            m_filename(filename),
            m_bufSize(bufSize),
            m_bufCount(bufCnt),
            // partition index will be initialized to correct value in GetPartitionInfo
            // MetadataId == -1 means that it doesn't need metadata
            // In the splitter operator, MetadataId is -1 if the partition type is TOKEN_NONE
            // and it won't call GetPartitionInfo to initialize the partition index
            // so we set the default value to 0 if MetadataId is -1
            m_partitionIndex(MetadataId != -1 ? (UINT64)-1 : 0),
            m_columngroupIndex(columngroupIndex),
            m_preferSSD(preferSSD),
            m_enableBloomFilter(enableBloomFilter),
            m_device(NULL),
            m_memMgr(NULL)
        {
        }

        void Init()
        {
            Init(Scanner::STYPE_Create);
        }

        void Init(Scanner::ScannerType dataScannerType, UINT32 bloomFilterBitCount = BloomFilter::DefaultBitCount)
        {
            AutoExecStats stats(this);
            // prepare the descriptor
            m_dataUnitDesc.m_dataColumnSizes = PolicyType::m_dataColumnSizes;
            m_dataUnitDesc.m_dataColumnCnt = array_size(PolicyType::m_dataColumnSizes);
            m_dataUnitDesc.m_indexColumnSizes = PolicyType::m_indexColumnSizes;
            m_dataUnitDesc.m_indexColumnCnt = array_size(PolicyType::m_indexColumnSizes);
            m_dataUnitDesc.m_sortKeys = PolicyType::m_dataPageSortKeys;
            m_dataUnitDesc.m_sortKeysCnt = PolicyType::m_dataPageSortKeysCnt;
            m_dataUnitDesc.m_descending = false; // doesn't matter for writer.
            m_dataUnitDesc.m_blockSize  = PolicyType::m_blockSize;
            m_dataUnitDesc.m_numBloomFilterKeys = m_enableBloomFilter? (BYTE)m_dataUnitDesc.m_sortKeysCnt : (BYTE)0;

            // hardcode for now
            m_dataUnitDesc.m_numOfBuffers = m_bufCount; 

            m_device = ScopeEngine::IOManager::GetGlobal()->GetDevice(m_filename);

            m_memMgr = MemoryManager::GetGlobal();

            m_dataScanner.reset(Scanner::CreateScanner(m_device, m_memMgr, dataScannerType, Configuration::GetGlobal().GetMaxOnDiskRowSize(), m_dataUnitDesc.m_blockSize, m_dataUnitDesc.m_numOfBuffers, m_preferSSD));

            m_dataScanner->Open();
            m_dataScanner->Start();

            m_dataUnit_ptr.reset(SSLibV3::DataUnitWriter::CreateWriter(m_dataScanner.get(), m_memMgr, bloomFilterBitCount));

            m_affinityId = m_dataScanner->InitializeStreamAffinity();

            m_statistics.reset(
                SSLibV3::SStreamStatistics::Create( PolicyType::m_columnNames, array_size(PolicyType::m_columnNames)));

            m_dataSchemaString = PolicyType::DataSchemaString();

            m_indexSchemaString = SSLibV3::GenerateIndexSchema(
                PolicyType::m_columnNames,
                PolicyType::m_columnTypes,
                array_size(PolicyType::m_columnNames),
                PolicyType::m_dataPageSortKeys,
                PolicyType::m_dataPageSortOrders,
                PolicyType::m_dataPageSortKeysCnt);

            // Alway use NULL for partitionMetadata when writing the partition. (column group)
            m_dataUnit_ptr->Open(m_dataUnitDesc, 
                &PolicyType::SerializeRow,
                m_dataSchemaString.c_str(),
                m_indexSchemaString.c_str(),
                m_affinityId, 
                m_statistics.get());
        }

        void GetPartitionInfo(PartitionMetadata* payload)
        {
            m_partitionIndex = 0;
            m_partitionKeyRangeSerialized.clear();

            // condition on template param.
            // PartitionKeyRange<MetadataSchema, MetadataId> is define only when there is a valid partition schema.
            if (MetadataId != -1)
            {
                typedef PartitionKeyRange<MetadataSchema, MetadataId> PartitionKeyRangeType;
                SCOPE_ASSERT( payload != nullptr);

                m_partitionIndex = payload->GetPartitionId();

                if (m_partitionIndex != PartitionMetadata::PARTITION_NOT_EXIST)
                {
                    MemoryOutputStream ostream;

                    PartitionKeyRangeType::SerializeForSS( &ostream, payload);

                    ostream.Flush();

                    auto buffer = (char*) ostream.GetOutputer().Buffer();
                    auto len = ostream.GetOutputer().Tellp();

                    m_partitionKeyRangeSerialized = string(buffer, len);
                    m_dataUnit_ptr->SetPartitionKeyRange(m_partitionKeyRangeSerialized);
                }
            }
        }

        void AppendRow(Schema & output)
        {
            AutoExecStats stats(this);
            m_dataUnit_ptr->AppendRow(&output);
            stats.IncreaseRowCount(1);
        }

        void Flush()
        {
            m_dataUnit_ptr->Flush();
        }

        void Close()
        {
            AutoExecStats stats(this);

            if (ValidPartition())
            {
                m_dataUnit_ptr->Close();
            }

            // Finish and close the data scanner
            if (m_dataScanner->IsOpened())
            {
                m_dataScanner->Finish();
                m_dataScanner->Close();
            }
        }

        bool ValidPartition() const
        {
            return m_partitionIndex != PARTITION_NOT_EXIST;
        }

        UINT64 Length() const
        {
            return m_dataUnit_ptr->Length();
        }

        UINT64 DataLength() const
        {
            return m_dataUnit_ptr->DataLength();
        }

        UINT64 RowCount() const
        {
            return m_dataUnit_ptr->RowCount();
        }

        std::shared_ptr<SSLibV3::SStreamStatistics> GetStatistics() 
        {
            return m_statistics;
        }

        GUID GetAffinityGuid() const
        {
            return m_affinityId;
        }

        int GetPartitionIndex() const
        {
            int partitionIndex = (int)m_partitionIndex;
            SCOPE_ASSERT(partitionIndex >= 0);
            return partitionIndex;
        }

        string GetPartitionKeyRange() const
        {
            return m_partitionKeyRangeSerialized;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            auto & node = root.AddElement("SStreamOutputStream");
            if (m_dataScanner != NULL)
            {
                m_dataScanner->WriteRuntimeStats(node);
            }
            if (m_dataUnit_ptr != NULL)
            {
                m_dataUnit_ptr->WriteRuntimeStats(node);
            }
            node.AddAttribute(RuntimeStats::MaxPartitionKeyRangeSize(), m_partitionKeyRangeSerialized.size());
        }

        LONGLONG GetTotalIoWaitTime()
        {
            if (m_dataScanner != NULL)
            {
                Scanner::Statistics statistics = m_dataScanner->GetStatistics();
                statistics.ConvertToMilliSeconds();
                return statistics.GetTotalIoWaitTime();
            }
            return 0;
        }
    };

    // Set of template to handle different text encoding conversion from/to utf8 encoding
    template<TextEncoding encoding>
    class TextEncodingConverter
    {
        FORCE_INLINE static SIZE_T SafeCopy(char * in, 
                                            char * inLast, 
                                            char * out, 
                                            char * outLast
                                            )
        {
            SIZE_T size = inLast - in;
            SIZE_T spaceRemain = outLast - out;

            SIZE_T nCopy = std::min(spaceRemain, size);

            //copy as much as possible to out buffer, 
            memcpy(out, in, nCopy);

            return nCopy;
        }

    public:
        TextEncodingConverter()
        {
        }

        // default implementation for UTF8 and Default encoding
        FORCE_INLINE static void ConvertToUtf8(char * in, 
                                               char * inLast, 
                                               char * & inMid,
                                               char * out, 
                                               char * outLast, 
                                               char *& outMid
                                               )
        {
            SIZE_T nCopy = SafeCopy(in, inLast, out, outLast);

            inMid = in + nCopy;
            outMid = out + nCopy;
        }

        // default implementation for UTF8, and Default encoding
        FORCE_INLINE static void ConvertFromUtf8(char * in, 
                                                 char * inLast, 
                                                 char * & inMid,
                                                 char * out, 
                                                 char * outLast, 
                                                 char *& outMid
                                                 )
        {
            SIZE_T nCopy = SafeCopy(in, inLast, out, outLast);

            inMid = in + nCopy;
            outMid = out + nCopy;
        }

        FORCE_INLINE static UINT64 GetEncodingAlignmentMask()
        {
            return 0;
        }
    };
    
    // Encoding converter beteen UTF8 and UTF8, it's not a convert but a validator.
    template<>
    class TextEncodingConverter<UTF8>
    {
        FORCE_INLINE static void SafeCopy(char * in, 
                                          char * inLast, 
                                          char*& inMid,
                                          char * out, 
                                          char * outLast,
                                          char*& outMid
                                          )
        {
            SIZE_T size = inLast - in;
            SIZE_T spaceRemain = outLast - out;

            SIZE_T nCopy = std::min(spaceRemain, size);

            //copy as much as possible to out buffer, 
            memcpy(out, in, nCopy);

            inMid  = in + nCopy;
            outMid = out + nCopy;
        }
        
        FORCE_INLINE static void CopyAndVerify(char * in, 
                                               char * inLast,
                                               char*& inMid,
                                               char * out, 
                                               char * outLast,
                                               char*& outMid
                                               )
        {
            inMid = in;
            outMid = out;
            
            for (; inMid != inLast && outMid != outLast; )
            {
                // each iteration starts from a new character
                unsigned char c = (unsigned char)(*inMid);
                SIZE_T length = ScopeEngine::UTF8ByteCount(c);

                // according RFC3629, max bytes for UTF8 is 4 bytes. https://en.wikipedia.org/wiki/UTF-8                    
                if (length > 4)
                {
                    throw ScopeStreamException(ScopeStreamException::InvalidCharacter, ConverterCharacterDump(in, inLast, inMid).Detail());                                            
                }

                if (inMid + length > inLast)
                {
                    // inbuffer has no complete UTF8 character, bail out, rely on next convertion
                    return;
                }  

                // inbuffer has complete UTF8 character, copy leading byte first, rembering the start of the character in case we need to report an error
                char * charStart = inMid;
                *outMid++ = *inMid++;

                // copy following byte and verify
                while(--length != 0)
                {
                    if(!IsValidUTF8NonLeadingByte(*inMid))
                    {
                        throw ScopeStreamException(ScopeStreamException::InvalidCharacter, ConverterCharacterDump(in, inLast, charStart).Detail());
                    }
                    else
                    {
                        *outMid++ = *inMid++;
                    }
                }           
            }
        }      

    public:
    
        TextEncodingConverter()
        {
        }

        FORCE_INLINE static void ConvertToUtf8(char * in, 
                                               char * inLast, 
                                               char * & inMid,
                                               char * out, 
                                               char * outLast, 
                                               char *& outMid)
        {
            CopyAndVerify(in, inLast, inMid, out, outLast, outMid);
        }

        FORCE_INLINE static void ConvertFromUtf8(char * in, 
                                                 char * inLast, 
                                                 char * &inMid,
                                                 char * out, 
                                                 char * outLast, 
                                                 char * &outMid)
        {
            // Output internal UTF8 is safe
            SafeCopy(in, inLast, inMid, out, outLast, outMid);
        }

        FORCE_INLINE static UINT64 GetEncodingAlignmentMask()
        {
            return 0;
        }
    };

    // Encoding converter beteen 7-bit ASCII and UTF8
    template<>
    class TextEncodingConverter<ASCII>
    {
 
    public:
        TextEncodingConverter()
        {
        }

        // 7-bit ASCII to UTF8
        FORCE_INLINE static void ConvertToUtf8(char * in, 
                                               char * inLast, 
                                               char * &inMid,
                                               char * out, 
                                               char * outLast, 
                                               char * &outMid)
        {
            inMid  = in;
            outMid = out;

            for (; inMid != inLast && outMid != outLast; ++inMid, ++outMid)
            {
                unsigned char c = (unsigned char)(*inMid);
                if (c < 0x80)
                {
                    *outMid = *inMid;
                }
                // 8-bit ascii, not support
                else
                {
                    throw ScopeStreamException(ScopeStreamException::InvalidCharacter, ConverterCharacterDump(in, inLast, inMid).Detail());
                }
            }
        }

        // UTF8 to 7-bit ASCII
        FORCE_INLINE static void ConvertFromUtf8(char * in, 
                                                 char * inLast, 
                                                 char * &inMid,
                                                 char * out, 
                                                 char * outLast, 
                                                 char * &outMid)
        {
            inMid  = in;
            outMid = out;

            for (; inMid != inLast && outMid != outLast; ++inMid, ++outMid)
            {
                unsigned char c = (unsigned char)(*inMid);
                SIZE_T length = ScopeEngine::UTF8ByteCount(c);

                // it's a 7-bit ascii
                if (length == 1)
                {
                    *outMid = *inMid;
                }
                // not valid ascii
                else
                {
                    throw ScopeStreamException(ScopeStreamException::InvalidCharacter, ConverterCharacterDump(in, inLast, inMid).Detail());
                }
            }
        }

        FORCE_INLINE static UINT64 GetEncodingAlignmentMask()
        {
            return 0;
        }

    };    
    
    template<>
    class TextEncodingConverter<Unicode>
    {
        typedef std::codecvt_utf8_utf16<char16_t, 0x10ffff, std::little_endian> ConverterType;

        mbstate_t         m_state;
        ConverterType     m_conv;      // converts between UTF-8 <-> UTF-16

    public:
        TextEncodingConverter():m_state {}
        {
        }

        void ConvertToUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            if (((inLast-in) & 0x1) != 0)
            {
                // if input data is not aligned properly, we will keep the remain during next round of conversion
                inLast = in + ((inLast-in) & (~(SIZE_T)0x1));
            }

            const char16_t * pInNext = NULL;

            int res = m_conv.out(m_state, 
                (const char16_t *)in, 
                (const char16_t *)inLast, 
                pInNext, 
                out, 
                outLast, 
                outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter, 
                                           ConverterCharacterDump(in, inLast, (char*)(pInNext-1)).Detail());
            }

            inMid = (char *) pInNext;

        }

        void ConvertFromUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            const char * pInNext = NULL;

            int res = m_conv.in(m_state, 
                in, 
                inLast, 
                pInNext, 
                (char16_t *)out, 
                (char16_t *)outLast, 
                (char16_t *&)outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter, 
                                           ConverterCharacterDump(in, inLast, pInNext-1).Detail());
            }

            if (((outMid-out) & 0x1) !=0)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            inMid = (char *)pInNext;
        }

        FORCE_INLINE static UINT64 GetEncodingAlignmentMask()
        {
            return 1;
        }
    };

    template<>
    class TextEncodingConverter<BigEndianUnicode>
    {
        typedef std::codecvt_utf8_utf16<char16_t, 0x10ffff, std::little_endian> ConverterType;

        mbstate_t        m_state;
        ConverterType    m_conv;      // converts between UTF-8 <-> UTF-16

    public:
        TextEncodingConverter():m_state {}
        {
        }

        void ConvertToUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            if (((inLast-in) & 0x1) != 0)
            {
                // if input data is not aligned properly, we will keep the remain during next round of conversion
                inLast = in + ((inLast-in) & (~(SIZE_T)0x1));
            }

            // Convert input to little endian first
            for(char * start = in; start < inLast; start+=sizeof(char16_t))
            {
                *(unsigned short *)start = ByteSwap(*(unsigned short *)start);
            }

            const char16_t * pInNext = NULL;

            int res = m_conv.out(m_state, 
                (const char16_t *)in, 
                (const char16_t *)inLast, 
                pInNext, 
                out, 
                outLast, 
                outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter, 
                                           ConverterCharacterDump(in, inLast, (char*)(pInNext-1)).Detail());
            }

            inMid = (char *) pInNext;

        }

        void ConvertFromUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            const char * pInNext = NULL;

            int res = m_conv.in(m_state, 
                in, 
                inLast, 
                pInNext, 
                (char16_t *)out, 
                (char16_t *)outLast, 
                (char16_t *&)outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter, 
                                           ConverterCharacterDump(in, inLast, pInNext-1).Detail());
            }

            if (((outMid-out) & 0x1) != 0)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            // Convert output to big endian 
            for(char * start = out; start < outMid; start+=sizeof(char16_t))
            {
                *(unsigned short *)start = ByteSwap(*(unsigned short *)start);
            }

            inMid = (char *) pInNext;
        }

        FORCE_INLINE static UINT64 GetEncodingAlignmentMask()
        {
            return 1;
        }

    };

    template<>
    class TextEncodingConverter<UTF32>
    {
        typedef std::codecvt_utf8<char32_t, 0x10ffff, std::little_endian> ConverterType;


        mbstate_t        m_state;
        ConverterType    m_conv;      // converts between UTF-8 <-> UTF-16

    public:
        TextEncodingConverter():m_state {}
        {
        }

        void ConvertToUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            if (((inLast-in) & 0x3) != 0)
            {
                // if input data is not aligned properly, we will keep the remain during next round of conversion
                inLast = in + ((inLast-in) & (~(SIZE_T)0x3));
            }

            const char32_t * pInNext = NULL;

            int res = m_conv.out(m_state, 
                (const char32_t *)in, 
                (const char32_t *)inLast, 
                pInNext, 
                out, 
                outLast, 
                outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter, 
                                           ConverterCharacterDump(in, inLast, (char*)(pInNext-1)).Detail());
            }

            inMid = (char *) pInNext;

        }

        void ConvertFromUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            const char * pInNext = NULL;

            int res = m_conv.in(m_state, 
                in, 
                inLast, 
                pInNext, 
                (char32_t *)out, 
                (char32_t *)outLast, 
                (char32_t *&)outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter, 
                                           ConverterCharacterDump(in, inLast, pInNext-1).Detail());
            }

            if (((outMid-out) & 0x3) != 0)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            inMid = (char *)pInNext;
        }

        FORCE_INLINE static UINT64 GetEncodingAlignmentMask()
        {
            return 3;
        }
    };

    template<>
    class TextEncodingConverter<BigEndianUTF32>
    {
        typedef std::codecvt_utf8<char32_t, 0x10ffff, std::little_endian> ConverterType;

        mbstate_t        m_state;
        ConverterType    m_conv;      // converts between UTF-8 <-> UTF-16

    public:
        TextEncodingConverter():m_state {}
        {
        }

        void ConvertToUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            if (((inLast-in) & 0x3) != 0)
            {
                // if input data is not aligned properly, we will keep the remain during next round of conversion
                inLast = in + ((inLast-in) & (~(SIZE_T)0x3));
            }

            // Convert input to little endian first
            for(char * start = in; start < inLast; start+=sizeof(char32_t))
            {
                *(ULONG*)start = ByteSwap(*(ULONG*)start);
            }

            const char32_t * pInNext = NULL;

            int res = m_conv.out(m_state, 
                (const char32_t *)in, 
                (const char32_t *)inLast, 
                pInNext, 
                out, 
                outLast, 
                outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter, 
                                           ConverterCharacterDump(in, inLast, (char*)(pInNext-1)).Detail());
            }

            inMid = (char *) pInNext;

        }

        void ConvertFromUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            const char * pInNext = NULL;

            int res = m_conv.in(m_state, 
                in, 
                inLast, 
                pInNext, 
                (char32_t *)out, 
                (char32_t *)outLast, 
                (char32_t *&)outMid);

            if (res == std::codecvt_base::error)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter, 
                                           ConverterCharacterDump(in, inLast, pInNext-1).Detail());
            }

            if (((outMid-out) & 0x3) != 0)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }

            // Convert output to big endian 
            for(char * start = out; start < outMid; start+=sizeof(char32_t))
            {
                *(ULONG*)start = ByteSwap(*(ULONG*)start);
            }

            inMid = (char *) pInNext;
        }

        FORCE_INLINE static UINT64 GetEncodingAlignmentMask()
        {
            return 3;
        }
    };

    template<>
    class TextEncodingConverter<UTF7>
    {
        // Variable to keep track of conversion state between different buffers.
        UINT64     m_bits;
        int     m_bitCount;
        bool    m_firstByte;
        int     m_savedCh;

        FORCE_INLINE static bool OutputUtf8Char(UINT c, char *& outMid, char * outLast)
        {
            // Output character in UTF8 encoding
            if (c < 0x80)
            {
                if (outMid < outLast)
                    *outMid++ = (char)c;
                else
                    return false;
            }
            else if (c < 0x800)
            {
                if (outMid + 1 < outLast)
                {
                    *outMid++ = (char)(0xc0 | (c >> 6));
                    *outMid++ = (char)(0x80 | (c & 0x3f));
                }
                else
                    return false;
            }
            else if (c < 0x10000)
            {
                if (outMid + 2 < outLast)
                {
                    *outMid++ = (char)(0xe0 | (c >> 12));
                    *outMid++ = (char)(0x80 | ((c >> 6) & 0x3f));
                    *outMid++ = (char)(0x80 | (c & 0x3f));
                }
                else 
                    return false;
            }
            else
            {
                if (outMid + 3 < outLast)
                {
                    *outMid++ = (char)(0xf0 | (c >> 18));
                    *outMid++ = (char)(0x80 | ((c >> 12) & 0x3f));
                    *outMid++ = (char)(0x80 | ((c >> 6) & 0x3f));
                    *outMid++ = (char)(0x80 | (c & 0x3f));
                }
                else 
                    return false;
            }

            return true;
        }

    public:
        TextEncodingConverter():m_bits(0), m_bitCount(-1), m_firstByte(false), m_savedCh(-1)
        {
        }

        void ConvertToUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            outMid = out;
            inMid = in;

            if (m_savedCh >= 0)
            {
                // Output character in UTF8 encoding
                if (!OutputUtf8Char(m_savedCh, outMid, outLast))
                {
                    return;
                }

                m_savedCh = -1;
            }

            // We may have had bits in the decoder that we couldn't output last time, so do so now
            if (m_bitCount >= 16)
            {
                // Check our decoder buffer
                int c = (m_bits >> (m_bitCount - 16)) & 0xFFFF;

                // Output character in UTF8 encoding
                if (!OutputUtf8Char(c, outMid, outLast))
                {
                    return;
                }

                // Used this one, clean up extra bits
                m_bitCount -= 16;
            }

            // contains previously read character if it was a leading character of UTF-16 surrogate pair
            UINT prev = 0;

            // Loop through the input
            for ( ; inMid < inLast ; inMid++)
            {
                unsigned char currentByte = *inMid;
                UINT c = 0;

                if (m_bitCount >= 0)
                {
                    //
                    // Modified base 64 encoding.
                    //
                    char v;
                    if (currentByte < 0x80 && ((v = x_UTF7DecoderArray[currentByte]) >=0))
                    {
                        m_firstByte = false;
                        m_bits = (m_bits << 6) | v;
                        m_bitCount += 6;
                        if (m_bitCount >= 16)
                        {
                            c = (m_bits >> (m_bitCount - 16)) & 0xFFFF;
                            m_bitCount -= 16;
                        }
                        else
                        {
                            // If not enough bits just continue
                            continue;
                        }
                    }
                    else
                    {
                        // If it wasn't a base 64 byte, everything's going to turn off base 64 mode
                        m_bitCount = -1;

                        if (currentByte != '-')
                        {
                            // it must be >= 0x80 (because of 1st if statemtn)
                            // We need this check since the base64Values[b] check below need b <= 0x7f.
                            // This is not a valid base 64 byte.  Terminate the shifted-sequence and
                            // emit this byte.

                            // not in base 64 table
                            // According to the RFC 1642 and the example code of UTF-7
                            // in Unicode 2.0, we should just zero-extend the invalid UTF7 byte

                            // We don't support fallback. .
                            continue;
                        }

                        //
                        // The encoding for '+' is "+-".
                        //
                        if (m_firstByte) 
                        {
                            c = '+';
                        }
                        else
                        {
                            // We just turn it off if not emitting a +, so we're done.
                            continue;
                        }
                    }
                }
                else if (currentByte == '+')
                {
                    //
                    // Found the start of a modified base 64 encoding block or a plus sign.
                    //
                    m_bitCount = 0;
                    m_firstByte = true;
                    continue;
                }
                else
                {
                    // Normal character
                    if (currentByte >= 0x80)
                    {
                        // We don't support fallback. So throw exception.
                        throw ScopeStreamException(ScopeStreamException::InvalidCharacter, 
                                                   ConverterCharacterDump(in, inLast, inMid).Detail());
                    }

                    // Use the normal character
                    c = currentByte;
                }

                // c is leading character of a UTF-16 surrogate pair
                if (0xD800 <= c && c <= 0xDBFF)
                {
                    // continue to reading the trailing character of the pair
                    prev = c;
                    continue;
                }

                // c is a UTF-16 surrogate pair
                // convert it to its Unicode code point
                if (prev != 0)
                {
                    c = ((prev & 0x03FF) << 10) + (c & 0x03FF) + 0x10000;
                    prev = 0;
                }
                
                // Output character in UTF8 encoding
                if (!OutputUtf8Char(c, outMid, outLast))
                {
                    // save c if we failed to output when there is not enough output buffer.
                    m_savedCh = c;
                    return;
                }
            }
        }

        //
        // Convert the data from UTF-8 to RFC 2060's UTF-7.
        void ConvertFromUtf8(    char * in, 
            char * inLast, 
            char * & inMid,
            char * out, 
            char * outLast, 
            char *& outMid)
        {
            SIZE_T u8len = inLast-in;

            inMid = in;
            outMid = out;

            // May have had too many left over
            while (m_bitCount >= 6)
            {
                // Try to add the next byte
                if (outMid < outLast)
                {
                    *outMid++ = x_Base64Chars[(m_bits >> (m_bitCount-6)) & 0x3F];
                }
                else
                {
                    return;
                }

                m_bitCount -= 6;
            }

            // save a check point for output  so that we can roll back if output buffer is not large enough.
            char * inCur = inMid;

            while(inCur < inLast)
            {
                unsigned char u8 = (unsigned char)(*inCur++);   // the first byte of a character
                UINT ch;    // the character
                int n = 0;  // the number of continuation bytes

                // u8 = 0xxx xxxx
                if (u8 < 0x80)  // u8 & 1000 0000
                {
                    // one-byte character
                    ch = u8;
                    n = 0;
                }
                // u8 = 10xx xxxx
                else if (u8 < 0xc0) // u8 & 1100 0000
                {
                    // continuation byte
                    throw ScopeStreamException(ScopeStreamException::InvalidCharacter, 
                                               ConverterCharacterDump(in, inLast, inCur-1).Detail());
                }
                // u8 = 110x xxxx
                else if (u8 < 0xe0) // u8 & 1110 0000
                {
                    // two-byte character
                    ch = u8 & 0x1f; // u8 & 0001 1111
                    n = 1;
                }
                // u8 = 1110 xxxx
                else if (u8 < 0xf0) // u8 & 1111 0000
                {
                    // three-byte character
                    ch = u8 & 0x0f; // u8 & 0000 1111
                    n = 2;
                }
                // u8 = 1111 0xxx
                else if (u8 < 0xf8) // u8 & 1111 1000
                {
                    // four-byte character
                    ch = u8 & 0x07; // u8 & 0000 0111
                    n = 3;
                }
                // u8 = 1111 10xx
                else if (u8 < 0xfc) // u8 & 1111 1100
                {
                    // five-byte character
                    ch = u8 & 0x03; // u8 & 0000 0011
                    n = 4;
                }
                // u8 = 1111 110x
                else if (u8 < 0xfe) // u8 & 1111 1110
                {
                    // six-byte character
                    ch = u8 & 0x01; // u8 & 0000 0001
                    n = 5;
                }
                // u8 = 1111 111x
                else
                {
                    // not a valid utf-8 character leading byte
                    throw ScopeStreamException(ScopeStreamException::InvalidCharacter, 
                                               ConverterCharacterDump(in, inLast, inCur-1).Detail());
                }

                u8len--;

                // not enough input stream, return
                if (n > u8len)
                {
                    return;
                }

                // read the whole character and write it's Unicode code point into ch
                for (int j = 0; j < n; j++)
                {
                    unsigned char o = (unsigned char)(*inCur++);

                    // o & 1100 0000 != 1000 0000
                    if ((o & 0xc0) != 0x80)
                    {
                        // o is not a continuation byte
                        throw ScopeStreamException(ScopeStreamException::InvalidCharacter, 
                                                   ConverterCharacterDump(in, inLast, inCur-1).Detail());
                    }
                    ch = (ch << 6) | (o & 0x3f);
                }

                u8len -= n;

                // Save the last read input position, because we have consumed one unicode character.
                inMid = inCur;

                if (ch < 0x80 && x_UTF7DirectChars[ch])
                {
                    if (m_bitCount >= 0)
                    {
                        if (m_bitCount > 0)
                        {
                            // Try to add the next byte
                            if (outMid < outLast)
                                *outMid++ = x_Base64Chars[(m_bits << (6 - m_bitCount)) & 0x3F];
                            else
                                return;

                            m_bitCount = 0;
                        }

                        // Need to get emit '-' and our char, 2 bytes total
                        if (outMid < outLast)
                        {
                            *outMid++ = '-';
                        }
                        else
                            return;

                        m_bitCount = -1;
                    }

                    // Need to emit our char
                    if (outMid < outLast)
                        *outMid++ = (char)ch;
                    else
                        return;
                }
                else if (m_bitCount < 0 && ch == (unsigned short)'+')
                {
                    if (outMid + 1 < outLast)
                    {
                        *outMid++ = '+';
                        *outMid++ = '-';
                    }
                    else
                        return;
                }
                else
                {
                    if (m_bitCount < 0)
                    {
                        // Need to emit a + and 12 bits (3 bytes)
                        // Only 12 of the 16 bits will be emitted this time, the other 4 wait 'til next time
                        if (outMid < outLast)
                        {
                            *outMid++ = '+';
                        }
                        else
                            return;

                        // We're now in bit mode, but haven't stored data yet
                        m_bitCount = 0;
                    }

                    // ch can be represented by a single UTF-16 code unit
                    if (ch < 0x10000)
                    {
                        // append UTF-16 code unit to m_bits
                        m_bits = m_bits << 16 | ch;
                        m_bitCount += 16;
                    }
                    // ch can be represented by a UTF-16 surrogate pair
                    else if (ch < 0x20000)
                    {
                        // append UTF-16 surrogate pair to m_bits
                        m_bits = m_bits << 16 | ((ch >> 10) + 0xD7C0);
                        m_bits = m_bits << 16 | ((ch & 0x3FF) + 0xDC00);
                        m_bitCount += 32;
                    }
                    // ch can't be represented in UTF-16 encoding, so it can't be represented in UTF-7
                    else
                    {
                        throw ScopeStreamException(ScopeStreamException::InvalidCharacter, 
                                                   ConverterCharacterDump(in, inLast, inCur-1).Detail());
                    }
                    
                    while (m_bitCount >= 6)
                    {
                        // Try to add the next byte
                        if (outMid < outLast)
                            *outMid++ = x_Base64Chars[(m_bits >> (m_bitCount-6)) & 0x3F];
                        else
                            return;

                        m_bitCount -= 6;
                    }

                    SCOPE_ASSERT(m_bitCount < 6);
                }

                //advance the output buffer since we have finished one loop. 
                //??? outMid = outMid;
            }

            // Now if we have bits left over we have to encode them.
            if (m_bitCount >= 0)
            {
                if (m_bitCount > 0)
                {
                    // Try to add the next byte
                    if (outMid < outLast)
                        *outMid++ = x_Base64Chars[(m_bits << (6 - m_bitCount)) & 0x3F];
                    else
                        return;

                    m_bitCount = 0;
                }

                // Need to get emit '-'
                if (outMid < outLast)
                {
                    *outMid++ = '-';
                }
                else
                    return;

                m_bitCount = -1;
            }
        }

        FORCE_INLINE static UINT64 GetEncodingAlignmentMask()
        {
            return 0;
        }
    };

    // class to read a buffer of text in different encoding. The result will be converted to utf-8 encoding.
    // InputType - cosmosInput, GZipInput
    template<typename InputStreamType>
    class TextEncodingReader
    {
        InputStreamType *m_inputStream; // input stream 
        unique_ptr<IncrementalAllocator>      m_alloc;       // allocate for translation target buffer

        char        * m_inStart;     // start position of input buffer
        char        * m_outStart;    // start position of output buffer. 
        SIZE_T        m_remain;      // remain elements in the input buffer

        bool          m_firstRead;   // flag for first time to read bytes from under layer stream.
        TextEncoding  m_encoding;
        bool          m_validateEncoding;

        static const int x_codeCvtBufSize = 8*1024*1024;
        static const int x_underflowBufSize = 20;

        SIZE_T        m_cbStop;      // stop and go point at input stream, encoding procedure must stop (and go) at this point
        SIZE_T        m_cbEncoded;   // byte counts for completed encoding, a.k.a bytes read from input stream
        bool          m_stopped;     // flag to indicate already stopped (once)

        // different encoding converter for code translation. They are needed since the encoding is runtime decision for extractor.
        TextEncodingConverter<UTF8>                 m_utf8Converter;
        TextEncodingConverter<ASCII>                m_asciiConverter;
        TextEncodingConverter<Unicode>              m_unicodeConverter;
        TextEncodingConverter<BigEndianUnicode>     m_beUnicodeConverter;
        TextEncodingConverter<UTF32>                m_utf32Converter;
        TextEncodingConverter<BigEndianUTF32>       m_beUtf32Converter;
        TextEncodingConverter<UTF7>                 m_utf7Converter;

        static TextEncoding DetectEncodingByBOM(unsigned char * buf, SIZE_T size)
        {
            if (HasUTF32LEBOM(buf, size))
            {
                return UTF32;
            }
            else if (HasUTF32BEBOM(buf, size))
            {
                return BigEndianUTF32;
            }
            else if (HasUTF16LEBOM(buf, size))
            {
                return Unicode;
            }
            else if (HasUTF16BEBOM(buf, size))
            {
                return BigEndianUnicode;
            }
            else if (HasUTF8BOM(buf, size))
            {
                return UTF8;
            }
            else if (HasUTF7BOM(buf, size))
            {
                return UTF7;
            }

            return Default;
        }

        static bool HasUTF32LEBOM(unsigned char * buf, SIZE_T size)
        {
            return size >= 4 &&
                buf[0] == 0xFF &&
                buf[1] == 0xFE &&
                buf[2] == 0 &&
                buf[3] == 0;
        }

        static bool HasUTF32BEBOM(unsigned char * buf, SIZE_T size)
        {
            return size >= 4 &&
                buf[0] == 0 &&
                buf[1] == 0 &&
                buf[2] == 0xFE &&
                buf[3] == 0xFF;
        }

        static bool HasUTF16LEBOM(unsigned char * buf, SIZE_T size)
        {
            return size >= 2 && 
                buf[0] == 0xFF &&
                buf[1] == 0xFE;
        }

        static bool HasUTF16BEBOM(unsigned char * buf, SIZE_T size)
        {
            return size >= 2 &&
                buf[0] == 0xFE &&
                buf[1] == 0xFF;
        }

        static bool HasUTF8BOM(unsigned char * buf, SIZE_T size)
        {
            return size >= 3 &&
                buf[0] == 0xEF &&
                buf[1] == 0xBB &&
                buf[2] == 0xBF;
        }

        static bool HasUTF7BOM(unsigned char * buf, SIZE_T size)
        {
            return size >= 4 &&
                buf[0] == 0x2B &&
                buf[1] == 0x2F &&
                buf[2] == 0x76 &&
                (buf[3] == 0x38 || buf[3] == 0x39 || buf[3] == 0x2B || buf[3] == 0x2F);
        }

        // pData has a partial character, try to make up a completed character by feeding more bytes from pExtraData.
        // returns count of bytes consumed from pExtraData
        // This method uses encoding method to validate that a complete character is made up.
        template<TextEncoding encoding>
        SIZE_T MakeupOneCompleteCharacter(TextEncodingConverter<encoding> * encoder, char* pData, SIZE_T cbData, char* pExtraData, SIZE_T cbExtraData)
        {
            SCOPE_ASSERT(pData != nullptr && pExtraData != nullptr);
            SCOPE_ASSERT(cbData > 0 && cbData < x_underflowBufSize);

            char inBuf[x_underflowBufSize];
            char outBuf[x_underflowBufSize];

            // pData should only have one partial character
            char* pInNext = nullptr;
            char* pOutNext = nullptr;
            encoder->ConvertToUtf8(pData,
                                   pData + cbData,
                                   pInNext,
                                   outBuf,
                                   outBuf + x_underflowBufSize,
                                   pOutNext);
            SCOPE_ASSERT(pInNext == pData && pOutNext == outBuf);

            memcpy(inBuf, pData, cbData);
            SIZE_T nCopy = 0;
            while (cbData + nCopy < x_underflowBufSize && nCopy < cbExtraData && nCopy < 4)  // one character should not exceed 4 bytes in any encoding
            {
                inBuf[cbData + nCopy] = pExtraData[nCopy];
                ++nCopy;

                encoder->ConvertToUtf8(inBuf,
                                       inBuf + cbData + nCopy,
                                       pInNext,
                                       outBuf,
                                       outBuf + x_underflowBufSize,
                                       pOutNext);

                if (pInNext != inBuf)
                {
                    SCOPE_ASSERT(pOutNext != outBuf);
                    return nCopy;
                }
            }

            if (cbData + nCopy == x_underflowBufSize || nCopy == 4)
            {
                throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
            }
            else
            {
                // pExtraData is still too small.
                return 0;
            }
        }

        // stop is a out parameter. True if we crossed stop and go point during this round of encoding. 
        // Otherwise false, i.e. we either did not reach stop and go point or already seen it on a previous round.
        template<TextEncoding encoding>
        void ConvertToUtf8(TextEncodingConverter<encoding> * encoder, SIZE_T & numRead, bool& stop)
        {
            char * pInNext = NULL;
            char * pOutNext = NULL;

            // stop encoding at the stop point, continue if stop point already passed.
            SCOPE_ASSERT(m_stopped || m_cbStop > m_cbEncoded);
            SIZE_T cbToConvert = m_stopped ? m_remain : std::min(m_remain, m_cbStop - m_cbEncoded);

            encoder->ConvertToUtf8(m_inStart, 
                                   m_inStart + cbToConvert, 
                                   pInNext, 
                                   m_outStart, 
                                   m_outStart+x_codeCvtBufSize, 
                                   pOutNext);

            // If nothing gets converted, we will need to feed more bytes and convert again
            if (m_inStart == pInNext)
            {
                // nothing gets output as well.
                SCOPE_ASSERT(m_outStart == pOutNext);

                // partial (multiple-byte) character in buffer, try giving more bytes.
                // maybe extra ConvertToUtf8 calls inside MakeupOneCompleteCharacter. 
                SIZE_T extra = MakeupOneCompleteCharacter(encoder, m_inStart, cbToConvert, m_inStart + cbToConvert, m_remain - cbToConvert);

                // Got one complete character, need
                // 1. Update m_cbStop
                // 2. Stop encoding here and return
                if (extra > 0)
                {
                    encoder->ConvertToUtf8(m_inStart,
                                           m_inStart + cbToConvert + extra,
                                           pInNext,
                                           m_outStart,
                                           m_outStart + x_codeCvtBufSize,
                                           pOutNext);

                    m_cbStop += extra;
                    m_cbEncoded += pInNext - m_inStart;
                    numRead = pOutNext - m_outStart;
                    m_remain -= (pInNext - m_inStart);
                    m_inStart = pInNext;

                    // check whether stop point reached.
                    stop = !m_stopped && (m_cbEncoded >= m_cbStop);
                    m_stopped = m_stopped || stop;
                    return;
                }

                // All bytes in m_inStart buffer are consumed but encoding still does not progress, need fetch one more buffer page
                if (m_remain < x_underflowBufSize)
                {
                    // copy all bytes out of m_inStart buffer
                    char underflowBuf[x_underflowBufSize];
                    SIZE_T underflowRemain = m_remain;
                    memcpy(underflowBuf, pInNext, m_remain);

                    // refill m_inStart buffer
                    m_remain = 0;
                    m_inStart = m_inputStream->GetNextPage(m_remain);

                    // if it is end of stream, raise exception since charater is not enough to convert
                    if (m_inStart == NULL || m_remain == 0)
                    {
                        // if there is remain code not converted, invalid character detected. 
                        throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
                    }

                    SCOPE_ASSERT(underflowRemain > 0);
                    
                    // copy bytes to the small buffer
                    SIZE_T nCopy = 0;
                    if (m_stopped)
                    {
                        nCopy = std::min(x_underflowBufSize - underflowRemain, m_remain);
                    }
                    else
                    {
                        nCopy = MakeupOneCompleteCharacter(encoder, underflowBuf, underflowRemain, m_inStart, m_remain);
                        if (nCopy == 0)
                        {
                            SCOPE_LOG_FMT_ERROR("TextEncodingReader ConvertToUtf8", "Failed to makeup one character, bytes [%I64u,%I64u]", underflowRemain, m_remain);
                            throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
                        }
                        
                        m_cbStop = std::max(m_cbEncoded + underflowRemain + nCopy, m_cbStop);
                    }
                    
                    memcpy(&underflowBuf[underflowRemain], m_inStart, nCopy);

                    encoder->ConvertToUtf8(underflowBuf, 
                                           underflowBuf + underflowRemain + nCopy, 
                                           pInNext, 
                                           m_outStart, 
                                           m_outStart+x_codeCvtBufSize, 
                                           pOutNext);
                    
                    if (m_outStart == pOutNext)
                    {
                        // Since underflow can only happen at the beginning of a new buffer, this should never happen.
                        throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
                    }

                    // number of character converted must larger than original underflowRemain.
                    SCOPE_ASSERT((SIZE_T)(pInNext - underflowBuf) > underflowRemain);

                    m_cbEncoded += pInNext - underflowBuf;
                    SIZE_T nProceed = pInNext - underflowBuf - underflowRemain;
                    m_inStart += nProceed; 
                    m_remain -= nProceed;
                    numRead = pOutNext - m_outStart;
                }
                else
                {
                    throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
                }
            }
            else
            {
                m_cbEncoded += pInNext - m_inStart;
                numRead = pOutNext - m_outStart;
                m_remain -= pInNext - m_inStart;
                m_inStart = pInNext;
            }

            // check whether stop point reached.
            stop = !m_stopped && (m_cbEncoded >= m_cbStop);
            m_stopped = m_stopped || stop;
        }

        // Pass through input buffer to output buffer.
        FORCE_INLINE SIZE_T PassThroughBuffer(bool& stop)
        {
            SIZE_T numRead = 0;
            if (m_stopped || m_cbEncoded + m_remain < m_cbStop)
            {
                m_outStart = m_inStart;
                numRead = m_remain;
                m_remain = 0;
                stop = false;
            }
            else
            {
                numRead = m_cbStop - m_cbEncoded;
                m_outStart = m_inStart;
                m_stopped = true;
                stop = true;

                // move m_inStart step forward
                m_inStart += numRead;
                m_remain -= numRead;
            }
            
            m_cbEncoded += numRead;
            return numRead;
        }

        // Bit mask for encoding alignment requirement
        FORCE_INLINE UINT64 GetEncodingAlignmentMask() const
        {
            UINT64 mask = 0;
            switch(m_encoding)
            {
            case TextEncoding::Unicode:
                return m_unicodeConverter.GetEncodingAlignmentMask();

            case TextEncoding::BigEndianUnicode:
                return m_beUnicodeConverter.GetEncodingAlignmentMask();

            case TextEncoding::UTF32:
                return m_utf32Converter.GetEncodingAlignmentMask();

            case TextEncoding::BigEndianUTF32:
                return m_beUtf32Converter.GetEncodingAlignmentMask();

            case TextEncoding::UTF7:
                return m_utf7Converter.GetEncodingAlignmentMask();

            case TextEncoding::ASCII:
                return m_asciiConverter.GetEncodingAlignmentMask();

            case TextEncoding::UTF8:
                return m_utf8Converter.GetEncodingAlignmentMask();

            case TextEncoding::Default:
                return 0;

            default:
                SCOPE_ASSERT(!"Unknown Encoding!");
            }

            return mask;
        }

    public:

        TextEncodingReader(TextEncoding encoding, bool validateEncoding): 
            m_inputStream(NULL), 
            m_inStart(NULL),
            m_outStart(NULL),
            m_remain(0),
            m_firstRead(true),
            m_encoding(encoding),
            m_validateEncoding(validateEncoding),
            m_stopped(false),
            m_cbEncoded(0)
        {
        }

        // Assign a stop and go point for input stream, default is UINT64_MAX which means no stop point before EOF.
        void Init(InputStreamType * inputStream, UINT64 cbStop)
        {
            SCOPE_ASSERT(m_inputStream == NULL);
            m_inputStream = inputStream;
            
            // Initialize the output buffer
            m_alloc.reset(new IncrementalAllocator(x_codeCvtBufSize*2, "TextEncodingReader"));
            m_outStart = m_alloc->Allocate(x_codeCvtBufSize);
            m_inStart = m_inputStream->GetCurrentBuffer();
            m_remain = m_inputStream->RemainBufferSize();

            // "Stop and go" encoding requires the stop point has correct alignment according to encoding.
            UINT64 mask = GetEncodingAlignmentMask();
            if (cbStop != UINT64_MAX && 0 != (cbStop & mask))
            {
                throw ScopeStreamExceptionWithEvidence(E_EXTRACT_INPUT_ENCODING_MISALIGNED, 
                          {GetEncodingName(m_encoding), mask + 1, cbStop, m_inputStream->StreamName()}, std::string());
            }
            
            m_cbStop = cbStop;
            m_cbEncoded = 0;
        }

        void Close()
        {
            m_inputStream = NULL;
            if (m_alloc)
            {
                m_alloc->Reset<IncrementalAllocator::ReclaimAllMemoryPolicy>();
            }
        }

        void Restart(UINT64 startOffset)
        {
            if (!m_inputStream->IsCompressed())
            {
                m_inputStream->Seek(startOffset);
            }
            else
            {
                throw RuntimeException(E_SYSTEM_NOT_SUPPORTED, "Seek compressed stream");
            }
            
            m_inStart = NULL;
            m_remain = 0;
            m_cbEncoded = 0;
            m_stopped = false;
            m_firstRead = true;
        }

        // Skip invalid leading bytes in current encoding configuration, before encoding
        // JM may slice stream in the middle of (multi-bytes) character, even though the split start offset is always 4 bytes aligned.
        void SkipInvalidLeadingBytes()
        {
            SCOPE_ASSERT(m_firstRead);
            SCOPE_ASSERT(m_inStart != nullptr);

            UINT64 value = 0;
            SIZE_T idx = 0;
            switch(m_encoding)
            {
            case TextEncoding::UTF7:
            case TextEncoding::ASCII:
            case TextEncoding::Default:
            case TextEncoding::UTF32:
            case TextEncoding::BigEndianUTF32:
                break;

            case TextEncoding::UTF8:
                while (idx < m_remain && (m_inStart[idx] & 0xC0) == 0x80) // non leading byte in UTF8, skip
                {
                    if (idx > 2)
                    {
                        // skip too much, corrupt characters
                        throw ScopeStreamException(ScopeStreamException::InvalidCharacter);
                    }
                    ++idx;
                }

                if (idx > 0)
                {
                    for (int i = 0 ; i < idx; ++i)
                    {
                        value = (value << 8) | m_inStart[i];
                    }
                    SCOPE_LOG_FMT_WARNING("TextEncodingReader UTF8", "WARNING: Skip invalid leading bytes 0x%I64X", value);
                }

                m_inStart += idx;
                m_cbEncoded += idx;
                m_remain -= idx;
                break;

            case TextEncoding::Unicode:
                SCOPE_ASSERT(m_remain > 1);
                value = (m_inStart[1] << 8) | m_inStart[0];
                if (value >= 0xDC00 && value <= 0xDFFF)
                {
                    // low surrogate (second half of surrogate pair)
                    // c++ std encoder will translate low surrogate to utf8 bytes without reporting error.
                    SCOPE_LOG_FMT_WARNING("TextEncodingReader Unicode", "WARNING: Skip invalid leading bytes 0x%I64X", value);
                }

                break;

            case TextEncoding::BigEndianUnicode:
                SCOPE_ASSERT(m_remain > 1);
                value = (m_inStart[0] << 8) | m_inStart[1];
                if (value >= 0xDC00 && value <= 0xDFFF)
                {
                    SCOPE_LOG_FMT_WARNING("TextEncodingReader BigEndianUnicode", "WARNING: Skip invalid leading bytes 0x%I64X", value);
                }

                break;

            default:
                SCOPE_ASSERT(false);
            }
        }
        
        // Convert the page read from inputStream to UTF-8 and return.
        // Default implementation has no conversion. It just delegate the call to cosmos stream.
        char * GetNextPage(SIZE_T & numRead, bool& stop)
        {
            SCOPE_ASSERT(m_inputStream != NULL);

            if (m_inStart == NULL || m_remain == 0)
            {
                m_inStart = m_inputStream->GetNextPage(m_remain);

                // if it is end of stream, return NULL
                if (m_inStart == NULL || m_remain == 0)
                {
                    numRead = 0;
                    return NULL;
                }
            }
            
            if (m_firstRead)
            {
                if (m_encoding == TextEncoding::Default)
                {
                    m_encoding = DetectEncodingByBOM((unsigned char *)m_inStart, m_remain);
                }
                else if (m_validateEncoding)
                {
                    // Error if an encoding was detected based on the file's BOM and it differs from the user-specified encoding.
                    // Ignore a detected UTF-7 BOM as it can clash with legal characters in other encodings. For example,
                    // one of the UTF-7 BOMs maps to the UTF-8 character sequence +/v8. A UTF-8 encoded file without a BOM
                    // but starting with the string +/v8 will be (wrongfully) detected as a UTF-7 encoded file. Do not
                    // throw an exception in this case.
                    auto detectedEncoding = DetectEncodingByBOM((unsigned char *)m_inStart, m_remain);
                    if (detectedEncoding != TextEncoding::Default
                        && detectedEncoding != m_encoding
                        && detectedEncoding != TextEncoding::UTF7)
                    {
                        throw ScopeStreamExceptionWithEvidence(E_EXTRACT_ENCODING_MISMATCH,
                        { GetEncodingName(m_encoding), GetEncodingName(detectedEncoding) },
                        ConverterCharacterDump(m_inStart, m_inStart + m_remain, m_inStart).Detail());
                    }
                }

                SkipInvalidLeadingBytes();
            }

            switch(m_encoding)
            {
            case Default:
                // There is no need for translation, just return the original buffer unless stop point reached.
                numRead = PassThroughBuffer(stop);
                break;

            case UTF8:
                if (m_validateEncoding)
                {
                    ConvertToUtf8<UTF8>(&m_utf8Converter, numRead, stop);
                }
                else
                {
                    numRead = PassThroughBuffer(stop);
                }
                break;
 
            case ASCII:
                if (m_validateEncoding)
                {
                    ConvertToUtf8<ASCII>(&m_asciiConverter, numRead, stop);
                }
                else
                {
                    numRead = PassThroughBuffer(stop);
                }
                break;
                
            case Unicode:
                ConvertToUtf8<Unicode>(&m_unicodeConverter, numRead, stop);
                break;

            case BigEndianUnicode:
                ConvertToUtf8<BigEndianUnicode>(&m_beUnicodeConverter, numRead, stop);
                break;

            case UTF7:
                ConvertToUtf8<UTF7>(&m_utf7Converter, numRead, stop);
                break;

            case UTF32:
                ConvertToUtf8<UTF32>(&m_utf32Converter, numRead, stop);
                break;

            case BigEndianUTF32:
                ConvertToUtf8<BigEndianUTF32>(&m_beUtf32Converter, numRead, stop);
                break;
            }

            if (m_firstRead)
            {
                // Remove BOM from the output buffer (UTF-8 representstion of U+FEFF occupies 3 bytes)
                // Do it after we perform conversion as UTF7 encoding requires it (for other encodings it doesn't matter)
                if (HasUTF8BOM((unsigned char *)m_outStart, numRead))
                {
                    m_outStart += 3;
                    numRead -= 3;
                }

                m_firstRead = false;
            
                // it could be true that the page return from scanner can only have BOM
                // in current implemenation, numRead == 0 means EOS
                // so try to read next page
                if (numRead == 0)
                {
                    return GetNextPage(numRead, stop);
                } 
            }

            return m_outStart;
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            if (m_alloc)
            {
                m_alloc->WriteRuntimeStats(root);
            }
        }
    };

    //
    // Static class that contains conversion logic from FString to all SCOPE-supported types.
    //
    class TextConversions
    {
        template<typename T>
        static INLINE bool ConvertToInfNaNHelper(const char * buf, int size, T & out)
        {
            static_assert(is_floating_point<T>::value, "This routine is valid only for floating point types");

            int len = size-1;

            if ((len == sizeof(x_NaN)) && (strncmp(buf, x_NaN, sizeof(x_NaN)) == 0))
            {
                out = numeric_limits<T>::quiet_NaN();
                return true;
            }
            else if ((len == sizeof(x_PositiveInf)) && (strncmp(buf, x_PositiveInf, sizeof(x_PositiveInf)) == 0))
            {
                out = numeric_limits<T>::infinity();
                return true;
            }
            else if ((len == sizeof(x_NegativeInf)) && (strncmp(buf, x_NegativeInf, sizeof(x_NegativeInf)) == 0))
            {
                out = -(numeric_limits<T>::infinity());
                return true;
            }

            return false;
        }

        template<typename T>
        static INLINE bool ConvertToInfNaN(const char *, int, T &)
        {
            static_assert(!is_floating_point<T>::value, "The two specializations below should handle float types");
            return false;
        }

        template<>
        static INLINE bool ConvertToInfNaN<double>(const char * buf, int size, double & out)
        {
            return ConvertToInfNaNHelper(buf, size, out);
        }

        template<>
        static INLINE bool ConvertToInfNaN<float>(const char * buf, int size, float & out)
        {
            return ConvertToInfNaNHelper(buf, size, out);
        }

    public:
#ifdef PLUGIN_TYPE_SYSTEM
        // parameter reason gives more information about conversion failure
        template<typename T>
        static INLINE ConvertResult DoTryFStringToT(FStringWithNull & str, T & out)
        {            
            if (is_floating_point<T>::value)
            {
                str.TrimWhiteSpace();

                if (ConvertToInfNaN(str.buffer(), str.size(), out))
                {        
                    return ConvertSuccess;
                }
            }
            
            return  NumericConvert(str.buffer(), str.size(), out);
        }

        // parameter reason gives more information about conversion failure
        template<typename T>
        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, T & out)
        {            
            return out.FromFString<FStringWithNull>(&str);
        }

        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, unsigned char & out)
        {                      
            return DoTryFStringToT(str, out);
        }

        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, char & out)
        {                      
            return DoTryFStringToT(str, out);
        }
        
        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, short & out)
        {                      
            return DoTryFStringToT(str, out);
        }

        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, unsigned short & out)
        {                      
            return DoTryFStringToT(str, out);
        }

        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, wchar_t & out)
        {                      
            return DoTryFStringToT(str, out);
        }
        
        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, int & out)
        {                      
            return DoTryFStringToT(str, out);
        }

        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, unsigned int & out)
        {                      
            return DoTryFStringToT(str, out);
        }

        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, __int64 & out)
        {                      
            return DoTryFStringToT(str, out);
        }

        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, unsigned __int64 & out)
        {                      
            return DoTryFStringToT(str, out);
        }

        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, float & out)
        {                      
            return DoTryFStringToT(str, out);
        }

        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, double & out)
        {                      
            return DoTryFStringToT(str, out);
        }
#else
        // parameter reason gives more information about conversion failure
        template<typename T>
        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, T & out)
        {            
            if (is_floating_point<T>::value)
            {
                str.TrimWhiteSpace();

                if (ConvertToInfNaN(str.buffer(), str.size(), out))
                {        
                    return ConvertSuccess;
                }
            }
            
            return  NumericConvert(str.buffer(), str.size(), out);
        }

        template<class I, typename T>
        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, ScopeSqlType::ArithmeticSqlType<I, T> & out)
        {
            T value;
            ConvertResult result = NumericConvert(str.buffer(), str.size(), value);
            out.setValue(value);
            return result;
        }

        template<>
        static INLINE ConvertResult TryFStringToT<FString>(FStringWithNull & str, FString & out)
        {
            // This transfers string buffer ownership to the "out" object
            return out.ConvertFrom(str);
        }

        template<>
        static INLINE ConvertResult TryFStringToT<FBinary>(FStringWithNull & str, FBinary & out)
        {
            // This transfers string buffer ownership to the "out" object
            return out.ConvertFrom(str);
        }

        // convert a Decimal from string
        template<>
        static INLINE ConvertResult TryFStringToT<ScopeDecimal>(FStringWithNull & str, ScopeDecimal & out)
        {
            try
            {
                str.TrimWhiteSpace();

                // Convert string to decimal
                out = ScopeDecimal::Parse(str.buffer(), str.DataSize());
            }
            catch(ScopeDecimalException & e)
            {
                return e.ToConvertResult();
            }

            return ConvertSuccess;
        }

        // convert a DateTime from string
        template<>
        static INLINE ConvertResult TryFStringToT<ScopeDateTime>(FStringWithNull & str, ScopeDateTime & out)
        {
            // Try and convert tmp to Date time using managed code
            return ScopeDateTime::TryParse(str.buffer(), out) ? ConvertSuccess : ConvertErrorUndefined;
        }

        // convert a SQL.DATETIME2 from string
        template<>
        static INLINE ConvertResult TryFStringToT<ScopeSqlType::SqlDateTime>(FStringWithNull & str, ScopeSqlType::SqlDateTime & out)
        {
            return ScopeSqlType::SqlDateTime::TryParseStr(str.buffer(), out) ? ConvertSuccess : ConvertErrorStringToSqlDatetime2;
        }

        template<>
        static INLINE ConvertResult TryFStringToT<ScopeGuid>(FStringWithNull & str, ScopeGuid & out)
        {
            str.TrimWhiteSpace();
            return ScopeGuid::TryParse(str.buffer(), str.DataSize(), out) ? ConvertSuccess : ConvertErrorUndefined;
        }

        // convert a ScopeSqlType::SqlDecimal from string
        template<>
        static INLINE ConvertResult TryFStringToT<ScopeSqlType::SqlDecimal>(FStringWithNull & str, ScopeSqlType::SqlDecimal & out)
        {
            try
            {
                str.TrimWhiteSpace();

                // Convert string to decimal
                out = ScopeSqlType::SqlDecimal::Parse(str.buffer(), str.DataSize());
            }
            catch (ScopeDecimalException & e)
            {
                return e.ToConvertResult();
            }

            return ConvertSuccess;
        }

        template <int N>
        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, ScopeSqlType::SqlStringFaceted<N> & out)
        {
            // This transfers string buffer ownership to the "out" object
            return out.ConvertFrom(str);
        }

#if !defined(SCOPE_NO_UDT)
        // deserialize a scope supported UDT type by invoke IScopeSerializableText interface.
        template<int UserDefinedTypeId, template<int> class UserDefinedType>
        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, UserDefinedType<UserDefinedTypeId> & out)
        {
            try
            {
                // convert tmp to UDT using managed code
                out.TextDeserialize(str.buffer());
            }
            catch(ScopeStreamException &)
            {
                return ConvertErrorUndefined;
            }

            return ConvertSuccess;
        }

#endif // SCOPE_NO_UDT

        template<typename T>
        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, NativeNullable<T> & out)
        {
            // Null or Empty string is treated as null for NativeNullable<T> type.
            if (str.IsNullOrEmpty())
            {
                out.SetNull();
                return ConvertSuccess;
            }
            
            ConvertResult result = TryFStringToT(str, out.get()); 
            
            if(result == ConvertSuccess)
            {
                out.ClearNull();
            }

            return result;
        }

        template<typename T>
        static INLINE ConvertResult TryFStringToT(FStringWithNull & str, ScopeSqlType::SqlNativeNullable<T> & out)
        {
            return TryFStringToT(str, (NativeNullable<T>&) out);
        }

#endif // PLUGIN_TYPE_SYSTEM

        // convert a bool from string
        // it only takes True and False
        template<>
        static INLINE ConvertResult TryFStringToT<bool>(FStringWithNull & str, bool & out)
        {
            str.TrimWhiteSpace();
            unsigned int len = str.DataSize();

            if ((len == sizeof(x_True)) && (_strnicmp(str.buffer(), x_True, sizeof(x_True)) == 0))
            {
                out = true;
            }
            else if ((len == sizeof(x_False)) && (_strnicmp(str.buffer(), x_False, sizeof(x_False)) == 0))
            {
                out = false;
            }
            else 
            {
                // literal string is not "true" or "false"
                return ConvertErrorInvalidCharacter;
            }

            return ConvertSuccess;
        }
    };
    
    //
    // TextInputStream traits for "batch" mode
    //
    template<UINT64       _delimiter, 
             UINT64       _rowDelimiter, 
             UINT64       _escapeChar,
             bool         _escape, 
             bool         _textQualifier, 
             TextEncoding _encoding, 
             bool         _validateEncoding,
             bool         _silent,
             bool         _haveNullEscape,
             int          _skipFirstRowsCount>
    struct TextInputStreamTraitsConst
    {
        static const UINT64 delimiter = _delimiter;
        static const UINT64 rowDelimiter = _rowDelimiter;
        static const UINT64 escapeChar = _escapeChar;
        static const bool haveNullEscape = _haveNullEscape;
        UINT64 * nullEscape = nullptr;             // points to UTF-8 "string" representation of null escape
        SIZE_T nullEscapeLength = 0;            // length of UTF-8 "string" representation of null escape
        static const bool escape = _escape;
        static const bool textQualifier = _textQualifier;
        static const TextEncoding encoding = _encoding;
        static const bool validateEncoding = _validateEncoding;  // actually this flag only control ASCII and UTF8 validation, to keep compatible with first party behavior
        static const bool silent = _silent;
        static const int skipFirstRowsCount = _skipFirstRowsCount;

        TextInputStreamTraitsConst(const InputStreamParameters & isp)
        {
            if (_haveNullEscape)
            {
                SCOPE_ASSERT(isp.nullEscape != nullptr);

                // count UTF-8 characters in isp.nullEscape
                for (const char *p = isp.nullEscape; *p != 0; ++p)
                {
                    // the first two bits do not match continuation byte pattern
                    // therefore this byte begins a character
                    // *p & 1100 0000 != 1000 0000
                    if (((*p) & 0xC0) != 0x80)
                        ++nullEscapeLength;
                }

                nullEscape = new UINT64[nullEscapeLength]();    // create null escape string and initialize its elements with zeros

                // count of characters currently pushed back into nullEscape array
                int pushedBackCount = 0;
                                
                for (const char *q = isp.nullEscape; *q != 0;)
                {
                    int count = 0;  // number of bytes in the current character;

                    unsigned char c = *q;
                    while (c & 0x80)
                    {
                        ++count;
                        c <<= 1;
                    }

                    if (count == 0)
                    {
                        count = 1;
                    }

                    // read the character
                    for (int i = 0; i < count; ++i, ++q)
                    {
                        nullEscape[pushedBackCount] = (nullEscape[pushedBackCount] << 8) | ((unsigned char)*q);
                    }

                    // move to the next character
                    ++pushedBackCount;
                }
            }
            else
            {
                SCOPE_ASSERT(isp.nullEscape == nullptr);
            }
        }

        ~TextInputStreamTraitsConst()
        {
            delete [] nullEscape;
        }
    };

    // A wrapper to provide text stream interface on top of Cosmos input which provides block reading interface.
    // It hides the page from the reader.
    // This class is used in DefaultTextExtractor.
    // InputStreamType - cosmosInput, GZipInput
    template<typename TextInputStreamTraits, typename InputStreamType>
    class TextInputStream : public TextInputStreamTraits, public TextConversions
    {
        unsigned char * m_buffer;        // current reading buffer
        unsigned char * m_startPoint;    // current reading start point
        unsigned char * m_startRecord;   // start of the current record
        SIZE_T  m_numRemain;             // number of character remain in the buffer
        __int64 m_recordLineNumber;      // Current record number
        int m_fieldNumber;               // Current field in record being processed
        int m_delimiterCount;            // number of delimiter has seen in current line.
        int m_numColumnExpected;         // number of columns expected from the schema.
        int m_numFirstRowsSkipped;       // number of rows skipped from the beginning of the stream.

        UINT64        m_cbRead;              // how many bytes already read
        UINT64        m_cbEncoded;           // how many bytes already encoded into utf8
        UINT64        m_cbSplitLengthUTF8;   // byte count (in utf8 encoding) for split length, default is uint64_max, encoder will update this field during encoding
        bool          m_adjustCursor;        // for split, this flag indicate whether cursor is adjusted to right offset before reading first record
        bool          m_matchedCustomNull;   // to indicate the token matches custom null

        InputFileInfo         m_input;            // input file info
        IOManager::StreamData m_inputStreamData;  // stream data for concatenated input group stream
        
        // Represents a UTF-8 character.
        // Every ASCII character is encoded the same way in UTF-8,
        // so all the C chars will work properly when used as UTF-8 chars.
        // Special values are all invalid UTF-8 sequences to avoid colliding with valid characters
        enum class Token: UINT64
        {
            TOKEN_NONE = -2,
            TOKEN_EOF = -1, 
            TOKEN_NEWLINE = '\n',  
            TOKEN_LINEFEED = '\r',  
            TOKEN_HASH = '#',
            TOKEN_QUOTE = '"',
        };

        Token m_pushBack;            // Last token pushed back (or Token::TOKEN_NONE if none)

        unique_ptr<InputStreamType>         m_asyncInput; // blocked cosmos input stream 
        IncrementalAllocator               *m_allocator;  // memory allocator pointer for deserialization
        TextEncodingReader<InputStreamType> m_codecvt;    // encoding converter instance

        __int64 m_delayBadConvertLine;           // Line in input at which we failed to convert column
        int m_delayBadConvertColumnIdx;          // Column idx in line at which we failed to convert column
        string m_delayBadConvertText;            // detail of delayed bad convert exception 
        bool m_delayBadConvertException;         // True if we have BadConvert exception stored.
        ConvertResult m_delayBadConvertReason;   // Reason of bad convertion 
        string m_delayBadConvertColumn;          // Column string literal which caused conversion error
        string m_delayBadConvertColumnHex;       // Column HEX string literal which caused conversion error

        enum TokenizerState
        {
            ReadOneToken = 0,
            NewLine = 1,
            EndOfFile = 2
        };
        
        // Refill the input buffer.
        bool Refill()
        {
            SIZE_T nRead=0;
            bool stopped = false;
            char * newBuf = m_codecvt.GetNextPage(nRead, stopped);
            if(newBuf == NULL || nRead==0)
            {
                return false;
            }

            m_buffer = (unsigned char *)newBuf;
            m_startPoint = m_buffer;
            m_startRecord = m_buffer;
            m_numRemain = nRead;
            m_cbEncoded += nRead;
            m_cbSplitLengthUTF8 = stopped ? m_cbEncoded : m_cbSplitLengthUTF8;
            return true;
        }

        // Get next charater from the input stream (or Token::TOKEN_EOF for end of file)
        // If we've pushed back a token - get it back, otherwise get next
        Token GetToken()
        {
            Token token = Token::TOKEN_NONE;
            if (m_pushBack != Token::TOKEN_NONE)
            {
                token = m_pushBack;
                m_pushBack = Token::TOKEN_NONE;
            }
            else
            {
                token = GetNextToken();
            }

            return token;
        }

        // Get next byte from the input stream.
        // Accessing empty buffer is undefined behavior.
        // !!! DO NOT REMOVE "FORCE_INLINE" as it will cause performance regression
        FORCE_INLINE unsigned char GetNextByte()
        {
            // return the next byte
            // increase read byte count
            m_numRemain--;
            m_cbRead++;
            
            return *m_startPoint++;
        }

        // Get next count bytes from the input stream
        // Accessing not large enough buffer is undefined behavior.
        // Returns a UINT64 value filled with bytes read aligned to the right
        // Should not be used to read more than 8 bytes
        FORCE_INLINE UINT64 GetNextBytes(SIZE_T count)
        {
            SCOPE_ASSERT(count <= 8);
            UINT64 tokenValue = 0;
            int c;
            while (count != 0)
            {
                c = GetNextByte();

                tokenValue = (tokenValue << 8) | c;

                --count;
            }

            return tokenValue;
        }

        // Get next token from the input stream (or Token::TOKEN_EOF for end of file)
        // - this is an optimization of GetToken above where we know we haven't pushed a token since the last call
        // If the stream ends before the character then the part of the character read is returned
        // Should work properly with up to 8 byte characters. Remove the assert in UTF-8 byte count to make it work
        FORCE_INLINE Token GetNextToken()
        {
            // If the buffer is empty - get a new one
            if (m_numRemain == 0)
            {
                if (!Refill())
                    return Token::TOKEN_EOF;
            }
            
            // Read the first byte
            UINT64 tokenValue = GetNextByte();

            // For ASCII/Default encoding only return one byte if validateEncoding == false,
            // to be compatible with first party behavior.
            if ((encoding == TextEncoding::ASCII || encoding == TextEncoding::Default) && !validateEncoding)
            {
                return (Token)tokenValue;
            }

            // Nearly always we are dealing with single byte characters
            // - optimize for this case
            // - all UTF continuation bytes have the top bit set (see https://en.wikipedia.org/wiki/UTF-8)
            if ((tokenValue & 0x80) == 0)
            {
                return (Token)tokenValue;
            }

            // Number of bytes left to read
            // We have already read one
            SIZE_T count = ScopeEngine::UTF8ByteCount((unsigned char)tokenValue) - 1;

            // previous buffer size
            SIZE_T buffSize;
            // The buffer is smaller than the number of bytes to be read
            while (m_numRemain < count)
            {
                // Read the remaining bytes from the buffer. Append them to tokenValue
                count -= m_numRemain;
                buffSize = m_numRemain;
                tokenValue = (tokenValue << (buffSize << 3)) | GetNextBytes(m_numRemain);

                // Try to refill buffer
                // return token read if refill failed
                if (!Refill())
                    return (Token)tokenValue;
            }

            tokenValue = (tokenValue << (count << 3)) | GetNextBytes(count);

            return (Token)tokenValue;
        }

        void PushToken(Token token)
        {
            SCOPE_ASSERT(m_pushBack == Token::TOKEN_NONE);
            m_pushBack = token;
        }

        // Returns token's 'escaped' value
        FORCE_INLINE Token UnEscapeToken(Token token)
        {
            SCOPE_ASSERT(escapeChar != 0);

            switch((UINT64)token)
            {
            case 'r':
                return (Token)'\r';
            case 'n':
                return (Token)'\n';
            // Uncomment these lines to allow interpreting '\t' as a tab character
            // case 't':
            //     return (Token)'\t';
            default:
                return token;
            }            
        }

        FORCE_INLINE bool IsRowDelimiter(Token token)
        {
            return (rowDelimiter == 0 && (token == Token::TOKEN_LINEFEED || token == Token::TOKEN_NEWLINE)) ||  //default row delimiter
                   (rowDelimiter != 0 && ((UINT64)token == rowDelimiter));  // customer row delimiter                    
        }
        
        // Read string with/without escape translation
        // "autoBuffer" can be NULL which means that data should be discarded
        FORCE_INLINE TokenizerState ReadStringInner(AutoExpandBuffer * autoBuffer)
        {
            SIZE_T nullMatchCount = 0;  // length of token string that matched null
            SIZE_T readTokensCount = 0; // the number of tokens read
            
            // If we've pushed back a token - get it back, otherwise get next
            Token token = GetToken();
            ++readTokensCount;
            
            bool hasData = false;
            bool prevEscape = false; // indicates if the last token read was escape character

            m_matchedCustomNull = false;
            bool customNullMatchFailure = false;
            bool betweenDoubleQuotes = false; // to indicate current token is inside double quotes.

            // Keep scanning until TOKEN_EOF
            while (token != Token::TOKEN_EOF)
            {
                if (haveNullEscape && !customNullMatchFailure)
                {
                    if (nullMatchCount < nullEscapeLength)
                    {
                        // match token to corresponding nullEscape character
                        if ((UINT64) token != nullEscape[nullMatchCount])
                        {
                            customNullMatchFailure = true;
                        }
                        else
                        {
                            ++nullMatchCount;
                        }
                    }
                }

                if (rowDelimiter == 0 && (token == Token::TOKEN_LINEFEED || token == Token::TOKEN_NEWLINE))
                {
                    // if we have data - return it and deal with the newline on the next call
                    if (hasData)
                    {
                        if (haveNullEscape && nullMatchCount == nullEscapeLength && readTokensCount == nullEscapeLength + 1)
                            m_matchedCustomNull = true;
                            
                        if (autoBuffer != nullptr) autoBuffer->Append('\0');
                        PushToken(token);
                        ++m_fieldNumber;
                        return ReadOneToken;
                    }

                    // 3 different forms of newline (from StreamReader.ReadLine .NET documentation)
                    // '\r' '\n'
                    // '\r'
                    // '\n'
                    //
                    if (token == Token::TOKEN_LINEFEED)
                    {
                        token = GetNextToken();
                        readTokensCount++;
                        if (token != Token::TOKEN_NEWLINE)
                        {
                            PushToken(token);
                        }
                    }
                    // increase line number and return newline
                    m_recordLineNumber++;
                    return NewLine;
                }
                else if (rowDelimiter != 0 && (UINT64)token == rowDelimiter)
                {
                    if (hasData)
                    {
                        if (haveNullEscape && nullMatchCount == nullEscapeLength && readTokensCount == nullEscapeLength + 1)
                            m_matchedCustomNull = true;
                            
                        if (autoBuffer != nullptr) autoBuffer->Append('\0');
                        PushToken(token);
                        ++m_fieldNumber;
                        return ReadOneToken;
                    }

                    // increase line number and return newline
                    m_recordLineNumber++;
                    return NewLine;
                }
                else if (prevEscape)
                {
                    // the previous character was the escape character
                    prevEscape = false;
                    if (autoBuffer != nullptr) autoBuffer->Append((UINT64)UnEscapeToken(token));
                    hasData = true;
                }
                else if (escapeChar != 0 && (UINT64) token == escapeChar)
                {
                    // the current character is escape character
                    // do not write it into the auto buffer
                    prevEscape = true;
                }
                else if (escape && token == Token::TOKEN_HASH && autoBuffer != nullptr)
                {
                    SIZE_T escapeStartPos = autoBuffer->Size();

                    autoBuffer->Append(x_hash);
                    hasData = true;
                    token = GetNextToken();
                    readTokensCount++;
                    
                    // Look for the end of the hash tag (or the end of the field)
                    // Default row delimiter
                    if (rowDelimiter == 0)
                    {
                        while (token != Token::TOKEN_HASH && token != Token::TOKEN_EOF && (UINT64) token != delimiter 
                            && token != Token::TOKEN_NEWLINE && token != Token::TOKEN_LINEFEED)
                        {
                            autoBuffer->Append((UINT64)token);
                            // Already set hasData when we saw opening '#'
                            token = GetNextToken();
                            readTokensCount++;
                        }
                    }
                    // User-defined row delimiter
                    else
                    {
                        while (token != Token::TOKEN_HASH && token != Token::TOKEN_EOF && (UINT64) token != delimiter
                            && (UINT64) token != rowDelimiter)
                        {
                            autoBuffer->Append((UINT64)token);
                            // Already set hasData when we saw opening '#'
                            token = GetNextToken();
                            readTokensCount++;
                        }
                    }
                    
                    if (token == Token::TOKEN_HASH)
                    {
                        // Found a potential hash tag - process it
                        autoBuffer->Append((UINT64)token);
                        // Already set hasData  when we saw opening '#'
                        ExpandHash(autoBuffer, escapeStartPos);
                    }
                    else
                    {
                        // Continue processing with token we ended on - aborted #tag is already in the buffer
                        continue;
                    }
                }
                // handle text qualifier
                else if (textQualifier && token == Token::TOKEN_QUOTE && autoBuffer != nullptr)
                {
                    if(!betweenDoubleQuotes)
                    {
                        if (readTokensCount == 1)
                        {
                            // only care about the most outter quotes
                            betweenDoubleQuotes = true;
                        }
                        else
                        {
                            autoBuffer->Append((UINT64)token);
                        }
                    }
                    else
                    {
                        int count = 0;                        
                        while (token == Token::TOKEN_QUOTE)
                        {
                            // handle paired-double-quotes inside "..."
                            // "Hello "" World"  -> Hello " World
                            count++;
                            if ((count & 0x1) == 0) autoBuffer->Append((UINT64)token);
                            token = GetNextToken();
                            readTokensCount++;
                        }

                        if ((count & 0x1) == 1)
                        {
                            betweenDoubleQuotes = false;
                        }

                        // If end quote mark passed, then delimter/row delimiter/EOF is expected
                        if (!betweenDoubleQuotes && 
                            (token != Token::TOKEN_EOF) && 
                            ((UINT64)token != delimiter) &&
                            !IsRowDelimiter(token))                      
                        {
                            throw ScopeStreamException(ScopeStreamException::InvalidCharacterAfterTextQualifier);
                        }
                        
                        continue;
                    }                    
                }
                // Check for delimiter
                else if ((UINT64)token == delimiter && !betweenDoubleQuotes)
                {
                    if (haveNullEscape && nullMatchCount == nullEscapeLength && readTokensCount == nullEscapeLength + 1)
                        m_matchedCustomNull = true;
                            
                    if (autoBuffer != nullptr) autoBuffer->Append('\0');
                    ++m_delimiterCount;
                    ++m_fieldNumber;
                    return ReadOneToken;
                }
                else
                {
                    // Otherwise add to string we are collecting
                    if (autoBuffer != nullptr) autoBuffer->Append((UINT64)token);
                    hasData = true;
                }

                token = GetNextToken();
                ++readTokensCount;
            }

            if (needCrossBoundary() && !IsStreamTail())
            {
                // EOF while looking for row delimiter
                throw ScopeStreamException(ScopeStreamException::StreamSplitUnexpectedEOF);
            }

            if (hasData)
            {
                if (haveNullEscape && nullMatchCount == nullEscapeLength && readTokensCount == nullEscapeLength + 1)
                    m_matchedCustomNull = true;
                            
                if (autoBuffer != nullptr) autoBuffer->Append('\0');
                PushToken(Token::TOKEN_EOF);
                ++m_fieldNumber;
                return ReadOneToken;
            }

            m_recordLineNumber++;            
            return EndOfFile;
        }


        // Expand a #hash# sequence replacing the expanded sequence into the autobuffer
        // #NULL# is not expanded but left alone, if a field contains nothing but #NULL# it will be turned into a null string
        // This is called with the potential # sequence at the end of the auto buffer and not null terminated
        void ExpandHash(AutoExpandBuffer * autoBuffer, SIZE_T escapeStartPos)
        {            
            char * escapeStringStart = &((*autoBuffer)[escapeStartPos]);

            // translate escape string
            switch(autoBuffer->Size() - escapeStartPos)
            {
            case 6:
                if (escapeStringStart[0] == x_hash &&
                    escapeStringStart[1] == 'N' &&
                    escapeStringStart[2] == 'U' &&
                    escapeStringStart[3] == 'L' &&
                    escapeStringStart[4] == 'L' &&
                    escapeStringStart[5] == x_hash )
                {
                    // we have a match for #NULL#, leave it for the caller to handle
                }
                else if (escapeStringStart[0] == x_hash &&
                    escapeStringStart[1] == 'H' &&
                    escapeStringStart[2] == 'A' &&
                    escapeStringStart[3] == 'S' &&
                    escapeStringStart[4] == 'H' &&
                    escapeStringStart[5] == x_hash )
                {
                    // we have a match for #HASH#, remove all but the first '#'
                    autoBuffer->RemoveEnd(5);
                }

                break;

            case 5:
                if (escapeStringStart[0] == x_hash &&
                    escapeStringStart[1] == 'T' &&
                    escapeStringStart[2] == 'A' &&
                    escapeStringStart[3] == 'B' &&
                    escapeStringStart[4] == x_hash)
                {
                    // we have a match for #TAB#
                    // (note this is bit misleading - #TAB# is a replacement for the current delimiter, even if it is not a tab)
                    autoBuffer->RemoveEnd(5);

                    autoBuffer->Append(delimiter);
                }
                break;
            case 3:
                if (escapeStringStart[0] == x_hash &&
                    escapeStringStart[1] == 'N' &&
                    escapeStringStart[2] == x_hash )
                {
                    // we have a match for #N#
                    escapeStringStart[0] = '\n';
                    autoBuffer->RemoveEnd(2);
                }
                else if (escapeStringStart[0] == x_hash &&
                    escapeStringStart[1] == 'R' &&
                    escapeStringStart[2] == x_hash )
                {
                    // we have a match for #R#

                    escapeStringStart[0] = '\r';
                    autoBuffer->RemoveEnd(2);
                }                
                break;
            }
        }

        // Read string with/without escape translation
        template <typename T>
        FORCE_INLINE TokenizerState ReadString(T & s)
        {
            AutoExpandBuffer autoBuffer(m_allocator);

            TokenizerState r = ReadStringInner(&autoBuffer);

            if (r == ReadOneToken)
            {
                bool matchedNull = false;

                if (escape && autoBuffer.Size() == 6 + 1 &&
                    autoBuffer[0] == x_hash &&
                    autoBuffer[1] == 'N' &&
                    autoBuffer[2] == 'U' &&
                    autoBuffer[3] == 'L' &&
                    autoBuffer[4] == 'L' &&
                    autoBuffer[5] == x_hash)
                {
                    matchedNull = true;
                }
                else if (haveNullEscape && m_matchedCustomNull)
                {
                    matchedNull = true;
                }
                
                if (matchedNull)
                {
                    s.SetNull();
                }
                else
                {
                    // take buffer and make FString of it
                    s.MoveFrom(autoBuffer);
                }
            }

            return r;
        }

        // For split, adjust cursor to right start offset        
        void AdjustCursor()
        {
            m_adjustCursor = true;
            
            // Do not skip rows in case of block aligned streams or first split in the stream.
            if (m_input.blockAligned || IsStreamHead())
            {
                return;
            }

            // otherwise, advance cursor to pass the first delimitor (\r, \n) to skip the first uncompleted record.
            // previous split should be responsible for the record crossing its split end point.
            Token token;

            // Default row delimiter
            if (rowDelimiter == 0)
            {
                do
                {
                    token = GetToken();
                }
                while (token != Token::TOKEN_LINEFEED && token != Token::TOKEN_NEWLINE && token != Token::TOKEN_EOF);

                if (token == Token::TOKEN_LINEFEED)
                {
                    token = GetNextToken();
                }

                if (token == Token::TOKEN_NEWLINE)
                {
                    token = GetNextToken(); 
                }
            }
            else
            {
                do
                {
                    token = GetToken();
                }
                while ((UINT64)token != rowDelimiter && token != Token::TOKEN_EOF);
            }

            // Handle TOKEN_EOF
            if (token == Token::TOKEN_EOF)
            {
                m_startRecord = NULL;
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }
            
            PushToken(token);
        }

        // Does cursor pass split end point. Yes - current split read should stop
        INLINE bool PassSplitEndPoint() const
        {
            // PassSplitEndPoint is used to determine if we should start reading new row at current position.
            // We should start reading new row if current position is less than (we're inside current split) 
            // or equal to localEndOffset. (we need to read the first row from the next split as it will be 
            // skipped when processing next split)
            if (!m_input.blockAligned && cbConsumed() > m_cbSplitLengthUTF8)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        // True if current split (channel) represents a head (first part) of the actual stream.
        INLINE bool IsStreamHead() const
        {
            return m_input.globalStartOffset == 0 && m_input.localStartOffset == 0;
        }

        // True if current split (channel) represents a tail (last part) of the actual stream.
        INLINE bool IsStreamTail() const
        {
            // No support for tail detection in block aligned streams.
            SCOPE_ASSERT(!m_input.blockAligned);

            // For non-tail splits, length should be greater than local end offset by at least max record size.
            return m_input.localEndOffset == m_input.length;
        }
        
    public:
        // constructor 
        TextInputStream(const InputFileInfo & input, IncrementalAllocator * allocator, SIZE_T bufSize, int bufCount, const InputStreamParameters & inputStreamPars) :
            TextInputStreamTraits(inputStreamPars),
            m_numRemain(0),
            m_buffer(NULL),
            m_cbRead(0),
            m_cbEncoded(0),
            m_cbSplitLengthUTF8(UINT64_MAX),
            m_adjustCursor(false),
            m_input(input),
            m_allocator(allocator),
            m_startPoint(NULL),
            m_startRecord(NULL),
            m_recordLineNumber(1),
            m_fieldNumber(0),
            m_delimiterCount(0),
            m_codecvt(encoding, validateEncoding),
            m_delayBadConvertException(false),
            m_numColumnExpected(0),
            m_numFirstRowsSkipped(0),
            m_pushBack(Token::TOKEN_NONE)
        {
            if (input.HasGroupId())
            {
                // In case of stream group get concatenated stream data from IOManager
                m_inputStreamData = IOManager::GetGlobal()->GetInputGroupConcatStream(input.groupId);
                m_input = InputFileInfo(m_inputStreamData.m_channel);
                m_asyncInput = make_unique<InputStreamType>(m_inputStreamData.m_device.get(), bufSize, bufCount);
            }
            else
            {
                m_asyncInput = make_unique<InputStreamType>(input.inputFileName, bufSize, bufCount);
            }
        }

        // Init 
        void Init()
        {
            if (m_input.blockAligned)
            {
                //blockAligned == true means reading from channel start to EOF.
                //ParseVertexCommmand method in parser.cpp already checked values on blockAligned == true,
                //so only check localStartOffset is zero, we do not care about other property e.g. length, localEndOffset.
                SCOPE_ASSERT(m_input.localStartOffset == 0);
            }
            else
            {
                SCOPE_ASSERT(m_input.localStartOffset <= m_input.localEndOffset);
                SCOPE_ASSERT(m_input.localEndOffset <= m_input.length);

                // JM and runtime contract - JM should slice stream at 4-bytes
                // boundary for extractor handling UTF32 encoding correctly.
                // Split start offset must be multiple of 4.
                // 
                // Above contract should be assert at the first place, e.g. in parser.cpp in scopehost, not here
                // The local run which does not include JM component will pass in arbitrary start offset
                // for non-UTF32 encoding input file.
            }

            m_asyncInput->Init();
            
            if (!m_asyncInput->IsCompressed())
            {
                m_asyncInput->Seek(m_input.localStartOffset);
            }
            else
            {
                SCOPE_ASSERT(m_input.localStartOffset == 0);
            }

            auto stop = m_input.blockAligned ? UINT64_MAX : m_input.localEndOffset - m_input.localStartOffset;
            m_codecvt.Init(m_asyncInput.get(), stop);
        }

        // Rewind the input stream
        void ReWind()
        {
            //Start to read from beginning again.
            m_codecvt.Restart(m_input.localStartOffset);
            Refill();

            m_recordLineNumber = 1;
            m_fieldNumber = 0;
            m_delimiterCount = 0;
            m_delayBadConvertException = false;
            m_numColumnExpected = 0;
            m_numFirstRowsSkipped = 0;
            m_pushBack = Token::TOKEN_NONE;
        }

        // Close stream
        void Close()
        {
            m_codecvt.Close();
            m_asyncInput->Close();

            m_buffer = NULL;
            m_startPoint = NULL;
            m_startRecord = NULL;
            m_numRemain = 0;
            m_pushBack = Token::TOKEN_NONE;
        }

        int StreamId() const
        {
            // Stream Id is required by fast streamset feature, where compiler will guarantee streamid created in algebra, Then JM will pick up stream id and pass to vertex.
            SCOPE_ASSERT(m_input.streamId != -1);
            return m_input.streamId;
        }

        void StoreBadConvertException(const ConvertResult& reason, const FStringWithNull& column)
        {
            // we only store first BadConvertException per row.
            if (!m_delayBadConvertException)
            {
                stringstream out;
                Dump(out);

                m_delayBadConvertException = true;
                m_delayBadConvertLine = CurrentLineNumber();
                m_delayBadConvertColumnIdx = CurrentField() - 1;
                m_delayBadConvertText = out.str();
                m_delayBadConvertReason = reason;

                stringstream col;
                col << column;
                m_delayBadConvertColumn = col.str();

                stringstream hexCol;
                for (char& c : m_delayBadConvertColumn) 
                {
                    ToHexStr(hexCol, c);

                }

                m_delayBadConvertColumnHex = hexCol.str();
            }
        }

        // deserialize a new line
        void EndRow(bool readNewLine)
        {
            if (readNewLine)
            {
                FStringWithNull tmp;

                TokenizerState r = ReadString(tmp);
                // we will ignore endoffile(last line does not contains newline) or newline.
                if (r != EndOfFile && r != NewLine)
                {
                    throw ScopeStreamException(ScopeStreamException::NewLineExpected);
                }
            }

            // If column does not match schema, throw exception NewLine
            if (m_delimiterCount+1 != m_numColumnExpected)
            {
                throw ScopeStreamException(ScopeStreamException::NewLine);
            }

            // If there is BadConvertException delayed, throw them now.
            if (m_delayBadConvertException)
            {
                throw ScopeStreamException(ScopeStreamException::BadConvert);
            }
        }

        // Count for bytes which are really consumed. This is different from m_cbRead (byte count read from underneath stream)
        // because of m_pushback (a.k.a. token buffer).
        INLINE UINT64 cbConsumed() const
        {
            if (m_pushBack == Token::TOKEN_NONE || m_pushBack == Token::TOKEN_EOF)
            {
                return m_cbRead;
            }
            else
            {
                // If m_pushBack != Token::TOKEN_NONE, means there is one token buffered in m_pushBack.
                // Because all token read from underneath stream will increase m_cbRead by 1, in 
                // this case m_cbRead >= 1, no underflow for (m_cbRead - 1)
                return m_cbRead - 1;
            }
        }        

        // Get current line number for error messages
        __int64 CurrentLineNumber()
        {
            return m_recordLineNumber;
        }

        // Get current field number for error messages
        int CurrentField()
        {
            return m_fieldNumber;
        }

        // Get number of column expected
        int ColumnExpected()
        {
            return m_numColumnExpected;
        }

        // Get current number of delimiters for error messages
        int CurrentDelimiter()
        {
            return m_delimiterCount;
        }

        // Return the filename associated with this stream
        std::string StreamName() const
        {
            return m_asyncInput->StreamName();
        }

        int NumFirstRowsSkipped() const
        {
            return m_numFirstRowsSkipped;
        }

        void ThrowBadConvertException(const char * columnNames[], int size)
        {
            auto colIndex = m_delayBadConvertColumnIdx;
            SCOPE_ASSERT(colIndex < size);
            char* columnName = columnNames == nullptr ? "" : columnNames[colIndex];
            
            switch(m_delayBadConvertReason)
            {
            case ConvertErrorOutOfRange:
                throw ScopeStreamExceptionWithEvidence(E_EXTRACT_COLUMN_CONVERSION_OUTOFRANGE_ERROR, 
                                                       {m_delayBadConvertColumnHex, m_delayBadConvertLine, colIndex, columnName}, 
                                                       m_delayBadConvertText);
            case ConvertErrorInvalidCharacter:
                throw ScopeStreamExceptionWithEvidence(E_EXTRACT_COLUMN_CONVERSION_INVALID_ERROR, 
                                                       { m_delayBadConvertColumnHex, m_delayBadConvertLine, colIndex, columnName},
                                                       m_delayBadConvertText); 

            case ConvertErrorEmpty:
                                throw ScopeStreamExceptionWithEvidence(E_EXTRACT_COLUMN_CONVERSION_EMPTY_ERROR, 
                                                       {m_delayBadConvertLine, colIndex, columnName}, 
                                                       m_delayBadConvertText);

            case ConvertErrorNull:
                throw ScopeStreamExceptionWithEvidence(E_EXTRACT_COLUMN_CONVERSION_NULL_ERROR, 
                                                       {m_delayBadConvertLine, colIndex, columnName}, 
                                                       m_delayBadConvertText);                                                       

            case ConvertErrorInvalidLength:
                throw ScopeStreamExceptionWithEvidence(E_EXTRACT_COLUMN_CONVERSION_INVALID_LENGTH, 
                                                       {m_delayBadConvertLine, colIndex, columnName}, 
                                                       m_delayBadConvertText);                                                       

            case ConvertErrorTooLong:
                throw ScopeStreamExceptionWithEvidence(E_EXTRACT_COLUMN_CONVERSION_TOO_LONG, 
                                                       {m_delayBadConvertLine, colIndex, columnName}, 
                                                       m_delayBadConvertText);                                                       

            case ConvertErrorUndefined:
                throw ScopeStreamExceptionWithEvidence(E_EXTRACT_COLUMN_CONVERSION_UNDEFINED_ERROR, 
                                                       {m_delayBadConvertColumnHex, m_delayBadConvertLine, colIndex, columnName}, 
                                                       m_delayBadConvertText); 

            case ConvertErrorStringToSqlDatetime2:
                throw ScopeStreamExceptionWithEvidence(E_EXTRACT_COLUMN_CONVERSION_TEXT_TO_SQLDATETIME2,
                                                       { m_delayBadConvertColumnHex, m_delayBadConvertLine, colIndex, columnName},
                                                       m_delayBadConvertText);

            default:
                SCOPE_ASSERT(false);
                return;
            }
        }

        // Convert a char to HEX string and append to out stream
        void ToHexStr(std::stringstream &out, unsigned char c) const
        {
            BYTE low = c & 0x0f;
            BYTE high = (c & 0xf0) >> 4;

            out << ToHexChar(high);
            out << ToHexChar(low);
        }

        // Convert a UINT64 to HEX string and append to out stream
        void UINT64ToHexStr(std::stringstream &out, UINT64 c) const
        {
            if (c == 0)
            {
                out << "0";
                return;
            }

            std::string res;
            while (c != 0)
            {
                res.insert(0, 1, ToHexChar(c & 0x0f));
                c >>= 4;
            }

            out << res;
        }

        // Convert a byte to HEX value
        unsigned char ToHexChar(BYTE value) const
        {
            if (0 <= value && value <= 9)
            {
                return (unsigned char)(value + '0');
            }
            else
            {
                return (unsigned char)(value - 10 + 'A');
            }
        }

        // Dump current state of stream for error messages
        void Dump(std::stringstream &out) const
        {
            // Try and display on a single line enough of the input stream to give context
            // - we'll assume an old fashioned 80 column screen width
            // - show upto 60 characters before the current position and 10 after
            // - this leaves 10 for unprintable expansion
            unsigned char *start = (m_startPoint - 60) < m_startRecord ? m_startRecord : (m_startPoint - 60);
            unsigned char *end = (m_numRemain < 10) ? m_startPoint + m_numRemain : m_startPoint + 10;
            int dataOutput = 0;
            int markerPos = 0;
            const char* delimiterStr = "Delimiter{HEX}:";
            const char* hexStr = "HEX:";
            const char* txtStr = "TEXT:";

            // between [start, end] it's possible there are multiple row, find the last row which is the one has problem
            vector<unsigned char*> row;
            bool newLineStart = false;
            for(unsigned char *pos = start; pos < end; ++pos)
            {                
                if (*pos == x_r || *pos == x_n)
                {
                    newLineStart = true;
                }
                else
                {
                    newLineStart = false;
                }

                // m_startPoint can be the byte just right after newline.
                // we are only intrested in the row starting before m_startPoint. 
                if (newLineStart && pos < m_startPoint - 2) row.push_back(pos);
            }

            // adjust start to single line
            start = row.empty() ? start : row.back();

            out << delimiterStr;
            UINT64ToHexStr(out, delimiter);
            out << "\n";

            out << hexStr;

            bool first = true;
            for (unsigned char *pos = start; pos < end; ++pos)
            {
                if (*pos == x_r || *pos == x_n)
                {
                    if (first)
                    {
                        first = false;
                        continue;
                    }
                    else
                    {
                        break;
                    }
                }

                if (pos == m_startPoint)
                {
                    markerPos = dataOutput;
                }

                ToHexStr(out, *pos);
                dataOutput += 2;
            }
            out << "\n";

            // Display a "marker" indicating where parsing stopped
            for (int c = 0; c < (markerPos ? markerPos : dataOutput) + strlen(hexStr); ++c)
                out << " ";
            out << "^\n";

            out << txtStr;

            dataOutput = 0;
            markerPos = 0;
            first = true;
            for(unsigned char *pos = start; pos < end; ++pos)
            {
                if (pos == m_startPoint)
                {
                    markerPos = dataOutput;
                }

                if (isprint(*pos))
                {
                    out << *pos;
                    ++dataOutput;
                }
                else if (*pos == x_r || *pos == x_n)
                {
                    if (first)
                    {
                        first = false;
                        continue; 
                    }
                    else
                    {
                        break;
                    }                    
                }
                else if (*pos == '\t')
                {
                    out << "\\t";
                    dataOutput += 2;
                }
                else
                {
                    out << ".";
                    ++dataOutput;
                }
            }
            out << "\n";

            // Display a "marker" indicating where parsing stopped
            for (int c = 0; c < (markerPos ? markerPos : dataOutput) + strlen(txtStr); ++c)
                out << " ";
            out << "^\n";
        }

        template<typename T>
        void DoRead(T & s)
        {
            if (str.IsEmpty())
            {
                // By default T can not be null or empty string.
                StoreBadConvertException(ConvertErrorEmpty, str);
            }
            else if (str.IsNull())
            {
                StoreBadConvertException(ConvertErrorNull, str);            
            }
            else
            {
                // convert to corresponding type
                FStringToT(str, s);
            }

            if (m_fieldNumber == m_numColumnExpected)
            {
                EndRow(true);
            }
        }

#ifdef PLUGIN_TYPE_SYSTEM
        template<typename T>
        void DoFStringToT(FStringWithNull & str, T & out, bool lastEmptyColumn)
        {
            // Raise a different exception so that we can handle last column is empty string case.
            // T can not accept empty string case by default, raise BadConvert error. 
            if (lastEmptyColumn && !silent)
            {
                // All column count mismatch case should already trigger exception at this point. 
                StoreBadConvertException(ConvertErrorNull, str);
                throw ScopeStreamException(ScopeStreamException::BadConvert);
            }

            if (str.IsEmpty())
            {
                // By default T can not be null or empty string.
                StoreBadConvertException(ConvertErrorEmpty, str);
            }
            else if (str.IsNull())
            {
                StoreBadConvertException(ConvertErrorNull, str);            
            }
            else
            {
                if (m_fieldNumber == m_numColumnExpected)
                {
                    EndRow(true);
                }
                auto result = TryFStringToT(str, out);
                if (result != ConvertSuccess)
                {
                    StoreBadConvertException(result, str);
                }
            }
        }

        template<typename T>
        void FStringToT(FStringWithNull & str, T & out, bool lastEmptyColumn)
        {
            out.Deserialize<TextInputStreamTraits, InputStreamType>(this, str, lastEmptyColumn);
        }

        INLINE void FStringToT(FStringWithNull & str, bool & out, bool lastEmptyColumn)
        {
            DoFStringToT(str, out, lastEmptyColumn);
        }

        INLINE void FStringToT(FStringWithNull & str, unsigned char & out, bool lastEmptyColumn)
        {
            DoFStringToT(str, out, lastEmptyColumn);
        }

        INLINE void FStringToT(FStringWithNull & str, char & out, bool lastEmptyColumn)
        {
            DoFStringToT(str, out, lastEmptyColumn);
        }

        INLINE void FStringToT(FStringWithNull & str, short & out, bool lastEmptyColumn)
        {
            DoFStringToT(str, out, lastEmptyColumn);
        }

        INLINE void FStringToT(FStringWithNull & str, unsigned short & out, bool lastEmptyColumn)
        {
            DoFStringToT(str, out, lastEmptyColumn);
        }

        INLINE void FStringToT(FStringWithNull & str, wchar_t & out, bool lastEmptyColumn)
        {
            DoFStringToT(str, out, lastEmptyColumn);
        }

        INLINE void FStringToT(FStringWithNull & str, int & out, bool lastEmptyColumn)
        {
            DoFStringToT(str, out, lastEmptyColumn);
        }

        INLINE void FStringToT(FStringWithNull & str, unsigned int & out, bool lastEmptyColumn)
        {
            DoFStringToT(str, out, lastEmptyColumn);
        }

        INLINE void FStringToT(FStringWithNull & str, __int64 & out, bool lastEmptyColumn)
        {
            DoFStringToT(str, out, lastEmptyColumn);
        }

        INLINE void FStringToT(FStringWithNull & str, unsigned __int64 & out, bool lastEmptyColumn)
        {
            DoFStringToT(str, out, lastEmptyColumn);
        }

        INLINE void FStringToT(FStringWithNull & str, float & out, bool lastEmptyColumn)
        {
            DoFStringToT(str, out, lastEmptyColumn);
        }

        INLINE void FStringToT(FStringWithNull & str, double & out, bool lastEmptyColumn)
        {
            DoFStringToT(str, out, lastEmptyColumn);
        }

        template<typename T>
        INLINE void Read(T & t)
        {
            FStringWithNull str;
            bool lastEmptyColumn = false;

            TokenizerState r = ReadString(str);
            if (r == EndOfFile || r == NewLine)
            {
                if (m_delimiterCount + 1 == m_numColumnExpected)
                {
                    lastEmptyColumn = true;
                }
                else
                {
                    // One column is expected to read, but got TOKEN_EOF. 
                    // Only raise EndOfFile exception when silent flag is true,
                    // because caller of Read method (in code-gened TextExtractPolicy) will stop
                    // extracting row when catching EndOfFile exception, and no re-throw.
                    if (r == EndOfFile && silent)
                    {
                        throw ScopeStreamException(ScopeStreamException::EndOfFile);
                    }
                    else if (r == EndOfFile && !silent && needCrossBoundary())
                    {
                        throw ScopeStreamException(ScopeStreamException::StreamSplitUnexpectedEOF);                        
                    }
                    else
                    {
                        throw ScopeStreamException(ScopeStreamException::NewLine);
                    }
                }
            }

            FStringToT(str, t, lastEmptyColumn);
        }

#else // PLUGIN_TYPE_SYSTEM
        template<typename T>
        void Read(T & s)
        {
            FStringWithNull str;

            TokenizerState r = ReadString(str);
            if (r == EndOfFile || r == NewLine)
            {
                // Raise a different exception so that we can handle last column is empty string case.
                // T can not accept empty string case by default, raise BadConvert error. 
                if ((m_delimiterCount + 1 == m_numColumnExpected) && !silent)
                {
                    // Need to throw instead of delay the exception. EndRow will not be called in this case. 
                    // All column count mismatch case should already trigger exception at this point. 
                    StoreBadConvertException(ConvertErrorNull, str);
                    throw ScopeStreamException(ScopeStreamException::BadConvert);
                }
                else
                {
                    // One column is expected to read, but got TOKEN_EOF. 
                    // Only raise EndOfFile exception when silent flag is true,
                    // because caller of Read method (in code-gened TextExtractPolicy) will stop
                    // extracting row when catching EndOfFile exception, and no re-throw.
                    if (r == EndOfFile && silent)
                    {
                        throw ScopeStreamException(ScopeStreamException::EndOfFile);
                    }
                    else if (r == EndOfFile && !silent && needCrossBoundary())
                    {
                        throw ScopeStreamException(ScopeStreamException::StreamSplitUnexpectedEOF);                        
                    }
                    else
                    {
                        throw ScopeStreamException(ScopeStreamException::NewLine);
                    }
                }
            }

            if (str.IsEmpty())
            {
                // By default T can not be null or empty string.
                StoreBadConvertException(ConvertErrorEmpty, str);
            }
            else if (str.IsNull())
            {
                StoreBadConvertException(ConvertErrorNull, str);            
            }
            else
            {
                // convert to corresponding type
                FStringToT(str, s);
            }

            if (m_fieldNumber == m_numColumnExpected)
            {
                EndRow(true);
            }
        }

        // deserialize a string from text
        template<>
        void Read<FString>(FString & str)
        {
            TokenizerState r = ReadString(str);
            if (r == EndOfFile || r == NewLine)
            {
                // Raise a different exception so that we can handle last column is empty string case.
                if (m_delimiterCount + 1 == m_numColumnExpected)
                {
                    // create an empty string
                    str.SetEmpty();

                    EndRow(false);
                    return;
                }
                else
                {
                    // One column is expected to read, but got TOKEN_EOF. 
                    // Only raise EndOfFile exception when silent flag is true,
                    // because caller of Read method (in code-gened TextExtractPolicy) will stop
                    // extracting row when catching EndOfFile exception, and no re-throw.
                    if (r == EndOfFile && silent)
                    {
                        throw ScopeStreamException(ScopeStreamException::EndOfFile);
                    }
                    else if (r == EndOfFile && !silent && needCrossBoundary())
                    {
                        throw ScopeStreamException(ScopeStreamException::StreamSplitUnexpectedEOF);                        
                    }                    
                    else
                    {
                        throw ScopeStreamException(ScopeStreamException::NewLine);
                    }
                }
            }

            if (m_fieldNumber == m_numColumnExpected)
            {
                EndRow(true);
            }
        }

        // deserialize a binary string from text
        template<>
        void Read<FBinary>(FBinary & bin)
        {
            FStringWithNull str;

            TokenizerState r = ReadString(str);
            if (r == EndOfFile || r == NewLine)
            {
                // Raise a different exception so that we can handle last column is empty binary string case.
                if (m_delimiterCount + 1 == m_numColumnExpected)
                {
                    // create an empty byte array
                    bin.SetEmpty();

                    EndRow(false);
                    return;
                }
                else
                {
                    // One column is expected to read, but got TOKEN_EOF. 
                    // Only raise EndOfFile exception when silent flag is true,
                    // because caller of Read method (in code-gened TextExtractPolicy) will stop
                    // extracting row when catching EndOfFile exception, and no re-throw.
                    if (r == EndOfFile && silent)
                    {
                        throw ScopeStreamException(ScopeStreamException::EndOfFile);
                    }
                    else if (r == EndOfFile && !silent && needCrossBoundary())
                    {
                        throw ScopeStreamException(ScopeStreamException::StreamSplitUnexpectedEOF);                        
                    }                    
                    else
                    {
                        throw ScopeStreamException(ScopeStreamException::NewLine);
                    }
                }
            }

            // Convert FString to FBinary
            FStringToT(str, bin);

            if (m_fieldNumber == m_numColumnExpected)
            {
                EndRow(true);
            }
        }

        // deserialize a scope supported nullable type
        template<typename T>
        void Read(NativeNullable<T> & s)
        {
            FStringWithNull tmp;

            TokenizerState r = ReadString(tmp);
            if (r == EndOfFile || r == NewLine)
            {
                // Raise a different exception so that we can handle last column is null case
                // Empty string is treated as null for NativeNullable<T> type.
                if (m_delimiterCount + 1 == m_numColumnExpected)
                {
                    s.SetNull();
                    EndRow(false);
                    return;
                }
                else
                {
                    // One column is expected to read, but got TOKEN_EOF. 
                    // Only raise EndOfFile exception when silent flag is true,
                    // because caller of Read method (in code-gened TextExtractPolicy) will stop
                    // extracting row when catching EndOfFile exception, and no re-throw.
                    if (r == EndOfFile && silent)
                    {
                        throw ScopeStreamException(ScopeStreamException::EndOfFile);
                    }
                    else if (r == EndOfFile && !silent && needCrossBoundary())
                    {
                        throw ScopeStreamException(ScopeStreamException::StreamSplitUnexpectedEOF);                        
                    }                    
                    else
                    {
                        throw ScopeStreamException(ScopeStreamException::NewLine);
                    }
                }
            }

            auto result = TryFStringToT(tmp, s);
            if (result != ConvertSuccess)
            {
                if (silent)
                {
                    s.SetNull();
                }
                else
                {
                    StoreBadConvertException(result, tmp);
                }
            }

            if (m_fieldNumber == m_numColumnExpected)
            {
                EndRow(true);
            }
        }

        // deserialize a scope supported sql nullable type
        template<typename T>
        void Read(ScopeSqlType::SqlNativeNullable<T> & s)
        {
            Read((NativeNullable<T> &)s);
        }

        template<typename T>
        void FStringToT(FStringWithNull & str, T & out)
        {
            auto result = TryFStringToT(str, out);
            if (result != ConvertSuccess)
            {
                StoreBadConvertException(result, str);
            }
        }

        // Two special cases for FString and FBinary which are nullable types but not handled by NativeNullable
        template<>
        void FStringToT<FString>(FStringWithNull & str, FString & out)
        {
            auto result = TryFStringToT(str, out);
            if (result != ConvertSuccess)
            {
                if (silent)
                {
                    out.SetNull();
                }
                else
                {
                    StoreBadConvertException(result, str);
                }
            }
        }

        template<>
        void FStringToT<FBinary>(FStringWithNull & str, FBinary & out)
        {
            auto result = TryFStringToT(str, out);
            if (result != ConvertSuccess)
            {
                if (silent)
                {
                    out.SetNull();
                }
                else
                {
                    StoreBadConvertException(result, str);
                }
            }
        }
#endif //PLUGIN_TYPE_SYSTEM

        // Skip all consecutive empty lines.
        // This function only called at the beginning of a row deserialization of the DefaultTextExtractor.
        void StartRow(int numColumnExpected)
        {
            // if have not adjusted curor to right offset, just do it once.
            if (!m_adjustCursor)
            {
                AdjustCursor();

                // We can only skip rows from the first segment of the stream.
                // Don't skip anything from am empty stream, we don't want 
                // to throw SkipFirstRowsUnexpectedEOF on empty stream.
                // In first party, if blockAligned == true, the input channel length can be 0
                if (IsStreamHead() && (m_input.blockAligned || m_input.length > 0))
                {
                    SCOPE_ASSERT(!PassSplitEndPoint());

                    while (m_numFirstRowsSkipped < skipFirstRowsCount)
                    {
                        // End of split is considered as EOF while skipping first rows,
                        // note that order of expressions in the condition is important.
                        if ((!SkipLine() || PassSplitEndPoint()) && m_input.length > 0)
                        {
                            // EOF is reached while skipping the rows, which means stream header doesn't fit
                            // in the first segment of the stream. We cannot proceed as this will require skipping
                            // rows from the rest of stream segments and this is not supported by design.
                            throw ScopeStreamException(ScopeStreamException::SkipFirstRowsUnexpectedEOF);
                        }

                        ++m_numFirstRowsSkipped;
                    }
                }
            }

            if (PassSplitEndPoint())
            {
                m_startRecord = NULL;
                throw ScopeStreamException(ScopeStreamException::PassSplitEndPoint);
            }

            // Reset allocator for each new Row.
            // This is necessary for default text extractor since the deseralize method will keep looping to skip
            // invalid rows.
            m_allocator->Reset();
            m_delayBadConvertException = false;
            m_fieldNumber = 0;
            m_delimiterCount = 0;
            m_numColumnExpected = numColumnExpected;

            // If we've pushed back a token - get it back, otherwise get next
            Token token = GetToken();

            // Default row delimiter
            if (rowDelimiter == 0)
            {
                while (token == Token::TOKEN_LINEFEED || token == Token::TOKEN_NEWLINE)
                {
                    if (token == Token::TOKEN_LINEFEED)
                    {
                        token = GetNextToken();
                        // If we see a linefeed by itself treat it as a newline
                        // otherwise we'll account for it below
                        if (token != Token::TOKEN_NEWLINE)
                            m_recordLineNumber++;
                    }
                    else
                    {
                        //increase line number
                        m_recordLineNumber++;
                        token = GetNextToken();
                    }
                }
            }
            // User-defined row delimiter
            else
            {
                while ((UINT64) token == rowDelimiter)
                {
                    m_recordLineNumber++;
                    token = GetNextToken();
                }
            }

            // Handle TOKEN_EOF
            if (token == Token::TOKEN_EOF)
            {
                m_startRecord = NULL;
                throw ScopeStreamException(ScopeStreamException::EndOfFile);
            }

            PushToken(token);
        }

        // Skip next column
        void SkipColumn()
        {
            TokenizerState r = ReadStringInner(nullptr);
            if (r == EndOfFile || r == NewLine)
            {
                // Raise a different exception so that we can handle last column is empty string case.
                if (m_delimiterCount + 1 == m_numColumnExpected)
                {
                    EndRow(false);
                    return;
                }
                else
                {
                    if (r == EndOfFile)
                    {
                        throw ScopeStreamException(ScopeStreamException::EndOfFile);
                    }
                    else
                    {
                        throw ScopeStreamException(ScopeStreamException::NewLine);
                    }
                }
            }

            if (m_fieldNumber == m_numColumnExpected)
            {
                EndRow(true);
            }
        }

        // Skip the current line, returns false if we got EOF trying to skip a line
        bool SkipLine()
        {
            // If we've pushed back a token - get it back, otherwise get next
            Token token = GetToken();

            // Default row delimiter
            if (rowDelimiter == 0)
            {
                // Read until we see end of line or TOKEN_EOF
                while (token != Token::TOKEN_NEWLINE && token != Token::TOKEN_LINEFEED && token != Token::TOKEN_EOF)
                {
                    token = GetNextToken();
                }

                // Handle CR LF
                if (token == Token::TOKEN_LINEFEED)
                {
                    token = GetNextToken();
                    if (token != Token::TOKEN_NEWLINE)
                    {
                        PushToken(token);
                    }                
                }
                else if (token == Token::TOKEN_EOF)
                {
                    PushToken(token);
                    return false;
                }
            }
            // User-defined row delimiter
            else 
            {
                while ((UINT64) token != rowDelimiter && token != Token::TOKEN_EOF)
                {
                    token = GetNextToken();
                }

                // Handle EOF
                if (token == Token::TOKEN_EOF)
                {
                    PushToken(token);
                    return false;
                }
            }

            //increase line number
            m_recordLineNumber++;
            return true;
        }

        PartitionMetadata * ReadMetadata()
        {
            return nullptr;
        }

        void DiscardMetadata()
        {
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            m_codecvt.WriteRuntimeStats(root);
            m_asyncInput->WriteRuntimeStats(root);
        }

		ScopeEngine::OperatorRequirements GetOperatorRequirements()
        {
			return ScopeEngine::OperatorRequirements().AddMemoryForIOStreams(1, m_ioBufSize, m_ioBufCount, ::OperatorRequirements::OperatorRequirementsConstants::Any__Const_TextConverterSize);
        }

        // return Inclusive time of input stream
        LONGLONG GetTotalIoWaitTime()
        {
            return m_asyncInput->GetTotalIoWaitTime();
        }

        // return Inclusive time of input stream
        LONGLONG GetInclusiveTimeMillisecond()
        {
            return m_asyncInput->GetInclusiveTimeMillisecond();
        }

        CosmosInput& GetInputer()
        {
            return *m_asyncInput;
        }

        bool needCrossBoundary()
        {
            return !m_input.blockAligned;
        }
    };

    // http input class
    class SCOPE_RUNTIME_API HttpInputStream : public BinaryInputStreamBase<HttpInput>
    {
    public:
        HttpInputStream(IncrementalAllocator * allocator, const std::string& hostname, unsigned short port, const std::string& ext, const std::string& inputParameterSets, const unsigned int partitionId) : BinaryInputStreamBase(allocator, hostname, port, ext, inputParameterSets, partitionId)
        {
        }

        HttpInputStream(IncrementalAllocator * allocator, const std::string& hostname, unsigned short port, const std::string& ext, const std::string& inputParameterSets, const std::vector<BlockDevice*>& devices) : BinaryInputStreamBase(allocator, hostname, port, ext, inputParameterSets, devices)
        {
        }

        void ReadException(BYTE& errorCode, int& numArgs, std::string detail, std::string args[4])
        {
            this->Read(errorCode);
            this->Read(numArgs);

            if (numArgs>4)
            {
                ////arg count limit is 4
                return;
            }

            for (int i = 0; i<numArgs; i++)
            {
                FString argTemp;
                this->Read(argTemp);
                args[i] = std::string((const char*)argTemp.buffer(), (size_t)argTemp.Length());
            }

            FString detailTemp;
            this->Read(detailTemp);
            detail = std::string((const char*)detailTemp.buffer(), (size_t)detailTemp.Length());
        }
    };

    // Cosmos output class.
    class SCOPE_RUNTIME_API CosmosOutput : public ExecutionStats
    {
    public:
        struct SettingFlags
        {
            bool DisableCompression : 1;
            bool MaintainBoundaries : 1;

            SettingFlags() :
                DisableCompression(false),
                MaintainBoundaries(false)
            {
            }
        };

    private:
        BlockDevice *           m_device;
        unique_ptr<Scanner, ScannerDeleter> m_scanner;
        Scanner::Statistics     m_statistics;
        const BufferDescriptor* m_outbuffer;
        SIZE_T                  m_outbufferSize;

        SIZE_T            m_maxOnDiskRowSize;
        SIZE_T            m_ioBufSize;
        int               m_ioBufCount;

        bool              m_started;
        char *            m_startPoint;   // write start position
        SIZE_T            m_numRemain;    // number of character remain in the buffer
        SIZE_T            m_position;
        SettingFlags      m_settingFlags;
        SIZE_T            m_committed;    // position of buffer that the data cannot be flush to disk, only used when maintainBoundariesMode is true
        std::string       m_recoverState; // cached state from load checkpoint

    public:
        CosmosOutput()
            : CosmosOutput(nullptr, IOManager::x_defaultOutputBufSize, IOManager::x_defaultOutputBufCount, SettingFlags())
        {
        }

        CosmosOutput(const std::string & filename, SIZE_T bufSize, int bufCount, bool maintainBoundaries = false)
            : CosmosOutput(filename, bufSize, bufCount, SettingFlags())
        {
            m_settingFlags.MaintainBoundaries = maintainBoundaries;
        }

        CosmosOutput(const std::string & filename, SIZE_T bufSize, int bufCount, SettingFlags settingFlags) :
            m_device(IOManager::GetGlobal()->GetDevice(filename)), 
            m_outbuffer(nullptr), 
            m_outbufferSize(0),
            m_maxOnDiskRowSize(Configuration::GetGlobal().GetMaxOnDiskRowSize()),
            m_ioBufSize(bufSize),
            m_ioBufCount(bufCount),
            m_started(false),
            m_settingFlags(settingFlags),
            m_committed(0)
        {
        }

        CosmosOutput(BlockDevice* device, SIZE_T bufSize, int bufCount, bool maintainBoundaries = false)
            : CosmosOutput(device, bufSize, bufCount, SettingFlags())
        {
            m_settingFlags.MaintainBoundaries = maintainBoundaries;
        }

        CosmosOutput(BlockDevice* device, SIZE_T bufSize, int bufCount, SettingFlags settingFlags) :
            m_device(device), 
            m_outbuffer(nullptr), 
            m_outbufferSize(0),
            m_maxOnDiskRowSize(Configuration::GetGlobal().GetMaxOnDiskRowSize()),
            m_ioBufSize(bufSize),
            m_ioBufCount(bufCount),
            m_started(false),
            m_settingFlags(settingFlags),
            m_committed(0)
        {
        }

        //
        // Init() for default constructor
        //
        void Init(const std::string & filename, SIZE_T bufSize, int bufCount)
        {
            m_device = IOManager::GetGlobal()->GetDevice(filename);
            m_ioBufSize = bufSize;
            m_ioBufCount = bufCount;

            Init();
        }

        //
        // Init() for CosmosOutput(std::string & filename, SIZE_T bufSize, int bufCount)
        //
        void Init()
        {
            AutoExecStats stats(this);

            //setup scanner and start scanning. 
            Scanner::SettingFlags scannerSettings;
            scannerSettings.DisableCompression = m_settingFlags.DisableCompression;
            m_scanner.reset(Scanner::CreateScanner(m_device, MemoryManager::GetGlobal(), Scanner::STYPE_Create, m_maxOnDiskRowSize, m_ioBufSize, m_ioBufCount, scannerSettings));
            m_scanner->Open();

            m_startPoint = nullptr;
            m_numRemain = 0;
            m_position = 0;

            if (m_recoverState.size() != 0)
            {
                m_scanner->LoadState(m_recoverState);
                m_recoverState.clear();
            }
        }

        void Close()
        {
            AutoExecStats stats(this);

            if (m_scanner)
            {
                if (m_scanner->IsOpened())
                {
                    if (m_started)
                    {
                        m_started = false;
                        m_scanner->Finish();
                    }

                    m_scanner->Close();
                }

                // Track statistics before scanner is destroyed.
                m_statistics = m_scanner->GetStatistics();
                m_statistics.ConvertToMilliSeconds();

                m_scanner.reset();
            }
        }

        bool IsOpened()
        {
            return m_scanner != nullptr;
        }

        SIZE_T GetCurrentPosition() const
        {
            return m_position;
        }

        SIZE_T RemainBufferSize() const
        {
            return m_numRemain;
        }

        void SetMaintainBoundaryMode(bool val)
        {
            m_settingFlags.MaintainBoundaries = val;
        }

        // it reads n bytes data before current position
        // NOTE: it'll only read data from current buffer
        int ReadBack(BYTE* pBuffer, int len)
        {
            if (m_startPoint == nullptr || m_outbuffer == nullptr)
            {
                return 0;
            }

            len = std::min(len, (int)(m_startPoint - (char*)m_outbuffer->m_buffer));
            
            memcpy(pBuffer, m_startPoint - len, len);

            return len;
        }

        void Write(const char * src, unsigned int size)
        {
            if (m_numRemain >= size)
            {
                memcpy(m_startPoint, src, size);
                m_numRemain -= size;
                m_startPoint += size;
                m_position += size;
            }
            else
            {
                if (m_settingFlags.MaintainBoundaries)
                {
                    if (m_committed != 0 || m_outbuffer == NULL)
                    {
                        Flush();
                        Write(src, size);
                    }
                    else 
                    {
                        ExpandPage();
                        Write(src, size);
                    }
                }
                else
                {
                    while (m_numRemain < size)
                    {
                        // This condition seems like it would be always true, but it is possible to hit this
                        // code right after calling WriteChar, which does not Flush, so m_numRemain would be 0.
                        if (m_numRemain != 0)
                        {
                            // Copy the remaining first, then fetch the next batch
                            memcpy(m_startPoint, src, m_numRemain);
                            src += static_cast<unsigned int>(m_numRemain);
                            size -= static_cast<unsigned int>(m_numRemain);
                            m_position += m_numRemain;
                            m_numRemain = 0;
                        }

                        Flush();
                    }

                    Write(src, size);
                }
            }
        }

        void WriteChar(char b)
        {
            if (m_numRemain >= 1)
            {
                m_numRemain--;
                *m_startPoint++ = b;
                m_position++;
            }
            else
            {
                Flush();

                // we must have get more buffer to writter.
                SCOPE_ASSERT(m_numRemain >= 1);
                WriteChar(b);
            }
        }

        void Commit()
        {
            m_committed = m_outbufferSize - m_numRemain;
        }

        // write the content to outputer
        void Flush(bool forcePersist = false)
        {
            FlushPage(forcePersist, true);
            if (forcePersist)
            {
                WaitForAllPendingOperation();
            }
        }

        void WaitForAllPendingOperation()
        {
            m_scanner->WaitForAllPendingOperations();
        }

        void Finish()
        {
            Commit();
            FlushPage(true, false);
        }

        // Get a new empty page buffer for write.
        char * GetNextPageBuffer(SIZE_T & bufSize)
        {
            AutoExecStats stats(this);

            if (!m_started)
            {
                m_scanner->Start();
                m_started = true;
            }

            bool next = m_scanner->GetNext(m_outbuffer, m_outbufferSize);
            if (!next)
            {
                return NULL;
            }

            bufSize = m_outbufferSize;
            m_startPoint = (char *)(m_outbuffer->m_buffer);
            m_numRemain = bufSize;
            return (char *)(m_outbuffer->m_buffer);
        }

        // Write out a page
        void IssueWritePage(SIZE_T size, bool persist, bool finish)
        {
            AutoExecStats stats(this);

            if (!m_started)
            {
                m_scanner->Start();
                m_started = true;
            }

            if (m_outbuffer)
            {
                m_scanner->PutNext(m_outbuffer, size, persist, finish);
                stats.IncreaseRowCount(size);
            }
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            RuntimeStats::WriteIOStats(root, m_statistics);
            auto & node = root.AddElement("IOBuffers");
            node.AddAttribute(RuntimeStats::MaxBufferCount(), m_ioBufCount);
            node.AddAttribute(RuntimeStats::MaxBufferSize(), m_ioBufSize);
            node.AddAttribute(RuntimeStats::MaxBufferMemory(), m_statistics.m_memorySize);
            node.AddAttribute(RuntimeStats::AvgBufferMemory(), m_statistics.m_memorySize);
        }

        LONGLONG GetTotalIoWaitTime()
        {
            return m_statistics.GetTotalIoWaitTime();
        }

        SIZE_T ExpandPage()
        {
            AutoExecStats stats(this);
            SIZE_T expandSize = 0;

            if (m_outbufferSize < m_maxOnDiskRowSize)
            {
                expandSize = std::min<SIZE_T>(m_maxOnDiskRowSize - m_outbufferSize, IOManager::x_defaultOutputBufSize);
                m_scanner->Expand(m_outbuffer, expandSize);
                m_outbufferSize += expandSize;
                m_numRemain += expandSize;
            }
            else
            {
                std::stringstream ss;
                ss << "The row has exceeded the maximum allowed size of " << m_maxOnDiskRowSize/(1024*1024) << "MB";
                throw RuntimeException(E_USER_ROW_TOO_BIG, ss.str().c_str());
            }

            return expandSize;
        }

        // Return the filename associated with this stream
        std::string StreamName() const
        {
            return m_scanner->GetStreamName();
        }

        void SaveState(BinaryOutputStream& output);
        void LoadState(BinaryInputStream& input);

    private:
        void FlushPage(bool persist, bool fGetNextPage)
        {
            SIZE_T previousPos = m_outbufferSize - m_numRemain;

            // if fGetNextPage is false, that means this is the last append, we have to call IssueWritePage 
            // even there is no data in the buffer
            if (m_outbufferSize != 0 && previousPos == 0 && (fGetNextPage && !persist))
            {
                return;
            }

            if (m_settingFlags.MaintainBoundaries)
            {
                IssueWritePage(m_committed, persist, !fGetNextPage /*seal if the last page*/);
                if (!fGetNextPage && previousPos != m_committed)
                {
                    SCOPE_LOG_FMT_ERROR("CosmosOutput", "Error: last flush %I64u, %I64u, %I64u", previousPos, m_committed, m_outbufferSize);
                    SCOPE_ASSERT(false);
                }
            }
            else
            {
                IssueWritePage(previousPos, persist, !fGetNextPage /*seal if the last page*/);
            }

            if (fGetNextPage)
            {
                SIZE_T bufSize;
                char* previousBuffer = m_outbuffer == NULL ? NULL : (char*)m_outbuffer->m_buffer;

                GetNextPageBuffer(bufSize);
                if (m_settingFlags.MaintainBoundaries && m_committed != previousPos)
                {
                    SCOPE_ASSERT(previousBuffer != NULL);

                    SIZE_T count = previousPos - m_committed;
                    // TODO: not perfect refactoring. scanner has to have at least two buffer and the 
                    // previous buffer is not used again until the current is full.
                    memcpy(m_startPoint, previousBuffer + m_committed, count);
                    m_startPoint += count;
                    m_numRemain -= count;
                }

                m_committed = 0;
            }
            else
            {
                m_outbufferSize = 0;
                m_outbuffer = NULL;
                m_startPoint = NULL;
                m_numRemain = 0;
                m_committed = 0;
            }
        }
    };

#pragma warning(disable: 4251)
    // wrap around MemoryOutputInternal
    // So that it can be used by BinaryOutputBase
    class SCOPE_RUNTIME_API MemoryOutput : public ExecutionStats
    {
        // Disallow Copy and Assign
        MemoryOutput(const MemoryOutput&);
        MemoryOutput& operator=(const MemoryOutput&);

        std::shared_ptr<AutoBuffer> m_inner;
        SIZE_T m_pos;

    public:
        MemoryOutput(const std::string filename, SIZE_T arg1, int arg2, bool arg3)
        {
            UNREFERENCED_PARAMETER(filename);
            UNREFERENCED_PARAMETER(arg1);
            UNREFERENCED_PARAMETER(arg2);
            UNREFERENCED_PARAMETER(arg3);

            m_inner.reset(new AutoBuffer(MemoryManager::GetGlobal()));
            m_pos = 0;
        }

        MemoryOutput(MemoryManager* memMgr)
            : m_inner(new AutoBuffer(memMgr)), m_pos(0)
        {  }

        MemoryOutput(std::shared_ptr<AutoBuffer>& inner)
            : m_inner(inner), m_pos(0)
        {  }

        // it reads n bytes data before current position
        // NOTE: it'll only read data from current buffer
        int ReadBack(BYTE* pBuffer, int len)
        {
            len = std::min((int)m_pos, len);
            memcpy(pBuffer, m_inner->Buffer() + (m_pos - len), len);
            return len;
        }

        void Write(const char * src, unsigned int size)
        {
            while (m_inner->Capacity() - m_pos < size)
            {
                // Copy the remaining first, then fetch the next batch
                SIZE_T copied = m_inner->Capacity() - m_pos;
                memcpy(m_inner->Buffer() + m_pos, src, copied);
                src += static_cast<unsigned int>(copied);
                size -= static_cast<unsigned int>(copied);
                m_pos += copied;
                m_inner->Expand();
            }

            memcpy(m_inner->Buffer() + m_pos, src, size);
            m_pos += size;
        }

        void WriteChar(char & b)
        {
            if (m_pos < m_inner->Capacity())
            {
                *(m_inner->Buffer() + m_pos) = b;
            }
            else
            {
                m_inner->Expand();
                WriteChar(b);
            }
            m_pos++;
        }

        void Commit()
        {
        }

        void Init()
        {
        }

        void Close()
        {
        }

        bool IsOpened()
        {
            return true;
        }

        void Flush(bool forcePersist = false)
        {
            UNREFERENCED_PARAMETER(forcePersist);
        }

        void Finish()
        {
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            if (m_inner)
            {
                m_inner->WriteRuntimeStats(root);
            }
        }

        LONGLONG GetTotalIoWaitTime()
        {
            return 0;
        }

        BYTE* Buffer() const
        {
            return m_inner->Buffer();
        }

        SIZE_T Tellp() const
        {
            return m_pos;
        }

        void SaveState(BinaryOutputStream&)
        {
        }

        void LoadState(BinaryInputStream&)
        {
        }
    };
#pragma warning(default: 4251)

    class SCOPE_RUNTIME_API DummyOutput
    {
        DummyOutput(const DummyOutput&);
        DummyOutput& operator=(const DummyOutput&);

        SIZE_T m_pos;

    public:
        DummyOutput(const std::string, SIZE_T, int, bool)
        {
            m_pos = 0;
        }

        void Write(const char *, unsigned int size)
        {
            m_pos += size;
        }

        void WriteChar(char &)
        {
            m_pos++;
        }

        SIZE_T GetCurrentPosition() const
        {
            return m_pos;
        }
    };

    // Output stream class to hide the block interface from the cosmos stream. 
    template<typename OutputType>
    class SCOPE_RUNTIME_API BinaryOutputStreamBase : public StreamWithManagedWrapper
    {
        OutputType    m_asyncOutput;

    public:

        BinaryOutputStreamBase(const std::string& filename, SIZE_T bufSize, int bufCount, bool maintainBoundaries = false) :
            m_asyncOutput(filename, bufSize, bufCount, maintainBoundaries)
        {
        }

        // write the content to outputer
        void Flush(bool forcePersist = false)
        {
            m_asyncOutput.Flush(forcePersist);
        }

        void Finish()
        {
            m_asyncOutput.Finish();
        }

        //
        // Init() for BinaryOutputStream(std::string filename, SIZE_T bufSize, int bufCount)
        //
        void Init()
        {
            m_asyncOutput.Init();
        }

        void Close()
        {
            m_asyncOutput.Close();
        }

        void Commit()
        {
            m_asyncOutput.Commit();
        }

        void Write(const char * src, unsigned int size)
        {
            m_asyncOutput.Write(src, size);
        }

        void WriteChar(char & b)
        {
            m_asyncOutput.WriteChar(b);
        }

        void WriteLen(unsigned int size)
        {
            char b;

            do
            {
                b = size & 0x7f;
                size = size >> 7;
                if (size > 0)
                    b |= 0x80;
                WriteChar(b);
            } while (size);
        }

#ifdef PLUGIN_TYPE_SYSTEM
        template <typename T>
        void Write(const T & s)
        {
            s.Serialize(this);
        }

        template<typename T>
        void DoWrite(const T & s)
        {
            Write((const char*) &s, sizeof(T));
        }

        void Write(const bool & s)
        {
            DoWrite(s);
        }

        void Write(const unsigned char & s)
        {
            DoWrite(s);
        }

        void Write(const char & s)
        {
            DoWrite(s);
        }

        void Write(const short & s)
        {
            DoWrite(s);
        }

        void Write(const unsigned short & s)
        {
            DoWrite(s);
        }

        void Write(const wchar_t & s)
        {
            DoWrite(s);
        }

        void Write(const int & s)
        {
            DoWrite(s);
        }

        void Write(const unsigned int & s)
        {
            DoWrite(s);
        }

        void Write(const __int64 & s)
        {
            DoWrite(s);
        }

        void Write(const unsigned __int64 & s)
        {
            DoWrite(s);
        }

        void Write(const float & s)
        {
            DoWrite(s);
        }

        void Write(const double & s)
        {
            DoWrite(s);
        }

        // For writing ScopeDecimal
        void Write(const ULONG & s)
        {
            DoWrite(s);
        }
#else // PLUGIN_TYPE_SYSTEM
        template<typename T>
        void Write(const T& s)
        {
            Write((const char*) &s, sizeof(T));
        }

        template<typename T>
        void WriteFixedArray(const FixedArrayType<T> & fixedArray)
        {
            unsigned int size = fixedArray.size();

            WriteLen(size);

            Write((const char*) fixedArray.buffer(), size);
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
        void Write(const ScopeMapNative<K, V>& m)
        {
            m.Serialize(this);
        }

        template<typename T>
        void Write(const ScopeArrayNative<T>& s)
        {
            s.Serialize(this);
        }

        void Write(const ScopeDateTime & s)
        {
            __int64 binaryTime = s.ToBinary();
            Write(binaryTime);
        }

        void Write(const ScopeSqlType::SqlDateTime & s)
        {
            __int64 binaryTime = s.ToBinaryTime();
            long binaryDate = s.ToBinaryDate();
            Write(binaryTime);
            Write(binaryDate);
        }

        template<class I, typename T>
        void Write(const ScopeSqlType::ArithmeticSqlType<I, T> & s)
        {
            T value = s.getValue();
            Write(value);
        }

        void Write(const ScopeDecimal & s)
        {
            Write(s.Lo32Bit());
            Write(s.Mid32Bit());
            Write(s.Hi32Bit());
            Write(s.SignScale32Bit());
        }

        void Write(const ScopeGuid & s)
        {
            Write((const char *)&s, sizeof(ScopeGuid));
        }

        template <int N>
        void Write(const ScopeSqlType::SqlStringFaceted<N> & str)
        {
            unsigned int size = str.Size();

            WriteLen(size);

            Write((const char*)str.Buffer(), str.Size());
        }

        template<typename T>
        void Write(const NativeNullable<T> & s)
        {
            // we will never call write a null value in binary outputer
            SCOPE_ASSERT(!s.IsNull());

            Write(s.get());
        }

        template<typename T>
        void Write(const ScopeSqlType::SqlNativeNullable<T> & s)
        {
            Write((NativeNullable<T> &)s);
        }

        void Write(const char& c)
        {
            Write<char>(c);
        }

        void Write(const int& c)
        {
            Write<int>(c);
        }

#endif // PLUGIN_TYPE_SYSTEM

#if !defined(SCOPE_NO_UDT)

        template<int UserDefinedTypeId, template<int> class UserDefinedType>
        void Write(const UserDefinedType<UserDefinedTypeId> & s, const ScopeManagedHandle & serializationContext)
        {
            // we will never call write a null value in binary outputer
            SCOPE_ASSERT(!s.IsNull());

            s.BinarySerialize(this, serializationContext);
        }

#endif // SCOPE_NO_UDT

        void WriteMetadata(PartitionMetadata * )
        {
            // do nothing
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            m_asyncOutput.WriteRuntimeStats(root);
        }

        LONGLONG GetTotalIoWaitTime()
        {
            return m_asyncOutput.GetTotalIoWaitTime();
        }

        LONGLONG GetInclusiveTimeMillisecond()
        {
            return m_asyncOutput.GetInclusiveTimeMillisecond();
        }

        OutputType& GetOutputer()
        {
            return m_asyncOutput;
        }
    };

    class SCOPE_RUNTIME_API BinaryOutputStream : public BinaryOutputStreamBase<CosmosOutput>
    {
    public:
        BinaryOutputStream(const std::string & filename, SIZE_T bufSize, int bufCount, bool maintainBoundaries = false) : 
            BinaryOutputStreamBase(filename, bufSize, bufCount, maintainBoundaries)
        { }
    };

    class SCOPE_RUNTIME_API MemoryOutputStream : public BinaryOutputStreamBase<MemoryOutput>
    {
    public:
        MemoryOutputStream() : BinaryOutputStreamBase("-"/*dummy*/, 1/*dummy*/, 1/*dummy*/)
        { }

    };

    class SCOPE_RUNTIME_API DummyOutputStream : public BinaryOutputStreamBase<DummyOutput>
    {
    public:
        DummyOutputStream() : BinaryOutputStreamBase("-"/*dummy*/, 1/*dummy*/, 1/*dummy*/)
        { }
    };

    //
    // Binary output stream with payload
    //
    template<typename Payload>
    class AugmentedBinaryOutputStream : public BinaryOutputStream
    {
    public:
        AugmentedBinaryOutputStream(const std::string & filename, SIZE_T bufSize, int bufCount, bool maintainBoundaries = false) : BinaryOutputStream(filename, bufSize, bufCount, maintainBoundaries)
        {
        }

        void WriteMetadata(PartitionMetadata * metadata)
        {
            if (!metadata)
            {
                throw MetadataException("Unexpected NULL metadata");
            }

            cout << "Writing metadata" << endl;
            static_cast<Payload*>(metadata)->Serialize(this);
        }
    };

    // A thin wrapper around AutoBuffer that provide scope type awareness 
    // used exclusively by SStream Data serialization.
    class SCOPE_RUNTIME_API SStreamDataOutputStream : public StreamWithManagedWrapper
    {
        // not owning.
        AutoBuffer* m_inner;

    public:
        explicit SStreamDataOutputStream(AutoBuffer* buffer) : m_inner(buffer) {}

        SIZE_T GetPosition() const
        {
            return m_inner->Tellp();
        }

        void WriteChar(char c)
        {
            m_inner->Put((BYTE) c);
        }

        void Write(const char * buf, SIZE_T size) 
        {
            m_inner->Write((BYTE*) buf, size);
        }

#ifdef PLUGIN_TYPE_SYSTEM
        template<typename T>
        void DoWrite(const T& s)
        {
            Write(reinterpret_cast<const char *>(&s), sizeof(T));
        }

        template <typename T>
        void Write(const T & s)
        {
            s.SStreamDataOutputSerialize<AutoBuffer>((void*)m_inner);
        }

        void Write(const bool & s)
        {
            DoWrite(s);
        }

        void Write(const unsigned char & s)
        {
            DoWrite(s);
        }

        void Write(const char & s)
        {
            DoWrite(s);
        }

        void Write(const short & s)
        {
            DoWrite(s);
        }

        void Write(const unsigned short & s)
        {
            DoWrite(s);
        }

        void Write(const wchar_t & s)
        {
            DoWrite(s);
        }

        void Write(const int & s)
        {
            DoWrite(s);
        }

        void Write(const unsigned int & s)
        {
            DoWrite(s);
        }

        void Write(const __int64 & s)
        {
            DoWrite(s);
        }

        void Write(const unsigned __int64 & s)
        {
            DoWrite(s);
        }

        void Write(const float & s)
        {
            DoWrite(s);
        }

        void Write(const double & s)
        {
            DoWrite(s);
        }

        void Write(const ULONG & s)
        {
            DoWrite(s);
        }
#else // PLUGIN_TYPE_SYSTEM
        template<typename T>
        void Write(const T& s)
        {
            Write(reinterpret_cast<const char *>(&s), sizeof(T));
        }

        template<>
        void Write(const FString & str)
        {
            Write(str.buffer(), str.size());
        }

        template<>
        void Write(const FBinary & bin)
        {
            Write((const char *) (bin.buffer()), bin.size());
        }

        template<>
        void Write<ScopeDateTime>(const ScopeDateTime & s)
        {
            __int64 binaryTime = s.ToBinary();
            Write(binaryTime);
        }

        template<>
        void Write<ScopeSqlType::SqlDateTime>(const ScopeSqlType::SqlDateTime & s)
        {
            __int64 binaryTime = s.ToBinaryTime();
            long binaryDate = s.ToBinaryDate();
            Write(binaryTime);
            Write(binaryDate);
        }

        template<class I, typename T>
        void Write(const ScopeSqlType::ArithmeticSqlType<I, T> & s)
        {
            T value = s.getValue();
            Write(value);
        }

        template<>
        void Write<ScopeDecimal>(const ScopeDecimal & s)
        {
            Write(s.Lo32Bit());
            Write(s.Mid32Bit());
            Write(s.Hi32Bit());
            Write(s.SignScale32Bit());
        }

        template<>
        void Write<ScopeGuid>(const ScopeGuid & s)
        {
            Write(reinterpret_cast<char *>(const_cast<ScopeGuid *>(&s)), sizeof(ScopeGuid));
        }

        template<int N>
        void Write(const ScopeSqlType::SqlStringFaceted<N> & str)
        {
            Write(str.Buffer(), str.Size());
        }

        template<typename K, typename V>
        void Write(const ScopeMapNative<K, V> & s)
        {
            s.Serialize(m_inner);
        }

        template<typename T>
        void Write(const ScopeArrayNative<T> & s)
        {
            s.Serialize(m_inner);
        }

        template<typename T>
        void Write(const NativeNullable<T> & s)
        {
            // we will never call write a null value in sstream outputer
            SCOPE_ASSERT(!s.IsNull());

            Write(s.get());
        }

        template<typename T>
        void Write(const ScopeSqlType::SqlNativeNullable<T> & s)
        {
            Write((NativeNullable<T> &)s);
        }

#endif // PLUGIN_TYPE_SYSTEM
#if !defined(SCOPE_NO_UDT)
        template<int UserDefinedTypeId, template<int> class UserDefinedType>
        void Write(const UserDefinedType<UserDefinedTypeId> & s, const ScopeManagedHandle & serializationContext)
        {
            // we will never call write a null value in sstream outputer
            SCOPE_ASSERT(!s.IsNull());

            s.BinarySerialize(this, serializationContext);
        }

        template<int UserDefinedTypeId, template<int> class UserDefinedType>
        void SSLibWrite(const UserDefinedType<UserDefinedTypeId> & s)
        {
            // we will never call write a null value in sstream outputer
            SCOPE_ASSERT(!s.IsNull());

            s.SSLibSerialize(this);
        }
#endif // SCOPE_NO_UDT
    };

    // class to buffer the output text and convert text from utf8 to specified encoding
    template<class TextEncodingWriterTraits>
    class TextEncodingWriter : protected TextEncodingWriterTraits
    {
        CosmosOutput   *  m_asyncOutput;
        char * m_startPoint;   // write start position
        char * m_linePoint;
        SIZE_T m_numRemain;    // number of character remain in the buffer
        SIZE_T m_bufferSize;   // total buffer size
        SIZE_T m_linePos;      // position of the new line withtin the buffer
        bool   m_insideLine;   // are we in the middle of writing a line

        FORCE_INLINE void OutputChar(const unsigned char & b)
        {
            if (m_numRemain >= 1)
            {
                m_numRemain--;
                *m_startPoint++ = (char)b;
            }
            else
            {
                FlushPage(false, true, true);
                if (m_numRemain >= 1)
                {
                    m_numRemain--;
                    *m_startPoint++ = (char)b;
                }
            }
        }

    public:
        TextEncodingWriter(CosmosOutput * output):
            m_asyncOutput(output), 
            m_startPoint(NULL),
            m_linePoint(NULL),
            m_numRemain(0),
            m_bufferSize(0),
            m_linePos(0),
            m_insideLine(false)
        {
        }

        TextEncodingWriter(CosmosOutput * output, const OutputStreamParameters & outputStreamParams) :
            TextEncodingWriterTraits(outputStreamParams),
            m_asyncOutput(output), 
            m_startPoint(NULL),
            m_linePoint(NULL),
            m_numRemain(0),
            m_bufferSize(0),
            m_linePos(0),
            m_insideLine(false)
        {
        }

        void FlushPage(bool fPersist, bool fGetNextPage, bool expandPage)
        {
            // the last flush automatically "starts" a new line
            if (!fGetNextPage)
            {
                StartNewLine();
            }

            if (m_insideLine)
            {
                if (expandPage)
                {
                    SCOPE_ASSERT(fGetNextPage);
                    SIZE_T expandSize = m_asyncOutput->ExpandPage();

                    m_bufferSize += expandSize;
                    m_numRemain += expandSize;
                }
            }
            else
            {
                // if we are not at the end of the processing, and we don't have any data available,
                // we don't have to flush the current buffer. A special case is that for the first write
                // we don't have a buffer at all, and in this case we have to excuse the below code to get the
                // buffer. This is a matching logic in CosmosOutput.IssueWritePage to ignore the first empty write              
                if (fGetNextPage && m_linePos == 0 && m_startPoint != nullptr && !fPersist)
                {
                    return;
                }

                m_asyncOutput->IssueWritePage(m_linePos, fPersist, !fGetNextPage /*seal if the last page*/);

                SIZE_T reminder = m_startPoint - m_linePoint;

                if (fGetNextPage)
                {
                    m_startPoint = m_asyncOutput->GetNextPageBuffer(m_bufferSize);
                    m_numRemain = m_bufferSize;

                    memcpy(m_startPoint, m_linePoint, reminder);
                    m_startPoint += reminder;
                    m_numRemain -= reminder;

                    m_linePos = 0;
                    m_insideLine = true;
                }
                else
                {
                    SCOPE_ASSERT(reminder == 0);
                    m_startPoint = NULL;
                    m_numRemain = 0;
                }
            }
        }

        void StartNewLine()
        {
            m_linePos = m_bufferSize - m_numRemain;
            m_linePoint = m_startPoint;
            m_insideLine = false;
        }

        // write string to output stream. It will handle output buffer full. 
        FORCE_INLINE void WriteToOutput(const char * src, SIZE_T size)
        {
            for(;;)
            {
                if (m_numRemain == 0)
                {
                    FlushPage(false, true, true);
                }

                char * pInNext = const_cast<char *>(src);
                char * pOutNext = m_startPoint;

                ConvertFromUtf8(const_cast<char *>(src),
                    const_cast<char *>(src)+size,
                    pInNext,
                    m_startPoint,
                    m_startPoint+m_numRemain,
                    pOutNext);

                SIZE_T nConverted = pInNext - src;

                // If not all input converted
                if (nConverted < size)
                {
                    src = pInNext;
                    size -= nConverted;

                    // Write out page 
                    m_numRemain -= pOutNext - m_startPoint; 
                    m_startPoint = pOutNext;
                    FlushPage(false, true, true);
                }
                else
                {
                    m_numRemain -= pOutNext - m_startPoint;
                    m_startPoint = pOutNext;
                    return;
                }
            }
        }

        void WriteChar(const unsigned char & b)
        {
            WriteToOutput((char*)&b, sizeof(char));
        }
    };

    //
    // TextOutputStream traits for "batch" mode
    //
    template<ULONG _delimiter, 
             ULONG _escapeCharacter, 
             bool _escape, 
             bool _escapeDelimiter, 
             bool _textQualifier, 
             bool _doubleToFloat, 
             bool _usingDateTimeFormat, 
             TextEncoding _encoding,
             bool _validateEncoding>
    struct TextOutputStreamTraitsConst
    {
        static const ULONG delimiter = _delimiter;
        SIZE_T delimiterLen;
        static const ULONG escapeCharacter = _escapeCharacter;
        SIZE_T escapeCharacterLen;
        static const bool escape = _escape;
        static const bool escapeDelimiter = _escapeDelimiter;
        static const bool textQualifier = _textQualifier;
        static const bool doubleToFloat = _doubleToFloat;
        static const bool usingDateTimeFormat = _usingDateTimeFormat;
        static const TextEncoding encoding = _encoding;
        static const bool validateEncoding = _validateEncoding; // actually this flag only control ASCII and UTF8 validation, to keep compatible with first party behavior

        const char * dateTimeFormat;

        const char * nullEscape;
        SIZE_T nullEscapeLen;

        const char * rowDelimiter;
        SIZE_T rowDelimiterLen;

    private:
        static SIZE_T UTF8ByteCount(ULONG c)
        {
            // special case : '\0' represents null value
            if (c == '\0')
            {
                return 0;
            }
            
            // Count total number of bits set in the utf-8 char 
            // from the left until the first 0
            SIZE_T count = 0;

            // align c to the left
            while (!(c & 0xFF000000))
            {
                c <<= 8;
            }

            // while the first byte is set
            while (c & 0x80000000)
            {
                count++;
                c <<= 1;
            }
            
            return (count != 0) ? count : 1;
        }  
        
    public:
        TextOutputStreamTraitsConst(const OutputStreamParameters & pars) :
            dateTimeFormat(pars.dateTimeFormat),
            rowDelimiter(pars.rowDelimiter),
            nullEscape(pars.nullEscape)
        {
            // character parameters length
            delimiterLen = UTF8ByteCount(delimiter);
            escapeCharacterLen = UTF8ByteCount(escapeCharacter);

            // string parameters length
            rowDelimiterLen = (rowDelimiter != nullptr) ? strlen(rowDelimiter) : 0;
            nullEscapeLen = (nullEscape != nullptr) ? strlen(nullEscape) : 0; 
        }

        void ConvertFromUtf8(char * inFirst, char * inLast, char *& inMid, char * outFirst, char * outLast, char *& outMid)
        {
            // For ASCII, Default and UTF8, does not check encoding if validateEncoding flag is false.
            if (!validateEncoding && (encoding == TextEncoding::ASCII || encoding == TextEncoding::UTF8 || encoding == TextEncoding::Default))
            {
                TextEncodingConverter<TextEncoding::NONE>::ConvertFromUtf8(inFirst, inLast, inMid, outFirst, outLast, outMid);
                return;
            }
            
            m_codecConverter.ConvertFromUtf8(inFirst, inLast, inMid, outFirst, outLast, outMid);
        }

    private:
        TextEncodingConverter<_encoding> m_codecConverter;
    };

    // Base class for all text output streams
    // Needed for the UserDefinedType::TextSerialize method
    class TextOutputStreamBase : public StreamWithManagedWrapper
    {
        typedef void (*WriteBufferStubType)(void *, const char *, SIZE_T);

        void * m_objectPtr; // object pointer
        WriteBufferStubType m_writePtr; // "write" method pointer

        template <class T, void (T::*TMethod)(const char *, SIZE_T)>
        static void WriteBufferStub(void* objectPtr, const char * buffer, SIZE_T count)
        {
            T* p = static_cast<T*>(objectPtr);
            return (p->*TMethod)(buffer, count);
        }

    public:
        template <class OutputStream>
        TextOutputStreamBase(OutputStream * objectPtr)
        {
            m_objectPtr = objectPtr;
            m_writePtr  = &WriteBufferStub<OutputStream, reinterpret_cast<void (OutputStream::*)(const char *, SIZE_T)>(&OutputStream::WriteToOutput)>;
        }

        void WriteToOutput(const char * buffer, SIZE_T count)
        {
            (*m_writePtr)(m_objectPtr, buffer, count);
        }
    };

    // Text Output stream class to hide the block interface from the cosmos stream. 
    // It hides the page from the writer. This class is used in defaulttextoutputer.
    template<class TextOutputStreamTraits>
#ifdef PLUGIN_TYPE_SYSTEM
    class TextOutputStream : public TextOutputStreamTraits, public TextOutputStreamBase
#else
    class TextOutputStream : protected TextOutputStreamTraits, public TextOutputStreamBase
#endif
    {
        CosmosOutput    m_asyncOutput;

        TextEncodingWriter<TextOutputStreamTraits> m_codecvt; // code convert for write

        const static __int64 x_epochTicks = 621355968000000000;
        const static __int64 x_unixTimeUpBoundTicks = 946708128000000000;

        bool CheckParameter(ULONG parameter, SIZE_T parameterLen, const char *ptr)
        {
            switch (parameterLen)
            {
            case 4:
                if (ptr[parameterLen - 4] != (char)((parameter >> 24) & 0xff))
                    return false;
                // Fall through
            case 3:
                if (ptr[parameterLen - 3] != (char)((parameter >> 16) & 0xff))
                    return false;
                // Fall through
            case 2:
                if (ptr[parameterLen - 2] != (char)((parameter >> 8) & 0xff))
                    return false;
                // Fall through
            case 1:
                return ptr[parameterLen - 1] == (char)(parameter & 0xff);
            default:
                SCOPE_ASSERT(false);
                return false;
            }
        }

        template<typename T, bool quoted>
        FORCE_INLINE void WriteInteger(T val, bool trimZero)
        {
            char buf[numeric_limits<T>::digits10+2];
            char *it = &buf[numeric_limits<T>::digits10];

            if(!(numeric_limits<T>::is_signed) || val>=0)
            {
                T div = val/100;

                while(div) 
                {
                    memcpy(it, &x_digit_pairs[2*(val-div*100)], 2);
                    val = div;
                    it-=2;
                    div = val/100;
                }

                memcpy(it,&x_digit_pairs[2*val],2);

                // if last pair is less single digital, adjust the start the point.
                if(trimZero && val<10)
                {
                    it++;
                }
            }
            else
            {
                T div = val/100;

                while(div)
                {
                    memcpy(it, &x_digit_pairs[-2*(val-div*100)], 2);
                    val = div;
                    it-=2;
                    div = val/100;
                }

                memcpy(it, &x_digit_pairs[-2*val], 2);

                // if last pair is less single digital, adjust the start the point.
                if(!trimZero || val <= -10)
                {
                    it--;
                }

                *it = '-';
            }

            // quotation mark
            WriteToOutput<quoted>(it, (SIZE_T)(&buf[numeric_limits<T>::digits10+2]-it));
        }

        void FlushPage(bool fPersist, bool fGetNextPage)
        {
            m_codecvt.FlushPage(fPersist, fGetNextPage, false);
        }

        //Handling special NaN and Infinity value for double/float type
        template<typename T, bool quoted>
        FORCE_INLINE bool WriteInfNaN(const T & s)
        {
            if (std::isnan(s))
            {
                WriteToOutput<quoted>(x_NaN, sizeof(x_NaN));
                return true;
            }
            else if (std::isinf(s) && !std::signbit(s))
            {
                WriteToOutput<quoted>(x_PositiveInf, sizeof(x_PositiveInf));
                return true;
            }
            else if (std::isinf(s) && std::signbit(s))
            {
                WriteToOutput<quoted>(x_NegativeInf, sizeof(x_NegativeInf));
                return true;
            }

            return false;
        }

    public:
        TextOutputStream(std::string filename, SIZE_T bufSize, int bufCount, bool maintainBoundary = false) :
            TextOutputStreamBase(this),
            m_asyncOutput(filename, bufSize, bufCount, maintainBoundary),  
            m_codecvt(&m_asyncOutput)
        {
        }

        TextOutputStream(std::string filename, SIZE_T bufSize, int bufCount, const OutputStreamParameters & outputStreamParams, bool maintainBoundary = false) :
            TextOutputStreamTraits(outputStreamParams),
            TextOutputStreamBase(this),
            m_asyncOutput(filename, bufSize, bufCount, maintainBoundary),
            m_codecvt(&m_asyncOutput, outputStreamParams)
        {
        }

        //
        // Init() for default constructor
        //
        void Init()
        {
            m_asyncOutput.Init();
        }

        void Close()
        {
            m_asyncOutput.Close();
        }

        // TODO: (weilin) we need refactor the code to remove the maintainboundaries logic in this class 
        // since we already did that in cosmosoutput
        void Commit()
        {
        }

        // write string to output stream. It will handle output buffer full.
        template<bool quoted = false>
        FORCE_INLINE void WriteToOutput(const char * src, SIZE_T size)
        {
            WriteQuoteToOutput<quoted>();
            m_codecvt.WriteToOutput(src, size);
            WriteQuoteToOutput<quoted>();            
        }

        template<bool quoted>
        FORCE_INLINE void WriteQuoteToOutput()
        {
            if (quoted)
            {
                m_codecvt.WriteToOutput(&x_quote, sizeof(x_quote));
            }
        }

        void StartNewLine()
        {
            m_codecvt.StartNewLine();
        }

        FORCE_INLINE void WriteNull()
        {
            SCOPE_ASSERT(escape || nullEscape != nullptr);

            if (escape)
            {
                WriteToOutput(x_null, sizeof(x_null));
            }
            else
            {
                WriteToOutput(nullEscape, nullEscapeLen);
            }  
        }        

        template<bool quoted>
        FORCE_INLINE void WriteString(const char * src, SIZE_T size)
        {
            // left quotation mark
            WriteQuoteToOutput<quoted>();
            
            SIZE_T start = 0;
            SIZE_T i = 0;

            for ( i = 0; i < size; i++)
            {
                // \r is escaped when escapeDelimiter flag is true
                if (src[i] == x_r && escapeDelimiter)
                {
                    if ( i > start)
                    {
                        WriteToOutput(&src[start], i-start);
                    }

                    start = i+1;
                    WriteToOutput(x_escR, sizeof(x_escR));
                }
                else if (src[i] == x_r && escapeCharacter != '\0')
                {
                    if ( i > start)
                    {
                        WriteToOutput(&src[start], i - start);
                    }

                    start = i+1;

                    WriteParameter(escapeCharacter, escapeCharacterLen);
                    WriteChar('r');
                }
                // \n is escaped when escapeDelimiter flag is true             
                else if (src[i] == x_n && escapeDelimiter)
                {
                    if ( i > start)
                    {
                        WriteToOutput(&src[start], i-start);
                    }

                    start = i+1;
                    WriteToOutput(x_escN, sizeof(x_escN));
                }
                else if (src[i] == x_n && escapeCharacter != '\0')
                {
                    if ( i > start)
                    {
                        WriteToOutput(&src[start], i - start);
                    }

                    start = i+1;

                    WriteParameter(escapeCharacter, escapeCharacterLen);
                    WriteChar('n');
                }
                // Quotation mark inside "..." becomes ""
                else if (quoted && src[i] == x_quote)                
                {
                    if ( i > start)
                    {
                        WriteToOutput(&src[start], i-start);
                    }

                    start = i+1;
                    WriteToOutput(x_pairquote, sizeof(x_pairquote));
                }
                else if (src[i] == x_hash && escape)
                {
                    // We only escape hash when escape flag is turned on.
                    if ( i > start)
                    {
                        WriteToOutput(&src[start], i-start);
                    }

                    start = i+1;
                    WriteToOutput(x_escHash, sizeof(x_escHash));
                }
                // column delimiter is escaped if not inside "...", and, escapeDelimiter flag is true
                else if (!quoted && escapeDelimiter && CheckParameter(delimiter, delimiterLen, src + i))
                {
                    if ( i > start)
                    {
                        WriteToOutput(&src[start], i-start);
                    }

                    start = i + delimiterLen;
                    WriteToOutput(x_tab, sizeof(x_tab));
                }
                else if (!quoted && escapeCharacter != '\0' && CheckParameter(delimiter, delimiterLen, src + i))
                {
                    if ( i > start)
                    {
                        WriteToOutput(&src[start], i-start);
                    }

                    start = i + delimiterLen;

                    WriteParameter(escapeCharacter, escapeCharacterLen);
                    WriteParameter(delimiter, delimiterLen);
                }
                else if (escapeCharacter != '\0' && CheckParameter(escapeCharacter, escapeCharacterLen, src + i))
                {
                    if (i > start)
                    {
                        WriteToOutput(&src[start], i - start);
                    }

                    start = i + escapeCharacterLen;

                    WriteParameter(escapeCharacter, escapeCharacterLen);
                    WriteParameter(escapeCharacter, escapeCharacterLen);
                }
            }

            // write out remaining
            if (i > start && start < size)
            {
                WriteToOutput(&src[start], i-start);
            }

            // right quotation mark
            WriteQuoteToOutput<quoted>();
        }

        void WriteChar(const char & b)
        {
            m_codecvt.WriteChar(b);
        }

        void Flush(bool forcePersist = false)
        {
            FlushPage(forcePersist, true);
            if (forcePersist)
            {
                m_asyncOutput.WaitForAllPendingOperation();
            }
        }

        void Finish()
        {
            FlushPage(true, false);
        }

#ifdef PLUGIN_TYPE_SYSTEM
        template<typename T>
        INLINE void Write(const T & s)
        {
            s.Serialize(this);
        }

#else // PLUGIN_TYPE_SYSTEM
        template<typename T, bool quoted = false>
        void Write(const T& s);

        template<typename T = FString, bool quoted = false>
        void Write(const FString & s)
        {
            if (s.IsNull())
            {
                // handle null value
                if (escape || nullEscape != nullptr)
                {
                    WriteNull();
                }

                return;
            }

            WriteString<quoted>(s.buffer(), s.size());
        }

        template<typename T = FBinary, bool quoted = false>
        void Write(const FBinary & s)
        {
            if (s.IsNull())
            {
                // handle null value
                if (escape || nullEscape != nullptr)
                {
                    WriteNull();
                }

                return;
            }

            // If it is an empty binary, nothing left to write
            if (s.IsEmpty())
            {
                return;
            }

            // convert to "X2" format string
            unique_ptr<char> buf(new char[s.size()*2]);

            unsigned int j = 0;
            const unsigned char * data = s.buffer();

            for(unsigned int i=0; i < s.size(); i++)
            {
                unsigned char t = data[i];
                unsigned char hi = (t>>4)&0xF;
                unsigned char low = t&0xF;

                buf.get()[j++] = x_HexTable[hi];
                buf.get()[j++] = x_HexTable[low];
            }

            WriteString<quoted>(buf.get(), j);
        }

        template<typename T, bool quoted = false>
        void Write(const NativeNullable<T> & s)
        {
            if(s.IsNull())
            {
                // handle null value
                if (escape || nullEscape != nullptr)
                {
                    WriteNull();
                }

                return;
            }

            Write<T,quoted>(s.get());
        }

        template<typename T>
        void Write(const ScopeSqlType::SqlNativeNullable<T> & s)
        {
            Write((NativeNullable<T> &)s);
        }

        template<typename T = ScopeDateTime, bool quoted = false>
        void Write(const ScopeDateTime & s)
        {
            char buffer [80];
            int n = 0;

            n = s.ToString(buffer, 80, dateTimeFormat);
            if ( n > 0 )
            {
                WriteString<quoted>(buffer, n);
            }
        }

        template<typename T = ScopeDecimal, bool quoted = false>
        void Write(const ScopeDecimal & s)
        {
            char finalOut[80];
            int n = ScopeDecimalToString(s, finalOut, 80);

            if (n > 0)
            {
                WriteString<quoted>(finalOut, n);
            }
        }

        template<typename T = ScopeGuid, bool quoted = false>
        void Write(const ScopeGuid & s)
        {
            string str = ScopeEngine::GuidToString(s.get());

            if (!str.empty())
            {
                WriteString<quoted>(str.c_str(), str.length());
            }
        }

        template<>
        void Write<ScopeSqlType::SqlDateTime, false>(const ScopeSqlType::SqlDateTime & s)
        {
            char buffer[80];
            int n = 0;

            n = s.ToString(buffer, 80);
            if (n > 0)
            {
                WriteString<false>(buffer, n);
            }
        }

        template<>
        void Write<ScopeSqlType::SqlDecimal, false>(const ScopeSqlType::SqlDecimal & s)
        {
            char finalOut[80];
            int n = SqlDecimalToString(s, finalOut, 80);

            if (n > 0)
            {
                WriteString<false>(finalOut, n);
            }
        }

        template<typename T = ScopeSqlType::SqlStringFaceted<N>, bool quoted = false, int N = -1>
        void Write(const ScopeSqlType::SqlStringFaceted<N> & s)
        {
            WriteString<quoted>(s.Buffer(), s.Size());
        }

#endif // PLUGIN_TYPE_SYSTEM

        template<typename T = char, bool quoted = false>
        void Write(const char &s)
        {
            WriteInteger<char, quoted>(s, true);
        }

        template<typename T = unsigned char, bool quoted = false>
        void Write(const unsigned char &s)
        {
            WriteInteger<unsigned char, quoted>(s, true);
        }

        template<typename T = short, bool quoted = false>
        void Write(const short &s)
        {
            WriteInteger<short, quoted>(s, true);
        }

        template<typename T = unsigned short, bool quoted = false>
        void Write(const unsigned short &s)
        {
            WriteInteger<unsigned short, quoted>(s, true);
        }

        template<typename T = wchar_t, bool quoted = false>
        void Write(const wchar_t &s)
        {
            WriteInteger<wchar_t, quoted>(s, true);
        }

        template<typename T = int, bool quoted = false>
        void Write(const int &s)
        {
            WriteInteger<int, quoted>(s, true);
        }

        template<typename T = unsigned int, bool quoted = false>
        void Write(const unsigned int &s)
        {
            WriteInteger<unsigned int, quoted>(s, true);
        }

        template<typename T = __int64, bool quoted = false>
        void Write(const __int64 &s)
        {
            WriteInteger<__int64, quoted>(s, true);
        }

        template<typename T = unsigned __int64, bool quoted = false>
        void Write(const unsigned __int64 &s)
        {
            WriteInteger<unsigned __int64, quoted>(s, true);
        }

        template<typename T = bool, bool quoted = false>
        void Write(const bool & s)
        {            
            if (s)
            {
                WriteToOutput<quoted>(x_True, sizeof(x_True));
            }
            else
            {
                WriteToOutput<quoted>(x_False, sizeof(x_False));
            }
        }

        template<bool quoted>
        FORCE_INLINE void WriteDouble(const double & s)
        {
#if _MSC_VER < 1900
            // Enable two digit for exponent
            _set_output_format(_TWO_DIGIT_EXPONENT);
#endif

            // Check if it is special float/double value that needs special handling.
            if (WriteInfNaN<double,quoted>(s))
            {
                return;
            }

            char buf[100];

            int n = sprintf_s(buf, "%.15G", s);

            // read the value back for round-trip testing.
            double tmp = strtod(buf, NULL);

            // If 15 digit precision is not enough for the round-trip, we need
            // to use 17 digit precision.
            if (tmp != s)
            {
                n = sprintf_s(buf, "%.17G", s);
            }

            SCOPE_ASSERT(n>0);

            if ( n > 0 )
            {
                WriteString<quoted>(buf, n);
            }
        }

        template<typename T = double, bool quoted = false>
        void Write(const double & s)
        {
            if (doubleToFloat)
            {
                float f = (float)s;
                Write(f);
            }
            else
            {
                WriteDouble<quoted>(s);
            }
        }

        template<typename T = float, bool quoted = false>
        void Write(const float & s)
        {
#if _MSC_VER < 1900
            // Enable two digit for exponent
            _set_output_format(_TWO_DIGIT_EXPONENT);
#endif

            // Check if it is special float/double value that needs special handling.
            if (WriteInfNaN<float,quoted>(s))
            {
                return;
            }

            char buf[100];

            int n = sprintf_s(buf, "%.7G", s);

            // read the value back for round-trip testing.
            double tmp = strtod(buf, NULL);
            if (tmp < -FLT_MAX || tmp > FLT_MAX)
            {
                // if value is overflow use 9 digit precision
                n = sprintf_s(buf, "%.9G", s);
            }
            else
            {
                // If 7 digit precision is not enough for the round-trip, we need
                // to use 9 digit precision.
                if (s != (float)tmp)
                {
                    n = sprintf_s(buf, "%.9G", s);
                }
            }

            SCOPE_ASSERT(n>0);

            if ( n > 0 )
            {
                WriteString<quoted>(buf, n);
            }
        }

        template<class I, typename T>
        void Write(const ScopeSqlType::SqlNativeNullable<ScopeSqlType::ArithmeticSqlType<I, T>> & s)
        {
            Write(s.get().getValue());
        }

#if !defined(SCOPE_NO_UDT)
        // Serialize a scope supported UDT type, no quotes printed
        template<int UserDefinedTypeId, template<int> class UserDefinedType>
        void Write(const UserDefinedType<UserDefinedTypeId> & s)
        {
            if(s.IsNull())
            {
                // handle null value
                if (escape || nullEscape != nullptr)
                {
                    WriteNull();
                }

                return;
            }

            s.TextSerialize(this);
        }

#endif // SCOPE_NO_UDT

        void WriteNewLine()
        {

            WriteToOutput(rowDelimiter, rowDelimiterLen);
            
            // signal the new line - possible start of a new extent
            StartNewLine();
        }

        void WriteParameter(unsigned long parameter, SIZE_T length)
        {
            // we don't need to escape the parameter
            // re-encode parameter back into characters
            switch (length)
            {
            default:
                SCOPE_ASSERT(false);
            case 4:
                WriteChar((parameter >> 24) & 0xff);
                // Fall through
            case 3:
                WriteChar((parameter >> 16) & 0xff);
                // Fall through
            case 2:
                WriteChar((parameter >> 8) & 0xff);
                // Fall through
            case 1:
                WriteChar(parameter & 0xff);
            }
        }

        void WriteDelimiter()
        {
            WriteParameter(delimiter, delimiterLen);
        }

        void WriteMetadata(PartitionMetadata *)
        {
            // do nothing
        }

        void WriteRuntimeStats(TreeNode & root)
        {
            m_asyncOutput.WriteRuntimeStats(root);
        }

        LONGLONG GetTotalIoWaitTime()
        {
            return m_asyncOutput.GetTotalIoWaitTime();
        }

        LONGLONG GetInclusiveTimeMillisecond()
        {
            return m_asyncOutput.GetInclusiveTimeMillisecond();
        }

        CosmosOutput& GetOutputer()
        {
            return m_asyncOutput;
        }
    };

    //
    // SchemaDef serialization
    //

    template<class InputStream>
    class StreamReader
    {
    public:
        template<class T>
        static void Do(InputStream & stream, T & t)
        {
            stream.Read(t);
        }
    };

    template<class OutputStream>
    class StreamWriter
    {
    public:
        template<class T>
        static void Do(OutputStream & stream, const T & t)
        {
            stream.Write(t);
        }
    };

    class NonExistentPartitionMetadata : public PartitionMetadata
    {
        __int64 m_partitionIndex;
    public:    
        static PartitionMetadata* CreateNonExistentPartitionMetadata()
        {
            return new NonExistentPartitionMetadata(PartitionMetadata::PARTITION_NOT_EXIST);
        }

        NonExistentPartitionMetadata(__int64 partitionIndex) : m_partitionIndex(partitionIndex)
        {
        }

        __int64 GetPartitionId() const
        {
            return m_partitionIndex;
        }

        int GetMetadataId() const
        {
            return -1;
        }

        void Serialize(BinaryOutputStream * stream)
        {
            stream->Write(m_partitionIndex);
            unsigned char flags = 0;
            stream->Write(flags);
        }

        void WriteRuntimeStats(TreeNode & )
        {
        }

		virtual ScopeEngine::OperatorRequirements GetOperatorRequirements()
        {
			return ScopeEngine::OperatorRequirements();
        }

    };

    struct StreamingInputParams
    {
        volatile UINT64     Sn;
        bool                NeedSkipInputRecovery;

        StreamingInputParams()
        {
            Sn = 0;
            NeedSkipInputRecovery = false;
        }
    };

#pragma pack(push)
#pragma pack(1)
    class ScopeCEPCheckpointManager
    {
    private:
        IncrementalAllocator* m_alloc;
        CRITICAL_SECTION m_criticalSection;
        UINT64 m_seqNumber;
        ScopeDateTime m_lastCTITime;
        ScopeDateTime m_lastCheckpointWallclockTime;
        ScopeDateTime m_lastCheckpointLogicalTime;
        ScopeDateTime m_startCTITime;
        std::string m_checkpointUriPrefix;
        std::string m_currentScopeCEPState;
        std::string m_lastScopeCEPCheckpointState;
        std::string m_startScopeCEPState;
        UINT m_minimalSecondsCheckpointWallclockInterval;
        UINT m_minimalSecondsCheckpointLogicalTimeInterval;
        UINT m_currentCheckpointId;
        UINT m_inputCount;
        ScopeCEPMode m_scopeCEPMode;
        bool m_startReport;
        StreamingInputParams* m_inputParams;

    public:
        const static UINT64 SYSTEM_RESERVED_SN_FOR_CACHE_METADATA = (UINT64)-1;

        ScopeCEPCheckpointManager() 
            : m_seqNumber(0), 
            m_currentCheckpointId(0), 
            m_inputParams(nullptr),
            m_startReport(false), 
            m_inputCount(0), 
            m_scopeCEPMode(ScopeCEPMode::SCOPECEP_MODE_NONE),
            m_alloc(NULL),
            m_minimalSecondsCheckpointWallclockInterval(300),
            m_minimalSecondsCheckpointLogicalTimeInterval(600)
        {
            InitializeCriticalSectionAndSpinCount(&m_criticalSection, 10);
        }

        void SetMinimalCheckpointInterval(UINT seconds) { m_minimalSecondsCheckpointWallclockInterval = seconds; }

        void Reset()
        {
            m_seqNumber = 0;
            m_currentCheckpointId = 0;
            if (m_inputParams) 
            {
                delete[] m_inputParams;
                m_inputParams = nullptr;
            }
            m_startReport = false;
            m_inputCount = 0;
            m_scopeCEPMode = SCOPECEP_MODE_NONE;
            m_lastCTITime = ScopeDateTime::MinValue;
            m_startCTITime = ScopeDateTime::MinValue;
            m_lastCheckpointWallclockTime = ScopeDateTime::MinValue;
            m_lastCheckpointLogicalTime = ScopeDateTime::MinValue;

            m_lastScopeCEPCheckpointState = "";
            m_currentScopeCEPState = "";
            m_startScopeCEPState = "";
            m_checkpointUriPrefix = "";
            
        }

        void SetScopeCEPModeAndStartState(ScopeCEPMode mode, const std::string& state, const std::string& checkpointUriPrefix)
        {
            m_scopeCEPMode = mode;
            if (state.compare("null") != 0)
            {
                m_startScopeCEPState = state;
            }
            m_checkpointUriPrefix = checkpointUriPrefix;
        }

        void StartReport()
        {
            m_startReport = true;
        }

        ~ScopeCEPCheckpointManager() 
        { 
            delete [] m_inputParams;
            if (!m_alloc)
            {
                delete m_alloc;
            }
        }

        std::string GenerateCheckpointUri()
        {
            char buf[64];
            memset(buf, 0, 64);
            _itoa_s(m_currentCheckpointId++, buf, 64, 10);
            return m_checkpointUriPrefix + buf;
        }

        UINT GetCurrentCheckpointId() const { return m_currentCheckpointId - 1; } 

        const string GetCurrentScopeCEPState()
        {
            AutoCriticalSection aCS(&m_criticalSection);
            string ret = m_lastScopeCEPCheckpointState;
            ret += "\n";
            ret += m_currentScopeCEPState;
            return ret;
        }

        string& GetStartScopeCEPState() {  return m_startScopeCEPState; }
        ScopeDateTime GetStartCTITime() {  return m_startCTITime; }

        void SetScopeCEPMode(ScopeCEPMode mode) { m_scopeCEPMode = mode; }
        ScopeCEPMode GetScopeCEPMode() { return m_scopeCEPMode; }

        void SetCheckpointUriPrefix(const std::string& prefix) { m_checkpointUriPrefix = prefix; }
        void UpdateCurrentScopeCEPState(const std::string * uri = nullptr)
        {
            AutoCriticalSection aCS(&m_criticalSection);

            if (m_startReport)
            {
                ScopeDateTime now = ScopeDateTime::Now();
                ostringstream ss;
                ss << m_lastCTITime.ToFileTime(true) << ";";
                ss << m_seqNumber << ";" << m_inputCount << ";";
                for (UINT32 i = 0; i < m_inputCount; i++)
                {
                    ss << m_inputParams[i].Sn << ";";
                }
                ss << "0;";
                m_currentScopeCEPState = ss.str();

                if (uri != nullptr)
                {
                    m_lastCheckpointLogicalTime = m_lastCTITime;
                    m_lastCheckpointWallclockTime = now;
                    ss << uri->c_str();
                    m_lastScopeCEPCheckpointState = ss.str();
                }
                char nowString[256];
                now.ToString(nowString, sizeof(nowString));
                if (uri != nullptr)
                {
                    SCOPE_LOG_FMT_INFO("CheckpointManager", "%s:Checkpoint Status %s", nowString, m_lastScopeCEPCheckpointState.c_str());
                }
                else
                {
                    SCOPE_LOG_FMT_INFO("CheckpointManager", "%s:CTI Status %s", nowString, m_currentScopeCEPState.c_str());
                }
            }
        }

        void SCOPE_ENGINE_API UpdateLocalDebugInfo(const string& streamPath);

        BinaryInputStream* GenerateScopeCEPCheckpointFromInitialState() 
        {
            if (!m_startScopeCEPState.empty())
            {
                char c;
                istringstream ss(m_startScopeCEPState);
                __int64 ts;
                ss >> ts;
                m_lastCTITime = ScopeDateTime::FromFileTime(ts, true);
                m_startCTITime = m_lastCTITime;
                ss >> c;
                ss >> m_seqNumber;
                ss >> c;
                UINT inputCount;
                ss >> inputCount;
                ss >> c;
                SCOPE_ASSERT(inputCount == m_inputCount);
                for (UINT32 i = 0; i < m_inputCount; i++)
                {
                    ss >> (UINT64)m_inputParams[i].Sn;
                    ss >> c;
                }

                UINT skipRecoveryInputCount;
                ss >> skipRecoveryInputCount;
                ss >> c;
                SCOPE_ASSERT(skipRecoveryInputCount <= m_inputCount);
                for (UINT32 i = 0; i < skipRecoveryInputCount; i++)
                {
                    UINT skippedInputIndex;
                    ss >> skippedInputIndex;
                    ss >> c;
                    SCOPE_ASSERT(skippedInputIndex < m_inputCount);
                    m_inputParams[skippedInputIndex].NeedSkipInputRecovery = true;
                }

                string uri = m_startScopeCEPState.substr(ss.tellg());
                SCOPE_LOG_FMT_INFO("CheckpointManager", "loading checkpoint from %s", uri.c_str());
                UpdateLocalDebugInfo(uri);
                IOManager::GetGlobal()->AddInputStream(uri, uri);

                if (!m_alloc)
                {
                    m_alloc = new IncrementalAllocator(MemoryManager::x_maxMemSize, "ScopeCEPCheckpointManager");
                }

                InputFileInfo input;
                input.inputFileName = uri;
                BinaryInputStream* checkpoint = new BinaryInputStream(input, m_alloc, 0x400000, 2);

                checkpoint->Init();

                return checkpoint;
            }
            else
            {
                return NULL;
            }
        }

        UINT64 IncrementSeqNumber()
        {
            return InterlockedIncrement64((volatile INT64 *)&m_seqNumber);
        }

        UINT64 DecrementSeqNumber()
        {
            return InterlockedDecrement64((volatile INT64 *)&m_seqNumber);
        }

        UINT64 GetCurrentSeqNumber()
        {
            return m_seqNumber;
        }

        void SetSeqNumber (UINT64 sn) { m_seqNumber = sn; }

        void UpdateLastCTITime(const ScopeDateTime& ts) { m_lastCTITime = ts; }

        void CreateTrackingSNArrayForExtract(UINT count)
        {
            SCOPE_ASSERT(m_inputParams == nullptr);
            if (count > 0)
            {
                m_inputParams = new StreamingInputParams[count];
            }
            m_inputCount = count;
        }

        StreamingInputParams* GetInputParameter(UINT index)
        {
            SCOPE_ASSERT(index < m_inputCount);
            return m_inputParams + index;
        }

        bool IsWorthyToDoCheckpoint(const ScopeDateTime& cti) 
        { 
            ScopeDateTime now = ScopeDateTime::Now();
            bool result = (ScopeTimeSpan(now.Ticks() - m_lastCheckpointWallclockTime.Ticks()).TotalSeconds() >= m_minimalSecondsCheckpointWallclockInterval ||
                ScopeTimeSpan(cti.Ticks() - m_lastCheckpointLogicalTime.Ticks()).TotalSeconds() >= m_minimalSecondsCheckpointLogicalTimeInterval);
            if (!result)
            {
                SCOPE_LOG_FMT_INFO(
                    "CheckpointManager", 
                    "Skip CTIWithCheckpoint %I64u at %I64u, last Checkpoint (time:%I64u, CTI %I64u)",
                    cti.ToFileTime(true), 
                    now.ToFileTime(true),
                    m_lastCheckpointWallclockTime.ToFileTime(true),
                    m_lastCheckpointLogicalTime.ToFileTime(true));
            }
            return result;
        }

        template<class Operator>
        void InitiateCheckPointChain(Operator* outputer)
        {
            BinaryOutputStream* checkpoint = InitiateCheckPointChainInternal(outputer);
            checkpoint->Finish();
            checkpoint->Close();
            delete checkpoint;
        }

        template<class Operator>
        BinaryOutputStream* InitiateCheckPointChainInternal(Operator* outputer)
        {
            ULONGLONG start = GetTickCount64();
            std::string uri = GenerateCheckpointUri();
            IOManager::GetGlobal()->AddOutputStream(uri, uri, "", ScopeTimeSpan::TicksPerWeek);
            BinaryOutputStream* checkpoint = new BinaryOutputStream(uri, 0x400000, 2);
            checkpoint->Init();
            outputer->DoScopeCEPCheckpoint(*checkpoint);
            checkpoint->Flush(true);
            UpdateCurrentScopeCEPState(&uri);
            printf("Completed checkpoint in %I64u milliseconds: %s\r\n", GetTickCount64() - start, uri.c_str());
#ifdef SCOPECEPTESTMODE
            SetEnvironmentVariable("SCOPECEPSTATUS", m_lastScopeCEPCheckpointState.c_str());
#endif
            return checkpoint;
        }
    };
#pragma pack(pop)

    class StreamingOutputChannel // I'll need rename it to streaming channel since it has functionality for both input and output,
                                 // but I'll do it in a separate checkin so I don't have to touch too many places in this version.
    {
    public:
        ~StreamingOutputChannel(){}
        virtual void SetAllowDuplicateRecord(bool allow) = 0;
        virtual bool TryAdvanceCTI(const ScopeDateTime& currentCTI, const ScopeDateTime& finalOutputMinCTI, bool hasBufferedData) = 0;
        virtual void UpdateCacheMetadata(char* buffer, UINT32 length) = 0;
        virtual void GetCacheMetadata(OUT char*& pBuffer, OUT UINT32& pLength) = 0;
        virtual void EnableEofPolling(bool enable) = 0;
    };
    
    inline void CosmosInput::SaveState(BinaryOutputStream& output, UINT64 position)
    {
        std::string stateString;
        m_scanner->SaveState(stateString, position);
        output.Write((unsigned int)stateString.size() + 1); //always include null terminator
        output.Write(stateString.c_str(), (unsigned int)stateString.size() + 1);
    }

    inline void CosmosInput::LoadState(BinaryInputStream& input)
    {
        unsigned int length;
        input.Read(length); 
        m_recoverState.resize(length - 1);
        input.Read(&m_recoverState[0], length);
    }

    inline void CosmosInput::ClearState()
    {
        m_recoverState.clear();
    }

    inline void CosmosOutput::SaveState(BinaryOutputStream& output)
    {
        std::string stateString;
        m_scanner->SaveState(stateString, 0);
        output.Write((unsigned int)stateString.size() + 1); //always include null terminator
        output.Write(stateString.c_str(), (unsigned int)stateString.size() + 1);
    }

    inline void CosmosOutput::LoadState(BinaryInputStream& input)
    {
        unsigned int length;
        input.Read(length); 
        m_recoverState.resize(length - 1);
        input.Read(&m_recoverState[0], length);
    }
} // namespace ScopeEngine

extern ScopeEngine::ScopeCEPCheckpointManager* g_scopeCEPCheckpointManager;
