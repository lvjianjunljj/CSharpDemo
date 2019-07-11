// This file defines all the constant for operator resource(e.g memory) requirements,
// which are shared by native and managed components.

// enforces creation of constant of unsigned long long type
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Any__Const_TypeCastMultiplier, 1)
// operator needs to load all rows from input stream to memory for optimal performance 
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Any__Const_MaxMemorySize, Any__Const_TypeCastMultiplier * 6 * 1024 * 1024 * 1024)

/************************************************************************************************************************
Declaration of constants shared with compiler
Constant name format: [Configuration]__Const_[Name]
Examples
1. Any__Const_DefaultInputBufSize
    Any - constant value is the same for all configurations
    DefaultInputBufSize - name of constant that represent default size of input stream buffer
2. Cosmos__Const_MaxInMemoryRowSize
    Cosmos - constant value for jobs compiled/executed in Cosmos environment
    MaxInMemoryRowSize - name of constant that represent default maximum allowed size of row
*************************************************************************************************************************/

// 4Mb is size of Azure append block, 250Mb is size of Cosmos extent
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Azure__Const_MaxInMemoryRowSize, 4 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(CosmosBigRow__Const_MaxInMemoryRowSize, Any__Const_TypeCastMultiplier * 512 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Cosmos__Const_MaxInMemoryRowSize, Any__Const_TypeCastMultiplier * 250 * 1024 * 1024)

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Azure__Const_MaxColumnSize, 4 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(CosmosBigRow__Const_MaxColumnSize, Any__Const_TypeCastMultiplier * 512 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Cosmos__Const_MaxColumnSize, Any__Const_TypeCastMultiplier * 250 * 1024 * 1024)

// memory requirements defaults for input/output physical devices
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Any__Const_DefaultInputBufSize, 4 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Any__Const_MinInputBufSize, 4096)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Any__Const_DefaultInputBufCount, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Any__Const_DefaultOutputBufSize, 4 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Any__Const_MinOutputBufSize, 4096)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Any__Const_DefaultOutputBufCount, 6)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Any__Const_DefaultNetworkInputBufSize, 1 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Any__Const_DefaultNetworkOutputBufSize, 1 * 1024 * 1024)

// additional constants for input/output code

// size of stream statistics (100 Mb) + size of key range (4Mb) + bloomfilter (1Mb) + index 16Mb + 250Mb intermediate page
// TODO some of limits (like statistics size) need to be verified and enforced
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Azure__Const_SStreamMetadataSize, Any__Const_TypeCastMultiplier * 13 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(CosmosBigRow__Const_SStreamMetadataSize, Any__Const_TypeCastMultiplier * 121 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Cosmos__Const_SStreamMetadataSize, Any__Const_TypeCastMultiplier * 121 * 1024 * 1024)

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Azure__Const_KeyRangeMaxMemory, Any__Const_TypeCastMultiplier * 16 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(CosmosBigRow__Const_KeyRangeMaxMemory, Cosmos__Const_MaxInMemoryRowSize / 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Cosmos__Const_KeyRangeMaxMemory, Cosmos__Const_MaxInMemoryRowSize / 2)

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Any__Const_TextConverterSize, Any__Const_TypeCastMultiplier * 16 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Any__Const_ReadThrottlingLimit, 250)

// UDO memory limit constants

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Azure__Const_UDODefaultMemory, Any__Const_TypeCastMultiplier * 512 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(CosmosBigRow__Const_UDODefaultMemory, Any__Const_TypeCastMultiplier * 1024 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Cosmos__Const_UDODefaultMemory, Any__Const_TypeCastMultiplier * 1024 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Azure__Const_UDFDefaultMemory, 2 * Azure__Const_MaxInMemoryRowSize)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(CosmosBigRow__Const_UDFDefaultMemory, 2 * Cosmos__Const_MaxInMemoryRowSize)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Cosmos__Const_UDFDefaultMemory, 2 * Cosmos__Const_MaxInMemoryRowSize)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Azure__Const_UDFPredicateDefaultMemory, Azure__Const_MaxInMemoryRowSize)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(CosmosBigRow__Const_UDFPredicateDefaultMemory, Cosmos__Const_MaxInMemoryRowSize)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Cosmos__Const_UDFPredicateDefaultMemory, Cosmos__Const_MaxInMemoryRowSize)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Azure__Const_UDADefaultMemory, Any__Const_TypeCastMultiplier * 512 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(CosmosBigRow__Const_UDADefaultMemory, Any__Const_TypeCastMultiplier * 1024 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Cosmos__Const_UDADefaultMemory, Any__Const_TypeCastMultiplier * 1024 * 1024 * 1024)

// miscellaneous constants for different runtime operators

// for Cosmos job 12 batches that can keep up two 400Mb
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Azure__Const_ParallelUnionAllMaxMemory, Any__Const_TypeCastMultiplier * 6 * 40 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(CosmosBigRow__Const_ParallelUnionAllMaxMemory, Any__Const_TypeCastMultiplier * 12 * 400 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Cosmos__Const_ParallelUnionAllMaxMemory, Any__Const_TypeCastMultiplier * 12 * 400 * 1024 * 1024)

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Azure__Const_HistogramMaxMemory, Any__Const_TypeCastMultiplier * 16 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(CosmosBigRow__Const_HistogramMaxMemory, Cosmos__Const_MaxInMemoryRowSize)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Cosmos__Const_HistogramMaxMemory, Cosmos__Const_MaxInMemoryRowSize)

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Azure__Const_SampleMaxMemory, Any__Const_TypeCastMultiplier * 32 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(CosmosBigRow__Const_SampleMaxMemory, Cosmos__Const_MaxInMemoryRowSize)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Cosmos__Const_SampleMaxMemory, Cosmos__Const_MaxInMemoryRowSize)

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Azure__Const_RangeBoundsMaxMemory, Any__Const_TypeCastMultiplier * 32 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(CosmosBigRow__Const_RangeBoundsMaxMemory, Cosmos__Const_MaxInMemoryRowSize)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Cosmos__Const_RangeBoundsMaxMemory, Cosmos__Const_MaxInMemoryRowSize)

// in sstream partition boundaries (2 keys) can't be bigger than 250Mb
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Azure__Const_PartitionPayloadMetadataSize, Any__Const_TypeCastMultiplier * 8 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(CosmosBigRow__Const_PartitionPayloadMetadataSize, Cosmos__Const_MaxInMemoryRowSize)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Cosmos__Const_PartitionPayloadMetadataSize, Cosmos__Const_MaxInMemoryRowSize)

/************************************************************************************************************************
Declaration of constants that represents memory requirements for different types of input/output stream
Constant name format: [Input|Output]_[StreamType]__[Unit]_[PropertyName]
Unit defines how required memory size is expressed. It can be bytes, rows or metadata
Examples
1. Input_UStream__Size_FormatterMemory
    UStream - unstructured stream
    Size - constant value is number of bytes
    FormatterMemory - property name for amount of memory to convert data from between in-memory and on-disk format
*************************************************************************************************************************/

// buffer can be expanded up to 250Mb asynchronously
// for unstructured stream buffer is only expanded for final outputs; correct max size is set by compiler during estimation of operator memory consumption
// for structured stream buffer should be able to contain full row so it can be expanded during input and output

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Input_UStream__Size_MaxBufferSize, Any__Const_DefaultInputBufSize)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Input_UStream__Size_BufferCount, Any__Const_DefaultInputBufCount)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Input_UStream__Size_FormatterMemory, 1)

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Input_SStream__Row_MaxBufferSize, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Input_SStream__Size_BufferCount, 4)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Input_SStream__Size_FormatterMemory, 1)

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Output_UStream__Size_MaxBufferSize, Any__Const_DefaultOutputBufSize)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Output_UStream__Size_BufferCount, Any__Const_DefaultOutputBufCount)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Output_UStream__Size_FormatterMemory, 1)

// sstream outputter uses intermediate page to reformat rows which can take up to 250Mb (max row size)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Output_SStream__Row_MaxBufferSize, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Output_SStream__Size_BufferCount, Any__Const_DefaultOutputBufCount)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Output_SStream__Row_FormatterMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Output_SStream__ConstSStreamMetadataSize_FormatterMemory, 1)

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Output_MetadataSStream__ConstSStreamMetadataSize_MaxBufferSize, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Output_MetadataSStream__Size_BufferCount, Any__Const_DefaultOutputBufCount)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Output_MetadataSStream__ConstSStreamMetadataSize_FormatterMemory, 1)

/************************************************************************************************************************
Declaration of constants that represents memory requirements for different types of row buffers
Constant name format: [RowBuffer]_[BufferType]__[Unit]_[PropertyName]
Unit defines how required memory size is expressed. It can be bytes, rows or metadata
Examples
1. RowBuffer_Hash__Count_OutputUStream
    Hash - buffer used in operators storing rows in hash structure
    Count - constant value is number streams/etc
    OutputUStream - buffer creates output unstructured stream for spilling
*************************************************************************************************************************/
// Minimal size of row buffer is 100Mb since 2 rows (8Mb) in SqlIp looks two small
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Any__Const_MinRowBufferSize, Any__Const_TypeCastMultiplier * 100 * 1024 * 1024)

// at least 2 rows must fit into memory for sorting input data
// memory that is required to read spilled buffer is managed by sorter separately
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(RowBuffer_Sorter__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(RowBuffer_Sorter__Size_OptimalMemory, Any__Const_MaxMemorySize)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(RowBuffer_Sorter__Count_OutputUStream, 1)

// hash combiner should be able to keep the whole input from one size in memory for optimal performance
// hash combiner can create up to PartitionCount number of output streams for spilling hashtable (defined in compiler)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(RowBuffer_HashWithSpill__Size_MinMemory, Any__Const_TypeCastMultiplier *  1024 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(RowBuffer_HashWithSpill__Size_OptimalMemory, Any__Const_MaxMemorySize)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(RowBuffer_HashWithSpill__Count_OutputUStream, 0)

// hash aggregation should be able to keep all different key/aggResult pairs in memory
// local hash aggregation return partial results instead of spilling
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(RowBuffer_HashNoSpill__Size_MinMemory, Any__Const_TypeCastMultiplier * 1024 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(RowBuffer_HashNoSpill__Size_OptimalMemory, Any__Const_MaxMemorySize)

// Buffer should be able to contain 1 row in memory and 1 row in extractor for spilled data
// Such buffer is used in merge join and spool
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(RowBuffer_RowCacheExtractor__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(RowBuffer_RowCacheExtractor__Size_OptimalMemory, Any__Const_MaxMemorySize)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(RowBuffer_RowCacheExtractor__Count_OutputUStream, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(RowBuffer_RowCacheExtractor__Count_InputUStream, 1)

/************************************************************************************************************************
Declaration of constants that represents memory requirements for physical operators
Constant name format: [OperatorClassName]_[OperatorProperty]__[Unit]_[RequirementName]
OperatorProperty is additional feature (e.g algorithm) that can change amount of memory used by operator
Unit defines how required memory size is expressed. It can be bytes, rows or metadata
Examples
1. Extractor__Row_MinMemory
    Row - constant value is number of rows
    MinMemory - minimal amount of memory required by operator to process data
2. Sorter__Count_RowBufferSorter
    HashPartitioner - algorithm used for data partitioning
    Count - number of buffers
    RowBufferSorter - type of row buffer used in operator
3. StreamAggregator__Size_Policy
    Policy - some of memory requirements are defined during compilation and passed through policy
*************************************************************************************************************************/

// unstructured stream extractor stores latest extracted row
// non-user extractor gets parameter from compiler whether to extract and store metadata or whether to use text convertor
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Extractor__Row_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Extractor__Count_InputUStream, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Extractor__Size_Policy, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(StreamingExtractor__Row_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(StreamingExtractor__Count_InputUStream, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(StreamingExtractor__Size_Policy, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(ScopeExtractor__Row_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(ScopeExtractor__Count_InputUStream, 1)

// structured stream extractor stores latest extracted row
// policy contains number of buffers in data unit reader and if extractor creates metadata (PartitionPayloadMetadata)
// compiler determines whether sstream extractor uses correlated schema for index lookup join or keyrange metafile
// we assume that managed extractor for V2 sstream consumes same amount of memory as native ones
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SStreamExtractor__Row_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SStreamExtractor__Count_InputSStream, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SStreamExtractor__Size_Policy, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(ScopeSStreamV2Extractor__Row_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(ScopeSStreamV2Extractor__Count_InputSStream, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(ScopeSStreamV2Extractor__Size_Policy, 0)

// stream statistics is the biggest part of metadata (up to 100Mb in Cosmos), estimate memory need as max row size
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SStreamMetadataExtractor__Row_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SStreamMetadataExtractor__Count_InputUStream, 1)

// structured stream extractor stores latest extracted row
// all metadata payload is stored in memory but we only need metadata for one partition (estimate as 250Mb)
// policy determines number and size of input buffers
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(IntermediateSStreamExtractor__Row_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(IntermediateSStreamExtractor__Count_InputSStream, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(IntermediateSStreamExtractor__Size_Policy, 0)

// operator doesn't creates rows by itself
// compiler determines whether RowGenerator uses keyrange metafile or creates metadata
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(RowGenerator__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(RowGenerator__Size_Policy, 0)

// separate SStreamOutputStream is created per each column group
// metadata is written separately after all data are stored on disk
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SStreamOutputer__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SStreamOutputer__Size_Policy, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(IntermediateSStreamOutputer__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(IntermediateSStreamOutputer__Count_OutputSStream, 1)
// metadata is kept in memory before written to disk
// stream statistics is the biggest part of metadata (up to 100Mb in Cosmos), estimate memory need as max row size
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SStreamMetadataOutputer__Row_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SStreamMetadataOutputer__Count_OutputMetadataSStream, 1)
// outputter to final stream need to preserve row boundaries so it may need to expand buffer up to 250Mb (max row size)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Outputer__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Outputer__Count_OutputUStream, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(ScopeOutputer__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(ScopeOutputer__Count_OutputUStream, 1)

// operators that don't consume additional memory
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Merger__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HistogramMerger__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(ScopeCreateContext__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(FilterTransformer__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(FilterTransformer__Size_Policy, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Topper__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Ranker__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(CoordinatedJoin__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Splitter__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Splitter__Size_Policy, 0)

// compiler tries to estimate how much memory can be taken by batches in the input queue
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(ParallelUnionAll__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(ParallelUnionAll__Size_Policy, 0)

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Sorter__Row_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Sorter__Count_RowBufferSorter, 1)

// Prefix sorter use additional memory for key
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(PrefixSorter__Row_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(PrefixSorter__Count_RowBufferSorter, 1)

// space for output row generated by user processor
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(ScopeProcessor__Row_MinMemory, 1)

// space for output row generated by user reducer and group key
// ScopeProcessor operator that calls ScopeReducerManagedImpl
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(ScopeProcessor_ManagedReducer__Row_MinMemory, 2)

// Partitioners
// Number of output streams is decided during compilation

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(IndexedPartitionProcessor_HashPartitioner__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(PartitionOutputer_HashPartitioner__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(PartitionOutputer_HashPartitioner__Count_OutputUStream, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(PartitionOutputer_HashPartitioner__Size_Policy, 0)

// range partitioner needs space for storing range boundaries
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(IndexedPartitionProcessor_RangePartitioner__ConstRangeBoundsMaxMemory_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(IndexedPartitionProcessor_RangePartitioner__Row_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(PartitionOutputer_RangePartitioner__ConstRangeBoundsMaxMemory_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(PartitionOutputer_RangePartitioner__Row_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(PartitionOutputer_RangePartitioner__Count_OutputUStream, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(PartitionOutputer_RangePartitioner__Size_Policy, 0)

// hash combiner should be able to keep the whole input from one size in memory for optimal performance
// hash combiner can create up to PartitionCount number of output streams for spilling hashtable (defined in compiler)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombiner_HashInnerJoiner__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombiner_HashInnerJoiner__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombiner_HashLeftOuterJoiner__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombiner_HashLeftOuterJoiner__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombiner_HashLeftSemiJoiner__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombiner_HashLeftSemiJoiner__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombiner_HashLeftAntiSemiJoiner__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombiner_HashLeftAntiSemiJoiner__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombiner_HashRightOuterJoiner__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombiner_HashRightOuterJoiner__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombiner_HashRightSemiJoiner__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombiner_HashRightSemiJoiner__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombiner_HashRightAntiSemiJoiner__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombiner_HashRightAntiSemiJoiner__Count_RowBufferHashWithSpill, 1)


DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerV2_HashInnerJoinerV2__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerV2_HashInnerJoinerV2__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerV2_HashLeftOuterJoinerV2__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerV2_HashLeftOuterJoinerV2__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerV2_HashLeftSemiJoinerV2__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerV2_HashLeftSemiJoinerV2__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerV2_HashLeftAntiSemiJoinerV2__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerV2_HashLeftAntiSemiJoinerV2__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerV2_HashRightOuterJoinerV2__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerV2_HashRightOuterJoinerV2__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerV2_HashRightSemiJoinerV2__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerV2_HashRightSemiJoinerV2__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerV2_HashRightAntiSemiJoinerV2__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerV2_HashRightAntiSemiJoinerV2__Count_RowBufferHashWithSpill, 1)

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerWithFallback_HashInnerJoinerV2__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerWithFallback_HashInnerJoinerV2__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerWithFallback_HashLeftOuterJoinerV2__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerWithFallback_HashLeftOuterJoinerV2__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerWithFallback_HashLeftSemiJoinerV2__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerWithFallback_HashLeftSemiJoinerV2__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerWithFallback_HashLeftAntiSemiJoinerV2__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerWithFallback_HashLeftAntiSemiJoinerV2__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerWithFallback_HashRightOuterJoinerV2__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerWithFallback_HashRightOuterJoinerV2__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerWithFallback_HashRightSemiJoinerV2__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerWithFallback_HashRightSemiJoinerV2__Count_RowBufferHashWithSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerWithFallback_HashRightAntiSemiJoinerV2__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HashCombinerWithFallback_HashRightAntiSemiJoinerV2__Count_RowBufferHashWithSpill, 1)

// nested loop combiner uses sstream buffer to keep rows from one side in memory
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NestedLoopCombiner__Size_MinMemory, 0)

// all types of join need space for left and right side keys
// merge join should be able to keep rows with the same key in memory for optimal performance
// merge join contains 2 buffers, 1 for each side
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_InnerJoiner__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_InnerJoiner__Count_RowBufferRowCacheExtractor, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_FullOuterJoiner__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_FullOuterJoiner__Count_RowBufferRowCacheExtractor, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_LeftOuterJoiner__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_LeftOuterJoiner__Count_RowBufferRowCacheExtractor, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_RightOuterJoiner__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_RightOuterJoiner__Count_RowBufferRowCacheExtractor, 2)

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_CrossLoopJoiner__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_CrossLoopJoiner__Count_RowBufferRowCacheExtractor, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_CrossLoopJoinerDrvSortFromLeft__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_CrossLoopJoinerDrvSortFromLeft__Count_RowBufferRowCacheExtractor, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_CrossLoopJoinerDrvSortFromRight__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_CrossLoopJoinerDrvSortFromRight__Count_RowBufferRowCacheExtractor, 2)

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_LeftSemiJoiner__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_RightSemiJoiner__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_LeftAntiSemiJoiner__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_RightAntiSemiJoiner__Row_MinMemory, 2)

DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_UnionJoiner__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_UnionAllJoiner__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_IntersectJoiner__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_IntersectAllJoiner__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_ExceptJoiner__Row_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_ExceptAllJoiner__Row_MinMemory, 2)
// space for output row generated by user combiner plus left and right key
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(NativeCombinerWrapper_SqlIpUdoJoiner__Row_MinMemory, 3)

// space for output row generated by user combiner
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(ScopeCombiner__Row_MinMemory, 1)

// space for output row generated by user multiprocessor
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(ScopeMultiProcessor__Row_MinMemory, 1)

// space for leftmost boundary row and current key
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HistogramCollector__Row_MinMemory, 2)
// space for boundary rows (default 8000 buckets)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HistogramCollector__ConstHistogramMaxMemory_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HistogramCollector__Count_OutputUStream, 1)

// space for sample rows (default 4000 samples)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SampleCollector__ConstSampleMaxMemory_MinMemory, 2)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SampleCollector__Count_OutputUStream, 1)

// by default we create ConsumerCount * 3 buffer
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SpoolConsumer__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SpoolConsumer__Count_RowBufferRowCacheExtractor, 3)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SpoolProducer__Size_MinMemory, 0)

// one buffer can be spilled or loaded
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SpoolQueue__Count_OutputUStream, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SpoolQueue__Count_InputUStream, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SpoolQueue__Row_MinMemory, 1)

// space for 1 key
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(HistogramReducer__Row_MinMemory, 1)

// space for 1 key
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SampleReducer__Row_MinMemory, 1)

// hash aggregation should be able to keep all different key/aggResult pairs in memory
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(LocalHashAggregator__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(LocalHashAggregator__Count_RowBufferHashNoSpill, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(LocalHashAggregatorV2__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(LocalHashAggregatorV2__Count_RowBufferHashNoSpill, 1)

// stream aggregation should be able to keep current key in memory plus memory taken by aggregates in policy class
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(StreamAggregator__Row_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(StreamAggregator__Size_Policy, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(StreamRollup__Row_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(StreamRollup__Size_Policy, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(WindowAggregator__Row_MinMemory, 1)

// space for 1 key
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Asserter__Row_MinMemory, 1)

// space for group key and order by key
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SequenceProject__Row_MinMemory, 2)

// Aggregates

// need memory enough for 1 column
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_FIRST_FixedLength__Column_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_FIRST_VariableLength__Column_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_ANY_VALUE_FixedLength__Column_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_ANY_VALUE_VariableLength__Column_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_LAST_FixedLength__Column_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_LAST_VariableLength__Column_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_MAX_FixedLength__Column_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_MAX_VariableLength__Column_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_MIN_FixedLength__Column_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_MIN_VariableLength__Column_MinMemory, 1)

// don't use additional memory
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_SUM_FixedLength__Column_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_SUM_VariableLength__Column_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_UNCHECKED_SUMx2_FixedLength__Column_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_UNCHECKED_SUMx2_VariableLength__Column_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_UNCHECKED_SUM_FixedLength__Column_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_UNCHECKED_SUM_VariableLength__Column_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_COUNT_FixedLength__Column_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_COUNT_VariableLength__Column_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_COUNTIF_FixedLength__Column_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_COUNTIF_VariableLength__Column_MinMemory, 0)

// DISTINT uses hash table with fixed size
// Ideally it should accept memory quota as constructor parameter but optimizer tries to eliminate plans that contains this operator
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_DISTINCT_FixedLength__Size_MinMemory, Any__Const_TypeCastMultiplier * 6 * 1024 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_DISTINCT_VariableLength__Size_MinMemory, Any__Const_TypeCastMultiplier * 12 * 1024 * 1024 * 1024)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_MAP_AGG__Column_MinMemory, 1)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(Aggregate_ARRAY_AGG__Column_MinMemory, 1)

// Requirements for in-built sampler operators
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(DistinctSampler__Count_PoolEntries, 192 * 1024) // about 200K; the combination chosen to be roughly page sized // see comment in ScopeDistinctSampler
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(ScopeRBNode__Size_Entry, 64) // see comment in ScopeDistinctSampler
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SamplerOperator_UniformSampler__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SamplerOperator_DistinctSampler__Size_MinMemory, DistinctSampler__Count_PoolEntries * ScopeRBNode__Size_Entry)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SamplerOperator_UniverseSampler__Size_MinMemory, 0)
DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(SamplerOperator_ValueBiasedSampler__Size_MinMemory, 0)