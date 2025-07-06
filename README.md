# LSM-Tree with HNSW-Enhanced Semantic Search

# 这是上海交通大学软件工程——高级数据结构课程项目
总评100分
## Project Overview
This project implements a high-performance key-value storage system based on Log-Structured Merge Tree (LSM Tree), enhanced with semantic search capabilities through HNSW (Hierarchical Navigable Small World) algorithm. The system is designed to handle large-scale data storage and retrieval efficiently, particularly in high-concurrency scenarios.

For detailed performance analysis and test results, please refer to the report.pdf in each iteration's documentation.

## Project Evolution

### Iteration 1: Core LSM-Tree Implementation
- Efficient key-value storage with Log-Structured Merge Tree architecture
- Memory-first write operations with batch disk writes
- Multi-level storage structure
- Core operations:
  - PUT (insert/update key-value pairs)
  - GET (retrieve key-value pairs)
  - DEL (delete key-value pairs)
- Optimizations:
  - Bloom Filter for quick existence checks
  - SSTable (Sorted Strings Table) for efficient storage
  - Logical key-value separation in SSTable structure

### Iteration 2: Semantic Search Integration
- Enhanced LSM-tree with semantic search capabilities
- Integration of Embedding Models for semantic understanding
- Added support for similarity-based queries
- Use cases:
  - Semantic product search in e-commerce
  - Content-based recommendations
  - Similar item discovery

### Iteration 3: HNSW Algorithm Implementation
- Integrated HNSW (Hierarchical Navigable Small World) algorithm
- Configurable parameters:
  - M (default: 6) - Maximum number of connections per layer
  - M_max (default: 8) - Maximum number of connections during construction
  - efConstruction (default: 30) - Size of dynamic candidate list
  - m_L (default: 6) - Number of layers in the graph

### Iteration 4: Persistence and Index Management
- Vector embedding persistence:
  - Efficient storage of embedding vectors to disk
  - Fast retrieval of stored embeddings
- HNSW index structure management:
  - Support for index deletion and modification
  - Index persistence to disk
  - Index reusability across system restarts

### Iteration 5: Parallel Processing Optimization
- Enhanced K-Nearest Neighbors (KNN) search with parallel processing
- Multi-threaded implementation for:
  - Vector similarity calculations
  - Top-K results retrieval
  - Metadata fetching
- Performance improvements:
  - Reduced latency for large-scale vector searches
  - Improved system throughput
  - Efficient utilization of multi-core CPU resources

## Technical Details

### Storage Structure
Current SSTable Structure:
```
[Header(32B)] [Bloom Filter(10KB)] [Index(key+offset)] [Data(values)]
```

Future Enhancement Path:
```
Index File (.index):
[Header] [Bloom Filter] [Index(key+data_file_pointer)]

Data File (.data):
[Values]
```

### Performance Optimizations
- Memory-first write operations
- Batch disk writes
- Bloom Filter for quick key existence checks
- Multi-threaded parallel processing
- Efficient index persistence and reuse
- Optimized vector similarity calculations
