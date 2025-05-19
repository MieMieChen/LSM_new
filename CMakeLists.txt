cmake_minimum_required(VERSION 3.10)
project(LSM_TREE)

# set(CMAKE_CXX_STANDARD 20)
# set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 添加编译器特性
set(CMAKE_CXX_COMPILER_ID "GNU")
set(CMAKE_CXX_COMPILER_VERSION "14.2.0")

find_package(OpenMP REQUIRED)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")  # 或 "Release"
endif()

if(OpenMP_CXX_FOUND)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
endif()

include_directories(
        embedding
        third_party/llama.cpp/common
        third_party/llama.cpp/include
        third_party/llama.cpp
        third_party/llama.cpp/ggml/include
        third_party/llama.cpp/ggml/src
)

add_executable(correctness correctness.cc kvstore_api.h kvstore.h
        kvstore.cc skiplist.cpp skiplist.h sstable.cpp sstable.h shared_data.h shared_data.cpp  
        bloom.cpp bloom.h MurmurHash3.h utils.h test.h embedding/embedding.cc
        sstablehead.cpp sstablehead.h embedding/embedding.h hnsw.cpp hnsw.h)
target_link_libraries(correctness PRIVATE llama common)


# add_executable(correctness2 correctness2.cc kvstore_api.h kvstore.h
#         kvstore.cc skiplist.cpp skiplist.h sstable.cpp sstable.h
#         bloom.cpp bloom.h MurmurHash3.h utils.h test.h embedding/embedding.cc
#         sstablehead.cpp sstablehead.h embedding/embedding.h hnsw.cpp hnsw.h)
# target_link_libraries(correctness2 PRIVATE llama common)


add_executable(E2E_test test/E2E_test.cpp kvstore_api.h kvstore.h
        kvstore.cc skiplist.cpp skiplist.h sstable.cpp sstable.h shared_data.h shared_data.cpp
        bloom.cpp bloom.h MurmurHash3.h utils.h test.h embedding/embedding.cc shared_data.h shared_data.cpp
        sstablehead.cpp sstablehead.h embedding/embedding.h third_party/llama.cpp/common/common.h hnsw.cpp hnsw.h)

target_link_libraries(E2E_test PRIVATE llama common)

add_executable(persistence persistence.cc kvstore_api.h kvstore.h kvstore.cc
        skiplist.cpp skiplist.h sstable.cpp sstable.h
        bloom.cpp bloom.h MurmurHash3.h utils.h test.h shared_data.h shared_data.cpp
        sstablehead.cpp sstablehead.h embedding/embedding.h embedding/embedding.cc hnsw.cpp hnsw.h)
target_link_libraries(persistence PRIVATE llama common)

add_executable(performance_test performance_test.cc kvstore_api.h kvstore.h kvstore.cc
        skiplist.cpp skiplist.h sstable.cpp sstable.h
        bloom.cpp bloom.h MurmurHash3.h utils.h test.h shared_data.h shared_data.cpp
        sstablehead.cpp sstablehead.h embedding/embedding.h embedding/embedding.cc hnsw.cpp hnsw.h)
target_link_libraries(performance_test PRIVATE llama common)

add_executable(performance_test_small performance_test_small.cc kvstore_api.h kvstore.h kvstore.cc
        skiplist.cpp skiplist.h sstable.cpp sstable.h
        bloom.cpp bloom.h MurmurHash3.h utils.h test.h shared_data.h shared_data.cpp
        sstablehead.cpp sstablehead.h embedding/embedding.h embedding/embedding.cc hnsw.cpp hnsw.h)
target_link_libraries(performance_test_small PRIVATE llama common)

add_executable(performance_test_large performance_test_large.cc kvstore_api.h kvstore.h kvstore.cc
        skiplist.cpp skiplist.h sstable.cpp sstable.h
        bloom.cpp bloom.h MurmurHash3.h utils.h test.h shared_data.h shared_data.cpp
        sstablehead.cpp sstablehead.h embedding/embedding.h embedding/embedding.cc hnsw.cpp hnsw.h)
target_link_libraries(performance_test_large PRIVATE llama common)

add_executable(HNSW_Delete_Test HNSW_Delete_Test.cpp kvstore_api.h kvstore.h kvstore.cc
        skiplist.cpp skiplist.h sstable.cpp sstable.h
        bloom.cpp bloom.h MurmurHash3.h utils.h test.h shared_data.h shared_data.cpp
        sstablehead.cpp sstablehead.h embedding/embedding.h embedding/embedding.cc hnsw.cpp hnsw.h)
target_link_libraries(HNSW_Delete_Test PRIVATE llama common)

add_executable(HNSW_Persistent_Test_Phase1 HNSW_Persistent_Test_Phase1.cpp kvstore_api.h kvstore.h kvstore.cc
        skiplist.cpp skiplist.h sstable.cpp sstable.h
        bloom.cpp bloom.h MurmurHash3.h utils.h test.h shared_data.h shared_data.cpp
        sstablehead.cpp sstablehead.h embedding/embedding.h embedding/embedding.cc hnsw.cpp hnsw.h)
target_link_libraries(HNSW_Persistent_Test_Phase1 PRIVATE llama common)

add_executable(HNSW_Persistent_Test_Phase2 HNSW_Persistent_Test_Phase2.cpp kvstore_api.h kvstore.h kvstore.cc
        skiplist.cpp skiplist.h sstable.cpp sstable.h
        bloom.cpp bloom.h MurmurHash3.h utils.h test.h shared_data.h shared_data.cpp
        sstablehead.cpp sstablehead.h embedding/embedding.h embedding/embedding.cc hnsw.cpp hnsw.h)
target_link_libraries(HNSW_Persistent_Test_Phase2 PRIVATE llama common)


add_executable(Vector_Persistent_Test_Phase1 Vector_Persistent_Test_Phase1.cpp kvstore_api.h kvstore.h kvstore.cc
        skiplist.cpp skiplist.h sstable.cpp sstable.h
        bloom.cpp bloom.h MurmurHash3.h utils.h test.h shared_data.h shared_data.cpp
        sstablehead.cpp sstablehead.h embedding/embedding.h embedding/embedding.cc hnsw.cpp hnsw.h)
target_link_libraries(Vector_Persistent_Test_Phase1 PRIVATE llama common)


add_executable(Vector_Persistent_Test_Phase2 Vector_Persistent_Test_Phase2.cpp kvstore_api.h kvstore.h kvstore.cc
        skiplist.cpp skiplist.h sstable.cpp sstable.h
        bloom.cpp bloom.h MurmurHash3.h utils.h test.h shared_data.h shared_data.cpp
        sstablehead.cpp sstablehead.h embedding/embedding.h embedding/embedding.cc hnsw.cpp hnsw.h)
target_link_libraries(Vector_Persistent_Test_Phase2 PRIVATE llama common)


add_executable(Search_knn_test_phase1 Search_knn_test_phase1.cpp kvstore_api.h kvstore.h kvstore.cc
        skiplist.cpp skiplist.h sstable.cpp sstable.h
        bloom.cpp bloom.h MurmurHash3.h utils.h test.h shared_data.h shared_data.cpp
        sstablehead.cpp sstablehead.h embedding/embedding.h embedding/embedding.cc hnsw.cpp hnsw.h)
target_link_libraries(Search_knn_test_phase1 PRIVATE llama common)


add_executable(Search_knn_test_phase2 Search_knn_test_phase2.cpp kvstore_api.h kvstore.h kvstore.cc
        skiplist.cpp skiplist.h sstable.cpp sstable.h
        bloom.cpp bloom.h MurmurHash3.h utils.h test.h shared_data.h  shared_data.cpp
        sstablehead.cpp sstablehead.h embedding/embedding.h embedding/embedding.cc hnsw.cpp hnsw.h)
target_link_libraries(Search_knn_test_phase2 PRIVATE llama common)








add_executable(HNSW_Persistent_Stronger_Test_Phase1  HNSW_Persistent_Stronger_Test_Phase1.cpp kvstore_api.h kvstore.h kvstore.cc
        skiplist.cpp skiplist.h sstable.cpp sstable.h
        bloom.cpp bloom.h MurmurHash3.h utils.h test.h shared_data.h shared_data.cpp
        sstablehead.cpp sstablehead.h embedding/embedding.h embedding/embedding.cc hnsw.cpp hnsw.h)
target_link_libraries(HNSW_Persistent_Stronger_Test_Phase1 PRIVATE llama common)

add_executable( HNSW_Persistent_Stronger_Test_Phase2  HNSW_Persistent_Stronger_Test_Phase2.cpp kvstore_api.h kvstore.h kvstore.cc
        skiplist.cpp skiplist.h sstable.cpp sstable.h
        bloom.cpp bloom.h MurmurHash3.h utils.h test.h shared_data.h shared_data.cpp
        sstablehead.cpp sstablehead.h embedding/embedding.h embedding/embedding.cc hnsw.cpp hnsw.h)
target_link_libraries(HNSW_Persistent_Stronger_Test_Phase2 PRIVATE llama common
)

add_executable(mapreduce-parallel 
        mapreduce-parallel.cpp)
target_link_libraries(mapreduce-parallel PRIVATE llama common)

add_executable(readfile-parallel
        readfile-parallel.cc)
target_link_libraries(readfile-parallel PRIVATE llama common)

add_executable(threadpool
        threadpool.cc)
target_link_libraries(threadpool PRIVATE llama common)

add_subdirectory(third_party/llama.cpp)

add_subdirectory(embedding)

# add_subdirectory(test)
