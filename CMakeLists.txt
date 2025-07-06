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

# Find OpenMP
find_package(OpenMP REQUIRED)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")  # 或 "Release"
endif()

if(OpenMP_CXX_FOUND)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
endif()

# Set directory paths
set(TEST_DIR ${PROJECT_SOURCE_DIR}/test)

# Include directories
include_directories(
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/embedding
    ${PROJECT_SOURCE_DIR}/third_party/llama.cpp/common
    ${PROJECT_SOURCE_DIR}/third_party/llama.cpp/include
    ${PROJECT_SOURCE_DIR}/third_party/llama.cpp
    ${PROJECT_SOURCE_DIR}/third_party/llama.cpp/ggml/include
    ${PROJECT_SOURCE_DIR}/third_party/llama.cpp/ggml/src
)

# Define source files
set(COMMON_SOURCES
    ${PROJECT_SOURCE_DIR}/kvstore.cc
    ${PROJECT_SOURCE_DIR}/skiplist.cpp
    ${PROJECT_SOURCE_DIR}/sstable.cpp
    ${PROJECT_SOURCE_DIR}/shared_data.cpp
    ${PROJECT_SOURCE_DIR}/bloom.cpp
    ${PROJECT_SOURCE_DIR}/sstablehead.cpp
    ${PROJECT_SOURCE_DIR}/embedding/embedding.cc
    ${PROJECT_SOURCE_DIR}/hnsw.cpp
)

# Define header files
set(COMMON_HEADERS
    ${PROJECT_SOURCE_DIR}/kvstore_api.h
    ${PROJECT_SOURCE_DIR}/kvstore.h
    ${PROJECT_SOURCE_DIR}/skiplist.h
    ${PROJECT_SOURCE_DIR}/sstable.h
    ${PROJECT_SOURCE_DIR}/shared_data.h
    ${PROJECT_SOURCE_DIR}/bloom.h
    ${PROJECT_SOURCE_DIR}/MurmurHash3.h
    ${PROJECT_SOURCE_DIR}/utils.h
    ${PROJECT_SOURCE_DIR}/test.h
    ${PROJECT_SOURCE_DIR}/sstablehead.h
    ${PROJECT_SOURCE_DIR}/embedding/embedding.h
    ${PROJECT_SOURCE_DIR}/hnsw.h
)

# Test executables
add_executable(correctness ${PROJECT_SOURCE_DIR}/test/correctness.cc ${COMMON_SOURCES})
target_link_libraries(correctness PRIVATE llama common)

add_executable(correctness2 ${PROJECT_SOURCE_DIR}/test/correctness2.cc ${COMMON_SOURCES})
target_link_libraries(correctness2 PRIVATE llama common)

add_executable(E2E_test ${PROJECT_SOURCE_DIR}/test/E2E_test.cpp ${COMMON_SOURCES})
target_link_libraries(E2E_test PRIVATE llama common)

add_executable(persistence ${PROJECT_SOURCE_DIR}/test/persistence.cc ${COMMON_SOURCES})
target_link_libraries(persistence PRIVATE llama common)

# Performance test executables
add_executable(performance_test ${PROJECT_SOURCE_DIR}/test/performance_test.cc ${COMMON_SOURCES})
target_link_libraries(performance_test PRIVATE llama common)

add_executable(performance_test_small ${PROJECT_SOURCE_DIR}/test/performance_test_small.cc ${COMMON_SOURCES})
target_link_libraries(performance_test_small PRIVATE llama common)

add_executable(performance_test_large ${PROJECT_SOURCE_DIR}/test/performance_test_large.cc ${COMMON_SOURCES})
target_link_libraries(performance_test_large PRIVATE llama common)

# HNSW test executables
add_executable(hnsw_delete_test ${PROJECT_SOURCE_DIR}/test/HNSW_Delete_Test.cpp ${COMMON_SOURCES})
target_link_libraries(hnsw_delete_test PRIVATE llama common)

add_executable(hnsw_persistent_test_p1 ${PROJECT_SOURCE_DIR}/test/HNSW_Persistent_Test_Phase1.cpp ${COMMON_SOURCES})
target_link_libraries(hnsw_persistent_test_p1 PRIVATE llama common)

add_executable(hnsw_persistent_test_p2 ${PROJECT_SOURCE_DIR}/test/HNSW_Persistent_Test_Phase2.cpp ${COMMON_SOURCES})
target_link_libraries(hnsw_persistent_test_p2 PRIVATE llama common)

# Vector persistence test executables
add_executable(vector_persistent_test_p1 ${PROJECT_SOURCE_DIR}/test/Vector_Persistent_Test_Phase1.cpp ${COMMON_SOURCES})
target_link_libraries(vector_persistent_test_p1 PRIVATE llama common)

add_executable(vector_persistent_test_p2 ${PROJECT_SOURCE_DIR}/test/Vector_Persistent_Test_Phase2.cpp ${COMMON_SOURCES})
target_link_libraries(vector_persistent_test_p2 PRIVATE llama common)

# Search KNN test executables
add_executable(search_knn_test_p1 ${PROJECT_SOURCE_DIR}/test/Search_knn_test_phase1.cpp ${COMMON_SOURCES})
target_link_libraries(search_knn_test_p1 PRIVATE llama common)

add_executable(search_knn_test_p2 ${PROJECT_SOURCE_DIR}/test/Search_knn_test_phase2.cpp ${COMMON_SOURCES})
target_link_libraries(search_knn_test_p2 PRIVATE llama common)

# HNSW stronger test executables
add_executable(hnsw_persistent_stronger_test_p1 ${PROJECT_SOURCE_DIR}/test/HNSW_Persistent_Stronger_Test_Phase1.cpp ${COMMON_SOURCES})
target_link_libraries(hnsw_persistent_stronger_test_p1 PRIVATE llama common)

add_executable(hnsw_persistent_stronger_test_p2 ${PROJECT_SOURCE_DIR}/test/HNSW_Persistent_Stronger_Test_Phase2.cpp ${COMMON_SOURCES})
target_link_libraries(hnsw_persistent_stronger_test_p2 PRIVATE llama common)

# Parallel processing examples
add_executable(mapreduce-parallel ${PROJECT_SOURCE_DIR}/mapreduce-parallel.cpp ${COMMON_SOURCES})
target_link_libraries(mapreduce-parallel PRIVATE llama common)

add_executable(readfile-parallel ${PROJECT_SOURCE_DIR}/readfile-parallel.cc ${COMMON_SOURCES})
target_link_libraries(readfile-parallel PRIVATE llama common)

add_executable(threadpool ${PROJECT_SOURCE_DIR}/threadpool.cc ${COMMON_SOURCES})
target_link_libraries(threadpool PRIVATE llama common)

add_subdirectory(third_party/llama.cpp)
add_subdirectory(embedding)

