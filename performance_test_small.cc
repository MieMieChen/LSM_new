#include "test.h"
#include <chrono>
#include <random>
#include <vector>
#include <iomanip>
#include <fstream>
#include <algorithm>
#include <numeric>

class PerformanceTest : public Test {
private:
    // 小数据量测试
    std::vector<uint64_t> dataSizes = {1000, 5000, 10000,20000,30000};
    uint64_t KEY_RANGE = 1000000;      // 键的范围
    const int VALUE_SIZE = 100;              // 值的大小（字节）
    
    struct TestResult {
        std::string operation;
        uint64_t dataSize;
        double avgLatency;
        double p50Latency;
        double p95Latency;
        double p99Latency;
        double throughput;
    };
    
    std::vector<TestResult> allResults;

    // 生成随机键
    std::vector<uint64_t> generateRandomKeys(uint64_t count) {
        std::vector<uint64_t> keys(count);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<uint64_t> dis(1, KEY_RANGE);

        for (uint64_t i = 0; i < count; i++) {
            keys[i] = dis(gen);
        }
        return keys;
    }

    // 生成固定大小的随机值
    std::string generateRandomValue() {
        static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        std::string value(VALUE_SIZE, 0);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, sizeof(charset) - 2);

        for (int i = 0; i < VALUE_SIZE; i++) {
            value[i] = charset[dis(gen)];
        }
        return value;
    }

    // 测试PUT操作
    void testPut(uint64_t numOperations, TestResult& result) {
        std::cout << "\nTesting PUT Operations with " << numOperations << " operations:" << std::endl;
        auto keys = generateRandomKeys(numOperations);
        std::vector<double> latencies;
        auto startTime = std::chrono::high_resolution_clock::now();

        for (uint64_t i = 0; i < numOperations; i++) {
            auto start = std::chrono::high_resolution_clock::now();
            store.put(keys[i], generateRandomValue());
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            latencies.push_back(duration.count());
        }

        auto endTime = std::chrono::high_resolution_clock::now();
        auto totalDuration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        // 计算统计数据
        double avgLatency = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
        double throughput = (numOperations * 1000.0) / totalDuration.count();
        
        // 计算百分位数
        std::sort(latencies.begin(), latencies.end());
        double p50 = latencies[latencies.size() * 0.5];
        double p95 = latencies[latencies.size() * 0.95];
        double p99 = latencies[latencies.size() * 0.99];

        std::cout << "Average Latency: " << std::fixed << std::setprecision(2) << avgLatency << " microseconds" << std::endl;
        std::cout << "P50 Latency: " << std::fixed << std::setprecision(2) << p50 << " microseconds" << std::endl;
        std::cout << "P95 Latency: " << std::fixed << std::setprecision(2) << p95 << " microseconds" << std::endl;
        std::cout << "P99 Latency: " << std::fixed << std::setprecision(2) << p99 << " microseconds" << std::endl;
        std::cout << "Throughput: " << std::fixed << std::setprecision(2) << throughput << " operations/second" << std::endl;
        
        // 保存结果
        result.operation = "PUT";
        result.dataSize = numOperations;
        result.avgLatency = avgLatency;
        result.p50Latency = p50;
        result.p95Latency = p95;
        result.p99Latency = p99;
        result.throughput = throughput;
    }

    // 测试GET操作
    void testGet(uint64_t numOperations, TestResult& result) {
        std::cout << "\nTesting GET Operations with " << numOperations << " operations:" << std::endl;
        auto keys = generateRandomKeys(numOperations);
        std::vector<double> latencies;
        auto startTime = std::chrono::high_resolution_clock::now();

        for (uint64_t i = 0; i < numOperations; i++) {
            auto start = std::chrono::high_resolution_clock::now();
            store.get(keys[i]);
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            latencies.push_back(duration.count());
        }

        auto endTime = std::chrono::high_resolution_clock::now();
        auto totalDuration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        // 计算统计数据
        double avgLatency = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
        double throughput = (numOperations * 1000.0) / totalDuration.count();
        
        // 计算百分位数
        std::sort(latencies.begin(), latencies.end());
        double p50 = latencies[latencies.size() * 0.5];
        double p95 = latencies[latencies.size() * 0.95];
        double p99 = latencies[latencies.size() * 0.99];

        std::cout << "Average Latency: " << std::fixed << std::setprecision(2) << avgLatency << " microseconds" << std::endl;
        std::cout << "P50 Latency: " << std::fixed << std::setprecision(2) << p50 << " microseconds" << std::endl;
        std::cout << "P95 Latency: " << std::fixed << std::setprecision(2) << p95 << " microseconds" << std::endl;
        std::cout << "P99 Latency: " << std::fixed << std::setprecision(2) << p99 << " microseconds" << std::endl;
        std::cout << "Throughput: " << std::fixed << std::setprecision(2) << throughput << " operations/second" << std::endl;
        
        // 保存结果
        result.operation = "GET";
        result.dataSize = numOperations;
        result.avgLatency = avgLatency;
        result.p50Latency = p50;
        result.p95Latency = p95;
        result.p99Latency = p99;
        result.throughput = throughput;
    }

    // 测试DEL操作
    void testDel(uint64_t numOperations, TestResult& result) {
        std::cout << "\nTesting DEL Operations with " << numOperations << " operations:" << std::endl;
        auto keys = generateRandomKeys(numOperations);
        std::vector<double> latencies;
        auto startTime = std::chrono::high_resolution_clock::now();

        for (uint64_t i = 0; i < numOperations; i++) {
            auto start = std::chrono::high_resolution_clock::now();
            store.del(keys[i]);
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            latencies.push_back(duration.count());
        }

        auto endTime = std::chrono::high_resolution_clock::now();
        auto totalDuration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        // 计算统计数据
        double avgLatency = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
        double throughput = (numOperations * 1000.0) / totalDuration.count();
        
        // 计算百分位数
        std::sort(latencies.begin(), latencies.end());
        double p50 = latencies[latencies.size() * 0.5];
        double p95 = latencies[latencies.size() * 0.95];
        double p99 = latencies[latencies.size() * 0.99];

        std::cout << "Average Latency: " << std::fixed << std::setprecision(2) << avgLatency << " microseconds" << std::endl;
        std::cout << "P50 Latency: " << std::fixed << std::setprecision(2) << p50 << " microseconds" << std::endl;
        std::cout << "P95 Latency: " << std::fixed << std::setprecision(2) << p95 << " microseconds" << std::endl;
        std::cout << "P99 Latency: " << std::fixed << std::setprecision(2) << p99 << " microseconds" << std::endl;
        std::cout << "Throughput: " << std::fixed << std::setprecision(2) << throughput << " operations/second" << std::endl;
        
        // 保存结果
        result.operation = "DEL";
        result.dataSize = numOperations;
        result.avgLatency = avgLatency;
        result.p50Latency = p50;
        result.p95Latency = p95;
        result.p99Latency = p99;
        result.throughput = throughput;
    }
    
    // 输出测试结果到CSV文件
    void outputResultsToCSV() {
        std::cout << "\nWriting test results to performance_results_small.csv" << std::endl;
        std::ofstream file("performance_results_small.csv");
        file << "Operation,DataSize,AvgLatency,P50Latency,P95Latency,P99Latency,Throughput\n";
        
        for (const auto& result : allResults) {
            file << result.operation << ","
                 << result.dataSize << ","
                 << result.avgLatency << ","
                 << result.p50Latency << ","
                 << result.p95Latency << ","
                 << result.p99Latency << ","
                 << result.throughput << "\n";
        }
        file.close();
    }

public:
    PerformanceTest(const std::string &dir) : Test(dir) {}

    void start_test(void *args = NULL) override {
        std::cout << "Starting Small Data Performance Test" << std::endl;
        std::cout << "Value size: " << VALUE_SIZE << " bytes" << std::endl;
        
        // 对每个数据量级别进行测试
        for (auto dataSize : dataSizes) {
            std::cout << "\n===== Testing with " << dataSize << " operations =====" << std::endl;
            
            // 清空存储
            store.reset();
            
            // 执行测试
            TestResult putResult;
            testPut(dataSize, putResult);
            allResults.push_back(putResult);
            
            // 预先插入一些数据，以便GET和DEL测试
            auto keys = generateRandomKeys(dataSize);
            for (uint64_t i = 0; i < dataSize; i++) {
                store.put(keys[i], generateRandomValue());
            }
            
            TestResult getResult;
            testGet(dataSize, getResult);
            allResults.push_back(getResult);
            
            TestResult delResult;
            testDel(dataSize, delResult);
            allResults.push_back(delResult);
        }
        
        // 输出CSV文件
        outputResultsToCSV();
    }
};

int main(int argc, char *argv[]) {
    PerformanceTest test("./data");
    test.start_test();
    return 0;
} 