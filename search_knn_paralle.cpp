#include <vector>
#include <string>
#include <cstdint>
#include <cmath> // For cosine_similarity calculations (dot product, magnitude)
#include <queue> // For std::priority_queue
#include <utility> // For std::pair
#include <future> // For std::async and std::future
#include <numeric> // For std::iota (optional, helpful for chunking)
#include <algorithm> // For std::sort, std::reverse
#include <iostream> // For demonstration output
#include <map> // To simulate Cache and sentence2line

// 假设这些函数/类成员在 KVStore 类外部或内部定义
// 模拟 cosine_similarity 函数
float cosine_similarity(const std::vector<float>& vec1, const std::vector<float>& vec2) {
    if (vec1.empty() || vec1.size() != vec2.size()) {
        return 0.0f; // 或者抛出异常
    }
    double dot_product = 0.0;
    double magnitude1 = 0.0;
    double magnitude2 = 0.0;
    for (size_t i = 0; i < vec1.size(); ++i) {
        dot_product += vec1[i] * vec2[i];
        magnitude1 += vec1[i] * vec1[i];
        magnitude2 += vec2[i] * vec2[i];
    }
    magnitude1 = std::sqrt(magnitude1);
    magnitude2 = std::sqrt(magnitude2);

    if (magnitude1 == 0.0 || magnitude2 == 0.0) {
        return 0.0f;
    }
    return static_cast<float>(dot_product / (magnitude1 * magnitude2));
}

// 模拟 KVStore 类结构
class KVStore {
public:
    // 模拟 Cache: key -> embedding
    std::map<std::uint64_t, std::vector<float>> Cache;
    // 模拟 key -> string value store
    std::map<std::uint64_t, std::string> ValueStore;
    // 模拟 query string -> embedding map
    std::map<std::string, std::vector<float>> sentence2line;

    // 模拟 get(key) 方法，获取 key 对应的字符串值
    std::string get(std::uint64_t key) const {
        auto it = ValueStore.find(key);
        if (it != ValueStore.end()) {
            // std::cout << "Getting value for key: " << key << std::endl; // 模拟获取操作
            return it->second;
        }
        return ""; // 或者抛出异常
    }

    // 原始的 search_knn (调用 query_knn)
    std::vector<std::pair<std::uint64_t, std::string>> search_knn(std::string query, int k) {
        // 在实际应用中，这里需要检查 query 是否存在于 sentence2line
        auto it = sentence2line.find(query);
        if (it == sentence2line.end()) {
            std::cerr << "Query embedding not found for: " << query << std::endl;
            return {}; // 返回空结果
        }
        std::vector<float> embStr = it->second;
        std::vector<std::pair<std::uint64_t, std::string>> result = query_knn_parallel(embStr, k); // 调用并行版本
        return result;
    }

    // 并行版本的 query_knn
    std::vector<std::pair<std::uint64_t, std::string>> query_knn_parallel(const std::vector<float>& embStr, int k);

private:
    // 辅助结构用于在优先级队列中存储 (相似度, key) 对
    using SimKey = std::pair<float, std::uint64_t>;

    // 辅助函数：在一个数据块中查找 Top-K (只返回 SimKey 对)
    // 使用迭代器范围来指定数据块
    std::vector<SimKey> find_top_k_in_chunk(
        typename std::map<std::uint64_t, std::vector<float>>::const_iterator begin,
        typename std::map<std::uint64_t, std::vector<float>>::const_iterator end,
        const std::vector<float>& query_embedding,
        int k_per_chunk); // 每个块找到的 Top-K 数量
};


// 辅助函数实现：在一个数据块中查找 Top-K
std::vector<KVStore::SimKey> KVStore::find_top_k_in_chunk(
    typename std::map<std::uint64_t, std::vector<float>>::const_iterator begin,
    typename std::map<std::uint64_t, std::vector<float>>::const_iterator end,
    const std::vector<float>& query_embedding,
    int k_per_chunk)
{
    // 小顶堆，用于维护块内 Top-K 相似度，堆顶是最小的相似度
    auto cmp = [](const SimKey& a, const SimKey& b) { return a.first > b.first; };
    std::priority_queue<SimKey, std::vector<SimKey>, decltype(cmp)> top_k_chunk(cmp);

    for (auto it = begin; it != end; ++it) {
        const auto& key = it->first;
        const auto& embedding = it->second;

        float sim = cosine_similarity(embedding, query_embedding);

        if (top_k_chunk.size() < k_per_chunk || sim > top_k_chunk.top().first) {
            top_k_chunk.push({sim, key});
            if (top_k_chunk.size() > k_per_chunk) {
                top_k_chunk.pop(); // 移除块内当前最小相似度的项
            }
        }
    }

    // 将堆中的元素转移到向量中
    std::vector<SimKey> result_chunk;
    result_chunk.reserve(top_k_chunk.size());
    while (!top_k_chunk.empty()) {
        result_chunk.push_back(top_k_chunk.top());
        top_k_chunk.pop();
    }
    // 不需要在这里排序，最终的合并步骤会处理总排序

    return result_chunk;
}


// 并行版本的 query_knn 实现
std::vector<std::pair<std::uint64_t, std::string>> KVStore::query_knn_parallel(
    const std::vector<float>& embStr,
    int k)
{
    if (Cache.empty() || k <= 0) {
        return {}; // 无数据或 k 无效
    }

    
    const size_t num_threads = std::thread::hardware_concurrency();


    std::vector<typename std::map<std::uint64_t, std::vector<float>>::const_iterator> chunk_starts;
    chunk_starts.push_back(Cache.begin());
    size_t cache_size = Cache.size();
    size_t elements_per_thread = cache_size / num_threads;

    auto current_it = Cache.begin();
    for (size_t i = 0; i < num_threads - 1; ++i) {
        // 尝试前进 approximate_elements_per_thread 个元素
        // 注意：std::map 的迭代器前进是 O(log N) 或 O(1) 平均，取决于实现，但不像 vector 那样 O(1) 保证
        size_t elements_advanced = 0;
        while(elements_advanced < elements_per_thread && current_it != Cache.end()){
             ++current_it;
             ++elements_advanced;
        }
        chunk_starts.push_back(current_it);
    }
    chunk_starts.push_back(Cache.end()); // 最后一个块的结束迭代器

    std::vector<std::future<std::vector<SimKey>>> map_futures;
    int k_per_chunk = k;
    for (size_t i = 0; i < num_threads; ++i) {
        map_futures.push_back(
            std::async(std::launch::async,
                       &KVStore::find_top_k_in_chunk, // 成员函数指针
                       this, // 调用成员函数需要传递对象指针
                       chunk_starts[i],    // 块开始迭代器
                       chunk_starts[i+1],  // 块结束迭代器
                       embStr,  // 查询 embedding，使用引用包装器避免拷贝大向量
                       k_per_chunk)
        );
    }

    auto cmp_global = [](const SimKey& a, const SimKey& b) { return a.first < b.first; };
    std::priority_queue<SimKey, std::vector<SimKey>, decltype(cmp_global)> top_k_global(cmp_global);

    for (auto& fut : map_futures) {
        try {
            std::vector<SimKey> chunk_results = fut.get(); // 等待并获取块内 Top-K
            for (const auto& sim_key : chunk_results) {
                if (top_k_global.size() < k || sim_key.first > top_k_global.top().first) {
                     top_k_global.push(sim_key);
                     if (top_k_global.size() > k) {
                        top_k_global.pop(); // 移除全局当前最小相似度的项
                     }
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Error in Map task: " << e.what() << std::endl;
            // 根据需要处理错误，可能需要中止或记录
        }
    }

    // 4. 提取最终 Top-K keys
    std::vector<SimKey> final_sim_keys;
    final_sim_keys.reserve(top_k_global.size());
    while(!top_k_global.empty()){
        final_sim_keys.push_back(top_k_global.top());
        top_k_global.pop();
    }
    // 此时 final_sim_keys 是按相似度升序排列的 Top-K SimKey 对
    std::reverse(final_sim_keys.begin(), final_sim_keys.end()); // 变为降序排列
    std::vector<std::future<std::string>> get_futures;
    std::vector<std::uint64_t> keys;
    get_futures.reserve(final_sim_keys.size());
    keys.reserve(final_sim_keys.size());

    for(const auto& sim_key : final_sim_keys){
        std::uint64_t key = sim_key.second;
        keys.push_back(key);
        get_futures.push_back(
            std::async(std::launch::async,
                       &KVStore::get, // 成员函数指针
                       this, // 调用成员函数需要传递对象指针
                       key)
        );
    }

    std::vector<std::pair<std::uint64_t, std::string>> result;
    result.reserve(get_futures.size());

    for (size_t i = 0; i < get_futures.size(); ++i) {
        try {
            result.emplace_back(keys[i], get_futures[i].get());
        } catch (const std::exception& e) {
            std::cerr << "Error in Get task: " << e.what() << std::endl;
            // 可以选择插入一个空结果或跳过
        }
    }

    return result;
}

