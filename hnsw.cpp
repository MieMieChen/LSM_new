#include "hnsw.h"
#include <cmath>
#include <algorithm>
#include <vector>
#include <utility>
#include <queue>
#include <string>

uint64_t HNSW::get_max_layer() const {
    return globalHeader.max_level;
}

int HNSW::rand_level() {
    int level = 0;
    while (level <globalHeader.M_max && ((double)rand() / RAND_MAX) < 1.0 / globalHeader.M) {
        level++;
    }
    return level;
}

float HNSW::cosine_similarity(std::vector<float> a, std::vector<float> b) {
    float result = 0.0f;
    result = dot_product(a, b) / (vector_norm(a) * vector_norm(b));
    return result;
}

float HNSW::dot_product(std::vector<float> a, std::vector<float> b) {
    float result = 0.0f;
    for(size_t i = 0; i < a.size(); i++) {
        result += a[i] * b[i];
    }
    return result;
}

float HNSW::vector_norm(std::vector<float> a) {
    float sum = 0.0f;
    for(float x : a) {
        sum += x * x;
    }
    return std::sqrt(sum);
}

void HNSW::insert(uint64_t key, const std::vector<float>& vector) {
    // 分配新的节点ID
    uint64_t node_id = next_node_id++;
    int layer = rand_level();
    
    // 创建新节点
    Node node(key, node_id, vector, layer);
    nodes[key] = node;
    
    // 确保layers至少有一层
    if(layers.empty()) {
        layers.resize(1);
    }
    
    int entry_points = get_entry_point();
    if(entry_points == -1) {
        entry_point = key;
        layer = 0;
        // 确保第0层存在
        if(layers.size() == 0) {
            layers.resize(1);
        }
        layers[0][key] = {};
        globalHeader.max_level = 0;
        return;
    } else {
        if(layer > globalHeader.max_level) {
            globalHeader.max_level = layer;
        }
    }
    
    // 确保layers的大小足够包含所有层
    if(layers.size() <= globalHeader.max_level) {
        layers.resize(globalHeader.max_level + 1);
    }
    
    // 第一步：自顶层向下逐层搜索，找到每层与q最接近的节点作为下一层的入口点
    uint64_t curr_entry_point = entry_point;
    for(int i = globalHeader.max_level; i > layer; i--) {
        float best_dist = -1.0f;
        uint64_t best_node = curr_entry_point;
        std::list<uint64_t> visited;
        std::list<uint64_t> candidates;
        candidates.push_back(curr_entry_point);
        visited.push_back(curr_entry_point);
        
        while(!candidates.empty()) {
            uint64_t current = candidates.front();
            candidates.pop_front();
            if(layers[i].find(current) == layers[i].end()) {
                continue; // 跳过不在该层的节点
            }
            for(uint64_t neighbor : layers[i][current]) {
                if(std::find(visited.begin(), visited.end(), neighbor) == visited.end()) {
                    visited.push_back(neighbor);
                    candidates.push_back(neighbor);
                    float dist = cosine_similarity(vector, nodes[neighbor].vector);
                    if(dist > best_dist) {
                        best_dist = dist;
                        best_node = neighbor;
                    }
                }
            }
        }
        curr_entry_point = best_node;
    }
    
    // 第二步：从layer层到第0层搜索，在每层将q与最近的efConstruction个点相连
    for(int i = layer; i >= 0; i--) {
        if(layers[i].find(key) == layers[i].end()) {
            layers[i][key] = {};
        }
        std::vector<std::pair<float, uint64_t>> neighbors; 
        std::list<uint64_t> visited;
        std::list<uint64_t> candidates;
        
        candidates.push_back(curr_entry_point);
        visited.push_back(curr_entry_point);
        
        // 计算与入口点的相似度
        float sim = cosine_similarity(vector, nodes[curr_entry_point].vector);
        neighbors.push_back({sim, curr_entry_point});
        
        // 贪心搜索过程
        while(!candidates.empty()) {
            uint64_t current = candidates.front();
            candidates.pop_front();
            if(layers[i].find(current) == layers[i].end()) {
                continue; // 跳过不在该层的节点
            }
            
            for(uint64_t neighbor : layers[i][current]) {
                if(std::find(visited.begin(), visited.end(), neighbor) == visited.end()) {
                    visited.push_back(neighbor);
                    float dist = cosine_similarity(vector, nodes[neighbor].vector);
                    neighbors.push_back({dist, neighbor});
                    candidates.push_back(neighbor);
                }
            }
        }
        
        std::sort(neighbors.begin(), neighbors.end(), [](const auto& a, const auto& b) {
            return a.first > b.first; // 相似度降序
        });
        
        int num_edges = std::min(globalHeader.M, static_cast<uint32_t>(neighbors.size()));
        for(int j = 0; j < num_edges; j++) {
            uint64_t neighbor_key = neighbors[j].second;
            layers[i][key].push_back(neighbor_key);
            layers[i][neighbor_key].push_back(key);
            //layers当中村的
            
            // 更新节点的连接关系
            nodes[key].layer_connections[i].push_back(neighbor_key);
            nodes[neighbor_key].layer_connections[i].push_back(key);
            
            if(layers[i][neighbor_key].size() > globalHeader.M_max) {
                std::vector<std::pair<float, uint64_t>> neighbor_edges;
                for(uint64_t edge : layers[i][neighbor_key]) {
                    float edge_sim = cosine_similarity(nodes[neighbor_key].vector, nodes[edge].vector);
                    neighbor_edges.push_back({edge_sim, edge});
                }
                std::sort(neighbor_edges.begin(), neighbor_edges.end(), [](const auto& a, const auto& b) {
                    return a.first > b.first;
                });
                std::vector<uint64_t> new_edges;
                for(int k = 0; k < globalHeader.M_max; k++) {
                    new_edges.push_back(neighbor_edges[k].second);
                }
                layers[i][neighbor_key] = new_edges;
                nodes[neighbor_key].layer_connections[i] = new_edges;
            }
        }
        if(!neighbors.empty()) {
            curr_entry_point = neighbors[0].second; // 最相似的节点作为下一层的入口点
        }
    }
}

// void HNSW::remove(uint64_t key) {
//     // 检查key是否存在
//     if (nodes.find(key) == nodes.end()) {
//         return;  // key不存在，直接返回
//     }

//     // 从所有层中删除该节点
//     for (auto& layer : layers) {
//         // 删除该节点的所有出边
//         layer.erase(key);

//         // 删除指向该节点的所有入边
//         for (auto& [neighbor_key, neighbor_edges] : layer) {
//             auto it = std::find(neighbor_edges.begin(), neighbor_edges.end(), key);
//             if (it != neighbor_edges.end()) {
//                 neighbor_edges.erase(it);
//                 // 更新邻居节点的连接关系
//                 nodes[neighbor_key].layer_connections[&layer - &layers[0]].erase(
//                     std::find(nodes[neighbor_key].layer_connections[&layer - &layers[0]].begin(),
//                              nodes[neighbor_key].layer_connections[&layer - &layers[0]].end(),
//                              key)
//                 );
//             }
//         }
//     }

//     // 如果是入口点，需要更新入口点
//     if (entry_point == key) {
//         // 找到第0层的任意其他节点作为新的入口点
//         if (!layers[0].empty()) {
//             entry_point = layers[0].begin()->first;
//         } else {
//             entry_point = -1;  // 如果没有其他节点，重置入口点
//         }
//     }

//     // 删除节点
//     nodes.erase(key);
// }

uint64_t HNSW::get_entry_point() const {
    return entry_point;
}

struct node
{
    uint64_t id;
    float dist;
    bool operator<(const node& other) const {
        return dist < other.dist; // 小顶堆
    }
    node(uint64_t id, float dist) : id(id), dist(dist) {};
};

std::vector<std::pair<std::uint64_t, std::string>> HNSW::query(const std::vector<float>& query_vector, int k) {
    std::vector<std::pair<std::uint64_t, std::string>> result;
    
    if(vectors.empty() || entry_point == -1) {
        return result; // 空结果
    }
    
    // 首先在最高层找到最近的节点
    uint64_t curr_entry_point = entry_point;
    
    // 从最高层开始向下搜索
    for(int i = globalHeader.max_level; i > 0; i--) {
        float best_dist = -1.0f;
        uint64_t best_node = curr_entry_point;
        
        std::list<uint64_t> visited;
        std::list<node> candidates;

        candidates.push_back(node(curr_entry_point,best_dist));
        visited.push_back(curr_entry_point);
        
        while(!candidates.empty()) {
            uint64_t current = candidates.front().id;
            candidates.pop_front();
            
            if(layers[i].find(current) == layers[i].end()) {
                continue;
            }
            
            for(uint64_t neighbor : layers[i][current]) {
                if(std::find(visited.begin(), visited.end(), neighbor) == visited.end()) {
                    float dist = cosine_similarity(query_vector, vectors[neighbor]);
                    visited.push_back(neighbor);
                    candidates.push_back(node(neighbor,dist));
                    if(candidates.size() > globalHeader.efConstruction) {
                        // 对list排序，按照dist降序（相似度越高越好）
                        candidates.sort([](const node& a, const node& b) {
                            return a.dist > b.dist; // 按相似度降序排列
                        });
                        // 保留前efConstruction个元素，删除其余元素
                        auto it = candidates.begin();
                        std::advance(it, globalHeader.efConstruction); // 移动到第efConstruction个元素位置
                        candidates.erase(it, candidates.end()); // 删除从it到末尾的所有元素
                    }
                    if( dist > best_dist) {
                        best_dist = dist;
                        best_node = neighbor;
                    }
                }
            }
        }
        
        curr_entry_point = best_node;
    }
    
    // 在第0层进行详细搜索，找到k个最近邻
    std::vector<std::pair<float, uint64_t>> top_candidates;
    std::list<uint64_t> visited;
    std::list<node> candidates;
    
    // 计算与入口点的相似度
    float initial_sim = cosine_similarity(query_vector, vectors[curr_entry_point]);
    candidates.push_back(node(curr_entry_point, initial_sim));
    visited.push_back(curr_entry_point);
    top_candidates.push_back({initial_sim, curr_entry_point});
    
    // 使用更大的efSearch参数来提高搜索精度（通常efSearch > efConstruction）
    int efSearch = std::max(globalHeader.efConstruction, (uint32_t)k);
    
    while(!candidates.empty()) {
        // 取出当前候选节点（相似度最高的）
        candidates.sort([](const node& a, const node& b) {
            return a.dist > b.dist; // 按相似度降序排列
        });
        
        uint64_t current = candidates.front().id;
        float current_dist = candidates.front().dist;
        candidates.pop_front();
        
        // 如果当前节点的相似度低于top_candidates中最小的，并且我们已经有至少efSearch个候选
        if(top_candidates.size() >= efSearch) {
            std::sort(top_candidates.begin(), top_candidates.end(),
                [](const auto& a, const auto& b) { return a.first < b.first; }); // 按相似度升序
            
            if(current_dist < top_candidates[0].first) {
                continue; // 剪枝：当前节点相似度太低，不继续展开
            }
        }
        
        if(layers[0].find(current) == layers[0].end()) {
            continue;
        }
        
        for(uint64_t neighbor : layers[0][current]) {
            if(std::find(visited.begin(), visited.end(), neighbor) == visited.end()) {
                visited.push_back(neighbor);
                
                float dist = cosine_similarity(query_vector, vectors[neighbor]);
                candidates.push_back(node(neighbor, dist));
                top_candidates.push_back({dist, neighbor});
                
                // 保持top_candidates的大小不超过efSearch
                if(top_candidates.size() > efSearch * 2) { // 允许增长到2倍大小再裁剪，减少排序次数
                    std::sort(top_candidates.begin(), top_candidates.end(),
                        [](const auto& a, const auto& b) { return a.first > b.first; }); // 按相似度降序
                    
                    if(top_candidates.size() > efSearch) {
                        top_candidates.resize(efSearch);
                    }
                }
            }
        }
    }
    
    // 最终按相似度降序排序
    std::sort(top_candidates.begin(), top_candidates.end(), 
        [](const auto& a, const auto& b) { return a.first > b.first; });
    
    // 取前k个结果
    int result_size = std::min(k, static_cast<int>(top_candidates.size()));
    for(int i = 0; i < result_size; i++) {
        // 注意：由于我们没有实际的字符串值，这里只返回键
        // 实际应用中，你需要从某处获取对应的字符串值
        result.push_back({top_candidates[i].second," "}); // 第二个值应该是对应的字符串
    }
    
    return result;
}