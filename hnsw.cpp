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
    int layer = rand_level();
    // 检查key是否已经存在
    // if (nodes.find(key) != nodes.end()) {
    //     nodes[key].is_deleted = false; // 标记为未删除
    //     nodes[key].vector = vector; // 更新向量
    //     nodes[key].max_level = std::max(nodes[key].max_level, layer); // 更新最大层级
    // }
    // // 创建新节点
    // else
    // { 
        Node node(key, 0, vector, layer);
        nodes.push_back(node);
        uint64_t node_id = nodes.size() - 1;
        nodes[node_id].id = node_id; // 设置节点ID
        // nodes[node_id].layer_connections.resize(layer + 1);
        // nodes[node_id].is_deleted = false; // 标记为未删除

    // }
    
    // 确保layers至少有一层
    if(layers.empty()) {
        layers.resize(1);
    }
    
    int entry_points = get_entry_point();

    if(entry_points == -1) {
        entry_point = node_id;
        layer = 0;
        // 确保第0层存在
        if(layers.size() == 0) {
            layers.resize(1);
        }
        layers[0][node_id] = {};
        globalHeader.max_level = 0;
        return;
    } else {
        if(layer > globalHeader.max_level) {
            globalHeader.max_level = layer;
            set_entry_point(node_id);
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
                //如果没用被访问过
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
        if(layers[i].find(node_id) == layers[i].end()) {
            layers[i][node_id] = {};
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
        
        int num_edges = std::min(globalHeader.M, static_cast<uint32_t>(neighbors.size())); //找到离自己近的M个节点
        for(int j = 0; j < num_edges; j++) {
            uint64_t neighbor_id = neighbors[j].second;
            layers[i][node_id].push_back(neighbor_id);
            layers[i][neighbor_id].push_back(node_id);

            nodes[node_id].layer_connections[i].push_back(neighbor_id);
            nodes[neighbor_id].layer_connections[i].push_back(node_id);
            
            if(layers[i][neighbor_id].size() > globalHeader.M_max) {
                std::vector<std::pair<float, uint64_t>> neighbor_edges;
                for(uint64_t edge : layers[i][neighbor_id]) {
                    float edge_sim = cosine_similarity(nodes[neighbor_id].vector, nodes[edge].vector);
                    neighbor_edges.push_back({edge_sim, edge});
                }
                std::sort(neighbor_edges.begin(), neighbor_edges.end(), [](const auto& a, const auto& b) {
                    return a.first > b.first;
                });
                std::vector<uint64_t> new_edges;
                for(int k = 0; k < globalHeader.M_max; k++) {
                    new_edges.push_back(neighbor_edges[k].second);
                }
                layers[i][neighbor_id] = new_edges;
                nodes[neighbor_id].layer_connections[i] = new_edges;
            }
        }
        if(!neighbors.empty()) {
            curr_entry_point = neighbors[0].second; // 最相似的节点作为下一层的入口点
        }
    }
}

uint64_t HNSW::get_entry_point() const {
    return entry_point;
}

void HNSW::set_entry_point(uint64_t id) {
    entry_point = id;
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
    
    if(nodes.empty()) {
        return result; // 空结果
    }
    if( entry_point == -1)
    {
        entry_point = layers[globalHeader.max_level-1].begin()->first; // 如果没有入口点，默认使用最高层的第一个节点
    }
    
    // 首先在最高层找到最近的节点
    uint64_t curr_entry_point = entry_point;
        // uint64_t curr_entry_point = 127;

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
                    float dist = cosine_similarity(query_vector, nodes[neighbor].vector);
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
    float initial_sim = cosine_similarity(query_vector, nodes[curr_entry_point].vector);
    candidates.push_back(node(curr_entry_point, initial_sim));
    visited.push_back(curr_entry_point);
    top_candidates.push_back({initial_sim, curr_entry_point});
        
    while(!candidates.empty()) {
        candidates.sort([](const node& a, const node& b) {
            return a.dist > b.dist; // 按相似度降序排列
        });
        
        uint64_t current = candidates.front().id;
        float current_dist = candidates.front().dist;
        candidates.pop_front();
        
        if(top_candidates.size() >= globalHeader.efConstruction) {
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
                
                float dist = cosine_similarity(query_vector, nodes[neighbor].vector);
                candidates.push_back(node(neighbor, dist));
                top_candidates.push_back({dist, neighbor});
                
                if(top_candidates.size() > globalHeader.efConstruction) { 
                    std::sort(top_candidates.begin(), top_candidates.end(),
                        [](const auto& a, const auto& b) { return a.first > b.first; }); 

                        // 为了保证之后能有k个 剪枝的时候要把已经delete的数量计算出来 最后再删除
                        // uint64_t deleted_count = 0;
                        // for(auto it = top_candidates.begin(); it != top_candidates.end(); ++it) {
                        //     if(nodes[it->second].is_deleted) {
                        //        deleted_count++;
                        //     }
                        // }
                        // top_candidates.resize(std::min(globalHeader.efConstruction+deleted_count,top_candidates.size()));
                        top_candidates.resize(std::min(globalHeader.efConstruction,(uint32_t)top_candidates.size()));
                    }
            }
        }
    }
    
    // 最终按相似度降序排序
    std::sort(top_candidates.begin(), top_candidates.end(), 
        [](const auto& a, const auto& b) { return a.first > b.first; });
    uint64_t counts = 0;
    // 取前k个结果
    // int result_size = std::min(k, static_cast<int>(top_candidates.size()));
    for(int i = 0; i < top_candidates.size(); i++) {

        if(nodes[top_candidates[i].second].is_deleted) {
            continue; // 跳过已删除的节点
        }
        if(counts >= k) {
            break;
        }
        result.push_back({top_candidates[i].second," "}); // 第二个值应该是对应的字符串 second对应的是key
        counts++;
    }
    
    return result;
}