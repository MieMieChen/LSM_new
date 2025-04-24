#include "../test.h"
#include <fstream>
#include <iostream>
#include <string>

#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <fstream>
#include <chrono>

static const std::string DEL = "~DELETED~";

std::vector<std::string> read_file(std::string filename) {
	std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr<<"Failed to open file: "<<filename<<std::endl;
        return {};
    }
    std::string line;
    std::vector<std::string> temp;
    while (std::getline(file, line)) {
        bool exist_alpha = false;
        for (auto c : line) {
            if (isalpha(c)) {
                exist_alpha = true;
                break;
            }
        }
        if (!exist_alpha) {
            continue;
        }
        if (line.empty())
            continue;
        if(line.size() < 70) {
            continue;
        }
        temp.push_back(line);
    }
    file.close();
    return temp;
}

class CorrectnessTest : public Test {
private:
    const uint64_t SIMPLE_TEST_MAX = 512;
    const uint64_t MIDDLE_TEST_MAX  = 1024 * 64;
    const uint64_t LARGE_TEST_MAX  = 1024 * 64;

	void text_test(uint64_t max) {
        using namespace std::chrono;
		uint64_t i;
		auto trimmed_text = read_file("../data/trimmed_text.txt");
		max	= std::min(max, (uint64_t)trimmed_text.size());

       // steady_clock::time_point start = steady_clock::now();
		for (i = 0; i < max; ++i) {
			store.put(i, trimmed_text[i]);
		}
       // steady_clock::time_point end = steady_clock::now();
       // microseconds duration = duration_cast<microseconds>(end - start);
      // std::cout << "put KV Elapsed time: " << duration.count() << " microseconds\n";


        //steady_clock::time_point start2 = steady_clock::now();
		for (i = 0; i < max; ++i)
			EXPECT(trimmed_text[i], store.get(i));
		// phase();
       // steady_clock::time_point end2 = steady_clock::now();
        //microseconds duration2 = duration_cast<microseconds>(end2 - start2);
       // std::cout << "get KV Elapsed time: " << duration2.count() << " microseconds\n";


		// run the search_knn, and compare the result to ./data/test_text_ans.txt
		auto test_text = read_file("../data/test_text.txt");
		max			   = std::min(max, (uint64_t)test_text.size());

		std::vector<std::string> ans;
        ans = read_file("../data/test_text_ans.txt");
        phase();
		int idx = 0, k = 3;

        //phase2
        // steady_clock::time_point start5;
        // steady_clock::time_point end5;
        // microseconds duration5;
        // start5 = steady_clock::now();
		// for (i = 0; i < max; ++i) {
		// 	auto res = store.search_knn(test_text[i], k);
        //    std::cout << "第i = " << i <<"searchKNN的耗时："<< duration5.count() << " microseconds\n";
		// 	for (auto j : res) {
        //         if(store.get(j.first) != j.second) {
        //             std::cerr << "TEST Error @" << __FILE__ << ":" << __LINE__;
        //             std::cerr << ", expected " << ans[idx];
        //             std::cerr << ", got " << j.second << std::endl;
        //         }
		// 		EXPECT(ans[idx], j.second);
		// 		idx++;
		// 	}
		// }
        // end5 = steady_clock::now();
        // duration5 = duration_cast<microseconds>(end5 - start5);

        // // printf("phase2 search time: %f\n",duration5);
        // std::cout << "search_knn PHASE1 Elapsed time: " << duration5.count() << " microseconds\n";



        // steady_clock::time_point start6;
        // steady_clock::time_point end6;
        // microseconds duration6;
        // start6 = steady_clock::now();
		// for (i = 0; i < max; ++i) {
		// 	auto res = store.search_knn_hnsw(test_text[i], k);
        //    // std::cout << "第i = " << i <<"searchKNN的耗时："<< duration5.count() << " microseconds\n";
		// 	for (auto j : res) {
        //         if(store.get(j.first) != j.second) {
        //             std::cerr << "TEST Error @" << __FILE__ << ":" << __LINE__;
        //             std::cerr << ", expected " << ans[idx];
        //             std::cerr << ", got " << j.second << std::endl;
        //         }
		// 		EXPECT(ans[idx], j.second);
		// 		idx++;
		// 	}
		// }
        // end6 = steady_clock::now();
        // duration6 = duration_cast<microseconds>(end6 - start6);
        // // printf("phase3 search time: %f\n",duration6);
        // std::cout << "search_knn_HNSW PHASE1 Elapsed time: " << duration6.count() << " microseconds\n";
        
        


        milliseconds total_time(0); 
        for (i = 0; i < max; ++i) {
        
            // auto res = store.search_knn_hnsw(test_text[i], k);
        
            std::vector<float> embStr = embedding_single(test_text[i]);
            // std::vector<std::pair<std::uint64_t, std::string>> result;
            auto start = high_resolution_clock::now();
            auto res = store.hnsw_index.query(embStr, k);
            for(int i = 0; i < res.size(); i++)
            {
                res[i].second = store.get(res[i].first);
                if(res[i].second == DEL)
                    res[i].second = "";
            }
            auto end = high_resolution_clock::now();
            total_time += duration_cast<milliseconds>(end - start);  // 累加耗时
        
            for (auto j : res) {
                if (store.get(j.first) != j.second) {
                    std::cerr << "TEST Error @" << __FILE__ << ":" << __LINE__;
                    std::cerr << ", expected " << ans[idx];
                    std::cerr << ", got " << j.second << std::endl;
                }
                EXPECT(ans[idx], j.second);
                idx++;
            }
        }
        std::cout << "search_knn_HNSW Elapsed time:" << total_time.count() << " ms" << std::endl;
        milliseconds total_time0(0); 
        for (i = 0; i < max; ++i) {
        
            // auto res = store.search_knn_hnsw(test_text[i], k);
        
            std::vector<float> embStr = embedding_single(test_text[i]);
            // std::vector<std::pair<std::uint64_t, std::string>> result;
            auto start = high_resolution_clock::now();
            auto res = store.query_knn(embStr, k);
            auto end = high_resolution_clock::now();
            total_time0 += duration_cast<milliseconds>(end - start);  // 累加耗时
        
            for (auto j : res) {
                if (store.get(j.first) != j.second) {
                    std::cerr << "TEST Error @" << __FILE__ << ":" << __LINE__;
                    std::cerr << ", expected " << ans[idx];
                    std::cerr << ", got " << j.second << std::endl;
                }
                EXPECT(ans[idx], j.second);
                idx++;
            }
        }
        std::cout << "search_knn Elapsed time:" << total_time0.count() << " ms" << std::endl;

       // steady_clock::time_point end3 = steady_clock::now();s
        //microseconds duration3 = duration_cast<microseconds>(end3 - start3);
        //std::cout << "search_knn_hnsw PHASE1 Elapsed time: " << duration3.count() << " microseconds\n";


		auto phase_with_tolerance = [this](double tolerance = 0.03) {
			// Report
			std::cout << "  Phase " << (nr_phases + 1) << ": ";
			std::cout << nr_passed_tests << "/" << nr_tests << " ";

			// Calculate tolerance
			double pass_rate		   = static_cast<double>(nr_passed_tests) / nr_tests;
			bool passed_with_tolerance = pass_rate >= (1.0 - tolerance);

			// Count
			++nr_phases;
			if (passed_with_tolerance) {
				++nr_passed_phases;
				std::cout << "[PASS]" << std::endl;
			} else {
                std::cout << "Accept Rate: " << pass_rate * 100 << "%\n";
                std::cout << "The Accept Rate we recommend is more than 85%.\nBecause the embedding model may not act strictly samely between each machine.\n";
			}

			std::cout.flush();

			// Reset
			nr_tests		= 0;
			nr_passed_tests = 0;
		};
        //steady_clock::time_point start4 = steady_clock::now();
		phase_with_tolerance(0.15);
        // steady_clock::time_point end4 = steady_clock::now();
        // microseconds duration4 = duration_cast<microseconds>(end4 - start4);
        // std::cout << "search_knn_hnsw PHASE2 Elapsed time: " << duration4.count() << " microseconds\n";
	}

public:
    CorrectnessTest(const std::string &dir, bool v = true) : Test(dir, v) {}

    void start_test(void *args = NULL) override {
        std::cout << "===========================" << std::endl;
        std::cout << "KVStore Correctness Test" << std::endl;
        
        store.reset();
        std::cout << "[Text Test]" << std::endl;
        text_test(120);
    }
};

int main(int argc, char *argv[]) {
    bool verbose = (argc == 2 && std::string(argv[1]) == "-v");
    freopen("/dev/null", "w", stderr); // 屏蔽所有 stderr 输出  好用哈哈哈

    std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
    std::cout << "  -v: print extra info for failed tests [currently ";
    std::cout << (verbose ? "ON" : "OFF") << "]" << std::endl;
    std::cout << std::endl;
    std::cout.flush();

    CorrectnessTest test("./data", verbose);

    test.start_test();

    return 0;
}

