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

        steady_clock::time_point start = steady_clock::now();
		for (i = 0; i < max; ++i) {
			store.put(i, trimmed_text[i]);
		}
        steady_clock::time_point end = steady_clock::now();
        microseconds duration = duration_cast<microseconds>(end - start);
        std::cout << "put KV Elapsed time: " << duration.count() << " microseconds\n";


        steady_clock::time_point start2 = steady_clock::now();
		for (i = 0; i < max; ++i)
			EXPECT(trimmed_text[i], store.get(i));
		// phase();
        steady_clock::time_point end2 = steady_clock::now();
        microseconds duration2 = duration_cast<microseconds>(end2 - start2);
        std::cout << "get KV Elapsed time: " << duration2.count() << " microseconds\n";


		// run the search_knn, and compare the result to ./data/test_text_ans.txt
		auto test_text = read_file("../data/test_text.txt");
		max			   = std::min(max, (uint64_t)test_text.size());

		std::vector<std::string> ans;
        ans = read_file("../data/test_text_ans.txt");
        phase();
		int idx = 0, k = 3;
        steady_clock::time_point start5;
        steady_clock::time_point end5;
        microseconds duration5;
        steady_clock::time_point start3 = steady_clock::now();
		for (i = 0; i < max; ++i) {
            start5 = steady_clock::now();
			auto res = store.search_knn(test_text[i], k);
            end5 = steady_clock::now();
            duration5 = duration_cast<microseconds>(end5 - start5);
            std::cout << "第i = " << i <<"searchKNN的耗时："<< duration5.count() << " microseconds\n";
			for (auto j : res) {
                if(store.get(j.first) != j.second) {
                    std::cerr << "TEST Error @" << __FILE__ << ":" << __LINE__;
                    std::cerr << ", expected " << ans[idx];
                    std::cerr << ", got " << j.second << std::endl;
                }
				EXPECT(ans[idx], j.second);
				idx++;
			}
		}
        steady_clock::time_point end3 = steady_clock::now();
        microseconds duration3 = duration_cast<microseconds>(end3 - start3);
        std::cout << "search_KNN PHASE1 Elapsed time: " << duration3.count() << " microseconds\n";


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
        steady_clock::time_point start4 = steady_clock::now();
		phase_with_tolerance(0.15);
        steady_clock::time_point end4 = steady_clock::now();
        microseconds duration4 = duration_cast<microseconds>(end4 - start4);
        std::cout << "search_KNN PHASE2 Elapsed time: " << duration4.count() << " microseconds\n";
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

    std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
    std::cout << "  -v: print extra info for failed tests [currently ";
    std::cout << (verbose ? "ON" : "OFF") << "]" << std::endl;
    std::cout << std::endl;
    std::cout.flush();

    CorrectnessTest test("./data", verbose);

    test.start_test();

    return 0;
}

