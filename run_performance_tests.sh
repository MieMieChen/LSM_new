#!/bin/bash

# 创建构建目录
mkdir -p build
cd build

# 编译项目
echo "编译项目..."
cmake ..
make performance_test_small performance_test_large

# 运行小数据量测试
echo "运行小数据量测试..."
./performance_test_small

# 运行大数据量测试
echo "运行大数据量测试..."
./performance_test_large

# 返回到项目根目录
cd ..

# 运行Python脚本生成图表
echo "生成性能图表..."
python plot_performance.py --csv_files build/performance_results_small.csv build/performance_results_large.csv

echo "测试完成! 图表已保存到 performance_charts 目录" 