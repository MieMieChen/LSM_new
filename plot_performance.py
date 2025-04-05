import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import glob
import argparse

def main():
    parser = argparse.ArgumentParser(description='Generate performance charts from CSV files')
    parser.add_argument('--csv_files', nargs='+', help='CSV files to process (wildcards supported, e.g., "*.csv")')
    parser.add_argument('--output_dir', default='performance_charts', help='Directory to save charts')
    args = parser.parse_args()

    # 创建输出目录
    output_dir = args.output_dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 查找CSV文件
    csv_files = []
    if args.csv_files:
        for pattern in args.csv_files:
            csv_files.extend(glob.glob(pattern))
    else:
        # 默认使用当前目录中的performance_results*.csv文件
        csv_files = glob.glob('performance_results*.csv')

    if not csv_files:
        print("No CSV files found. Please specify CSV files with --csv_files option.")
        return

    # 读取并合并所有CSV文件
    print(f"Reading data from {len(csv_files)} CSV files...")
    dfs = []
    for csv_file in csv_files:
        if os.path.exists(csv_file):
            print(f"Reading {csv_file}...")
            df = pd.read_csv(csv_file)
            # 添加文件名列以便于跟踪数据来源
            df['source_file'] = os.path.basename(csv_file)
            dfs.append(df)
        else:
            print(f"Warning: File {csv_file} not found, skipping.")

    if not dfs:
        print("No valid CSV files found. Exiting.")
        return

    # 合并所有数据框
    df = pd.concat(dfs, ignore_index=True)
    
    # 按操作类型和数据大小去重（如果有重复）
    df = df.drop_duplicates(subset=['Operation', 'DataSize'])
    
    # 按数据大小排序
    df = df.sort_values(by=['Operation', 'DataSize'])
    
    print(f"Combined data has {len(df)} rows with operations: {df['Operation'].unique()}")
    print(f"Data sizes in combined data: {sorted(df['DataSize'].unique())}")

    # 设置可视化样式
    sns.set(style="whitegrid")
    plt.rcParams.update({'font.size': 12})

    # 生成图表
    plot_latency_charts(df, output_dir)
    plot_throughput_charts(df, output_dir)
    plot_comparison_charts(df, output_dir)

    print(f"Chart generation complete! All charts saved to the '{output_dir}' directory.")

# 生成平均延迟图表
def plot_latency_charts(df, output_dir):
    # 按操作类型分组的平均延迟
    plt.figure(figsize=(12, 8))
    for operation in df['Operation'].unique():
        operation_data = df[df['Operation'] == operation]
        plt.plot(operation_data['DataSize'], operation_data['AvgLatency'], 
                marker='o', linewidth=2, label=operation)

    plt.xlabel('Data Size (number of operations)')
    plt.ylabel('Average Latency (microseconds)')
    plt.title('Average Latency vs Data Size by Operation Type')
    plt.legend()
    plt.grid(True)
    # 对于非常不同的数据量级，使用对数刻度
    if max(df['DataSize']) / min(df['DataSize']) > 10:
        plt.xscale('log')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/avg_latency_by_data_size.png')
    print(f"Saved chart to {output_dir}/avg_latency_by_data_size.png")
    plt.close()
    
    # 分位数延迟图表 (P50, P95, P99)
    for operation in df['Operation'].unique():
        plt.figure(figsize=(12, 8))
        operation_data = df[df['Operation'] == operation]
        
        plt.plot(operation_data['DataSize'], operation_data['P50Latency'], 
                marker='o', linewidth=2, label='P50')
        plt.plot(operation_data['DataSize'], operation_data['P95Latency'], 
                marker='s', linewidth=2, label='P95')
        plt.plot(operation_data['DataSize'], operation_data['P99Latency'], 
                marker='^', linewidth=2, label='P99')
        
        plt.xlabel('Data Size (number of operations)')
        plt.ylabel('Latency (microseconds)')
        plt.title(f'{operation} Latency Percentiles vs Data Size')
        plt.legend()
        plt.grid(True)
        # 对于非常不同的数据量级，使用对数刻度
        if max(df['DataSize']) / min(df['DataSize']) > 10:
            plt.xscale('log')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/{operation}_percentiles.png')
        print(f"Saved chart to {output_dir}/{operation}_percentiles.png")
        plt.close()

# 生成吞吐量图表
def plot_throughput_charts(df, output_dir):
    plt.figure(figsize=(12, 8))
    for operation in df['Operation'].unique():
        operation_data = df[df['Operation'] == operation]
        plt.plot(operation_data['DataSize'], operation_data['Throughput'], 
                marker='o', linewidth=2, label=operation)

    plt.xlabel('Data Size (number of operations)')
    plt.ylabel('Throughput (operations/second)')
    plt.title('Throughput vs Data Size by Operation Type')
    plt.legend()
    plt.grid(True)
    # 对于非常不同的数据量级，使用对数刻度
    if max(df['DataSize']) / min(df['DataSize']) > 10:
        plt.xscale('log')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/throughput_by_data_size.png')
    print(f"Saved chart to {output_dir}/throughput_by_data_size.png")
    plt.close()

# 生成操作类型对比柱状图
def plot_comparison_charts(df, output_dir):
    # 选择一个数据量进行对比
    for data_size in sorted(df['DataSize'].unique()):
        data_size_df = df[df['DataSize'] == data_size]
        
        if len(data_size_df) < 2:  # 至少需要2个操作才能比较
            continue
        
        # 延迟对比
        plt.figure(figsize=(10, 6))
        chart = sns.barplot(x='Operation', y='AvgLatency', data=data_size_df)
        for i, bar in enumerate(chart.patches):
            chart.text(bar.get_x() + bar.get_width()/2., 
                      bar.get_height() + 5,
                      f'{bar.get_height():.2f}',
                      ha='center')
        plt.title(f'Average Latency by Operation (Data Size: {data_size})')
        plt.ylabel('Average Latency (microseconds)')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/latency_comparison_{data_size}.png')
        print(f"Saved chart to {output_dir}/latency_comparison_{data_size}.png")
        plt.close()
        
        # 吞吐量对比
        plt.figure(figsize=(10, 6))
        chart = sns.barplot(x='Operation', y='Throughput', data=data_size_df)
        for i, bar in enumerate(chart.patches):
            chart.text(bar.get_x() + bar.get_width()/2., 
                      bar.get_height() + 5,
                      f'{bar.get_height():.2f}',
                      ha='center')
        plt.title(f'Throughput by Operation (Data Size: {data_size})')
        plt.ylabel('Throughput (operations/second)')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/throughput_comparison_{data_size}.png')
        print(f"Saved chart to {output_dir}/throughput_comparison_{data_size}.png")
        plt.close()

if __name__ == "__main__":
    main() 