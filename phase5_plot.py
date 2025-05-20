import pandas as pd
import matplotlib.pyplot as plt
import io # To read the string data as a file
import os

# Data provided by the user as a string
data_string = """
128次 36线程
Paralle Total time: 1208 milliseconds

128次 32线程
Paralle Total time: 1161 milliseconds

128次 28线程
Paralle Total time: 1016 milliseconds

128次 24线程
Paralle Total time: 1116 milliseconds

128次 16线程
Paralle Total time: 1213 milliseconds

128次 8线程
Paralle Total time: 1831 milliseconds

128次 4线程
Paralle Total time: 2642 milliseconds

128次 2线程
Paralle Total time: 5018 milliseconds

128次 单线程
Paralle Total time: 9772 milliseconds
"""

def analyze_and_visualize_thread_performance(data_str, iterations=128, cpu_cores=24, cpu_threads=32):
    """
    Parses thread performance data, calculates metrics, saves to CSV, and generates plots.

    Args:
        data_str (str): The raw string data containing thread counts and times.
        iterations (int): The number of times the operation was repeated for each test.
        cpu_cores (int): Number of physical CPU cores.
        cpu_threads (int): Number of logical CPU threads (includes hyper-threading).
    """
    data_io = io.StringIO(data_str)
    lines = data_io.readlines()

    threads = []
    total_times_ms = []

    i = 0
    while i < len(lines):
        thread_line = lines[i].strip()
        
        if not thread_line:
            i += 1
            continue

        if "次" in thread_line and ("线程" in thread_line or "单线程" in thread_line):
            if "单线程" in thread_line:
                threads.append(1)
            else:
                parts = thread_line.split('次 ')
                if len(parts) > 1:
                    thread_count_str = parts[1].replace('线程', '')
                    try:
                        thread_count = int(thread_count_str)
                        threads.append(thread_count)
                    except ValueError:
                        print(f"Warning: Could not parse thread count from '{thread_line}'. Skipping.")
                        i += 1
                        continue
                else:
                    print(f"Warning: Unexpected format for thread line '{thread_line}'. Skipping.")
                    i += 1
                    continue

            if i + 1 < len(lines):
                time_line = lines[i+1].strip()
                if "Paralle Total time:" in time_line:
                    try:
                        total_time = int(time_line.split(': ')[1].replace(' milliseconds', ''))
                        total_times_ms.append(total_time)
                        i += 2
                    except (IndexError, ValueError):
                        print(f"Warning: Could not parse time from '{time_line}'. Skipping pair.")
                        threads.pop()
                        i += 2
                else:
                    print(f"Warning: Expected time line but got '{time_line}'. Skipping pair.")
                    threads.pop()
                    i += 1
            else:
                print(f"Warning: Expected time line after '{thread_line}', but reached end of data. Skipping.")
                threads.pop()
                i += 1
        else:
            print(f"Warning: Unrecognized line format '{thread_line}'. Skipping.")
            i += 1

    if len(threads) != len(total_times_ms):
        print(f"Error: Mismatch in number of parsed threads ({len(threads)}) and times ({len(total_times_ms)}). Data might be corrupted.")
        min_len = min(len(threads), len(total_times_ms))
        threads = threads[:min_len]
        total_times_ms = total_times_ms[:min_len]
        if min_len == 0:
            print("No valid data pairs parsed. Exiting analysis.")
            return

    # Create a DataFrame
    df = pd.DataFrame({
        'Threads': threads,
        'Total_Time_ms': total_times_ms
    })

    # Sort by threads for plotting purposes
    df = df.sort_values('Threads')

    # Calculate time per iteration (in milliseconds)
    df['Time_Per_Iteration_ms'] = df['Total_Time_ms'] / iterations

    # Save to CSV
    output_csv_filename = "thread_performance_summary.csv"
    df.to_csv(output_csv_filename, index=False)
    print(f"Data saved to {output_csv_filename}")

    # --- Plotting ---
    plt.style.use('ggplot') # A nice plotting style

    # Plot 1: Total Time vs. Threads
    plt.figure(figsize=(12, 7))
    # --- ALL LABELS/TITLES ARE NOW ENGLISH ---
    plt.plot(df['Threads'], df['Total_Time_ms'], marker='o', linestyle='-', color='b', label='Total Time (ms)')
    plt.title('Total Execution Time vs. Number of Threads')
    plt.xlabel('Number of Threads')
    plt.ylabel('Total Time (milliseconds)')
    plt.xticks(df['Threads']) # Ensure all thread counts are shown on x-axis
    plt.grid(True)
    plt.legend() # Legend will now display English labels
    
    # Add vertical lines for physical cores and logical threads
    plt.axvline(x=cpu_cores, color='r', linestyle='--', label=f'Physical Cores ({cpu_cores})')
    plt.axvline(x=cpu_threads, color='g', linestyle='--', label=f'Logical Threads ({cpu_threads})')
    
    # Add text labels for min/max (using English text)
    min_time_row = df.loc[df['Total_Time_ms'].idxmin()]
    plt.text(min_time_row['Threads'], min_time_row['Total_Time_ms'],
             f'Min: {min_time_row["Total_Time_ms"]}ms ({min_time_row["Threads"]}T)',
             ha='center', va='bottom', color='darkred', weight='bold')

    plt.tight_layout()
    plt.savefig('total_time_vs_threads.png')
    print("Plot saved: total_time_vs_threads.png")
    plt.close()

    # Plot 2: Time Per Iteration vs. Threads
    plt.figure(figsize=(12, 7))
    # --- ALL LABELS/TITLES ARE NOW ENGLISH ---
    plt.plot(df['Threads'], df['Time_Per_Iteration_ms'], marker='x', linestyle='--', color='purple', label='Time Per Iteration (ms)')
    plt.title('Average Time Per Iteration vs. Number of Threads')
    plt.xlabel('Number of Threads')
    plt.ylabel('Time Per Iteration (milliseconds)')
    plt.xticks(df['Threads'])
    plt.grid(True)
    plt.legend() # Legend will now display English labels
    
    # Add vertical lines for physical cores and logical threads
    plt.axvline(x=cpu_cores, color='r', linestyle='--', label=f'Physical Cores ({cpu_cores})')
    plt.axvline(x=cpu_threads, color='g', linestyle='--', label=f'Logical Threads ({cpu_threads})')
    
    min_time_per_iter_row = df.loc[df['Time_Per_Iteration_ms'].idxmin()]
    plt.text(min_time_per_iter_row['Threads'], min_time_per_iter_row['Time_Per_Iteration_ms'],
             f'Min: {min_time_per_iter_row["Time_Per_Iteration_ms"]:.2f}ms ({min_time_per_iter_row["Threads"]}T)',
             ha='center', va='bottom', color='darkred', weight='bold')

    plt.tight_layout()
    plt.savefig('time_per_iteration_vs_threads.png')
    print("Plot saved: time_per_iteration_vs_threads.png")
    plt.close()

    print("\nAnalysis complete. Check your current directory for the CSV and PNG files.")

if __name__ == "__main__":
    analyze_and_visualize_thread_performance(data_string)