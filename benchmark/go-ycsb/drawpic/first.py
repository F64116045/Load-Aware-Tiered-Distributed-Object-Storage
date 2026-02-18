import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# 1. 準備實驗數據
data = {
    'Size': [100, 500, 1000, 1500],
    'Replication_OPS': [317.3, 323.8, 238.2, 268.4],
    'ErasureCoding_OPS': [230.2, 190.5, 118.9, 184.0],
    'Hybrid_OPS': [264.0, 257.1, 293.4, 240.9],
    'Replication_Update_Lat': [96.43, 95.17, 127.31, 113.71],
    'ErasureCoding_Update_Lat': [132.88, 155.95, 251.20, 163.83],
    'Hybrid_Update_Lat': [117.81, 120.66, 105.45, 128.95]
}

df = pd.DataFrame(data)

# 設定繪圖參數
plt.rcParams.update({'font.size': 12})

# 第一張圖：吞吐量趨勢圖 (Throughput Trends)
plt.figure(figsize=(10, 6))
plt.plot(df['Size'], df['Replication_OPS'], marker='o', color='red', label='Replication', linewidth=2)
plt.plot(df['Size'], df['ErasureCoding_OPS'], marker='s', color='blue', label='Erasure Coding', linewidth=2)
plt.plot(df['Size'], df['Hybrid_OPS'], marker='^', color='green', label='Field-Hybrid', linewidth=3)

plt.xlabel('Data Size (KB)')
plt.ylabel('Throughput (OPS)')
plt.title('Throughput Performance Comparison (30 Threads)')
plt.xticks(df['Size'])
plt.legend()
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig('throughput_comparison.png')

# 第二張圖：寫入延遲對比長條圖 (Update Latency)
x = np.arange(len(df['Size']))
width = 0.25

plt.figure(figsize=(10, 6))
plt.bar(x - width, df['Replication_Update_Lat'], width, label='Replication', color='salmon')
plt.bar(x, df['ErasureCoding_Update_Lat'], width, label='Erasure Coding', color='skyblue')
plt.bar(x + width, df['Hybrid_Update_Lat'], width, label='Field-Hybrid', color='lightgreen')

plt.xlabel('Data Size (KB)')
plt.ylabel('Avg Update Latency (ms)')
plt.title('Update Latency Comparison')
plt.xticks(x, df['Size'])
plt.legend()
plt.grid(axis='y', linestyle='--', alpha=0.7)

plt.tight_layout()
plt.savefig('latency_comparison.png')

print("圖表已生成：throughput_comparison.png, latency_comparison.png")