import pandas as pd
from pathlib import Path

# 参数：是否转换为CSV文件
convert_to_csv = True# 设置为 True 时，会将 feather 文件转换为同文件夹下的 CSV 文件

feather_path = rf"E:\Quant\data\CoinKline\现货行情数据1d\2025-11-13.feather"
df = pd.read_feather(feather_path)
print(df)
df1=df.head()
print('ok')

# 如果参数为 True，则转换为 CSV
if convert_to_csv:
    feather_file = Path(feather_path)
    csv_path = feather_file.with_suffix('.csv')
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f'已转换为 CSV 文件：{csv_path}')