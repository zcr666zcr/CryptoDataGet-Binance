"""
合并文件夹中的feather文件，按开盘时间排序并保存为CSV
"""
import pandas as pd
import os
from pathlib import Path
from datetime import datetime


def merge_feather_files(folder_path, output_filename=None):
    """
    合并文件夹中的所有feather文件，按开盘时间排序
    
    Args:
        folder_path: 文件夹路径
        output_filename: 输出文件名（可选，默认使用合并后的文件名）
    """
    folder = Path(folder_path)
    
    # 获取所有feather文件
    feather_files = list(folder.glob('*.feather'))
    
    if not feather_files:
        print(f"文件夹 {folder_path} 中没有找到feather文件")
        return
    
    print(f"找到 {len(feather_files)} 个feather文件:")
    for f in feather_files:
        print(f"  - {f.name}")
    
    # 读取所有文件
    dataframes = []
    for file_path in feather_files:
        try:
            df = pd.read_feather(file_path)
            print(f"  读取 {file_path.name}: {len(df)} 条记录")
            dataframes.append(df)
        except Exception as e:
            print(f"  读取 {file_path.name} 失败: {e}")
            continue
    
    if not dataframes:
        print("没有成功读取任何文件")
        return
    
    # 合并所有DataFrame
    print("\n正在合并数据...")
    merged_df = pd.concat(dataframes, ignore_index=True)
    print(f"合并后总记录数: {len(merged_df)}")
    
    # 检查是否有开盘时间列
    if '开盘时间' not in merged_df.columns:
        print("警告: 未找到'开盘时间'列，无法排序")
        print(f"可用列: {merged_df.columns.tolist()}")
        return
    
    # 将开盘时间转换为datetime类型（如果是字符串）
    if merged_df['开盘时间'].dtype == 'object':
        merged_df['开盘时间'] = pd.to_datetime(merged_df['开盘时间'])
    
    # 按开盘时间排序
    print("正在按开盘时间排序...")
    merged_df = merged_df.sort_values('开盘时间').reset_index(drop=True)
    
    # 显示时间范围
    print(f"\n时间范围: {merged_df['开盘时间'].min()} 到 {merged_df['开盘时间'].max()}")
    
    # 生成输出文件名（CSV）
    if output_filename is None:
        first_file = feather_files[0].stem
        if '_' in first_file:
            symbol = first_file.split('_')[0]
            start_time = merged_df['开盘时间'].min().strftime('%Y%m%d_%H%M%S')
            end_time = merged_df['开盘时间'].max().strftime('%Y%m%d_%H%M%S')
            output_filename = f"{symbol}_aggtrades_{start_time}_to_{end_time}_merged.csv"
        else:
            output_filename = f"merged_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    output_path = folder / output_filename
    
    # 如果输出文件已存在，添加时间戳后缀（CSV）
    if output_path.exists():
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = folder / f"{output_path.stem}_{timestamp}.csv"
    
    # 保存合并后的文件为CSV（不删除原文件）
    print(f"\n正在保存到: {output_path}")
    merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"合并完成!")
    print(f"   输出文件: {output_path.name}")
    print(f"   总记录数: {len(merged_df)}")
    print(f"   文件大小: {output_path.stat().st_size / 1024 / 1024:.2f} MB")
    
    return merged_df


if __name__ == '__main__':
    # 设置文件夹路径
    folder_path = r"E:\Quant\data\CoinOptions\期权行情数据1d"
    
    # 执行合并
    merged_df = merge_feather_files(folder_path)
    
    # 显示前几行数据（可选）
    if merged_df is not None:
        print("\n合并后的数据前5行:")
        print(merged_df.head())

