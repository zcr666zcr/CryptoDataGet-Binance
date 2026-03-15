"""
Feather 文件读取和合并工具模块
用于处理历史成交数据的本地文件操作
"""
import pandas as pd
from pathlib import Path
from typing import List, Optional, Tuple
from datetime import datetime, timedelta
import glob
import re


def find_feather_files(data_dir: Path, symbol: str) -> List[Path]:
    """
    查找指定交易对的所有 feather 文件
    
    :param data_dir: 数据目录路径
    :param symbol: 交易对名称，如 'BTCUSDT'
    :return: feather 文件路径列表，按文件名排序
    """
    if not data_dir.exists():
        return []
    
    # 查找所有匹配的 feather 文件
    pattern = f'{symbol}_historical_trades*.feather'
    files = list(data_dir.glob(pattern))
    
    # 按文件名排序（通常文件名包含时间戳，排序后可以按时间顺序）
    files.sort()
    
    return files


def read_all_feather_files(data_dir: Path, symbol: str) -> pd.DataFrame:
    """
    读取指定交易对的所有 feather 文件并合并为一个 DataFrame
    
    :param data_dir: 数据目录路径
    :param symbol: 交易对名称，如 'BTCUSDT'
    :return: 合并后的 DataFrame，如果没有任何文件则返回空 DataFrame
    """
    files = find_feather_files(data_dir, symbol)
    
    if not files:
        print(f"未找到 {symbol} 的 feather 文件")
        return pd.DataFrame()
    
    print(f"找到 {len(files)} 个 feather 文件，开始读取...")
    
    all_dataframes = []
    for file_path in files:
        try:
            df = pd.read_feather(file_path)
            all_dataframes.append(df)
            print(f"  已读取: {file_path.name} ({len(df)} 条记录)")
        except Exception as e:
            print(f"  读取文件失败 {file_path.name}: {e}")
            continue
    
    if not all_dataframes:
        print("所有文件读取失败")
        return pd.DataFrame()
    
    # 合并所有 DataFrame
    print("正在合并数据...")
    merged_df = pd.concat(all_dataframes, ignore_index=True)
    
    # 去重（基于成交ID）
    original_count = len(merged_df)
    merged_df = merged_df.drop_duplicates(subset=['成交ID'], keep='first')
    duplicate_count = original_count - len(merged_df)
    
    if duplicate_count > 0:
        print(f"去重完成，删除了 {duplicate_count} 条重复记录")
    
    # 按成交ID排序
    merged_df = merged_df.sort_values('成交ID').reset_index(drop=True)
    
    print(f"合并完成，共 {len(merged_df)} 条记录")
    return merged_df


def get_max_from_id(data_dir: Path, symbol: str) -> Optional[int]:
    """
    从本地 feather 文件中获取最大的成交ID（fromId）
    优化版本：只读取文件名中结束时间最大的文件，避免读取和合并所有文件
    
    文件名格式：{symbol}_historical_trades_{start_time}_to_{end_time}.feather
    例如：BTCUSDT_historical_trades_20251102_054102_to_20251107_140953.feather
    
    :param data_dir: 数据目录路径
    :param symbol: 交易对名称，如 'BTCUSDT'
    :return: 最大的成交ID，如果没有找到文件则返回 None
    """
    import re
    
    files = find_feather_files(data_dir, symbol)
    
    if not files:
        return None
    
    # 从文件名中提取结束时间，找到结束时间最大的文件
    # 文件名格式：{symbol}_historical_trades_{start}_to_{end}.feather
    # 例如：BTCUSDT_historical_trades_20251102_054102_to_20251107_140953.feather
    pattern = rf'{re.escape(symbol)}_historical_trades_\d{{8}}_\d{{6}}_to_(\d{{8}})_(\d{{6}})\.feather'
    
    latest_file = None
    latest_end_time = None
    
    for file_path in files:
        match = re.search(pattern, file_path.name)
        if match:
            end_date = match.group(1)  # 例如：20251107
            end_time = match.group(2)  # 例如：140953
            end_datetime_str = f"{end_date}_{end_time}"  # 例如：20251107_140953
            
            # 解析为datetime对象用于比较
            try:
                end_datetime = datetime.strptime(end_datetime_str, '%Y%m%d_%H%M%S')
                if latest_end_time is None or end_datetime > latest_end_time:
                    latest_end_time = end_datetime
                    latest_file = file_path
            except ValueError:
                # 如果解析失败，跳过这个文件
                continue
    
    # 如果没有找到匹配的文件，回退到读取所有文件的方法
    if latest_file is None:
        print("警告：无法从文件名提取时间信息，回退到读取所有文件的方法")
        df = read_all_feather_files(data_dir, symbol)
        if df.empty:
            return None
        if '成交ID' not in df.columns:
            print("警告：DataFrame 中未找到 '成交ID' 列")
            return None
        max_id = df['成交ID'].max()
        print(f"本地数据中最大的成交ID: {max_id}")
        return int(max_id)
    
    # 只读取结束时间最大的文件
    print(f"找到最新的文件（按结束时间）: {latest_file.name}")
    print(f"文件结束时间: {latest_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        df = pd.read_feather(latest_file)
        
        if df.empty:
            print("警告：最新文件为空，尝试读取所有文件")
            # 回退到读取所有文件
            df = read_all_feather_files(data_dir, symbol)
            if df.empty:
                return None
        
        if '成交ID' not in df.columns:
            print("警告：DataFrame 中未找到 '成交ID' 列")
            return None
        
        max_id = df['成交ID'].max()
        print(f"本地数据中最大的成交ID: {max_id} (来自文件: {latest_file.name})")
        return int(max_id)
    except Exception as e:
        print(f"读取文件失败 {latest_file.name}: {e}")
        print("回退到读取所有文件的方法")
        # 回退到读取所有文件
        df = read_all_feather_files(data_dir, symbol)
        if df.empty:
            return None
        if '成交ID' not in df.columns:
            print("警告：DataFrame 中未找到 '成交ID' 列")
            return None
        max_id = df['成交ID'].max()
        print(f"本地数据中最大的成交ID: {max_id}")
        return int(max_id)


def merge_and_save_feather(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    save_path: Path,
    remove_duplicates: bool = True
) -> pd.DataFrame:
    """
    合并新旧 DataFrame 并保存为 feather 文件
    
    :param old_df: 旧的 DataFrame（本地已有数据）
    :param new_df: 新的 DataFrame（刚获取的数据）
    :param save_path: 保存路径
    :param remove_duplicates: 是否去重，默认 True
    :return: 合并后的 DataFrame
    """
    if old_df.empty and new_df.empty:
        print("新旧数据都为空，无需保存")
        return pd.DataFrame()
    
    if old_df.empty:
        merged_df = new_df.copy()
        print("本地数据为空，只保存新数据")
    elif new_df.empty:
        merged_df = old_df.copy()
        print("新数据为空，只保存本地数据")
    else:
        # 合并数据
        print(f"合并数据：本地 {len(old_df)} 条 + 新数据 {len(new_df)} 条")
        merged_df = pd.concat([old_df, new_df], ignore_index=True)
        
        if remove_duplicates:
            # 去重（基于成交ID）
            original_count = len(merged_df)
            merged_df = merged_df.drop_duplicates(subset=['成交ID'], keep='first')
            duplicate_count = original_count - len(merged_df)
            
            if duplicate_count > 0:
                print(f"去重完成，删除了 {duplicate_count} 条重复记录")
    
    # 按成交ID排序
    merged_df = merged_df.sort_values('成交ID').reset_index(drop=True)
    
    # 确保保存目录存在
    save_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 保存为 feather 文件
    merged_df.to_feather(save_path)
    print(f"数据已保存到: {save_path}")
    print(f"合并后共 {len(merged_df)} 条记录")
    
    return merged_df


def get_latest_feather_file(data_dir: Path, symbol: str) -> Optional[Path]:
    """
    获取最新的 feather 文件路径（按修改时间）
    
    :param data_dir: 数据目录路径
    :param symbol: 交易对名称，如 'BTCUSDT'
    :return: 最新的文件路径，如果没有找到则返回 None
    """
    files = find_feather_files(data_dir, symbol)
    
    if not files:
        return None
    
    # 按修改时间排序，返回最新的
    latest_file = max(files, key=lambda p: p.stat().st_mtime)
    return latest_file


def get_data_time_range(df: pd.DataFrame, tz=None) -> Optional[Tuple[datetime, datetime]]:
    """
    从DataFrame中获取实际的时间范围
    
    :param df: 历史成交数据DataFrame
    :param tz: 时区对象，如果为None则尝试从DataFrame推断
    :return: (start_time, end_time) 元组，如果DataFrame为空则返回None
    """
    if df.empty or '成交时间' not in df.columns:
        return None
    
    # 转换成交时间为datetime
    df_time = pd.to_datetime(df['成交时间'])
    
    # 获取最小和最大时间
    min_time = df_time.min()
    max_time = df_time.max()
    
    # 确保是datetime对象
    if isinstance(min_time, pd.Timestamp):
        min_time = min_time.to_pydatetime()
    if isinstance(max_time, pd.Timestamp):
        max_time = max_time.to_pydatetime()
    
    # 如果时区信息缺失，尝试添加
    if tz is not None:
        if min_time.tzinfo is None:
            min_time = tz.localize(min_time)
        if max_time.tzinfo is None:
            max_time = tz.localize(max_time)
    
    return (min_time, max_time)


def save_and_merge_by_date(
    data_dir: Path,
    symbol: str,
    new_df: pd.DataFrame,
    save_format: str = 'feather',
    tz=None,
    delete_old_files: bool = True
) -> List[Path]:
    """
    按日期合并新数据：如果新数据在同一天，就与文件夹中同样日期的文件合并
    主要用于定时更新和从最大fromid获取数据时的合并
    
    工作流程：
    1. 检查新数据的时间范围，确定涉及哪些日期
    2. 对于每个日期，查找该日期对应的现有文件
    3. 如果找到同一天的文件，合并它们
    4. 如果没找到，就保存为新文件
    5. 命名逻辑保持原样（使用实际数据的时间范围）
    
    :param data_dir: 数据目录路径
    :param symbol: 交易对名称，如 'BTCUSDT'
    :param new_df: 新下载的数据DataFrame
    :param save_format: 保存格式，'feather' 或 'csv'
    :param tz: 时区对象，用于时间格式化
    :param delete_old_files: 是否删除已合并的旧文件，默认True
    :return: 保存的文件路径列表（可能跨天，所以返回列表）
    """
    if new_df.empty:
        print("新数据为空，跳过保存")
        return []
    
    # 确保保存目录存在
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # 获取新数据的时间范围
    time_range = get_data_time_range(new_df, tz=tz)
    if time_range is None:
        print("警告：无法获取数据时间范围，使用默认保存方式")
        return [save_new_data_only(data_dir, symbol, new_df, save_format, tz)]
    
    start_time, end_time = time_range
    
    # 确保是datetime对象
    if isinstance(start_time, pd.Timestamp):
        start_time = start_time.to_pydatetime()
    if isinstance(end_time, pd.Timestamp):
        end_time = end_time.to_pydatetime()
    
    # 如果时区信息缺失，尝试添加
    if tz is not None:
        if start_time.tzinfo is None:
            start_time = tz.localize(start_time)
        if end_time.tzinfo is None:
            end_time = tz.localize(end_time)
    
    # 获取新数据涉及的日期列表（使用副本避免修改原始数据）
    new_df_copy = new_df.copy()
    new_df_time = pd.to_datetime(new_df_copy['成交时间'])
    new_df_copy['日期'] = new_df_time.dt.date
    unique_dates = sorted(new_df_copy['日期'].unique())
    
    print(f"新数据涉及 {len(unique_dates)} 个日期: {', '.join([str(d) for d in unique_dates])}")
    
    saved_files = []
    files_to_delete = []
    
    # 对每个日期分别处理
    for date in unique_dates:
        print(f"\n处理日期: {date}")
        
        # 获取该日期的数据
        date_df = new_df_copy[new_df_copy['日期'] == date].copy()
        date_df = date_df.drop(columns=['日期'], errors='ignore')
        
        if date_df.empty:
            continue
        
        # 获取该日期数据的时间范围
        date_time_range = get_data_time_range(date_df, tz=tz)
        if date_time_range is None:
            print(f"警告：无法获取日期 {date} 的时间范围，跳过")
            continue
        
        date_start, date_end = date_time_range
        
        # 查找该日期对应的现有文件
        existing_files = find_feather_files(data_dir, symbol)
        matching_files = []
        
        for file_path in existing_files:
            # 从文件名解析时间范围
            file_time_range = _parse_filename_time_range(file_path.name, symbol)
            if file_time_range:
                file_start, file_end = file_time_range
                # 检查文件是否包含该日期
                # 如果文件的开始或结束时间在该日期内，或者该日期在文件的时间范围内
                file_start_date = file_start.date()
                file_end_date = file_end.date()
                
                if file_start_date <= date <= file_end_date:
                    matching_files.append(file_path)
        
        if matching_files:
            print(f"  找到 {len(matching_files)} 个同一天的文件，开始合并...")
            
            # 读取所有匹配的文件
            old_dataframes = []
            for file_path in matching_files:
                try:
                    old_df = pd.read_feather(file_path)
                    if not old_df.empty:
                        old_dataframes.append(old_df)
                        print(f"    已读取: {file_path.name} ({len(old_df)} 条记录)")
                except Exception as e:
                    print(f"    读取文件失败 {file_path.name}: {e}")
                    continue
            
            if old_dataframes:
                # 合并所有旧数据
                old_merged = pd.concat(old_dataframes, ignore_index=True)
                
                # 合并新旧数据
                print(f"    合并数据：旧数据 {len(old_merged)} 条 + 新数据 {len(date_df)} 条")
                merged_df = pd.concat([old_merged, date_df], ignore_index=True)
                
                # 去重（基于成交ID）
                original_count = len(merged_df)
                merged_df = merged_df.drop_duplicates(subset=['成交ID'], keep='first')
                duplicate_count = original_count - len(merged_df)
                
                if duplicate_count > 0:
                    print(f"    去重完成，删除了 {duplicate_count} 条重复记录")
                
                # 获取合并后数据的时间范围
                merged_time_range = get_data_time_range(merged_df, tz=tz)
                if merged_time_range:
                    merged_start, merged_end = merged_time_range
                    start_str = merged_start.strftime('%Y%m%d_%H%M%S')
                    end_str = merged_end.strftime('%Y%m%d_%H%M%S')
                    filename = f'{symbol}_historical_trades_{start_str}_to_{end_str}'
                else:
                    # 如果无法获取时间范围，使用日期范围
                    start_str = date_start.strftime('%Y%m%d_%H%M%S')
                    end_str = date_end.strftime('%Y%m%d_%H%M%S')
                    filename = f'{symbol}_historical_trades_{start_str}_to_{end_str}'
                
                # 保存合并后的数据
                if save_format == 'feather':
                    file_path = data_dir / f'{filename}.feather'
                else:
                    file_path = data_dir / f'{filename}.csv'
                
                # 按成交ID排序
                merged_df = merged_df.sort_values('成交ID').reset_index(drop=True)
                
                if save_format == 'feather':
                    merged_df.to_feather(file_path)
                else:
                    merged_df.to_csv(file_path, index=False, encoding='utf-8-sig')
                
                saved_files.append(file_path)
                print(f"    合并后的数据已保存: {file_path.name} ({len(merged_df)} 条记录)")
                
                # 标记旧文件待删除
                if delete_old_files:
                    files_to_delete.extend(matching_files)
            else:
                # 没有有效的旧数据，直接保存新数据
                print(f"  没有有效的旧数据，直接保存新数据")
                file_path = save_new_data_only(data_dir, symbol, date_df, save_format, tz)
                if file_path:
                    saved_files.append(file_path)
        else:
            # 没有找到同一天的文件，直接保存新数据
            print(f"  未找到同一天的文件，直接保存新数据")
            file_path = save_new_data_only(data_dir, symbol, date_df, save_format, tz)
            if file_path:
                saved_files.append(file_path)
    
    # 删除已合并的旧文件
    if delete_old_files and files_to_delete:
        print(f"\n删除 {len(files_to_delete)} 个已合并的旧文件...")
        for old_file in files_to_delete:
            try:
                # 确保不删除刚保存的新文件
                if old_file not in saved_files:
                    old_file.unlink()
                    print(f"  已删除: {old_file.name}")
            except Exception as e:
                print(f"  删除文件失败 {old_file.name}: {e}")
    
    print(f"\n按日期合并完成，共保存 {len(saved_files)} 个文件")
    return saved_files


def save_new_data_only(
    data_dir: Path,
    symbol: str,
    new_df: pd.DataFrame,
    save_format: str = 'feather',
    tz=None
) -> Optional[Path]:
    """
    只保存新数据，不合并现有文件，不删除旧文件
    文件名使用实际数据的时间范围
    
    :param data_dir: 数据目录路径
    :param symbol: 交易对名称，如 'BTCUSDT'
    :param new_df: 新下载的数据DataFrame
    :param save_format: 保存格式，'feather' 或 'csv'
    :param tz: 时区对象，用于时间格式化
    :return: 保存的文件路径，如果数据为空则返回None
    """
    if new_df.empty:
        print("新数据为空，跳过保存")
        return None
    
    # 获取新数据的时间范围
    time_range = get_data_time_range(new_df, tz=tz)
    if time_range is None:
        print("警告：无法获取数据时间范围，使用时间戳作为文件名")
        from datetime import datetime
        timestamp = datetime.now(tz).strftime('%Y%m%d_%H%M%S')
        filename = f'{symbol}_historical_trades_{timestamp}'
    else:
        start_time, end_time = time_range
        # 格式化时间字符串
        start_str = start_time.strftime('%Y%m%d_%H%M%S')
        end_str = end_time.strftime('%Y%m%d_%H%M%S')
        filename = f'{symbol}_historical_trades_{start_str}_to_{end_str}'
    
    # 保存新数据
    if save_format == 'feather':
        file_path = data_dir / f'{filename}.feather'
    else:
        file_path = data_dir / f'{filename}.csv'
    
    # 确保保存目录存在
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 按成交ID排序
    new_df_sorted = new_df.sort_values('成交ID').reset_index(drop=True)
    
    if save_format == 'feather':
        new_df_sorted.to_feather(file_path)
    else:
        new_df_sorted.to_csv(file_path, index=False, encoding='utf-8-sig')
    
    print(f"新数据已保存到: {file_path}")
    print(f"共 {len(new_df_sorted)} 条记录")
    if time_range:
        print(f"数据时间范围: {time_range[0].strftime('%Y-%m-%d %H:%M:%S')} 到 {time_range[1].strftime('%Y-%m-%d %H:%M:%S')}")
    
    return file_path


def merge_with_existing_and_save(
    data_dir: Path,
    symbol: str,
    new_df: pd.DataFrame,
    save_format: str = 'feather',
    tz=None,
    delete_old_files: bool = True
) -> Optional[Path]:
    """
    合并现有feather文件和新数据，去重后保存，文件名使用实际数据的时间范围
    注意：此函数保留用于以后需要合并时使用，下载数据时请使用 save_new_data_only
    
    :param data_dir: 数据目录路径
    :param symbol: 交易对名称，如 'BTCUSDT'
    :param new_df: 新下载的数据DataFrame
    :param save_format: 保存格式，'feather' 或 'csv'
    :param tz: 时区对象，用于时间格式化
    :param delete_old_files: 是否删除旧的feather文件（合并后），默认True
    :return: 保存的文件路径，如果数据为空则返回None
    """
    if new_df.empty:
        print("新数据为空，跳过保存")
        return None
    
    # 步骤1：读取所有现有的feather文件
    print(f"\n开始合并 {symbol} 的数据...")
    old_df = read_all_feather_files(data_dir, symbol)
    
    # 步骤2：合并新旧数据
    if old_df.empty:
        merged_df = new_df.copy()
        print("本地没有现有数据，只保存新数据")
    else:
        print(f"合并数据：本地 {len(old_df)} 条 + 新数据 {len(new_df)} 条")
        merged_df = pd.concat([old_df, new_df], ignore_index=True)
        
        # 去重（基于成交ID）
        original_count = len(merged_df)
        merged_df = merged_df.drop_duplicates(subset=['成交ID'], keep='first')
        duplicate_count = original_count - len(merged_df)
        
        if duplicate_count > 0:
            print(f"去重完成，删除了 {duplicate_count} 条重复记录")
    
    # 按成交ID排序
    merged_df = merged_df.sort_values('成交ID').reset_index(drop=True)
    
    # 步骤3：获取实际数据的时间范围
    time_range = get_data_time_range(merged_df, tz=tz)
    if time_range is None:
        print("警告：无法获取数据时间范围，使用时间戳作为文件名")
        from datetime import datetime
        timestamp = datetime.now(tz).strftime('%Y%m%d_%H%M%S')
        filename = f'{symbol}_historical_trades_{timestamp}'
    else:
        start_time, end_time = time_range
        # 格式化时间字符串
        start_str = start_time.strftime('%Y%m%d_%H%M%S')
        end_str = end_time.strftime('%Y%m%d_%H%M%S')
        filename = f'{symbol}_historical_trades_{start_str}_to_{end_str}'
    
    # 步骤4：保存合并后的数据
    if save_format == 'feather':
        file_path = data_dir / f'{filename}.feather'
    else:
        file_path = data_dir / f'{filename}.csv'
    
    # 确保保存目录存在
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    if save_format == 'feather':
        merged_df.to_feather(file_path)
    else:
        merged_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    
    print(f"合并后的数据已保存到: {file_path}")
    print(f"合并后共 {len(merged_df)} 条记录")
    if time_range:
        print(f"数据时间范围: {time_range[0].strftime('%Y-%m-%d %H:%M:%S')} 到 {time_range[1].strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 步骤5：删除旧的feather文件（如果启用）
    if delete_old_files and not old_df.empty:
        old_files = find_feather_files(data_dir, symbol)
        # 排除刚保存的新文件
        old_files = [f for f in old_files if f != file_path]
        if old_files:
            print(f"删除 {len(old_files)} 个旧文件...")
            for old_file in old_files:
                try:
                    old_file.unlink()
                    print(f"  已删除: {old_file.name}")
                except Exception as e:
                    print(f"  删除文件失败 {old_file.name}: {e}")
    
    return file_path


def _parse_filename_time_range(filename: str, symbol: str) -> Optional[Tuple[datetime, datetime]]:
    """
    从文件名中解析时间范围
    
    :param filename: 文件名，格式如：BTCUSDT_historical_trades_20251107_144336_to_20251107_151204.feather
    :param symbol: 交易对名称，如 'BTCUSDT'
    :return: (start_time, end_time) 元组，如果解析失败则返回None
    """
    # 匹配格式：{symbol}_historical_trades_{YYYYMMDD}_{HHMMSS}_to_{YYYYMMDD}_{HHMMSS}.feather
    pattern = rf'{re.escape(symbol)}_historical_trades_(\d{{8}})_(\d{{6}})_to_(\d{{8}})_(\d{{6}})\.feather'
    match = re.search(pattern, filename)
    
    if not match:
        return None
    
    try:
        start_date = match.group(1)  # 例如：20251107
        start_time = match.group(2)  # 例如：144336
        end_date = match.group(3)     # 例如：20251107
        end_time = match.group(4)     # 例如：151204
        
        start_datetime_str = f"{start_date}_{start_time}"
        end_datetime_str = f"{end_date}_{end_time}"
        
        start_dt = datetime.strptime(start_datetime_str, '%Y%m%d_%H%M%S')
        end_dt = datetime.strptime(end_datetime_str, '%Y%m%d_%H%M%S')
        
        return (start_dt, end_dt)
    except ValueError:
        return None


def _has_millisecond_precision(time_str) -> bool:
    """
    判断时间字符串是否包含毫秒精度
    
    :param time_str: 时间字符串，格式如 '2025-11-07 14:30:25.123' 或 '2025-11-07 14:30:25'
    :return: 如果包含毫秒精度则返回True，否则返回False
    """
    if pd.isna(time_str):
        return False
    time_str = str(time_str)
    # 检查是否包含毫秒格式：YYYY-MM-DD HH:MM:SS.mmm
    return '.' in time_str and len(time_str.split('.')[-1]) <= 3


def _is_already_split_by_day(filename: str, symbol: str) -> bool:
    """
    检查文件名是否已经按天分割（从00:00:00到23:59:59，且是同一天）
    
    :param filename: 文件名
    :param symbol: 交易对名称
    :return: 如果已经按天分割则返回True
    """
    time_range = _parse_filename_time_range(filename, symbol)
    if time_range is None:
        return False
    
    start_dt, end_dt = time_range
    
    # 检查是否是同一天
    if start_dt.date() != end_dt.date():
        return False
    
    # 检查开始时间是否是当天的00:00:00
    if start_dt.hour != 0 or start_dt.minute != 0 or start_dt.second != 0:
        return False
    
    # 检查结束时间是否是当天的23:59:59（文件名中只精确到秒，所以检查23:59:59）
    if end_dt.hour != 23 or end_dt.minute != 59 or end_dt.second != 59:
        return False
    
    return True


def reorganize_feather_files_by_day(
    data_dir: Path,
    symbol: str,
    tz=None,
    delete_old_files: bool = True
) -> List[Path]:
    """
    对目标路径文件夹内的所有feather文件进行重新分割
    以一天（0点0分0秒到23点59分59秒999毫秒）为一个文件储存交易记录的跨度
    
    文件名格式：{symbol}_historical_trades_{YYYYMMDD}_{HHMMSS}_to_{YYYYMMDD}_{HHMMSS}.feather
    例如：BTCUSDT_historical_trades_20251107_000000_to_20251107_235959.feather
    
    在开始分割前先读取文件名，如果时间跨度已经符合要求（同一天，从00:00:00到23:59:59）就无需重排
    
    如果有一天数据无法覆盖全天，那么命名就按照最早交易时间到最晚交易时间来
    
    :param data_dir: 数据目录路径
    :param symbol: 交易对名称，如 'BTCUSDT'
    :param tz: 时区对象，用于时间处理
    :param delete_old_files: 是否删除旧的feather文件（重新分割后），默认True
    :return: 新创建的文件路径列表
    """
    if not data_dir.exists():
        print(f"数据目录不存在: {data_dir}")
        return []
    
    # 查找所有匹配的 feather 文件
    files = find_feather_files(data_dir, symbol)
    
    if not files:
        print(f"未找到 {symbol} 的 feather 文件")
        return []
    
    print(f"\n开始检查 {len(files)} 个 feather 文件...")
    
    # 检查哪些文件需要重新分割
    files_to_process = []
    files_already_split = []
    
    for file_path in files:
        if _is_already_split_by_day(file_path.name, symbol):
            files_already_split.append(file_path)
            print(f"  已按天分割（跳过）: {file_path.name}")
        else:
            files_to_process.append(file_path)
            print(f"  需要重新分割: {file_path.name}")
    
    # 如果所有文件都已经按天分割，直接返回
    if not files_to_process:
        print(f"\n所有文件已经按天分割，无需重新分割")
        return [f for f in files_already_split]
    
    print(f"\n需要处理 {len(files_to_process)} 个文件，开始读取和合并数据...")
    
    # 读取所有需要处理的文件
    all_dataframes = []
    for file_path in files_to_process:
        try:
            df = pd.read_feather(file_path)
            if not df.empty:
                all_dataframes.append(df)
                print(f"  已读取: {file_path.name} ({len(df)} 条记录)")
        except Exception as e:
            print(f"  读取文件失败 {file_path.name}: {e}")
            continue
    
    if not all_dataframes:
        print("所有文件读取失败或为空")
        return []
    
    # 合并所有 DataFrame
    print("\n正在合并数据...")
    merged_df = pd.concat(all_dataframes, ignore_index=True)
    
    # 去重（基于成交ID，保留时间精度更高的记录）
    original_count = len(merged_df)
    
    # 添加精度标记列
    merged_df['_has_millisecond'] = merged_df['成交时间'].apply(_has_millisecond_precision)
    
    # 按成交ID分组，对于重复的ID，保留时间精度更高的记录
    # 如果精度相同，保留第一条
    def keep_higher_precision(group):
        """对于同一成交ID的多条记录，保留时间精度更高的"""
        if len(group) == 1:
            return group
        
        # 按精度排序：毫秒级（True）优先于秒级（False）
        # 使用 stable sort 保持相同精度内的原始顺序
        group_sorted = group.sort_values('_has_millisecond', ascending=False, kind='mergesort')
        
        # 返回精度最高的第一条记录
        return group_sorted.iloc[[0]]
    
    merged_df = merged_df.groupby('成交ID', group_keys=False).apply(keep_higher_precision).reset_index(drop=True)
    
    # 删除辅助列
    merged_df = merged_df.drop(columns=['_has_millisecond'], errors='ignore')
    
    duplicate_count = original_count - len(merged_df)
    
    if duplicate_count > 0:
        print(f"去重完成，删除了 {duplicate_count} 条重复记录（保留时间精度更高的记录）")
    
    # 检查是否有成交时间列
    if '成交时间' not in merged_df.columns:
        print("错误：DataFrame 中未找到 '成交时间' 列")
        return []
    
    # 解析成交时间（支持带毫秒和不带毫秒的格式）
    print("\n正在解析成交时间...")
    df_time = pd.to_datetime(merged_df['成交时间'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
    if df_time.isna().any():
        df_time = pd.to_datetime(merged_df['成交时间'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    
    # 添加时间列用于分组
    merged_df['成交时间_dt'] = df_time
    
    # 移除无法解析时间的记录
    valid_mask = ~df_time.isna()
    if not valid_mask.all():
        invalid_count = (~valid_mask).sum()
        print(f"警告：有 {invalid_count} 条记录无法解析时间，将被忽略")
        merged_df = merged_df[valid_mask].copy()
    
    if merged_df.empty:
        print("合并后的数据为空")
        return []
    
    # 按日期分组
    print("\n正在按天分组数据...")
    merged_df['日期'] = merged_df['成交时间_dt'].dt.date
    
    # 获取所有日期
    unique_dates = sorted(merged_df['日期'].unique())
    print(f"找到 {len(unique_dates)} 个不同的日期")
    
    # 为每个日期创建文件
    new_files = []
    
    for date in unique_dates:
        day_df = merged_df[merged_df['日期'] == date].copy()
        
        if day_df.empty:
            continue
        
        # 获取当天的实际时间范围
        day_start = day_df['成交时间_dt'].min()
        day_end = day_df['成交时间_dt'].max()
        
        # 确保是datetime对象
        if isinstance(day_start, pd.Timestamp):
            day_start = day_start.to_pydatetime()
        if isinstance(day_end, pd.Timestamp):
            day_end = day_end.to_pydatetime()
        
        # 如果时区信息缺失，尝试添加
        if tz is not None:
            if day_start.tzinfo is None:
                day_start = tz.localize(day_start)
            if day_end.tzinfo is None:
                day_end = tz.localize(day_end)
        
        # 检查是否是完整的一天（从00:00:00.000到23:59:59.999）
        # 由于文件名只精确到秒，我们检查：
        # 1. 开始时间是否是当天的00:00:00（允许毫秒为0-999）
        # 2. 结束时间是否至少是当天的23:59:59（允许毫秒为0-999）
        day_start_normalized = day_start.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end_min = day_end.replace(hour=23, minute=59, second=59, microsecond=0)
        
        # 检查开始时间是否在00:00:00到00:00:00.999之间
        is_start_at_midnight = (day_start.date() == day_start_normalized.date() and 
                                day_start.hour == 0 and day_start.minute == 0 and day_start.second == 0)
        
        # 检查结束时间是否至少是23:59:59.000
        is_end_at_late = (day_end.date() == day_end_min.date() and 
                          day_end.hour == 23 and day_end.minute == 59 and day_end.second == 59)
        
        is_full_day = is_start_at_midnight and is_end_at_late
        
        if is_full_day:
            # 完整的一天，使用标准格式：00:00:00 到 23:59:59
            start_str = day_start_normalized.strftime('%Y%m%d_%H%M%S')
            day_end_normalized = day_end.replace(hour=23, minute=59, second=59, microsecond=0)
            end_str = day_end_normalized.strftime('%Y%m%d_%H%M%S')
        else:
            # 不完整的一天，使用实际时间范围
            start_str = day_start.strftime('%Y%m%d_%H%M%S')
            end_str = day_end.strftime('%Y%m%d_%H%M%S')
        
        # 生成文件名
        filename = f'{symbol}_historical_trades_{start_str}_to_{end_str}.feather'
        file_path = data_dir / filename
        
        # 删除时间相关的辅助列
        day_df_clean = day_df.drop(columns=['成交时间_dt', '日期'], errors='ignore')
        
        # 再次去重检查（虽然理论上不应该有重复，但为了保险起见）
        # 添加精度标记列
        day_df_clean['_has_millisecond'] = day_df_clean['成交时间'].apply(_has_millisecond_precision)
        
        # 按成交ID分组，对于重复的ID，保留时间精度更高的记录
        def keep_higher_precision_local(group):
            """对于同一成交ID的多条记录，保留时间精度更高的"""
            if len(group) == 1:
                return group
            group_sorted = group.sort_values('_has_millisecond', ascending=False, kind='mergesort')
            return group_sorted.iloc[[0]]
        
        day_df_clean = day_df_clean.groupby('成交ID', group_keys=False).apply(keep_higher_precision_local).reset_index(drop=True)
        
        # 删除辅助列
        day_df_clean = day_df_clean.drop(columns=['_has_millisecond'], errors='ignore')
        
        # 按成交ID排序
        day_df_clean = day_df_clean.sort_values('成交ID').reset_index(drop=True)
        
        # 保存文件
        day_df_clean.to_feather(file_path)
        new_files.append(file_path)
        
        print(f"  已保存: {filename} ({len(day_df_clean)} 条记录, {day_start.strftime('%Y-%m-%d %H:%M:%S')} 到 {day_end.strftime('%Y-%m-%d %H:%M:%S')})")
    
    # 删除旧文件（如果需要）
    if delete_old_files and files_to_process:
        print(f"\n删除 {len(files_to_process)} 个旧文件...")
        for old_file in files_to_process:
            try:
                old_file.unlink()
                print(f"  已删除: {old_file.name}")
            except Exception as e:
                print(f"  删除文件失败 {old_file.name}: {e}")
    
    print(f"\n重新分割完成！共创建 {len(new_files)} 个新文件")
    
    # 返回所有文件（新创建的 + 已经按天分割的）
    return new_files + files_already_split


def _extract_symbol_from_filename(filename: str) -> Optional[str]:
    """
    从文件名中提取交易对符号
    
    :param filename: 文件名，格式如：BTCUSDT_historical_trades_20251107_000000_to_20251107_235959.feather
    :return: 交易对符号，如果解析失败则返回None
    """
    # 匹配格式：{symbol}_historical_trades_*.feather
    pattern = r'^([A-Z0-9]+)_historical_trades_.*\.feather$'
    match = re.match(pattern, filename)
    
    if match:
        return match.group(1)
    return None


def merge_all_feather_files(
    data_dir: Path,
    save_merged: bool = True,
    delete_old_files: bool = False
) -> dict:
    """
    合并指定目录下所有交易对的所有 feather 文件
    
    :param data_dir: 数据目录路径
    :param save_merged: 是否保存合并后的文件，默认True
    :param delete_old_files: 是否删除旧的feather文件（合并后），默认False
    :return: 字典，键为交易对符号，值为合并后的DataFrame或文件路径
    """
    if not data_dir.exists():
        print(f"数据目录不存在: {data_dir}")
        return {}
    
    # 查找所有 feather 文件
    all_files = list(data_dir.glob('*_historical_trades*.feather'))
    
    if not all_files:
        print(f"未找到任何 feather 文件")
        return {}
    
    print(f"\n找到 {len(all_files)} 个 feather 文件，开始按交易对分组...")
    
    # 按交易对分组文件
    symbol_files = {}
    for file_path in all_files:
        symbol = _extract_symbol_from_filename(file_path.name)
        if symbol:
            if symbol not in symbol_files:
                symbol_files[symbol] = []
            symbol_files[symbol].append(file_path)
        else:
            print(f"警告：无法从文件名提取交易对: {file_path.name}")
    
    if not symbol_files:
        print("未找到有效的交易对文件")
        return {}
    
    print(f"找到 {len(symbol_files)} 个不同的交易对: {', '.join(symbol_files.keys())}")
    
    # 对每个交易对合并文件
    results = {}
    
    for symbol, files in symbol_files.items():
        print(f"\n{'='*60}")
        print(f"处理交易对: {symbol} ({len(files)} 个文件)")
        print(f"{'='*60}")
        
        # 使用现有的 read_all_feather_files 函数合并
        merged_df = read_all_feather_files(data_dir, symbol)
        
        if merged_df.empty:
            print(f"警告：{symbol} 合并后的数据为空")
            continue
        
        results[symbol] = merged_df
        
        # 如果需要保存合并后的文件
        if save_merged:
            # 获取数据的时间范围
            from datetime import datetime
            time_range = get_data_time_range(merged_df)
            
            if time_range:
                start_time, end_time = time_range
                start_str = start_time.strftime('%Y%m%d_%H%M%S')
                end_str = end_time.strftime('%Y%m%d_%H%M%S')
                filename = f'{symbol}_historical_trades_{start_str}_to_{end_str}.feather'
            else:
                # 如果无法获取时间范围，使用时间戳
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'{symbol}_historical_trades_merged_{timestamp}.feather'
            
            merged_file_path = data_dir / filename
            merged_df.to_feather(merged_file_path)
            print(f"\n合并后的文件已保存: {merged_file_path.name}")
            print(f"共 {len(merged_df)} 条记录")
            
            results[symbol] = merged_file_path
            
            # 如果需要删除旧文件
            if delete_old_files:
                print(f"\n删除 {len(files)} 个旧文件...")
                for old_file in files:
                    try:
                        # 确保不删除刚保存的合并文件
                        if old_file != merged_file_path:
                            old_file.unlink()
                            print(f"  已删除: {old_file.name}")
                    except Exception as e:
                        print(f"  删除文件失败 {old_file.name}: {e}")
    
    print(f"\n{'='*60}")
    print(f"合并完成！共处理 {len(results)} 个交易对")
    print(f"{'='*60}")
    
    return results


def main():
    """
    主函数：对 Settings 中配置的历史成交数据目录下的所有 feather 文件按日期进行分割
    """
    try:
        # 尝试两种导入方式以兼容不同的运行环境
        try:
            from Settings import data_path, TIMEZONE
        except ImportError:
            from CoinOrderbook.Settings import data_path, TIMEZONE
        
        import pytz
        
        # 获取数据目录路径
        data_dir = data_path['result_path_historical_trades']
        
        # 获取时区对象
        tz = pytz.timezone(TIMEZONE) if TIMEZONE else None
        
        print(f"数据目录: {data_dir}")
        print(f"时区: {TIMEZONE}")
        print(f"{'='*60}\n")
        
        if not data_dir.exists():
            print(f"数据目录不存在: {data_dir}")
            return
        
        # 查找所有 feather 文件
        all_files = list(data_dir.glob('*_historical_trades*.feather'))
        
        if not all_files:
            print("未找到任何 feather 文件")
            return
        
        print(f"找到 {len(all_files)} 个 feather 文件，开始识别交易对...\n")
        
        # 提取所有唯一的交易对符号
        symbols = set()
        for file_path in all_files:
            symbol = _extract_symbol_from_filename(file_path.name)
            if symbol:
                symbols.add(symbol)
        
        if not symbols:
            print("未能从文件名中识别出任何交易对")
            return
        
        symbols = sorted(symbols)
        print(f"识别出 {len(symbols)} 个交易对: {', '.join(symbols)}\n")
        print(f"{'='*60}\n")
        
        # 对每个交易对进行按日期分割
        all_results = {}
        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i}/{len(symbols)}] 处理交易对: {symbol}")
            print(f"{'-'*60}")
            
            try:
                # 按日期分割该交易对的所有文件
                # delete_old_files=False: 不删除旧文件（安全起见）
                result_files = reorganize_feather_files_by_day(
                    data_dir=data_dir,
                    symbol=symbol,
                    tz=tz,
                    delete_old_files=False
                )
                
                all_results[symbol] = result_files
                print(f"\n✓ {symbol} 处理完成，共 {len(result_files)} 个文件")
                
            except Exception as e:
                print(f"\n✗ {symbol} 处理失败: {e}")
                import traceback
                traceback.print_exc()
                all_results[symbol] = []
        
        # 汇总结果
        print(f"\n{'='*60}")
        print(f"\n处理完成！")
        print(f"共处理 {len(symbols)} 个交易对")
        
        total_files = sum(len(files) for files in all_results.values())
        print(f"共生成 {total_files} 个文件")
        
        print(f"\n各交易对文件数量:")
        for symbol in symbols:
            file_count = len(all_results.get(symbol, []))
            print(f"  {symbol}: {file_count} 个文件")
            
    except ImportError as e:
        print(f"导入错误: {e}")
        print("请确保 Settings.py 文件存在且配置正确")
    except Exception as e:
        print(f"执行错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
