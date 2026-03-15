"""
CSV转Feather工具模块
用于将币安官网下载的CSV文件转换为feather格式，并核对数据一致性
"""
import pandas as pd
import pytz
from pathlib import Path
from typing import Optional, Tuple, Dict, List
from datetime import datetime
import warnings
import re
from Settings import data_path, TIMEZONE

warnings.simplefilter(action='ignore', category=FutureWarning)

# 币安官网下载的CSV文件列名（按顺序）
BINANCE_CSV_COLUMNS = ['id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker', 'isBestMatch']


def _format_datetime_with_milliseconds(dt_series):
    """
    格式化datetime系列为字符串，保留毫秒精度
    格式：'YYYY-MM-DD HH:MM:SS.mmm'（保留3位毫秒）
    
    :param dt_series: pandas Series of datetime objects
    :return: pandas Series of strings with millisecond precision
    """
    # 使用 %f 格式（微秒），然后截取前23个字符（去掉最后3位微秒，保留前3位毫秒）
    time_str = dt_series.dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    # 截取前23个字符：'YYYY-MM-DD HH:MM:SS.mmm'（23个字符）
    return time_str.str[:23]


def convert_csv_to_feather(
    csv_folder: Optional[Path] = None,
    output_folder: Optional[Path] = None,
    symbol: Optional[str] = None,
    tz_str: Optional[str] = None,
    delete_csv: bool = False
) -> List[Path]:
    """
    将文件夹中的所有CSV文件转换为feather格式
    
    :param csv_folder: CSV文件所在的文件夹路径，如果为None则使用Settings中的result_path_historical_trades/csv
    :param output_folder: 输出feather文件的文件夹路径，如果为None则使用base_folder（即与CSV的父目录）
    :param symbol: 交易对名称（如'BTCUSDT'），如果为None则从CSV文件名中提取
    :param tz_str: 时区字符串，如果为None则使用Settings中的TIMEZONE
    :param delete_csv: 转换成功后是否删除原始CSV文件，默认False
    :return: 转换后的feather文件路径列表
    """
    # 使用Settings中的路径配置
    if csv_folder is None:
        base_folder = Path(data_path['result_path_historical_trades'])
        csv_folder = base_folder / 'csv'  # CSV文件现在在csv子文件夹中
    csv_folder = Path(csv_folder)
    
    if not csv_folder.exists():
        raise ValueError(f"CSV文件夹不存在: {csv_folder}")
    
    if output_folder is None:
        # 默认输出到CSV文件夹的父目录（base_folder），而不是csv子文件夹
        output_folder = csv_folder.parent
    else:
        output_folder = Path(output_folder)
        output_folder.mkdir(parents=True, exist_ok=True)
    
    if tz_str is None:
        tz_str = TIMEZONE
    tz = pytz.timezone(tz_str)
    
    # 查找所有CSV文件
    csv_files = list(csv_folder.glob('*.csv'))
    if not csv_files:
        print(f"在 {csv_folder} 中未找到CSV文件")
        return []
    
    print(f"找到 {len(csv_files)} 个CSV文件，开始转换...")
    
    converted_files = []
    
    for csv_file in csv_files:
        try:
            print(f"\n处理文件: {csv_file.name}")
            
            # 从CSV文件名中提取symbol（格式：BTCUSDT-trades-2025-11-03.csv）
            file_symbol = _extract_symbol_from_filename(csv_file.name)
            if file_symbol:
                current_symbol = file_symbol
            else:
                current_symbol = symbol
            
            # 读取CSV文件（币安官网下载的CSV没有列名，第一行就是数据）
            df = pd.read_csv(csv_file, encoding='utf-8-sig', header=None, names=BINANCE_CSV_COLUMNS)
            
            if df.empty:
                print(f"  警告: {csv_file.name} 为空，跳过")
                continue
            
            print(f"  读取CSV后，数据量: {len(df)}")
            
            # 按照API下载代码的方式处理数据（完全一致）
            df = _process_csv_like_api(df, current_symbol, tz)
            
            # 生成输出文件名（从数据中获取时间范围）
            output_filename = _generate_feather_filename(csv_file, df, current_symbol, tz)
            output_path = output_folder / output_filename
            
            # 保存为feather文件
            df.to_feather(output_path)
            print(f"  ✓ 已转换: {output_path.name} ({len(df)} 条记录)")
            converted_files.append(output_path)
            
            # 如果启用，删除原始CSV文件
            if delete_csv:
                csv_file.unlink()
                print(f"  ✓ 已删除原始CSV文件: {csv_file.name}")
        
        except Exception as e:
            print(f"  ✗ 转换失败 {csv_file.name}: {e}")
            continue
    
    print(f"\n转换完成，共转换 {len(converted_files)} 个文件")
    return converted_files


def _process_csv_like_api(df: pd.DataFrame, symbol: str, tz: pytz.BaseTzInfo) -> pd.DataFrame:
    """
    按照API下载代码的方式处理CSV数据，确保格式完全一致
    
    :param df: 原始DataFrame（包含币安CSV列名：id, price, qty, quoteQty, time, isBuyerMaker, isBestMatch）
    :param symbol: 交易对名称
    :param tz: 时区对象
    :return: 处理后的DataFrame（格式与API下载代码完全一致）
    """
    df = df.copy()
    
    # 步骤1：转换数值列和布尔列
    # 处理数值列：去除字符串列的前后空格，转换为数值类型
    numeric_columns = ['id', 'price', 'qty', 'quoteQty', 'time']
    for col in numeric_columns:
        if col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
                df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 处理布尔列：转换为布尔值
    boolean_columns = ['isBuyerMaker', 'isBestMatch']
    for col in boolean_columns:
        if col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip().str.upper()
                df[col] = df[col].map({'TRUE': True, 'FALSE': False, '1': True, '0': False, 'T': True, 'F': False})
    
    # 过滤掉无效数据（时间戳为NaN的行）
    if 'time' in df.columns:
        original_count = len(df)
        df = df[df['time'].notna()].copy()
        if len(df) < original_count:
            print(f"  过滤掉 {original_count - len(df)} 行无效时间戳数据")
    
    if df.empty:
        return df
    
    # 步骤2：重命名列为中文（与API代码完全一致）
    df.rename(columns={
        'id': '成交ID',
        'price': '成交价',
        'qty': '成交量',
        'quoteQty': '成交额',
        'time': '成交时间',
        'isBuyerMaker': '是否为买方主动',
        'isBestMatch': '是否为最优撮合'
    }, inplace=True)
    
    # 步骤3：处理时间戳（与API代码完全一致）
    # CSV中的时间戳可能是微秒级（如1762214400080490），需要转换为毫秒
    # 检查时间戳格式：如果大于1e15，说明是微秒级，需要除以1000
    if df['成交时间'].dtype in ['int64', 'int32', 'float64', 'float32']:
        sample_value = df['成交时间'].iloc[0] if len(df) > 0 else None
        if sample_value is not None and sample_value > 1e15:
            # 微秒级时间戳，转换为毫秒
            df['成交时间'] = df['成交时间'] / 1000.0
            print(f"  检测到微秒级时间戳，已转换为毫秒")
    
    # 按照API代码的方式处理时间：unit='ms', utc=True, 然后转换时区
    df['成交时间'] = pd.to_datetime(df['成交时间'], unit='ms', utc=True, errors='coerce').dt.tz_convert(tz)
    
    # 过滤掉时间转换失败的行
    if df['成交时间'].isna().any():
        na_count = df['成交时间'].isna().sum()
        print(f"  警告: {na_count} 个时间戳转换失败，将被过滤")
        df = df[df['成交时间'].notna()].copy()
    
    if df.empty:
        return df
    
    # 步骤4：添加获取时间和货币对列（与API代码完全一致）
    df['获取时间'] = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    df['货币对'] = symbol
    
    # 步骤5：按ID排序（与API代码完全一致）
    df = df.sort_values('成交ID').reset_index(drop=True)
    
    # 步骤6：调整列顺序（与API代码完全一致）
    df = df[['成交时间', '获取时间', '货币对', '成交ID', '成交价', '成交量', '成交额', 
             '是否为买方主动', '是否为最优撮合']]
    
    # 步骤7：格式化时间显示（保留毫秒精度：精确到毫秒）
    df['成交时间'] = _format_datetime_with_milliseconds(df['成交时间'])
    
    return df


def _clean_and_convert_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    清理DataFrame中的数值列：去除字符串列的前后空格，并尝试转换为数值类型
    
    :param df: 原始DataFrame
    :return: 清理后的DataFrame
    """
    df = df.copy()
    
    # 定义应该转换为数值类型的列（币安CSV的列名）
    numeric_columns = ['id', 'price', 'qty', 'quoteQty', 'time']
    boolean_columns = ['isBuyerMaker', 'isBestMatch']
    
    # 处理数值列
    for col in numeric_columns:
        if col in df.columns:
            original_dtype = df[col].dtype
            original_sample = df[col].head(3).tolist() if len(df) > 0 else []
            original_values = df[col].copy()  # 保存原始值
            
            # 如果是字符串类型，先去除前后空格
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
                # 尝试转换为数值类型
                try:
                    numeric_col = pd.to_numeric(df[col], errors='coerce')
                    nan_count = numeric_col.isna().sum()
                    
                    # 对于time列，如果转换后所有值都是NaN，保持原始字符串格式
                    if col == 'time' and nan_count == len(df):
                        print(f"  调试: 列 {col} 转换后所有值都是NaN，保持原始字符串格式（可能是日期时间字符串）")
                        print(f"  调试: 列 {col} 原始样本值: {original_sample}")
                        df[col] = original_values.astype(str).str.strip()
                    else:
                        df[col] = numeric_col
                        if nan_count > 0 and col == 'time':
                            print(f"  调试: 列 {col} 转换后，NaN数量: {nan_count} / {len(df)}")
                            print(f"  调试: 列 {col} 转换前样本值: {original_sample}")
                            print(f"  调试: 列 {col} 转换后样本值: {df[col].head(3).tolist()}")
                except Exception as e:
                    print(f"  警告: 列 {col} 转换为数值类型失败: {e}")
                    # 转换失败，保持原样
                    df[col] = original_values
            else:
                # 如果已经是数值类型，也检查一下
                if col == 'time' and len(df) > 0:
                    print(f"  调试: 列 {col} 已经是数值类型: {original_dtype}")
                    print(f"  调试: 列 {col} 样本值: {original_sample}")
                    print(f"  调试: 列 {col} NaN数量: {df[col].isna().sum()}")
    
    # 处理布尔列
    for col in boolean_columns:
        if col in df.columns:
            # 如果是字符串类型，先去除前后空格，然后转换为布尔值
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
                # 尝试转换为布尔值（处理 'TRUE'/'FALSE' 或 'True'/'False'）
                def convert_bool(val):
                    val_upper = str(val).upper().strip()
                    if val_upper in ['TRUE', '1', 'T']:
                        return True
                    elif val_upper in ['FALSE', '0', 'F']:
                        return False
                    else:
                        return val  # 保持原值
                df[col] = df[col].apply(convert_bool)
    
    return df


def _standardize_columns(df: pd.DataFrame, symbol: Optional[str] = None) -> pd.DataFrame:
    """
    标准化DataFrame的列名，将英文列名转换为中文列名
    注意：此函数已不再使用，保留仅为兼容性
    
    :param df: 原始DataFrame
    :param symbol: 交易对名称
    :return: 标准化后的DataFrame
    """
    # 定义列名映射（币安CSV可能的列名格式）
    column_mapping = {
        # 英文列名 -> 中文列名
        'Trade ID': '成交ID',
        'trade_id': '成交ID',
        'id': '成交ID',
        'ID': '成交ID',
        
        'Price': '成交价',
        'price': '成交价',
        
        'Quantity': '成交量',
        'quantity': '成交量',
        'qty': '成交量',
        'Qty': '成交量',
        
        'Quote Quantity': '成交额',
        'quote_quantity': '成交额',
        'quoteQty': '成交额',
        'QuoteQty': '成交额',
        'quote_qty': '成交额',
        
        'Time': '成交时间',
        'time': '成交时间',
        'timestamp': '成交时间',
        'Timestamp': '成交时间',
        
        'Is Buyer Maker': '是否为买方主动',
        'is_buyer_maker': '是否为买方主动',
        'isBuyerMaker': '是否为买方主动',
        'IsBuyerMaker': '是否为买方主动',
        
        'Is Best Match': '是否为最优撮合',
        'is_best_match': '是否为最优撮合',
        'isBestMatch': '是否为最优撮合',
        'IsBestMatch': '是否为最优撮合',
        
        'Symbol': '货币对',
        'symbol': '货币对',
        'pair': '货币对',
    }
    
    # 重命名列
    df = df.rename(columns=column_mapping)
    
    # 如果货币对列不存在，从symbol参数填充（symbol会从文件名获取）
    # 注意：CSV文件本身不包含货币对列，所以这里总是需要添加
    if '货币对' not in df.columns:
        if symbol:
            df['货币对'] = symbol  # pandas会自动广播到所有行
        else:
            df['货币对'] = 'UNKNOWN'
    
    return df


def _process_time_column(df: pd.DataFrame, tz: pytz.BaseTzInfo) -> pd.DataFrame:
    """
    处理时间列，转换为标准格式
    注意：此函数已不再使用，保留仅为兼容性，实际处理在 _process_csv_like_api 中完成
    
    :param df: DataFrame
    :param tz: 时区对象
    :return: 处理后的DataFrame
    """
    if '成交时间' not in df.columns:
        print("  警告: 未找到成交时间列")
        return df
    
    # 记录原始数据量
    original_count = len(df)
    print(f"  调试: 开始处理时间列，原始数据量: {original_count}")
    print(f"  调试: 成交时间列数据类型: {df['成交时间'].dtype}")
    if original_count > 0:
        print(f"  调试: 成交时间列前5个值: {df['成交时间'].head().tolist()}")
        print(f"  调试: 成交时间列是否有NaN: {df['成交时间'].isna().sum()}")
    
    # 尝试不同的时间格式
    try:
        # 保存原始值，以防转换失败
        original_time_values = df['成交时间'].copy()
        
        # 如果是字符串类型，先尝试转换为数值
        if df['成交时间'].dtype == 'object':
            print("  调试: 成交时间列是字符串类型，尝试转换为数值")
            # 去除前后空格
            df['成交时间'] = df['成交时间'].astype(str).str.strip()
            # 尝试转换为数值类型（可能是字符串格式的时间戳）
            try:
                numeric_time = pd.to_numeric(df['成交时间'], errors='coerce')
                nan_count = numeric_time.isna().sum()
                print(f"  调试: 转换为数值后，NaN数量: {nan_count} / {len(df)}")
                
                # 如果转换后所有值都是NaN，说明不是时间戳格式，保持原样用日期时间解析
                if nan_count == len(df):
                    print("  调试: 所有值转换为NaN，可能是日期时间字符串格式，将尝试日期时间解析")
                    df['成交时间'] = original_time_values.astype(str).str.strip()
                else:
                    df['成交时间'] = numeric_time
            except Exception as e:
                print(f"  调试: 转换为数值失败: {e}，将尝试日期时间解析")
                df['成交时间'] = original_time_values.astype(str).str.strip()
        
        # 如果是时间戳（毫秒或秒）
        if df['成交时间'].dtype in ['int64', 'int32', 'float64', 'float32']:
            print("  调试: 成交时间列是数值类型，按时间戳处理")
            # 先检查有多少有效值
            valid_before_filter = df['成交时间'].notna().sum()
            print(f"  调试: 过滤前有效值数量: {valid_before_filter} / {len(df)}")
            
            # 如果所有值都是NaN，说明转换失败，尝试用原始字符串值解析
            if valid_before_filter == 0:
                print("  警告: 所有时间戳值都是NaN，可能是日期时间字符串格式，尝试用原始值解析")
                # 尝试从原始值恢复（如果还有原始值的话）
                if 'original_time_values' in locals() and len(original_time_values) > 0:
                    df['成交时间'] = original_time_values.astype(str).str.strip()
                    print("  调试: 已恢复为原始字符串格式，将尝试日期时间解析")
                else:
                    print("  错误: 无法恢复原始值，所有数据将被过滤")
                    return pd.DataFrame()
            
            # 检查时间戳格式：如果是微秒级时间戳（大于1e15），需要转换为毫秒
            # 微秒时间戳通常是16-17位数字，毫秒时间戳是13位数字
            sample_value = df['成交时间'].iloc[0] if len(df) > 0 and pd.notna(df['成交时间'].iloc[0]) else None
            if sample_value is not None and sample_value > 1e15:
                print(f"  调试: 检测到微秒级时间戳（样本值: {sample_value}），转换为毫秒")
                # 微秒时间戳需要除以1000转换为毫秒
                df['成交时间'] = df['成交时间'] / 1000.0
                print(f"  调试: 转换后样本值: {df['成交时间'].iloc[0]}")
            
            # 过滤掉无效值（NaN、负数、过大的值）
            # 毫秒时间戳最大约为 9.9e12（到2286年），但为了安全起见，允许到1e15
            # 如果还有大于1e15的值，可能是数据错误，需要过滤
            valid_mask = df['成交时间'].notna() & (df['成交时间'] >= 0) & (df['成交时间'] < 1e16)
            if not valid_mask.all():
                invalid_count = (~valid_mask).sum()
                print(f"  警告: 发现 {invalid_count} 个异常时间戳值，将被过滤")
                print(f"  调试: 无效值详情 - NaN: {(~df['成交时间'].notna()).sum()}, 负数: {(df['成交时间'] < 0).sum()}, 过大: {(df['成交时间'] >= 1e16).sum()}")
                df = df[valid_mask].copy()
            
            if len(df) == 0:
                print("  错误: 过滤后数据为空，无法处理时间列")
                print(f"  调试: 原始数据量: {original_count}, 过滤后数据量: {len(df)}")
                # 如果还有原始值，尝试用原始值解析
                if 'original_time_values' in locals() and len(original_time_values) > 0:
                    print("  调试: 尝试用原始字符串值重新解析")
                    df = pd.DataFrame({'成交时间': original_time_values.astype(str).str.strip()})
                    # 继续后续的字符串解析流程
                else:
                    return pd.DataFrame()
            
            print(f"  调试: 过滤后数据量: {len(df)}")
            
            # 判断是毫秒还是秒
            sample_value = df['成交时间'].iloc[0]
            print(f"  调试: 样本时间戳值: {sample_value}, 判断为: {'毫秒' if pd.notna(sample_value) and sample_value > 1e12 else '秒'}")
            
            if pd.notna(sample_value) and sample_value > 1e12:  # 毫秒时间戳
                df['成交时间'] = pd.to_datetime(df['成交时间'], unit='ms', utc=True, errors='coerce').dt.tz_convert(tz)
            else:  # 秒时间戳
                df['成交时间'] = pd.to_datetime(df['成交时间'], unit='s', utc=True, errors='coerce').dt.tz_convert(tz)
            
            # 过滤掉转换失败的值（NaT）
            if df['成交时间'].isna().any():
                na_count = df['成交时间'].isna().sum()
                print(f"  警告: {na_count} 个时间戳转换失败，将被过滤")
                df = df[df['成交时间'].notna()].copy()
        else:
            print("  调试: 成交时间列不是数值类型，尝试解析为字符串格式的时间")
            # 尝试解析字符串格式的时间
            df['成交时间'] = pd.to_datetime(df['成交时间'], utc=True, errors='coerce').dt.tz_convert(tz)
            
            # 过滤掉转换失败的值（NaT）
            if df['成交时间'].isna().any():
                na_count = df['成交时间'].isna().sum()
                print(f"  警告: {na_count} 个时间字符串转换失败，将被过滤")
                df = df[df['成交时间'].notna()].copy()
        
        if len(df) == 0:
            print("  错误: 所有时间值都无效，无法处理时间列")
            print(f"  调试: 原始数据量: {original_count}, 最终数据量: {len(df)}")
            return df
        
        print(f"  调试: 时间转换后数据量: {len(df)}")
        
        # 转换为字符串格式，精确到毫秒（保留前3位微秒作为毫秒）
        # 格式：%Y-%m-%d %H:%M:%S.%f 然后截取前3位毫秒
        time_str = df['成交时间'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        df['成交时间'] = time_str.str.slice(0, -3)  # 去掉最后3位微秒，保留前3位作为毫秒
        
        print(f"  调试: 时间列处理完成，最终数据量: {len(df)}")
        
    except Exception as e:
        print(f"  错误: 时间列处理失败: {e}")
        import traceback
        traceback.print_exc()
    
    return df


def _add_missing_columns(df: pd.DataFrame, symbol: Optional[str], tz: pytz.BaseTzInfo) -> pd.DataFrame:
    """
    添加缺失的列，确保输出9列
    注意：此函数已不再使用，保留仅为兼容性，实际处理在 _process_csv_like_api 中完成
    
    :param df: DataFrame
    :param symbol: 交易对名称（从文件名获取）
    :param tz: 时区对象
    :return: 补充后的DataFrame
    """
    # 添加获取时间列 - 用当前时间填满所有行（格式：%Y-%m-%d %H:%M:%S）
    if '获取时间' not in df.columns:
        now = datetime.now(tz)
        current_time_str = now.strftime('%Y-%m-%d %H:%M:%S')
        df['获取时间'] = current_time_str  # pandas会自动广播到所有行
    
    # 确保货币对列存在 - 从symbol参数（从文件名获取）填满所有行
    if '货币对' not in df.columns:
        df['货币对'] = symbol if symbol else 'UNKNOWN'
    else:
        # 如果货币对列已存在但值不对，用symbol覆盖
        df['货币对'] = symbol if symbol else df['货币对'].iloc[0] if len(df) > 0 else 'UNKNOWN'
    
    # 确保所有必需的9列都存在（用None填充缺失的列）
    required_columns = [
        '成交时间', '获取时间', '货币对', '成交ID', '成交价', 
        '成交量', '成交额', '是否为买方主动', '是否为最优撮合'
    ]
    
    for col in required_columns:
        if col not in df.columns:
            df[col] = None
            print(f"  警告: 添加缺失列 {col}（值为None）")
    
    # 调整列顺序，确保严格按照9列的顺序输出
    available_columns = [col for col in required_columns if col in df.columns]
    other_columns = [col for col in df.columns if col not in required_columns]
    df = df[available_columns + other_columns]
    
    return df


def _extract_symbol_from_filename(filename: str) -> Optional[str]:
    """
    从CSV文件名中提取交易对符号
    格式：BTCUSDT-trades-2025-11-03.csv
    
    :param filename: CSV文件名
    :return: 交易对符号，如果无法提取则返回None
    """
    # 匹配格式：SYMBOL-trades-YYYY-MM-DD.csv
    match = re.match(r'^([A-Z0-9]+)-trades-\d{4}-\d{2}-\d{2}\.csv$', filename, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return None


def _generate_feather_filename(
    csv_file: Path, 
    df: pd.DataFrame, 
    symbol: Optional[str], 
    tz: pytz.BaseTzInfo
) -> str:
    """
    生成feather文件名
    格式：BTCUSDT_historical_trades_20251102_054102_to_20251104_235959.feather
    
    :param csv_file: 原始CSV文件路径
    :param df: DataFrame
    :param symbol: 交易对名称
    :param tz: 时区对象
    :return: 文件名
    """
    # 获取symbol
    if symbol:
        sym = symbol
    elif '货币对' in df.columns and not df.empty and df['货币对'].iloc[0]:
        sym = str(df['货币对'].iloc[0])
    else:
        # 尝试从文件名提取
        file_symbol = _extract_symbol_from_filename(csv_file.name)
        sym = file_symbol if file_symbol else 'UNKNOWN'
    
    # 从DataFrame获取时间范围
    if '成交时间' in df.columns and not df.empty:
        try:
            # 解析成交时间（现在所有数据都是毫秒级格式：%Y-%m-%d %H:%M:%S.%f，但只保留前3位毫秒）
            # 先尝试带毫秒的格式（新格式：%Y-%m-%d %H:%M:%S.%f，但只保留前3位毫秒，即23个字符）
            # 使用 infer_datetime_format=True 让pandas自动推断格式
            df_time = pd.to_datetime(df['成交时间'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
            # 如果解析失败（可能是旧格式），尝试不带毫秒的格式（兼容旧数据）
            if df_time.isna().any():
                df_time = pd.to_datetime(df['成交时间'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            
            start_time = df_time.min()
            end_time = df_time.max()
            
            # 格式化时间字符串（精确到秒，用于文件名）
            # 格式：YYYYMMDD_HHMMSS
            start_str = start_time.strftime('%Y%m%d_%H%M%S')
            end_str = end_time.strftime('%Y%m%d_%H%M%S')
            
            return f'{sym}_historical_trades_{start_str}_to_{end_str}.feather'
        except Exception as e:
            print(f"  警告: 从数据获取时间范围失败: {e}")
    
    # 如果无法从数据获取时间范围，尝试从CSV文件名提取日期
    # 格式：BTCUSDT-trades-2025-11-03.csv
    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', csv_file.name)
    if match:
        year, month, day = match.groups()
        date_str = f'{year}{month}{day}'
        # 使用当天的开始和结束时间
        start_str = f'{date_str}_000000'
        end_str = f'{date_str}_235959'
        return f'{sym}_historical_trades_{start_str}_to_{end_str}.feather'
    
    # 最后的备选方案：使用当前时间戳
    base_name = csv_file.stem
    return f'{sym}_historical_trades_{base_name}.feather'


def compare_feather_files(
    file1_path: Path,
    file2_path: Path,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    tz_str: str = 'Asia/Hong_Kong'
) -> Dict:
    """
    比较两个feather文件中固定时间段的数据是否一致
    
    :param file1_path: 第一个feather文件路径
    :param file2_path: 第二个feather文件路径
    :param start_time: 开始时间（datetime对象），如果为None则比较所有数据
    :param end_time: 结束时间（datetime对象），如果为None则比较所有数据
    :param tz_str: 时区字符串，默认'Asia/Hong_Kong'
    :return: 比较结果字典，包含是否一致、数量差异、ID差异等信息
    """
    file1_path = Path(file1_path)
    file2_path = Path(file2_path)
    tz = pytz.timezone(tz_str)
    
    if not file1_path.exists():
        raise ValueError(f"文件1不存在: {file1_path}")
    if not file2_path.exists():
        raise ValueError(f"文件2不存在: {file2_path}")
    
    print(f"\n开始比较文件:")
    print(f"  文件1: {file1_path.name}")
    print(f"  文件2: {file2_path.name}")
    
    # 读取两个文件
    print("\n读取文件...")
    df1 = pd.read_feather(file1_path)
    df2 = pd.read_feather(file2_path)
    
    print(f"  文件1: {len(df1)} 条记录")
    print(f"  文件2: {len(df2)} 条记录")
    
    # 处理时间列（支持毫秒精度）
    # 注意：feather文件中的成交时间是字符串格式，需要转换为datetime并添加时区信息
    if '成交时间' in df1.columns:
        # 尝试带毫秒的格式，如果失败则尝试不带毫秒
        df1['成交时间_dt'] = pd.to_datetime(df1['成交时间'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
        if df1['成交时间_dt'].isna().any():
            df1['成交时间_dt'] = pd.to_datetime(df1['成交时间'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        # 将tz-naive转换为tz-aware（假设数据是保存在指定时区的）
        if df1['成交时间_dt'].dt.tz is None:
            df1['成交时间_dt'] = df1['成交时间_dt'].dt.tz_localize(tz)
    if '成交时间' in df2.columns:
        df2['成交时间_dt'] = pd.to_datetime(df2['成交时间'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
        if df2['成交时间_dt'].isna().any():
            df2['成交时间_dt'] = pd.to_datetime(df2['成交时间'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        # 将tz-naive转换为tz-aware（假设数据是保存在指定时区的）
        if df2['成交时间_dt'].dt.tz is None:
            df2['成交时间_dt'] = df2['成交时间_dt'].dt.tz_localize(tz)
    
    # 如果指定了时间范围，进行过滤
    if start_time is not None or end_time is not None:
        if start_time is not None and not isinstance(start_time, datetime):
            start_time = pd.to_datetime(start_time).to_pydatetime()
        if end_time is not None and not isinstance(end_time, datetime):
            end_time = pd.to_datetime(end_time).to_pydatetime()
        
        # 确保时区一致
        if start_time is not None and start_time.tzinfo is None:
            start_time = tz.localize(start_time)
        if end_time is not None and end_time.tzinfo is None:
            end_time = tz.localize(end_time)
        
        print(f"\n过滤时间范围:")
        if start_time:
            print(f"  开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
            df1 = df1[df1['成交时间_dt'] >= start_time]
            df2 = df2[df2['成交时间_dt'] >= start_time]
        if end_time:
            print(f"  结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
            df1 = df1[df1['成交时间_dt'] <= end_time]
            df2 = df2[df2['成交时间_dt'] <= end_time]
        
        print(f"  过滤后 - 文件1: {len(df1)} 条记录")
        print(f"  过滤后 - 文件2: {len(df2)} 条记录")
    
    # 检查必需的列
    required_columns = ['成交ID']
    for col in required_columns:
        if col not in df1.columns:
            raise ValueError(f"文件1缺少必需列: {col}")
        if col not in df2.columns:
            raise ValueError(f"文件2缺少必需列: {col}")
    
    # 比较结果
    result = {
        'file1_path': str(file1_path),
        'file2_path': str(file2_path),
        'file1_count': len(df1),
        'file2_count': len(df2),
        'count_match': len(df1) == len(df2),
        'count_diff': len(df1) - len(df2),
        'id_match': False,
        'data_match': False,
        'missing_in_file2': [],
        'missing_in_file1': [],
        'different_data': []
    }
    
    # 比较ID
    print("\n比较成交ID...")
    ids1 = set(df1['成交ID'].dropna())
    ids2 = set(df2['成交ID'].dropna())
    
    missing_in_file2 = sorted(list(ids1 - ids2))
    missing_in_file1 = sorted(list(ids2 - ids1))
    
    result['missing_in_file2'] = missing_in_file2[:100]  # 只保存前100个
    result['missing_in_file1'] = missing_in_file1[:100]
    result['id_match'] = (ids1 == ids2)
    
    if result['id_match']:
        print("  ✓ 成交ID完全一致")
    else:
        print(f"  ✗ 成交ID不一致")
        print(f"    文件1独有的ID数量: {len(missing_in_file2)}")
        print(f"    文件2独有的ID数量: {len(missing_in_file1)}")
        if missing_in_file2:
            print(f"    文件1独有的ID示例（前10个）: {missing_in_file2[:10]}")
        if missing_in_file1:
            print(f"    文件2独有的ID示例（前10个）: {missing_in_file1[:10]}")
    
    # 比较数据内容（只比较共同存在的ID）
    print("\n比较数据内容...")
    common_ids = ids1 & ids2
    
    if common_ids:
        df1_common = df1[df1['成交ID'].isin(common_ids)].sort_values('成交ID').reset_index(drop=True)
        df2_common = df2[df2['成交ID'].isin(common_ids)].sort_values('成交ID').reset_index(drop=True)
        
        # 比较所有列（除了可能的时间戳列）
        compare_columns = [col for col in df1_common.columns 
                          if col not in ['成交时间_dt', '获取时间'] and col in df2_common.columns]
        
        # 合并两个DataFrame进行比较
        merged = df1_common[['成交ID'] + compare_columns].merge(
            df2_common[['成交ID'] + compare_columns],
            on='成交ID',
            suffixes=('_file1', '_file2'),
            how='outer'
        )
        
        # 检查每列是否一致
        different_rows = []
        for col in compare_columns:
            col1 = f'{col}_file1'
            col2 = f'{col}_file2'
            if col1 in merged.columns and col2 in merged.columns:
                diff_mask = merged[col1] != merged[col2]
                # 处理NaN值
                diff_mask = diff_mask | (merged[col1].isna() != merged[col2].isna())
                if diff_mask.any():
                    diff_ids = merged[diff_mask]['成交ID'].tolist()
                    different_rows.extend(diff_ids)
        
        different_rows = sorted(list(set(different_rows)))
        result['different_data'] = different_rows[:100]  # 只保存前100个
        
        if not different_rows:
            result['data_match'] = True
            print("  ✓ 数据内容完全一致")
        else:
            print(f"  ✗ 数据内容不一致")
            print(f"    不一致的记录ID数量: {len(different_rows)}")
            if different_rows:
                print(f"    不一致的记录ID示例（前10个）: {different_rows[:10]}")
    else:
        print("  ⚠ 没有共同的ID，无法比较数据内容")
        result['data_match'] = False
    
    # 总结
    print("\n" + "="*60)
    print("比较结果总结:")
    print("="*60)
    print(f"数量是否一致: {'✓ 是' if result['count_match'] else '✗ 否'} (差异: {result['count_diff']})")
    print(f"ID是否一致: {'✓ 是' if result['id_match'] else '✗ 否'}")
    print(f"数据是否一致: {'✓ 是' if result['data_match'] else '✗ 否'}")
    
    overall_match = result['count_match'] and result['id_match'] and result['data_match']
    result['overall_match'] = overall_match
    print(f"\n总体是否一致: {'✓ 是' if overall_match else '✗ 否'}")
    print("="*60)
    
    return result


if __name__ == '__main__':
    # 将 BTCUSDT-trades-2025-11-04.csv 转换为 feather 并与现有 feather 文件中的 11.04 数据对比
    
    # 获取数据路径
    base_folder = Path(data_path['result_path_historical_trades'])
    csv_folder = base_folder / 'csv'  # CSV文件现在在csv子文件夹中
    csv_file = csv_folder / 'BTCUSDT-trades-2025-11-03.csv'
    #existing_feather = base_folder / 'BTCUSDT_historical_trades_20251102_054102_to_20251107_140953.feather'
    
    print("=" * 80)
    print("CSV转Feather并对比数据一致性")
    print("=" * 80)
    
    # 步骤1: 转换CSV文件
    print(f"\n步骤1: 转换CSV文件")
    print(f"  CSV文件: {csv_file}")
    
    if not csv_file.exists():
        print(f"  错误: CSV文件不存在: {csv_file}")
        exit(1)
    
    # 转换指定的CSV文件（直接处理单个文件）
    tz = pytz.timezone(TIMEZONE)
    print(f"\n处理文件: {csv_file.name}")
    
    # 从CSV文件名中提取symbol
    file_symbol = _extract_symbol_from_filename(csv_file.name)
    current_symbol = file_symbol if file_symbol else 'BTCUSDT'
    
    # 读取CSV文件（币安官网下载的CSV没有列名，第一行就是数据）
    df = pd.read_csv(csv_file, encoding='utf-8-sig', header=None, names=BINANCE_CSV_COLUMNS)
    
    if df.empty:
        print(f"  错误: CSV文件为空")
        exit(1)
    
    print(f"  读取CSV后，数据量: {len(df)}")
    
    # 按照API下载代码的方式处理数据（完全一致）
    df = _process_csv_like_api(df, current_symbol, tz)
    
    # 生成输出文件名
    output_filename = _generate_feather_filename(csv_file, df, current_symbol, tz)
    new_feather_file = base_folder / output_filename
    
    # 保存为feather文件
    df.to_feather(new_feather_file)
    print(f"  ✓ 已转换: {new_feather_file.name} ({len(df)} 条记录)")
    """
    # 步骤2: 对比数据
    print(f"\n步骤2: 对比数据（2025-11-04 这一天的数据）")
    print(f"  新文件: {new_feather_file.name}")
    print(f"  现有文件: {existing_feather.name}")
    
    if not existing_feather.exists():
        print(f"  错误: 现有feather文件不存在: {existing_feather}")
        exit(1)
    
    # 设置对比的时间范围：2025-11-04 00:00:00 到 23:59:59.999
    tz = pytz.timezone(TIMEZONE)
    start_time = tz.localize(datetime(2025, 11, 4, 8, 0, 0))
    end_time = tz.localize(datetime(2025, 11, 4, 23, 59, 59, 999000))  # 999毫秒
    
    print(f"\n  对比时间范围:")
    print(f"    开始: {start_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
    print(f"    结束: {end_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
    
    # 执行对比
    result = compare_feather_files(
        file1_path=new_feather_file,
        file2_path=existing_feather,
        start_time=start_time,
        end_time=end_time,
        tz_str=TIMEZONE
    )
    
    # 步骤3: 输出对比结果
    print(f"\n步骤3: 对比结果")
    print("=" * 80)
    print(f"文件1 (新转换): {result['file1_path']}")
    print(f"  记录数: {result['file1_count']}")
    print(f"文件2 (现有): {result['file2_path']}")
    print(f"  记录数: {result['file2_count']}")
    print(f"\n数量对比:")
    print(f"  数量是否一致: {'✓ 是' if result['count_match'] else '✗ 否'}")
    print(f"  数量差异: {result['count_diff']}")
    print(f"\nID对比:")
    print(f"  ID是否一致: {'✓ 是' if result['id_match'] else '✗ 否'}")
    if result['missing_in_file2']:
        print(f"  文件2中缺失的ID数量: {len(result['missing_in_file2'])}")
        if len(result['missing_in_file2']) <= 10:
            print(f"    缺失ID: {result['missing_in_file2']}")
    if result['missing_in_file1']:
        print(f"  文件1中缺失的ID数量: {len(result['missing_in_file1'])}")
        if len(result['missing_in_file1']) <= 10:
            print(f"    缺失ID: {result['missing_in_file1']}")
    print(f"\n数据内容对比:")
    print(f"  数据是否完全一致: {'✓ 是' if result['data_match'] else '✗ 否'}")
    if result['different_data']:
        print(f"  存在差异的记录数: {len(result['different_data'])}")
        if len(result['different_data']) <= 5:
            print(f"    差异记录示例:")
            for diff in result['different_data'][:5]:
                print(f"      成交ID {diff['id']}: {diff['differences']}")
    
    print("=" * 80)
    
    if result['data_match']:
        print("✓ 结论: 数据完全一致！")
    else:
        print("✗ 结论: 数据存在差异，请检查上述详细信息。")
"""
