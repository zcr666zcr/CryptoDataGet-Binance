"""
异步版本：币安成交归集数据获取工具（毫秒级精度）
使用asyncio + binance.spot实现并发下载，大幅提升下载效率
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import pytz
import pandas as pd
from datetime import datetime, timedelta
from binance.spot import Spot as Client
from Settings import *
import time
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


class GetAggTradesData_ms_async():
    """
    异步版本：获取币安成交归集数据（毫秒级精度）
    使用asyncio实现并发请求，大幅提升下载效率
    """

    def __init__(self, symbol=None, max_concurrent=10):
        """
        初始化异步币安成交归集数据获取器
        :param symbol: 交易对，默认从配置文件读取
        :param max_concurrent: 最大并发请求数，默认10（可根据API限制调整）
        """
        self.symbol = symbol or AGG_TRADES_CONFIG['default_symbol']
        self.tz = pytz.timezone(TIMEZONE)
        self.max_concurrent = max_concurrent  # 并发数，避免触发API限流
        
        # 创建线程池执行器，用于在线程中运行同步的binance.spot调用
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent)
        
        # 确保数据目录存在
        data_path['result_path_aggtrades'].mkdir(parents=True, exist_ok=True)
    
    def __del__(self):
        """清理资源"""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)

    def _format_datetime_ms(self, dt):
        """格式化datetime对象为毫秒级字符串"""
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    def _format_datetime_filename_ms(self, dt):
        """格式化datetime对象为文件名格式（毫秒级）"""
        return dt.strftime('%Y%m%d_%H%M%S%f')[:-3]

    def _split_time_range(self, start_time, end_time, max_duration_ms=3600000):
        """
        将时间范围分割成多个子时间段，用于并发下载
        :param start_time: 起始时间戳（毫秒）
        :param end_time: 结束时间戳（毫秒）
        :param max_duration_ms: 每个子时间段的最大时长（毫秒），默认1小时
        :return: 子时间段列表 [(start1, end1), (start2, end2), ...]
        """
        time_ranges = []
        current_start = start_time
        
        while current_start < end_time:
            current_end = min(current_start + max_duration_ms, end_time)
            time_ranges.append((current_start, current_end))
            current_start = current_end + 1  # +1避免重复
        
        return time_ranges

    def _fetch_single_batch_sync(self, start_timestamp, end_timestamp, limit=1000, retry_count=0):
        """
        同步获取单个时间批次的数据（使用binance.spot）
        注意：每个线程使用独立的Client实例，确保线程安全
        :param start_timestamp: 起始时间戳（毫秒）
        :param end_timestamp: 结束时间戳（毫秒）
        :param limit: 每次请求返回数量
        :param retry_count: 重试次数
        :return: 数据列表
        """
        # 为每个线程创建独立的Client实例（线程安全）
        client = Client()
        try:
            # 使用binance.spot客户端获取数据
            agg_trades_data = client.agg_trades(
                symbol=self.symbol,
                startTime=start_timestamp,
                endTime=end_timestamp,
                limit=limit
            )
            
            return agg_trades_data if isinstance(agg_trades_data, list) else []
        except Exception as e:
            error_str = str(e).lower()
            if '429' in error_str or 'rate limit' in error_str:
                print(f"请求过于频繁，等待后重试...")
                time.sleep(RETRY_SLEEP_TIME)
                if retry_count < 3:
                    return self._fetch_single_batch_sync(start_timestamp, end_timestamp, limit, retry_count + 1)
                return []
            else:
                print(f'获取数据失败：{e}，重试中...')
                if retry_count < 3:
                    time.sleep(RETRY_SLEEP_TIME)
                    return self._fetch_single_batch_sync(start_timestamp, end_timestamp, limit, retry_count + 1)
                return []

    async def _fetch_single_batch(self, start_timestamp, end_timestamp, limit=1000, retry_count=0):
        """
        异步获取单个时间批次的数据（在线程池中运行同步调用）
        :param start_timestamp: 起始时间戳（毫秒）
        :param end_timestamp: 结束时间戳（毫秒）
        :param limit: 每次请求返回数量
        :param retry_count: 重试次数
        :return: 数据列表
        """
        # 在线程池中运行同步的binance.spot调用
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self._fetch_single_batch_sync,
            start_timestamp,
            end_timestamp,
            limit,
            retry_count
        )

    async def _fetch_time_range(self, start_timestamp, end_timestamp, limit=1000):
        """
        获取指定时间范围内的所有数据（自动分页）
        :param start_timestamp: 起始时间戳（毫秒）
        :param end_timestamp: 结束时间戳（毫秒）
        :param limit: 每次请求返回数量
        :return: 数据列表
        """
        all_data = []
        current_start = start_timestamp
        
        while current_start < end_timestamp:
            # 获取当前批次
            batch_data = await self._fetch_single_batch(current_start, end_timestamp, limit)
            
            if not batch_data:
                break
            
            all_data.extend(batch_data)
            
            # 如果返回的数据少于limit，说明已经获取完所有数据
            if len(batch_data) < limit:
                break
            
            # 获取最后一条数据的时间戳
            last_timestamp = batch_data[-1]['T']
            
            # 如果最后一条数据的时间戳等于或大于结束时间，停止
            if last_timestamp >= end_timestamp:
                break
            
            # 下一批次的起始时间
            current_start = last_timestamp + 1
            
            # 避免请求过快
            await asyncio.sleep(AGG_TRADES_CONFIG['request_interval'])
        
        return all_data

    async def _fetch_all_ranges(self, time_ranges, limit=1000):
        """
        并发获取所有时间段的数据（带进度显示）
        :param time_ranges: 时间段列表 [(start1, end1), (start2, end2), ...]
        :param limit: 每次请求返回数量
        :return: 所有数据列表
        """
        import time as time_module
        total_ranges = len(time_ranges)
        completed = 0
        total_records = 0
        start_time = time_module.time()
        
        # 创建信号量限制并发数和锁保护进度更新
        semaphore = asyncio.Semaphore(self.max_concurrent)
        progress_lock = asyncio.Lock()
        
        async def fetch_with_semaphore(start_ts, end_ts, range_index):
            async with semaphore:
                data = await self._fetch_time_range(start_ts, end_ts, limit)
                # 更新进度（使用锁保护）
                async with progress_lock:
                    nonlocal completed, total_records
                    completed += 1
                    total_records += len(data)
                    elapsed = time_module.time() - start_time
                    progress = (completed / total_ranges) * 100
                    avg_time_per_range = elapsed / completed if completed > 0 else 0
                    remaining_ranges = total_ranges - completed
                    estimated_remaining = avg_time_per_range * remaining_ranges
                    
                    print(f"[进度] {completed}/{total_ranges} 时间段已完成 ({progress:.1f}%) | "
                          f"已获取 {total_records:,} 条记录 | "
                          f"已用时 {elapsed:.1f}s | "
                          f"预计剩余 {estimated_remaining:.1f}s")
                return data
        
        # 并发执行所有请求
        tasks = [
            fetch_with_semaphore(start_ts, end_ts, idx)
            for idx, (start_ts, end_ts) in enumerate(time_ranges)
        ]
        
        # 等待所有任务完成
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 合并所有结果
        all_data = []
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"获取时间段 {idx+1} 数据时出错: {result}")
                continue
            all_data.extend(result)
        
        total_elapsed = time_module.time() - start_time
        print(f"[完成] 所有时间段下载完成，总耗时 {total_elapsed:.1f}s，共获取 {len(all_data):,} 条记录")
        
        return all_data

    async def get_agg_trades_by_time_range_async(self, start_time, end_time, limit=None, split_duration_hours=1):
        """
        异步获取指定时间范围内的成交归集数据
        :param start_time: 起始时间（datetime对象或时间戳毫秒）
        :param end_time: 结束时间（datetime对象或时间戳毫秒）
        :param limit: 每次请求返回数量，最大1000，默认从配置文件读取
        :param split_duration_hours: 分割时间段的小时数，默认1小时（可根据数据量调整）
        :return: DataFrame，包含完整时间范围内的归集成交数据
        """
        limit = limit or AGG_TRADES_CONFIG['default_limit']
        
        # 转换时间格式
        if isinstance(start_time, datetime):
            if start_time.tzinfo is None:
                start_time = self.tz.localize(start_time)
            start_timestamp = int(start_time.timestamp() * 1000)
        else:
            start_timestamp = int(start_time)
        
        if isinstance(end_time, datetime):
            if end_time.tzinfo is None:
                end_time = self.tz.localize(end_time)
            end_timestamp = int(end_time.timestamp() * 1000)
        else:
            end_timestamp = int(end_time)
        
        start_dt = datetime.fromtimestamp(start_timestamp/1000, tz=self.tz)
        end_dt = datetime.fromtimestamp(end_timestamp/1000, tz=self.tz)
        print(f"开始异步获取 {self.symbol} 从 {self._format_datetime_ms(start_dt)} 到 {self._format_datetime_ms(end_dt)} 的成交归集数据...")
        
        # 分割时间范围
        split_duration_ms = split_duration_hours * 3600 * 1000
        time_ranges = self._split_time_range(start_timestamp, end_timestamp, split_duration_ms)
        print(f"时间范围已分割为 {len(time_ranges)} 个时间段，并发下载...")
        
        # 并发获取所有数据
        all_data = await self._fetch_all_ranges(time_ranges, limit)
        
        if not all_data:
            return pd.DataFrame()
        
        # 转换为DataFrame
        df = pd.DataFrame(all_data)
        
        # 重命名列
        df.rename(columns={
            'a': '归集成交ID',
            'p': '成交价',
            'q': '成交量',
            'f': '首个成交ID',
            'l': '末个成交ID',
            'T': '成交时间',
            'm': '是否为主动卖出',
            'M': '是否为最优撮合'
        }, inplace=True)
        
        # 处理时间戳，转换为可读时间
        df['成交时间'] = pd.to_datetime(df['成交时间'], unit='ms', utc=True).dt.tz_convert(self.tz)
        df['获取时间'] = self._format_datetime_ms(datetime.now(self.tz))
        df['货币对'] = self.symbol
        
        # 过滤掉超出时间范围的数据（确保数据准确）
        df = df[(df['成交时间'] >= start_dt) & (df['成交时间'] <= end_dt)]
        
        # 按时间排序
        df = df.sort_values('成交时间').reset_index(drop=True)
        
        # 调整列顺序
        df = df[['成交时间', '获取时间', '货币对', '归集成交ID', '首个成交ID', '末个成交ID',
                 '成交价', '成交量', '是否为主动卖出', '是否为最优撮合']]
        
        # 格式化时间显示（毫秒级）
        df['成交时间'] = df['成交时间'].apply(self._format_datetime_ms)
        
        print(f"数据获取完成，共 {len(df)} 条记录")
        return df

    def get_agg_trades_by_time_range(self, start_time, end_time, limit=None, split_duration_hours=1):
        """
        同步接口：异步获取指定时间范围内的成交归集数据
        :param start_time: 起始时间（datetime对象或时间戳毫秒）
        :param end_time: 结束时间（datetime对象或时间戳毫秒）
        :param limit: 每次请求返回数量，默认从配置文件读取
        :param split_duration_hours: 分割时间段的小时数，默认1小时
        :return: DataFrame
        """
        return asyncio.run(self.get_agg_trades_by_time_range_async(start_time, end_time, limit, split_duration_hours))

    def save_agg_trades_by_time(self, df, start_time, end_time, save_format=None):
        """
        保存成交归集数据到文件（按时间范围模式，文件名包含时间范围信息，毫秒级）
        """
        save_format = save_format or AGG_TRADES_CONFIG['default_save_format']
        if df.empty:
            print('成交归集数据为空，跳过保存')
            return
        
        # 处理时间格式
        if isinstance(start_time, datetime):
            if start_time.tzinfo is None:
                start_time = self.tz.localize(start_time)
            start_str = self._format_datetime_filename_ms(start_time)
        else:
            start_str = self._format_datetime_filename_ms(datetime.fromtimestamp(start_time/1000, tz=self.tz))
        
        if isinstance(end_time, datetime):
            if end_time.tzinfo is None:
                end_time = self.tz.localize(end_time)
            end_str = self._format_datetime_filename_ms(end_time)
        else:
            end_str = self._format_datetime_filename_ms(datetime.fromtimestamp(end_time/1000, tz=self.tz))
        
        # 生成文件名（包含时间范围，毫秒级）
        filename = f'{self.symbol}_aggtrades_async_{start_str}_to_{end_str}'
        
        if save_format == 'feather':
            file_path = data_path['result_path_aggtrades'] / f'{filename}.feather'
            df.to_feather(file_path)
        else:
            file_path = data_path['result_path_aggtrades'] / f'{filename}.csv'
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print(f'成交归集数据已保存：{file_path}')

    def get_agg_trades_by_limit(self, limit=None):
        """
        模式1：按照最近笔数下载数据（同步方法，因为只请求一次，无需异步）
        :param limit: 返回数量，最大1000，默认从配置文件读取
        :return: DataFrame，包含归集成交数据
        """
        limit = limit or AGG_TRADES_CONFIG['default_limit']
        print(f"[进度] 正在获取 {self.symbol} 最近 {limit} 条成交归集数据...")
        client = Client()
        while True:
            try:
                # 调用币安成交归集接口
                agg_trades_data = client.agg_trades(
                    symbol=self.symbol,
                    limit=limit
                )
                
                if not agg_trades_data:
                    return pd.DataFrame()
                
                # 转换为DataFrame
                df = pd.DataFrame(agg_trades_data)
                
                # 重命名列为中文
                df.rename(columns={
                    'a': '归集成交ID',
                    'p': '成交价',
                    'q': '成交量',
                    'f': '首个成交ID',
                    'l': '末个成交ID',
                    'T': '成交时间',
                    'm': '是否为主动卖出',
                    'M': '是否为最优撮合'
                }, inplace=True)
                
                # 处理时间戳，转换为可读时间
                df['成交时间'] = pd.to_datetime(df['成交时间'], unit='ms', utc=True).dt.tz_convert(self.tz)
                df['获取时间'] = self._format_datetime_ms(datetime.now(self.tz))
                df['货币对'] = self.symbol
                
                # 调整列顺序
                df = df[['成交时间', '获取时间', '货币对', '归集成交ID', '首个成交ID', '末个成交ID',
                         '成交价', '成交量', '是否为主动卖出', '是否为最优撮合']]
                
                # 格式化时间显示（毫秒级）
                df['成交时间'] = df['成交时间'].apply(self._format_datetime_ms)
                
                print(f"[完成] 数据获取完成，共 {len(df)} 条记录")
                return df
                
            except Exception as e:
                print(f'获取成交归集数据失败：{e}，暂停{RETRY_SLEEP_TIME}s后重试')
                time.sleep(RETRY_SLEEP_TIME)

    def save_agg_trades(self, df, save_format=None):
        """
        保存成交归集数据到文件（按笔数模式）
        :param df: 成交归集数据DataFrame
        :param save_format: 保存格式，'feather' 或 'csv'，默认从配置文件读取
        """
        save_format = save_format or AGG_TRADES_CONFIG['default_save_format']
        if df.empty:
            print('成交归集数据为空，跳过保存')
            return
        
        # 生成文件名（使用时间戳，包含毫秒）
        timestamp = self._format_datetime_filename_ms(datetime.now(self.tz))
        filename = f'{self.symbol}_aggtrades_async_{timestamp}'
        
        if save_format == 'feather':
            file_path = data_path['result_path_aggtrades'] / f'{filename}.feather'
            df.to_feather(file_path)
        else:
            file_path = data_path['result_path_aggtrades'] / f'{filename}.csv'
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print(f'成交归集数据已保存：{file_path}')

    def download_by_limit(self, limit=None, save_format=None):
        """
        模式1：按照最近笔数下载并保存数据（便捷方法）
        :param limit: 返回数量，默认从配置文件读取
        :param save_format: 保存格式，默认从配置文件读取
        :return: DataFrame
        """
        limit = limit or AGG_TRADES_CONFIG['default_limit']
        save_format = save_format or AGG_TRADES_CONFIG['default_save_format']
        print(f"=========开始获取 {self.symbol} 成交归集数据=========")
        print(f"模式：按笔数下载（limit={limit}）")
        
        df = self.get_agg_trades_by_limit(limit=limit)
        self.save_agg_trades(df, save_format=save_format)
        
        print(f"=========数据获取完成，共 {len(df)} 条记录=========")
        return df

    def download_by_time_range(self, start_time, end_time, save_format=None, split_duration_hours=1):
        """
        便捷方法：按照固定时间范围下载并保存数据（异步版本）
        :param start_time: 起始时间（datetime对象或时间戳毫秒）
        :param end_time: 结束时间（datetime对象或时间戳毫秒）
        :param save_format: 保存格式，默认从配置文件读取
        :param split_duration_hours: 分割时间段的小时数，默认1小时
        :return: DataFrame
        """
        save_format = save_format or AGG_TRADES_CONFIG['default_save_format']
        print(f"=========开始异步获取 {self.symbol} 成交归集数据=========")
        print(f"模式：按时间范围下载（毫秒级精度，异步并发）")
        
        import time as time_module
        start_t = time_module.time()
        
        df = self.get_agg_trades_by_time_range(start_time, end_time, split_duration_hours=split_duration_hours)
        
        elapsed = time_module.time() - start_t
        print(f"总耗时: {elapsed:.2f} 秒")
        
        if not df.empty:
            self.save_agg_trades_by_time(df, start_time, end_time, save_format=save_format)
        
        print(f"=========数据获取完成，共 {len(df)} 条记录=========")
        return df

    def get_yesterday_data(self, save_format=None):
        """
        获取昨天的成交归集数据并保存（便捷方法）
        :param save_format: 保存格式，'feather' 或 'csv'，默认从配置文件读取
        :return: DataFrame，包含昨天的成交归集数据
        """
        save_format = save_format or AGG_TRADES_CONFIG['default_save_format']
        # 计算昨天的时间范围（香港时区）
        now = datetime.now(self.tz)
        yesterday = now - timedelta(days=1)
        
        # 昨天开始时间：00:00:00.000
        start_time = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        # 昨天结束时间：23:59:59.999
        end_time = yesterday.replace(hour=23, minute=59, second=59, microsecond=999000)
        
        return self.download_by_time_range(start_time, end_time, save_format=save_format)

    def get_data_by_date(self, date, save_format=None):
        """
        获取指定日期的成交归集数据并保存（便捷方法）
        :param date: 日期，可以是datetime对象或日期字符串（格式：'YYYY-MM-DD'）
        :param save_format: 保存格式，'feather' 或 'csv'，默认从配置文件读取
        :return: DataFrame，包含指定日期的成交归集数据
        """
        save_format = save_format or AGG_TRADES_CONFIG['default_save_format']
        # 处理日期参数
        if isinstance(date, str):
            date = datetime.strptime(date, '%Y-%m-%d')
            date = self.tz.localize(date)
        elif isinstance(date, datetime):
            if date.tzinfo is None:
                date = self.tz.localize(date)
        
        # 指定日期的开始和结束时间
        start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = date.replace(hour=23, minute=59, second=59, microsecond=999000)
        
        return self.download_by_time_range(start_time, end_time, save_format=save_format)


# 如果当前文件是主程序，执行测试
if __name__ == '__main__':
    # ========== 初始化 ==========
    # 创建异步数据获取器实例
    # symbol: 交易对，如 'BTCUSDT', 'ETHUSDT' 等
    # max_concurrent: 最大并发请求数，建议5-10，避免触发API限流
    gatd_async = GetAggTradesData_ms_async(symbol='BTCUSDT', max_concurrent=10)
    
    # ========== 模式1：按笔数下载（获取最近的N条交易数据）==========
    # 适用场景：只需要最近的一批交易数据，不关心具体时间范围
    # 特点：只请求一次API，速度快，但数据量有限（最多1000条）
    """
    df = gatd_async.download_by_limit(
        limit=1000,           # 获取最近1000条交易数据（最大1000）
        save_format='csv'     # 保存格式：'csv' 或 'feather'，默认从配置文件读取
    )
    print(f"获取到 {len(df)} 条数据")
    """
    
    # ========== 模式2：按时间范围下载（推荐用于大量数据下载）==========
    # 适用场景：需要下载指定时间范围内的所有交易数据
    # 特点：自动分割时间段并发下载，速度快，适合下载大量历史数据
    """
    from datetime import datetime, timedelta
    
    # 示例2.1：下载指定时间范围的数据（精确到毫秒）
    start = datetime(2025, 11, 3, 23, 58, 58, 999000)  # 开始时间
    end = datetime(2025, 11, 4, 0, 0, 0, 0)            # 结束时间
    
    df = gatd_async.download_by_time_range(
        start_time=start,
        end_time=end,
        save_format='csv',              # 保存格式：'csv' 或 'feather'
        split_duration_hours=1          # 将时间范围分割成1小时的小段并发下载
    )
    
    # 示例2.2：下载多天的数据（更能体现异步并发优势）
    start = datetime(2025, 11, 1, 0, 0, 0, 0)
    end = datetime(2025, 11, 4, 0, 0, 0, 0)
    df = gatd_async.download_by_time_range(
        start_time=start,
        end_time=end,
        split_duration_hours=1          # 每小时一段，共72段并发下载
    )
    
    # 示例2.3：使用时间戳（毫秒）下载
    start_timestamp = 1728000000000  # 毫秒时间戳
    end_timestamp = 1728086400000
    df = gatd_async.download_by_time_range(
        start_time=start_timestamp,
        end_time=end_timestamp,
        split_duration_hours=1
    )
    """
    
    # ========== 模式3：下载昨天的数据（便捷方法）==========
    # 适用场景：每天定时下载前一天的数据
    # 特点：自动计算昨天00:00:00.000到23:59:59.999的时间范围
    """
    df = gatd_async.get_yesterday_data(
        save_format='csv'  # 保存格式：'csv' 或 'feather'
    )
    print(f"昨天共 {len(df)} 条交易数据")
    """
    
    # ========== 模式4：下载指定日期的数据（便捷方法）==========
    # 适用场景：下载某个特定日期的所有交易数据
    # 特点：自动计算指定日期00:00:00.000到23:59:59.999的时间范围
    """
    # 方式1：使用日期字符串
    df = gatd_async.get_data_by_date(
        date='2025-11-03',  # 格式：'YYYY-MM-DD'
        save_format='csv'
    )
    
    # 方式2：使用datetime对象
    from datetime import datetime
    date = datetime(2025, 11, 3)
    df = gatd_async.get_data_by_date(
        date=date,
        save_format='feather'  # 使用feather格式，读取更快
    )
    """
    
    # ========== 模式5：只获取数据，不保存（用于进一步处理）==========
    # 适用场景：需要对数据进行处理后再保存，或只用于分析
    """
    from datetime import datetime
    
    # 只获取数据，不保存
    df = gatd_async.get_agg_trades_by_time_range(
        start_time=datetime(2025, 11, 3, 0, 0, 0),
        end_time=datetime(2025, 11, 3, 23, 59, 59),
        split_duration_hours=1
    )
    
    # 对数据进行处理
    # ... 你的数据处理代码 ...
    
    # 手动保存
    gatd_async.save_agg_trades_by_time(
        df=df,
        start_time=datetime(2025, 11, 3, 0, 0, 0),
        end_time=datetime(2025, 11, 3, 23, 59, 59),
        save_format='csv'
    )
    """
    
    # ========== 性能优化建议 ==========
    """
    1. 并发数设置（max_concurrent）：
       - 默认10，可根据网络和API限制调整
       - 如果频繁遇到429错误（限流），降低到5
       - 如果网络很好，可以提高到15-20
    
    2. 时间分割设置（split_duration_hours）：
       - 默认1小时，适合大多数情况
       - 数据量大的时间段（如活跃交易时段），可以设置为0.5小时（0.5）
       - 数据量小的时间段，可以设置为2-4小时提高效率
    
    3. 保存格式选择：
       - 'csv': 通用格式，可用Excel打开，但文件较大
       - 'feather': 二进制格式，文件小，读取快，但需要pandas支持
    """
    
    # ========== 实际测试示例 ==========
    print("\n========= 异步版本测试 =========")
    from datetime import datetime
    
    # 测试：下载小时间范围的数据（几分钟）
    start = datetime(2025, 11, 3, 23, 58, 58, 999000)
    end = datetime(2025, 11, 4, 0, 0, 0, 0)
    
    print(f"测试时间范围: {start} 到 {end}")
    print(f"并发数: {gatd_async.max_concurrent}")
    print(f"分割时间段: 1小时")
    print("=" * 50)
    
    df = gatd_async.download_by_time_range(
        start, 
        end, 
        split_duration_hours=1  # 每1小时为一个时间段，并发下载
    )
    
    print("=" * 50)
    print(f"\n获取到的数据条数: {len(df)}")
    if not df.empty:
        print(f"时间范围: {df['成交时间'].min()} 到 {df['成交时间'].max()}")

