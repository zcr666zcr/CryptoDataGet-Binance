from binance.spot import Spot as Client
import pytz
import pandas as pd
import os
from datetime import datetime, timedelta
from Settings import *
import time
import warnings
import numpy as np

warnings.simplefilter(action='ignore', category=FutureWarning)


class GetAggTradesData_ms():
    """
    获取币安近期成交归集数据（毫秒级精度版本）
    支持两种下载模式：
    1. 按照最近笔数下载数据
    2. 按照固定时间范围下载数据
    所有时间相关的格式化都精确到毫秒级
    """

    def __init__(self, symbol=None):
        """
        初始化币安成交归集数据获取器
        :param symbol: 交易对，默认从配置文件读取
        """
        # 币安API初始化，获取行情数据通常不需要API密钥
        self.client = Client()
        self.symbol = symbol or AGG_TRADES_CONFIG['default_symbol']
        self.tz = pytz.timezone(TIMEZONE)
        
        # 确保数据目录存在
        data_path['result_path_aggtrades'].mkdir(parents=True, exist_ok=True)

    def _format_datetime_ms(self, dt):
        """
        格式化datetime对象为毫秒级字符串
        :param dt: datetime对象
        :return: 格式化的字符串，格式：YYYY-MM-DD HH:MM:SS.mmm
        """
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # 保留前3位微秒作为毫秒

    def _format_datetime_filename_ms(self, dt):
        """
        格式化datetime对象为文件名格式（毫秒级）
        :param dt: datetime对象
        :return: 格式化的字符串，格式：YYYYMMDD_HHMMSSmmm
        """
        return dt.strftime('%Y%m%d_%H%M%S%f')[:-3]  # 保留前3位微秒作为毫秒

    def get_agg_trades_by_limit(self, limit=None):
        """
        模式1：按照最近笔数下载数据
        :param limit: 返回数量，最大1000，默认从配置文件读取
        :return: DataFrame，包含归集成交数据
        """
        limit = limit or AGG_TRADES_CONFIG['default_limit']
        while True:
            try:
                # 调用币安成交归集接口
                agg_trades_data = self.client.agg_trades(
                    symbol=self.symbol,
                    limit=limit
                )
                
                if not agg_trades_data:
                    return pd.DataFrame()
                
                # 转换为DataFrame
                df = pd.DataFrame(agg_trades_data)
                
                # 重命名列为中文（根据API文档，agg_trades返回的字段名是缩写）
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
                
                return df
                
            except Exception as e:
                print(f'获取成交归集数据失败：{e}，暂停{RETRY_SLEEP_TIME}s后重试')
                time.sleep(RETRY_SLEEP_TIME)

    def get_agg_trades_by_time_range(self, start_time, end_time, limit=None):
        """
        模式2：按照固定时间范围下载数据（自动处理分页，获取完整数据）
        :param start_time: 起始时间（datetime对象或时间戳毫秒）
        :param end_time: 结束时间（datetime对象或时间戳毫秒）
        :param limit: 每次请求返回数量，最大1000，默认从配置文件读取
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
        
        all_data = []
        current_start = start_timestamp
        request_count = 0
        max_requests = AGG_TRADES_CONFIG['max_requests']  # 防止无限循环
        
        start_dt = datetime.fromtimestamp(start_timestamp/1000, tz=self.tz)
        end_dt = datetime.fromtimestamp(end_timestamp/1000, tz=self.tz)
        print(f"开始获取 {self.symbol} 从 {self._format_datetime_ms(start_dt)} 到 {self._format_datetime_ms(end_dt)} 的成交归集数据...")
        
        while current_start < end_timestamp and request_count < max_requests:
            try:
                # 获取当前批次数据
                params = {
                    'symbol': self.symbol,
                    'startTime': current_start,
                    'endTime': end_timestamp,
                    'limit': limit
                }
                
                agg_trades_data = self.client.agg_trades(**params)
                
                if not agg_trades_data:
                    break
                
                all_data.extend(agg_trades_data)
                
                # 获取最后一条数据的时间戳，作为下一批次的起始时间
                last_timestamp = agg_trades_data[-1]['T']
                
                # 如果返回的数据少于limit，说明已经获取完所有数据
                if len(agg_trades_data) < limit:
                    break
                
                # 如果最后一条数据的时间戳等于或大于结束时间，停止
                if last_timestamp >= end_timestamp:
                    break
                
                # 下一批次的起始时间设为最后一条数据的时间戳+1
                current_start = last_timestamp + 1
                request_count += 1
                
                print(f"已获取 {len(all_data)} 条记录，继续获取...")
                time.sleep(AGG_TRADES_CONFIG['request_interval'])  # 避免请求过快
                
            except Exception as e:
                print(f'获取数据失败：{e}，暂停{RETRY_SLEEP_TIME}s后重试')
                time.sleep(RETRY_SLEEP_TIME)
        
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
        
        # 调整列顺序
        df = df[['成交时间', '获取时间', '货币对', '归集成交ID', '首个成交ID', '末个成交ID',
                 '成交价', '成交量', '是否为主动卖出', '是否为最优撮合']]
        
        # 格式化时间显示（毫秒级）
        df['成交时间'] = df['成交时间'].apply(self._format_datetime_ms)
        
        print(f"数据获取完成，共 {len(df)} 条记录")
        return df

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
        filename = f'{self.symbol}_aggtrades_{timestamp}'
        
        if save_format == 'feather':
            file_path = data_path['result_path_aggtrades'] / f'{filename}.feather'
            df.to_feather(file_path)
        else:
            file_path = data_path['result_path_aggtrades'] / f'{filename}.csv'
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print(f'成交归集数据已保存：{file_path}')

    def save_agg_trades_by_time(self, df, start_time, end_time, save_format=None):
        """
        保存成交归集数据到文件（按时间范围模式，文件名包含时间范围信息，毫秒级）
        :param df: 成交归集数据DataFrame
        :param start_time: 起始时间（datetime对象或时间戳毫秒）
        :param end_time: 结束时间（datetime对象或时间戳毫秒）
        :param save_format: 保存格式，'feather' 或 'csv'，默认从配置文件读取
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
        filename = f'{self.symbol}_aggtrades_{start_str}_to_{end_str}'
        
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

    def download_by_time_range(self, start_time, end_time, save_format=None):
        """
        模式2：按照固定时间范围下载并保存数据（便捷方法）
        :param start_time: 起始时间（datetime对象或时间戳毫秒）
        :param end_time: 结束时间（datetime对象或时间戳毫秒）
        :param save_format: 保存格式，默认从配置文件读取
        :return: DataFrame
        """
        save_format = save_format or AGG_TRADES_CONFIG['default_save_format']
        print(f"=========开始获取 {self.symbol} 成交归集数据=========")
        print(f"模式：按时间范围下载（毫秒级精度）")
        
        df = self.get_agg_trades_by_time_range(start_time, end_time)
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
    # 测试获取成交归集数据（使用配置文件中的默认参数）
    gatd = GetAggTradesData_ms()
    
    # ========== 模式1：按照最近笔数下载数据 ==========
    #print("\n模式1：按照最近笔数下载数据")
    #df1 = gatd.download_by_limit()  # 参数会从配置文件读取
    
    # 也可以手动指定参数覆盖配置
    # df1 = gatd.download_by_limit(limit=1000, save_format='feather')
    
    # ========== 模式2：按照固定时间范围下载数据（毫秒级） ==========
    print("\n模式2：按照固定时间范围下载数据（毫秒级精度）")
    from datetime import datetime

    start = datetime(2025, 11, 4, 17, 0, 0, 0)
    end = datetime(2025, 11, 4, 23, 59, 59, 999000)
    df2 = gatd.download_by_time_range(start, end)  # save_format从配置文件读取
    
    # ========== 便捷方法：获取昨天的数据 ==========
    # print("\n便捷方法：获取昨天的数据")
    # df3 = gatd.get_yesterday_data()  # save_format从配置文件读取
    
    # ========== 便捷方法：获取指定日期的数据 ==========
    # print("\n便捷方法：获取指定日期的数据")
    # df4 = gatd.get_data_by_date('2024-01-01')  # save_format从配置文件读取

