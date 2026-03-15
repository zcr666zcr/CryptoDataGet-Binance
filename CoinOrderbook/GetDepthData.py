from binance.spot import Spot as Client
import pytz
import pandas as pd
import os
from datetime import datetime
from Settings import *
import time
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


class GetDepthData():
    """
    获取币安深度信息（订单簿）数据
    注意：深度数据是实时数据，不支持历史时间范围查询，只能获取当前订单簿
    """

    def __init__(self, symbol=None):
        """
        初始化币安深度数据获取器
        :param symbol: 交易对，默认从配置文件读取
        """
        # 币安API初始化，获取行情数据通常不需要API密钥
        self.client = Client()
        self.symbol = symbol or DEPTH_CONFIG['default_symbol']
        self.tz = pytz.timezone(TIMEZONE)
        
        # 确保数据目录存在
        data_path['result_path_depth'].mkdir(parents=True, exist_ok=True)

    def get_depth_by_limit(self, limit=None):
        """
        模式1：按照最近笔数下载数据（获取当前订单簿）
        :param limit: 深度数量，可选值：5, 10, 20, 50, 100, 500, 1000, 5000，默认从配置文件读取
        :return: DataFrame，包含买盘和卖盘数据
        """
        limit = limit or DEPTH_CONFIG['default_limit']
        while True:
            try:
                # 调用币安深度接口
                depth_data = self.client.depth(
                    symbol=self.symbol,
                    limit=limit
                )
                
                # 获取当前时间戳
                current_time = datetime.now(self.tz)
                timestamp_ms = int(current_time.timestamp() * 1000)
                # 转换为可读的日期时间格式（包含毫秒）
                timestamp_readable = current_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # 保留毫秒
                
                # 处理买盘数据（bids）
                bids_df = pd.DataFrame(depth_data['bids'], columns=['价格', '数量'])
                bids_df['方向'] = '买盘'
                bids_df['时间戳'] = timestamp_readable  # 使用可读格式
                bids_df['获取时间'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
                bids_df['货币对'] = self.symbol
                bids_df = bids_df[['时间戳', '获取时间', '货币对', '方向', '价格', '数量']]
                
                # 处理卖盘数据（asks）
                asks_df = pd.DataFrame(depth_data['asks'], columns=['价格', '数量'])
                asks_df['方向'] = '卖盘'
                asks_df['时间戳'] = timestamp_readable  # 使用可读格式
                asks_df['获取时间'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
                asks_df['货币对'] = self.symbol
                asks_df = asks_df[['时间戳', '获取时间', '货币对', '方向', '价格', '数量']]
                
                # 合并买盘和卖盘数据
                df = pd.concat([bids_df, asks_df], ignore_index=True)
                
                # 添加lastUpdateId信息（作为元数据保存）
                df['lastUpdateId'] = depth_data.get('lastUpdateId', '')
                
                return df
                
            except Exception as e:
                print(f'获取深度信息失败：{e}，暂停{RETRY_SLEEP_TIME}s后重试')
                time.sleep(RETRY_SLEEP_TIME)

    def save_depth(self, df, save_format=None):
        """
        保存深度数据到文件
        :param df: 深度数据DataFrame
        :param save_format: 保存格式，'feather' 或 'csv'，默认从配置文件读取
        """
        save_format = save_format or DEPTH_CONFIG['default_save_format']
        if df.empty:
            print('深度数据为空，跳过保存')
            return
        
        # 生成文件名（使用时间戳）
        timestamp = datetime.now(self.tz).strftime('%Y%m%d_%H%M%S')
        filename = f'{self.symbol}_depth_{timestamp}'
        
        if save_format == 'feather':
            file_path = data_path['result_path_depth'] / f'{filename}.feather'
            df.to_feather(file_path)
        else:
            file_path = data_path['result_path_depth'] / f'{filename}.csv'
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print(f'深度数据已保存：{file_path}')

    def download_and_save(self, limit=None, save_format=None):
        """
        下载并保存深度数据（便捷方法）
        :param limit: 深度数量，默认从配置文件读取
        :param save_format: 保存格式，默认从配置文件读取
        """
        limit = limit or DEPTH_CONFIG['default_limit']
        save_format = save_format or DEPTH_CONFIG['default_save_format']
        print(f"=========开始获取 {self.symbol} 深度数据=========")
        print(f"模式：按笔数下载（当前订单簿，limit={limit}）")
        
        df = self.get_depth_by_limit(limit=limit)
        self.save_depth(df, save_format=save_format)
        
        print(f"=========数据获取完成，共 {len(df)} 条记录=========")
        return df


# 如果当前文件是主程序，执行测试
if __name__ == '__main__':
    # 测试获取深度数据（使用配置文件中的默认参数）
    gdd = GetDepthData()
    
    # 下载并保存深度数据（参数会从配置文件读取）
    gdd.download_and_save()
    
    # 也可以手动指定参数覆盖配置
    # gdd.download_and_save(limit=100, save_format='feather')

