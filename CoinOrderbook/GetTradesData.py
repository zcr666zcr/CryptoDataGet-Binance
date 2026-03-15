from binance.spot import Spot as Client
import pytz
import pandas as pd
from datetime import datetime
from Settings import *
import time
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


class GetTradesData():
    """
    获取币安近期成交数据
    注意：trades API 只支持获取最近的成交数据，不支持时间范围查询
    参数：symbol（必需），limit（可选，默认500，最大1000）
    """

    def __init__(self, symbol=None):
        """
        初始化币安成交数据获取器
        :param symbol: 交易对，默认从配置文件读取
        """
        # 币安API初始化，获取行情数据通常不需要API密钥
        self.client = Client()
        self.symbol = symbol or TRADES_CONFIG['default_symbol']
        self.tz = pytz.timezone(TIMEZONE)
        
        # 确保数据目录存在
        data_path['result_path_trades'].mkdir(parents=True, exist_ok=True)

    def get_trades_by_limit(self, limit=None):
        """
        按照最近笔数下载数据
        :param limit: 返回数量，最大1000，默认从配置文件读取
        :return: DataFrame，包含成交数据
        """
        limit = limit or TRADES_CONFIG['default_limit']
        while True:
            try:
                # 调用币安近期成交接口
                trades_data = self.client.trades(
                    symbol=self.symbol,
                    limit=limit
                )
                
                if not trades_data:
                    return pd.DataFrame()
                
                # 转换为DataFrame
                df = pd.DataFrame(trades_data)
                
                # 重命名列为中文
                df.rename(columns={
                    'id': '成交ID',
                    'price': '成交价',
                    'qty': '成交量',
                    'quoteQty': '成交额',
                    'time': '成交时间',
                    'isBuyerMaker': '是否为买方主动',
                    'isBestMatch': '是否为最优撮合'
                }, inplace=True)
                
                # 处理时间戳，转换为可读时间
                df['成交时间'] = pd.to_datetime(df['成交时间'], unit='ms', utc=True).dt.tz_convert(self.tz)
                df['获取时间'] = datetime.now(self.tz).strftime('%Y-%m-%d %H:%M:%S')
                df['货币对'] = self.symbol
                
                # 调整列顺序
                df = df[['成交时间', '获取时间', '货币对', '成交ID', '成交价', '成交量', '成交额', 
                         '是否为买方主动', '是否为最优撮合']]
                
                # 格式化时间显示
                df['成交时间'] = df['成交时间'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                return df
                
            except Exception as e:
                print(f'获取近期成交数据失败：{e}，暂停{RETRY_SLEEP_TIME}s后重试')
                time.sleep(RETRY_SLEEP_TIME)

    def save_trades(self, df, save_format=None):
        """
        保存成交数据到文件
        :param df: 成交数据DataFrame
        :param save_format: 保存格式，'feather' 或 'csv'，默认从配置文件读取
        """
        save_format = save_format or TRADES_CONFIG['default_save_format']
        if df.empty:
            print('成交数据为空，跳过保存')
            return
        
        # 生成文件名（使用时间戳）
        timestamp = datetime.now(self.tz).strftime('%Y%m%d_%H%M%S')
        filename = f'{self.symbol}_trades_{timestamp}'
        
        if save_format == 'feather':
            file_path = data_path['result_path_trades'] / f'{filename}.feather'
            df.to_feather(file_path)
        else:
            file_path = data_path['result_path_trades'] / f'{filename}.csv'
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print(f'成交数据已保存：{file_path}')

    def download_and_save(self, limit=None, save_format=None):
        """
        下载并保存成交数据（便捷方法）
        :param limit: 返回数量，默认从配置文件读取，最大1000
        :param save_format: 保存格式，默认从配置文件读取
        """
        save_format = save_format or TRADES_CONFIG['default_save_format']
        
        print(f"=========开始获取 {self.symbol} 近期成交数据=========")
        print(f"limit={limit or TRADES_CONFIG['default_limit']}")
        
        df = self.get_trades_by_limit(limit=limit)
        self.save_trades(df, save_format=save_format)
        
        print(f"=========数据获取完成，共 {len(df)} 条记录=========")
        return df


# 如果当前文件是主程序，执行测试
if __name__ == '__main__':
    # 测试获取成交数据（使用配置文件中的默认参数）
    gtd = GetTradesData()
    
    # 下载并保存成交数据（参数会从配置文件读取）
    gtd.download_and_save()
    
    # 也可以手动指定参数覆盖配置
    # gtd.download_and_save(limit=1000, save_format='feather')

