from binance.spot import Spot as Client
import pytz
from datetime import datetime, timedelta
from Settings import TIMEZONE, RETRY_SLEEP_TIME
import time


class GetDailyTradeCount():
    """
    获取币安交易对的每日交易笔数
    支持两种方式：
    1. 获取24小时滚动窗口的交易笔数（快速，使用ticker/24hr接口）
    2. 统计指定日期的实际交易笔数（精确，使用aggTrades接口）
    """

    def __init__(self):
        """
        初始化币安交易笔数获取器
        """
        # 币安API初始化，获取行情数据通常不需要API密钥
        self.client = Client()
        self.tz = pytz.timezone(TIMEZONE)

    def get_24hr_trade_count(self, symbol):
        """
        方式1：获取24小时滚动窗口的交易笔数（快速）
        使用 ticker/24hr 接口，返回最近24小时的统计数据
        
        :param symbol: 交易对，如 'BTCUSDT'
        """
        while True:
            try:
                ticker_data = self.client.ticker_24hr(symbol=symbol)
                trade_count = ticker_data.get('count', 0)
                print(f"{symbol} 最近24小时交易笔数: {trade_count}")
                return trade_count
                    
            except Exception as e:
                print(f"获取24小时交易笔数失败: {e}")
                print(f"等待 {RETRY_SLEEP_TIME} 秒后重试...")
                time.sleep(RETRY_SLEEP_TIME)

    def get_daily_trade_count_by_date(self, symbol, target_date):
        """
        方式2：统计指定日期的实际交易笔数（精确）
        通过统计aggTrades接口的数据来计算
        
        :param symbol: 交易对，如 'BTCUSDT'
        :param target_date: 目标日期，datetime对象或日期字符串 'YYYY-MM-DD'
        :return: int，交易笔数
        """
        # 处理日期参数
        if isinstance(target_date, str):
            target_date = datetime.strptime(target_date, '%Y-%m-%d')
        if target_date.tzinfo is None:
            target_date = self.tz.localize(target_date)
        
        # 计算时间范围（目标日期的00:00:00到23:59:59.999）
        start_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = start_time + timedelta(days=1) - timedelta(milliseconds=1)
        
        # 转换为毫秒时间戳
        start_timestamp = int(start_time.timestamp() * 1000)
        end_timestamp = int(end_time.timestamp() * 1000)
        
        trade_count = 0
        request_count = 0
        max_requests = 10000  # 防止无限循环
        
        print(f"正在统计 {symbol} 在 {target_date.strftime('%Y-%m-%d')} 的交易笔数...")
        
        # 使用aggTrades接口
        from_id = None
        
        while request_count < max_requests:
            try:
                params = {
                    'symbol': symbol,
                    'startTime': start_timestamp,
                    'endTime': end_timestamp,
                    'limit': 1000
                }
                
                if from_id is not None:
                    params['fromId'] = from_id
                
                agg_trades = self.client.agg_trades(**params)
                
                if not agg_trades:
                    break
                
                # 过滤出在时间范围内的交易
                valid_trades = [
                    trade for trade in agg_trades
                    if start_timestamp <= trade['T'] <= end_timestamp
                ]
                
                trade_count += len(valid_trades)
                
                # 检查是否还有更多数据
                if len(agg_trades) < 1000:
                    break
                
                # 更新fromId用于下一次请求
                last_trade_id = agg_trades[-1]['a']
                from_id = last_trade_id + 1
                
                # 检查是否已经超出时间范围
                if agg_trades[-1]['T'] > end_timestamp:
                    break
                
                request_count += 1
                time.sleep(0.2)  # 避免请求过快
                
            except Exception as e:
                print(f"请求失败: {e}")
                print(f"等待 {RETRY_SLEEP_TIME} 秒后重试...")
                time.sleep(RETRY_SLEEP_TIME)
        
        print(f"{symbol} 在 {target_date.strftime('%Y-%m-%d')} 的交易笔数: {trade_count}")
        return trade_count


if __name__ == "__main__":
    # 示例用法
    
    # 创建实例
    gdtc = GetDailyTradeCount()
    
    # 方式1：获取24小时滚动窗口的交易笔数（快速）
    print("=" * 50)
    print("方式1: 获取24小时滚动窗口的交易笔数")
    print("=" * 50)
    gdtc.get_24hr_trade_count('BTCUSDT')
    
    # 方式2：获取指定日期的实际交易笔数（精确，但较慢）
    #print("\n" + "=" * 50)
    #print("方式2: 获取指定日期的实际交易笔数")
    #print("=" * 50)
    #yesterday = datetime.now(gdtc.tz) - timedelta(days=1)
    #gdtc.get_daily_trade_count_by_date('BTCUSDT', yesterday)
    
    # 也可以使用日期字符串
    # gdtc.get_daily_trade_count_by_date('BTCUSDT', '2024-01-01')

