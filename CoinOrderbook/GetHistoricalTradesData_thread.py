from binance.spot import Spot as Client
import pytz
import pandas as pd
import os
from datetime import datetime, timedelta
from Settings import *
import time
import warnings
import schedule
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from feather_utils import (
    read_all_feather_files,
    get_max_from_id,
    save_new_data_only
)

warnings.simplefilter(action='ignore', category=FutureWarning)


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


class GetHistoricalTradesData():
    """
    获取币安历史成交数据
    支持四种下载模式：
    1. 按照fromId分页下载数据（从指定ID开始，下载指定数量）
    2. 按照固定时间范围下载数据（通过fromId分页，根据time字段判断）
    3. 从本地数据续传（推荐用于日常更新）
    4. 定时更新（推荐用于日常自动更新）
    """

    def __init__(self, symbol=None, api_key=None, api_secret=None, max_concurrent=2):
        """
        初始化币安历史成交数据获取器
        :param symbol: 交易对，默认从配置文件读取
        :param api_key: API密钥（可选，如果提供则使用）
        :param api_secret: API密钥（可选，如果提供则使用）
        :param max_concurrent: 最大并发数（用于批量处理时的并发控制，避免触发API限流）
        """
        # 从api_dic读取API密钥（如果Settings中有配置）
        if api_key and api_secret:
            self.client = Client(api_key=api_key, api_secret=api_secret)
            self.api_key = api_key
            self.api_secret = api_secret
        elif api_dic.get('api_key') and api_dic.get('api_secret'):
            self.client = Client(api_key=api_dic['api_key'], api_secret=api_dic['api_secret'])
            self.api_key = api_dic['api_key']
            self.api_secret = api_dic['api_secret']
        else:
            # 尝试从环境变量读取
            import os
            api_key_env = os.environ.get('BINANCE_API_KEY')
            api_secret_env = os.environ.get('BINANCE_API_SECRET')
            if api_key_env and api_secret_env:
                self.client = Client(api_key=api_key_env, api_secret=api_secret_env)
                self.api_key = api_key_env
                self.api_secret = api_secret_env
            else:
                
                print("请通过以下方式之一提供API密钥：")
                print("1. 在Settings.py的api_dic中配置")
                print("2. 通过环境变量 BINANCE_API_KEY 和 BINANCE_API_SECRET")
                print("3. 在初始化时传入 api_key 和 api_secret 参数")
                self.client = Client()  # 即使没有密钥也初始化，但调用会失败
                self.api_key = None
                self.api_secret = None
        
        self.symbol = symbol or HISTORICAL_TRADES_CONFIG['default_symbol']
        self.tz = pytz.timezone(TIMEZONE)
        self.max_concurrent = max_concurrent  # 并发数，避免触发API限流
        
        # 确保数据目录存在
        data_path['result_path_historical_trades'].mkdir(parents=True, exist_ok=True)

    def get_historical_trades_by_fromid(self, from_id=None, limit=None):
        """
        模式1：按照fromId分页下载数据
        :param from_id: 起始成交ID，如果为None则从最早的可用ID开始
        :param limit: 返回数量，最大1000，默认从配置文件读取
        :return: DataFrame，包含历史成交数据
        """
        limit = limit or HISTORICAL_TRADES_CONFIG['default_limit']
        
        # 如果未指定from_id，尝试获取最早的可用ID
        if from_id is None:
            from_id = HISTORICAL_TRADES_CONFIG.get('default_from_id', 0)
        
        all_data = []
        current_from_id = from_id
        request_count = 0
        max_requests = HISTORICAL_TRADES_CONFIG.get('max_requests_per_batch', 100)  # 防止无限循环
        
        print(f"开始获取 {self.symbol} 从ID {current_from_id} 开始的历史成交数据...")
        
        while request_count < max_requests:
            try:
                # 构建请求参数
                params = {
                    'symbol': self.symbol,
                    'limit': limit
                }
                if current_from_id is not None:
                    params['fromId'] = current_from_id
                
                # 调用币安历史成交接口
                trades_data = self.client.historical_trades(**params)
                
                if not trades_data:
                    break
                
                all_data.extend(trades_data)
                
                # 获取最后一条数据的ID
                last_id = trades_data[-1]['id']
                
                # 如果返回的数据少于limit，说明已经获取完所有数据
                if len(trades_data) < limit:
                    break
                
                # 下一批次的起始ID设为最后一条数据的ID+1
                current_from_id = last_id + 1
                request_count += 1
                
                print(f"已获取 {len(all_data)} 条记录，继续获取...")
                time.sleep(HISTORICAL_TRADES_CONFIG['request_interval'])  # 避免请求过快
                
            except Exception as e:
                error_str = str(e)
                if 'API' in error_str or 'key' in error_str.lower() or '401' in error_str:
                    print(f'错误：需要API密钥才能使用historicalTrades接口')
                    print(f'请配置API密钥后重试')
                else:
                    print(f'获取数据失败：{e}，暂停{RETRY_SLEEP_TIME}s后重试')
                time.sleep(RETRY_SLEEP_TIME)
                # 如果是因为API密钥问题，不再继续尝试
                if 'API' in error_str or 'key' in error_str.lower() or '401' in error_str:
                    break
        
        if not all_data:
            return pd.DataFrame()
        
        # 转换为DataFrame
        df = pd.DataFrame(all_data)
        
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
        
        # 按ID排序（从早到晚）
        df = df.sort_values('成交ID').reset_index(drop=True)
        
        # 调整列顺序
        df = df[['成交时间', '获取时间', '货币对', '成交ID', '成交价', '成交量', '成交额', 
                 '是否为买方主动', '是否为最优撮合']]
        
        # 格式化时间显示（保留毫秒精度）
        df['成交时间'] = _format_datetime_with_milliseconds(df['成交时间'])
        
        return df

    def _find_start_id_by_binary_search(self, target_time, limit=None, max_search_requests=50):
        """
        辅助方法：使用二分查找找到接近目标时间的起始ID
        :param target_time: 目标时间戳（毫秒）
        :param limit: 每次请求的数量
        :param max_search_requests: 最大搜索请求次数
        :return: 接近目标时间的ID，如果找不到则返回None
        """
        limit = limit or HISTORICAL_TRADES_CONFIG['default_limit']
        
        # 先获取最新数据，确定ID范围
        try:
            latest_data = self.client.historical_trades(symbol=self.symbol, limit=1)
            if not latest_data:
                return None
            max_id = latest_data[0]['id']
            latest_time = latest_data[0]['time']
            
            # 如果目标时间比最新数据还新，返回None（从最新开始）
            if target_time > latest_time:
                return None
            
            # 如果目标时间很旧，从较小的ID开始
            min_id = 0
            if target_time < latest_time - 86400000 * 365:  # 如果超过1年，从0开始
                return min_id
            
            # 二分查找：在min_id和max_id之间查找
            print(f"使用二分查找定位时间 {datetime.fromtimestamp(target_time/1000, tz=self.tz)}...")
            left_id, right_id = min_id, max_id
            best_id = None
            best_time_diff = float('inf')
            
            search_count = 0
            while search_count < max_search_requests and left_id <= right_id:
                mid_id = (left_id + right_id) // 2
                
                try:
                    test_data = self.client.historical_trades(symbol=self.symbol, fromId=mid_id, limit=limit)
                    if not test_data:
                        break
                    
                    # 获取这批数据的时间范围
                    first_time = test_data[0]['time']
                    last_time = test_data[-1]['time']
                    
                    # 如果目标时间在范围内，找到合适的ID
                    if first_time <= target_time <= last_time:
                        # 找到最接近目标时间的ID
                        for trade in test_data:
                            if trade['time'] >= target_time:
                                return trade['id']
                        return test_data[0]['id']
                    
                    # 根据时间调整搜索范围
                    if target_time > last_time:
                        # 目标时间在这批数据之后，向右搜索
                        left_id = test_data[-1]['id'] + 1
                        if abs(last_time - target_time) < best_time_diff:
                            best_id = test_data[-1]['id'] + 1
                            best_time_diff = abs(last_time - target_time)
                    else:
                        # 目标时间在这批数据之前，向左搜索
                        right_id = test_data[0]['id'] - 1
                        if abs(first_time - target_time) < best_time_diff:
                            best_id = test_data[0]['id']
                            best_time_diff = abs(first_time - target_time)
                    
                    search_count += 1
                    time.sleep(HISTORICAL_TRADES_CONFIG['request_interval'])
                    
                except Exception as e:
                    print(f'二分查找过程中出错：{e}')
                    break
            
            return best_id if best_id is not None else min_id
            
        except Exception as e:
            print(f'二分查找初始化失败：{e}')
            return None

    def get_trade_id_by_time(self, target_time, limit=None, show_details=False):
        """
        根据时间戳获取对应的订单ID（可用于作为fromId参数）
        :param target_time: 目标时间（datetime对象或时间戳毫秒）
        :param limit: 每次请求的数量，默认从配置文件读取
        :param show_details: 是否显示详细信息（包括该时间点附近的数据）
        :return: int，订单ID；如果未找到则返回None
        """
        limit = limit or HISTORICAL_TRADES_CONFIG['default_limit']
        
        # 转换时间格式
        if isinstance(target_time, datetime):
            if target_time.tzinfo is None:
                target_time = self.tz.localize(target_time)
            target_timestamp = int(target_time.timestamp() * 1000)
        else:
            target_timestamp = int(target_time)
        
        print(f"正在查找时间 {datetime.fromtimestamp(target_timestamp/1000, tz=self.tz)} 对应的订单ID...")
        
        # 使用二分查找定位
        trade_id = self._find_start_id_by_binary_search(target_timestamp, limit=limit)
        
        if trade_id is None:
            print("未找到对应的订单ID")
            return None
        
        # 如果用户需要详细信息，显示该ID附近的数据
        if show_details:
            try:
                # 获取该ID附近的数据
                nearby_data = self.client.historical_trades(
                    symbol=self.symbol, 
                    fromId=max(0, trade_id - limit // 2), 
                    limit=limit
                )
                
                if nearby_data:
                    # 转换为DataFrame以便查看
                    df = pd.DataFrame(nearby_data)
                    df['time_readable'] = pd.to_datetime(df['time'], unit='ms', utc=True).dt.tz_convert(self.tz)
                    
                    # 找到目标时间附近的记录
                    print(f"\n找到的订单ID: {trade_id}")
                    print(f"\n该ID附近的数据（显示前10条）：")
                    print(df[['id', 'time', 'time_readable', 'price', 'qty']].head(10).to_string())
                    
                    # 找到最接近目标时间的记录
                    df['time_diff'] = abs(df['time'] - target_timestamp)
                    closest = df.loc[df['time_diff'].idxmin()]
                    print(f"\n最接近目标时间的订单：")
                    print(f"  订单ID: {closest['id']}")
                    print(f"  时间: {closest['time_readable']}")
                    print(f"  价格: {closest['price']}")
                    print(f"  成交量: {closest['qty']}")
                    
            except Exception as e:
                print(f"获取详细信息时出错：{e}")
        
        return trade_id
    
    def get_trades_around_time(self, target_time, before_count=5, after_count=5, limit=None):
        """
        获取指定时间点前后的订单数据（用于查看某个时间点的订单ID）
        :param target_time: 目标时间（datetime对象或时间戳毫秒）
        :param before_count: 显示目标时间之前多少条记录
        :param after_count: 显示目标时间之后多少条记录
        :param limit: 每次请求的数量，默认从配置文件读取
        :return: DataFrame，包含目标时间前后的订单数据
        """
        limit = limit or HISTORICAL_TRADES_CONFIG['default_limit']
        
        # 转换时间格式
        if isinstance(target_time, datetime):
            if target_time.tzinfo is None:
                target_time = self.tz.localize(target_time)
            target_timestamp = int(target_time.timestamp() * 1000)
        else:
            target_timestamp = int(target_time)
        
        print(f"正在获取时间 {datetime.fromtimestamp(target_timestamp/1000, tz=self.tz)} 前后的订单数据...")
        
        # 先找到接近目标时间的ID
        start_id = self._find_start_id_by_binary_search(target_timestamp, limit=limit)
        
        if start_id is None:
            print("未找到对应的起始ID")
            return pd.DataFrame()
        
        # 从该ID开始获取数据
        all_data = []
        from_id = max(0, start_id - limit // 2)  # 稍微往前一点，确保能包含目标时间
        
        try:
            trades_data = self.client.historical_trades(
                symbol=self.symbol, 
                fromId=from_id, 
                limit=limit * 2  # 获取更多数据以确保包含目标时间前后
            )
            
            if not trades_data:
                return pd.DataFrame()
            
            # 过滤出目标时间前后的数据
            target_data = []
            for trade in trades_data:
                time_diff = trade['time'] - target_timestamp
                target_data.append(trade)
            
            # 按时间差排序（最接近目标时间的在前）
            target_data.sort(key=lambda x: abs(x['time'] - target_timestamp))
            
            # 取前N条（目标时间前后，包括目标时间本身）
            all_data = target_data[:before_count + after_count + 1]
            
            if not all_data:
                return pd.DataFrame()
            
            # 转换为DataFrame
            df = pd.DataFrame(all_data)
            df = df.sort_values('time').reset_index(drop=True)
            
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
            df['货币对'] = self.symbol
            
            # 添加时间差列（用于标识哪条最接近目标时间）
            df['时间差(秒)'] = (df['成交时间'].astype('int64') / 1e9 - target_timestamp / 1000).round(2)
            
            # 调整列顺序
            df = df[['成交时间', '时间差(秒)', '货币对', '成交ID', '成交价', '成交量', '成交额', 
                     '是否为买方主动', '是否为最优撮合']]
            
            # 格式化时间显示（保留毫秒精度）
            df['成交时间'] = _format_datetime_with_milliseconds(df['成交时间'])
            
            return df
            
        except Exception as e:
            error_str = str(e)
            if 'API' in error_str or 'key' in error_str.lower() or '401' in error_str:
                print(f'错误：需要API密钥才能使用historicalTrades接口')
                print(f'请配置API密钥后重试')
            else:
                print(f'获取数据失败：{e}')
            return pd.DataFrame()

    def get_historical_trades_by_time_range(self, start_time, end_time, limit=None, split_interval_hours=1, max_interval_per_batch=12, use_split=True):
        """
        模式2：按照固定时间范围下载数据
        策略：先找到end_time对应的订单ID，然后往前批量下载，直到time <= start_time时停止
        如果时间范围较大，会自动分割成多个小区间并行获取以加速
        
        :param start_time: 起始时间（datetime对象或时间戳毫秒）
        :param end_time: 结束时间（datetime对象或时间戳毫秒）
        :param limit: 每次请求返回数量，最大1000，默认从配置文件读取
        :param split_interval_hours: 分割时间间隔（小时），默认1小时
        :param max_interval_per_batch: 每个批次的最大时间间隔（小时），默认12小时
        :param use_split: 是否使用split函数（强制分割），None表示自动判断，True表示强制使用，False表示强制不使用
        :return: DataFrame，包含完整时间范围内的历史成交数据
        """
        limit = limit or HISTORICAL_TRADES_CONFIG['default_limit']
        
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
        
        if start_timestamp >= end_timestamp:
            print(f"错误：起始时间必须早于结束时间")
            return pd.DataFrame()
        
        # 计算时间范围（小时）
        time_range_hours = (end_timestamp - start_timestamp) / (1000 * 3600)
        
        print(f"开始获取 {self.symbol} 从 {datetime.fromtimestamp(start_timestamp/1000, tz=self.tz)} 到 {datetime.fromtimestamp(end_timestamp/1000, tz=self.tz)} 的历史成交数据...")
        print(f"时间范围：{time_range_hours:.2f} 小时")
        
        # 根据use_split参数决定是否使用split函数
        if use_split is True:
            # 强制使用split
            print(f"强制使用split函数，将分割成多个小区间以加速获取...")
            return self._get_historical_trades_by_time_range_split(
                start_time, end_time, limit, split_interval_hours, max_interval_per_batch
            )
        elif use_split is False:
            # 强制不使用split
            print(f"强制不使用split函数，直接获取数据...")
            return self._get_historical_trades_by_time_range_single(start_time, end_time, limit)
        else:
            # 自动判断：如果时间范围较大，分割成多个小区间
            if time_range_hours > max_interval_per_batch:
                print(f"时间范围较大（{time_range_hours:.2f} 小时），将分割成多个小区间以加速获取...")
                return self._get_historical_trades_by_time_range_split(
                    start_time, end_time, limit, split_interval_hours, max_interval_per_batch
                )
            else:
                # 时间范围较小，直接获取
                return self._get_historical_trades_by_time_range_single(start_time, end_time, limit)
    
    def _get_historical_trades_by_time_range_single(self, start_time, end_time, limit=None):
        """
        单个时间范围的数据获取（内部方法）
        """
        limit = limit or HISTORICAL_TRADES_CONFIG['default_limit']
        
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
        
        print(f"开始获取 {self.symbol} 从 {datetime.fromtimestamp(start_timestamp/1000, tz=self.tz)} 到 {datetime.fromtimestamp(end_timestamp/1000, tz=self.tz)} 的历史成交数据...")
        
        # 步骤1：找到end_time对应的订单ID
        print(f"正在查找结束时间 {datetime.fromtimestamp(end_timestamp/1000, tz=self.tz)} 对应的订单ID...")
        end_id = self._find_start_id_by_binary_search(end_timestamp, limit=limit)
        
        if end_id is None:
            print("警告：未找到结束时间对应的订单ID，尝试从最新数据开始...")
            # 如果找不到，尝试获取最新数据
            try:
                latest_data = self.client.historical_trades(symbol=self.symbol, limit=1)
                if latest_data:
                    end_id = latest_data[0]['id']
                else:
                    print("错误：无法获取最新数据")
                    return pd.DataFrame()
            except Exception as e:
                print(f"错误：获取最新数据失败：{e}")
                return pd.DataFrame()
        
        print(f"找到结束时间对应的订单ID: {end_id}")
        
        # 步骤2：从end_id开始往前批量下载数据
        all_data = []
        current_from_id = end_id
        request_count = 0
        max_requests = HISTORICAL_TRADES_CONFIG['max_requests']
        should_stop = False
        min_id_seen = end_id  # 记录已获取的最小ID，用于去重和判断是否继续往前追溯
        
        print(f"开始从ID {current_from_id} 往前批量下载数据...")
        
        while request_count < max_requests and not should_stop:
            try:
                # 构建请求参数
                params = {
                    'symbol': self.symbol,
                    'fromId': current_from_id,
                    'limit': limit
                }
                
                # 调用币安历史成交接口
                trades_data = self.client.historical_trades(**params)
                
                if not trades_data:
                    print("未获取到数据，停止下载")
                    break
                
                # 检查这批数据的时间，判断是否需要停止
                batch_data = []
                earliest_time = None
                for trade in trades_data:
                    trade_time = trade['time']
                    trade_id = trade['id']
                    
                    # 更新最小ID
                    if trade_id < min_id_seen:
                        min_id_seen = trade_id
                    
                    # 如果时间在范围内，添加到结果中
                    if start_timestamp <= trade_time <= end_timestamp:
                        batch_data.append(trade)
                    # 如果时间小于start_time，标记停止（不再往前追溯）
                    elif trade_time < start_timestamp:
                        should_stop = True
                    # 如果时间大于end_time，跳过（因为我们已经从end_id开始，这种情况不应该太多）
                    
                    # 记录最早的时间
                    if earliest_time is None or trade_time < earliest_time:
                        earliest_time = trade_time
                
                all_data.extend(batch_data)
                
                # 如果已经标记停止，不再继续
                if should_stop:
                    print(f"已到达起始时间，停止下载")
                    break
                
                # 获取第一条和最后一条数据的信息
                first_trade = trades_data[0]
                first_id = first_trade['id']
                first_time = first_trade['time']
                last_trade = trades_data[-1]
                last_id = last_trade['id']
                last_time = last_trade['time']
                
                # 如果最早的数据已经小于等于start_time，停止下载
                if first_time <= start_timestamp:
                    should_stop = True
                    print(f"已到达起始时间，停止下载")
                    break
                
                # 如果返回的数据少于limit，说明已经获取完所有数据
                if len(trades_data) < limit:
                    print("已获取完所有可用数据")
                    break
                
                # 继续往前追溯：减小fromId（往前获取更老的数据）
                # 计算下一批次的起始ID：从当前批次最早的数据ID往前推limit个ID
                # 但要注意不能小于0
                next_from_id = max(0, first_id - limit)
                
                # 如果next_from_id和current_from_id相同或更大，说明无法继续往前追溯
                if next_from_id >= current_from_id:
                    print("无法继续往前追溯，停止下载")
                    break
                
                # 如果next_from_id已经小于0，说明已经到达最早的数据
                if next_from_id <= 0:
                    print("已到达最早的数据，停止下载")
                    break
                
                current_from_id = next_from_id
                request_count += 1
                
                print(f"已获取 {len(all_data)} 条记录，继续往前追溯... (当前批次时间范围: {datetime.fromtimestamp(first_time/1000, tz=self.tz)} 到 {datetime.fromtimestamp(last_time/1000, tz=self.tz)})")
                time.sleep(HISTORICAL_TRADES_CONFIG['request_interval'])
                
            except Exception as e:
                error_str = str(e)
                if 'API' in error_str or 'key' in error_str.lower() or '401' in error_str:
                    print(f'错误：需要API密钥才能使用historicalTrades接口')
                    print(f'请配置API密钥后重试')
                    break
                else:
                    print(f'获取数据失败：{e}，暂停{RETRY_SLEEP_TIME}s后重试')
                time.sleep(RETRY_SLEEP_TIME)
        
        if not all_data:
            print("未获取到任何数据")
            return pd.DataFrame()
        
        # 步骤3：转换为DataFrame并截断，只保留时间范围内的数据
        df = pd.DataFrame(all_data)
        
        # 按时间排序（从早到晚）
        df = df.sort_values('time').reset_index(drop=True)
        
        # 去重（基于ID）
        df = df.drop_duplicates(subset=['id'], keep='first')
        
        # 验证：检查是否完整覆盖了时间范围
        if len(df) > 0:
            actual_min_time = df['time'].min()
            actual_max_time = df['time'].max()
            
            # 检查是否到达了start_time（允许1秒的误差，因为可能有重叠）
            if actual_min_time > start_timestamp + 1000:  # 1秒 = 1000毫秒
                print(f"警告：获取的数据最早时间为 {datetime.fromtimestamp(actual_min_time/1000, tz=self.tz)}，"
                      f"但目标起始时间为 {datetime.fromtimestamp(start_timestamp/1000, tz=self.tz)}，"
                      f"可能存在遗漏！")
                print(f"这可能是因为请求次数限制或其他原因导致提前停止。")
            
            # 检查是否覆盖了end_time
            if actual_max_time < end_timestamp - 1000:  # 1秒 = 1000毫秒
                print(f"警告：获取的数据最晚时间为 {datetime.fromtimestamp(actual_max_time/1000, tz=self.tz)}，"
                      f"但目标结束时间为 {datetime.fromtimestamp(end_timestamp/1000, tz=self.tz)}，"
                      f"可能存在遗漏！")
        
        # 截断：只保留 start_timestamp <= time <= end_timestamp 的数据
        df = df[(df['time'] >= start_timestamp) & (df['time'] <= end_timestamp)]
        
        if df.empty:
            print("截断后数据为空，可能是时间范围内没有数据")
            return pd.DataFrame()
        
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
        
        # 格式化时间显示（保留毫秒精度）
        df['成交时间'] = _format_datetime_with_milliseconds(df['成交时间'])
        
        print(f"数据获取完成，共 {len(df)} 条记录")
        return df
    
    def _get_historical_trades_by_time_range_split(self, start_time, end_time, limit=None, split_interval_hours=1, max_interval_per_batch=12):
        """
        分割时间范围获取数据（内部方法，参考 GetMinKine.py 的实现）
        """
        limit = limit or HISTORICAL_TRADES_CONFIG['default_limit']
        
        # 转换时间格式
        if isinstance(start_time, datetime):
            if start_time.tzinfo is None:
                start_time = self.tz.localize(start_time)
        else:
            start_time = datetime.fromtimestamp(start_time/1000, tz=self.tz)
        
        if isinstance(end_time, datetime):
            if end_time.tzinfo is None:
                end_time = self.tz.localize(end_time)
        else:
            end_time = datetime.fromtimestamp(end_time/1000, tz=self.tz)
        
        # 生成时间列表（按小时分割，参考 GetMinKine.py）
        time_list = pd.date_range(start_time, end_time, freq=f'{split_interval_hours}H')
        # 确保包含结束时间
        if len(time_list) == 0 or time_list[-1] < end_time:
            time_list = pd.DatetimeIndex(list(time_list) + [end_time])
        
        # 将时间列表分割成多个批次（每个批次最多 max_interval_per_batch 小时）
        # 为了确保不遗漏交易，每个批次的起始时间会稍微提前（重叠策略）
        time_batches = []
        for i in range(0, len(time_list) - 1):
            batch_start = time_list[i]
            batch_end = time_list[i + 1]
            
            # 如果不是第一个批次，将start_time提前，确保与前一个批次有重叠
            # 这样可以避免因为边界问题导致的遗漏
            # 使用1秒的重叠，因为交易时间戳是毫秒级的，1秒可以覆盖很多交易
            if i > 0:
                # 提前1秒，确保覆盖边界交易（包括边界时间的所有交易）
                batch_start = batch_start - timedelta(seconds=1)
            
            time_batches.append((batch_start, batch_end))
        
        # 如果批次太多，进一步合并（参考 GetMinKine.py 的 min_list 分割方式）
        merged_batches = []
        current_batch_start = None
        current_batch_end = None
        
        for batch_start, batch_end in time_batches:
            if current_batch_start is None:
                current_batch_start = batch_start
                current_batch_end = batch_end
            else:
                # 计算当前批次的时间范围
                batch_hours = (batch_end - current_batch_start).total_seconds() / 3600
                if batch_hours <= max_interval_per_batch:
                    # 可以合并到当前批次
                    current_batch_end = batch_end
                else:
                    # 不能合并，保存当前批次，开始新批次
                    merged_batches.append((current_batch_start, current_batch_end))
                    current_batch_start = batch_start
                    current_batch_end = batch_end
        
        # 添加最后一个批次
        if current_batch_start is not None:
            merged_batches.append((current_batch_start, current_batch_end))
        
        print(f"时间范围已分割成 {len(merged_batches)} 个批次")
        print(f"使用并发处理，最大并发数：{self.max_concurrent}")
        
        # 定义获取单个批次的函数（用于线程池）
        def fetch_batch(batch_info):
            """获取单个批次的数据（线程安全：每个线程使用独立的Client实例）"""
            i, batch_start, batch_end = batch_info
            try:
                # 为每个线程创建独立的实例和Client（线程安全）
                # 这样可以避免多线程访问共享client时的线程安全问题
                temp_instance = GetHistoricalTradesData(
                    symbol=self.symbol,
                    api_key=self.api_key,
                    api_secret=self.api_secret,
                    max_concurrent=1  # 临时实例不需要并发
                )
                batch_df = temp_instance._get_historical_trades_by_time_range_single(batch_start, batch_end, limit)
                return (i, batch_df)
            except Exception as e:
                print(f"批次 {i+1} 获取失败：{e}")
                import traceback
                traceback.print_exc()
                return (i, pd.DataFrame())
        
        # 使用线程池并发获取数据
        all_dfs = []
        batch_infos = [(i, batch_start, batch_end) for i, (batch_start, batch_end) in enumerate(merged_batches)]
        
        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            # 提交所有任务
            future_to_batch = {executor.submit(fetch_batch, batch_info): batch_info for batch_info in batch_infos}
            
            # 使用tqdm显示进度
            with tqdm(total=len(merged_batches), desc="获取历史成交数据") as pbar:
                # 按完成顺序收集结果
                for future in as_completed(future_to_batch):
                    i, batch_df = future.result()
                    if not batch_df.empty:
                        all_dfs.append((i, batch_df))
                    pbar.update(1)
        
        # 按批次顺序排序（保持时间顺序）
        all_dfs.sort(key=lambda x: x[0])
        all_dfs = [df for _, df in all_dfs]
        
        # 合并所有批次的数据
        if not all_dfs:
            print("所有批次都未获取到数据")
            return pd.DataFrame()
        
        print(f"\n合并 {len(all_dfs)} 个批次的数据...")
        merged_df = pd.concat(all_dfs, ignore_index=True)
        
        # 去重（基于成交ID）
        original_count = len(merged_df)
        merged_df = merged_df.drop_duplicates(subset=['成交ID'], keep='first')
        duplicate_count = original_count - len(merged_df)
        
        if duplicate_count > 0:
            print(f"去重完成，删除了 {duplicate_count} 条重复记录")
        
        # 按成交ID排序
        merged_df = merged_df.sort_values('成交ID').reset_index(drop=True)
        
        # 验证：检查时间范围是否完整覆盖
        if not merged_df.empty:
            # 将成交时间转换回时间戳进行比较
            # 成交时间可能是字符串格式（如 '2024-01-01 12:00:00'）或Timestamp对象
            try:
                merged_df_time = pd.to_datetime(merged_df['成交时间'])
                # 转换为毫秒时间戳
                if merged_df_time.dtype == 'datetime64[ns]':
                    merged_df_time_ms = merged_df_time.astype('int64') // 10**6
                else:
                    # 如果是字符串格式，先转换为datetime再转时间戳
                    merged_df_time_ms = pd.to_datetime(merged_df['成交时间']).astype('int64') // 10**6
                
                actual_min_time = int(merged_df_time_ms.min())
                actual_max_time = int(merged_df_time_ms.max())
                
                start_timestamp = int(start_time.timestamp() * 1000)
                end_timestamp = int(end_time.timestamp() * 1000)
                
                # 检查是否完整覆盖了起始时间（允许1秒误差）
                if actual_min_time > start_timestamp + 1000:
                    print(f"\n⚠️  警告：获取的数据最早时间为 {datetime.fromtimestamp(actual_min_time/1000, tz=self.tz)}，"
                          f"但目标起始时间为 {datetime.fromtimestamp(start_timestamp/1000, tz=self.tz)}，"
                          f"可能存在遗漏！")
                
                # 检查是否完整覆盖了结束时间（允许1秒误差）
                if actual_max_time < end_timestamp - 1000:
                    print(f"\n⚠️  警告：获取的数据最晚时间为 {datetime.fromtimestamp(actual_max_time/1000, tz=self.tz)}，"
                          f"但目标结束时间为 {datetime.fromtimestamp(end_timestamp/1000, tz=self.tz)}，"
                          f"可能存在遗漏！")
            except Exception as e:
                print(f"验证时间范围时出错：{e}，跳过时间范围验证")
            
            # 检查成交ID的连续性（如果有大的跳跃，可能有遗漏）
            if len(merged_df) > 1:
                id_diff = merged_df['成交ID'].diff()
                max_id_gap = id_diff.max()
                # 如果ID跳跃超过1000，给出提示（但这不是绝对的，因为ID可能不连续）
                if max_id_gap > 1000:
                    gap_locations = merged_df[id_diff > 1000].index.tolist()
                    if gap_locations:
                        print(f"\n⚠️  提示：检测到成交ID有较大跳跃（最大跳跃：{max_id_gap}），"
                              f"这可能是正常的（因为ID可能不连续），但请检查是否有遗漏。")
        
        print(f"数据获取完成，共 {len(merged_df)} 条记录")
        return merged_df

    def save_historical_trades(self, df, save_format=None):
        """
        保存历史成交数据到文件（只保存新数据，不合并不删除旧文件）
        :param df: 历史成交数据DataFrame
        :param save_format: 保存格式，'feather' 或 'csv'，默认从配置文件读取
        """
        save_format = save_format or HISTORICAL_TRADES_CONFIG['default_save_format']
        if df.empty:
            print('历史成交数据为空，跳过保存')
            return
        
        # 只保存新数据，不合并现有文件，不删除旧文件
        data_dir = data_path['result_path_historical_trades']
        file_path = save_new_data_only(
            data_dir=data_dir,
            symbol=self.symbol,
            new_df=df,
            save_format=save_format,
            tz=self.tz
        )
        
        if file_path:
           
           print(f'历史成交数据已保存：{file_path}')
    

    def save_historical_trades_by_time(self, df, start_time, end_time, save_format=None):
        """
        保存历史成交数据到文件（只保存新数据，不合并不删除旧文件，文件名使用实际数据的时间范围）
        :param df: 历史成交数据DataFrame
        :param start_time: 起始时间（datetime对象或时间戳毫秒）- 仅用于日志显示，实际文件名使用数据的时间范围
        :param end_time: 结束时间（datetime对象或时间戳毫秒）- 仅用于日志显示，实际文件名使用数据的时间范围
        :param save_format: 保存格式，'feather' 或 'csv'，默认从配置文件读取
        """
        save_format = save_format or HISTORICAL_TRADES_CONFIG['default_save_format']
        if df.empty:
            print('历史成交数据为空，跳过保存')
            return
        
        # 只保存新数据，不合并现有文件，不删除旧文件
        # 文件名将根据实际数据的时间范围自动生成
        data_dir = data_path['result_path_historical_trades']
        file_path = save_new_data_only(
            data_dir=data_dir,
            symbol=self.symbol,
            new_df=df,
            save_format=save_format,
            tz=self.tz
        )
        
        if file_path:
           
           print(f'历史成交数据已保存：{file_path}')

    def download_by_fromid(self, from_id=None, limit=None, save_format=None):
        """
        模式1：按照fromId分页下载并保存数据（便捷方法）
        :param from_id: 起始成交ID，如果为None则从最早的可用ID开始
        :param limit: 返回数量，默认从配置文件读取
        :param save_format: 保存格式，默认从配置文件读取
        :return: DataFrame
        """
        limit = limit or HISTORICAL_TRADES_CONFIG['default_limit']
        save_format = save_format or HISTORICAL_TRADES_CONFIG['default_save_format']
        print(f"=========开始获取 {self.symbol} 历史成交数据=========")
        print(f"模式：按fromId分页下载（from_id={from_id}, limit={limit}）")
        
        df = self.get_historical_trades_by_fromid(from_id=from_id, limit=limit)
        self.save_historical_trades(df, save_format=save_format)
        
        print(f"=========数据获取完成，共 {len(df)} 条记录=========")
        return df

    def download_by_time_range(self, start_time, end_time, save_format=None, use_split=True, split_interval_hours=1, max_interval_per_batch=12):
        """
        模式2：按照固定时间范围下载并保存数据（便捷方法）
        :param start_time: 起始时间（datetime对象或时间戳毫秒）
        :param end_time: 结束时间（datetime对象或时间戳毫秒）
        :param save_format: 保存格式，默认从配置文件读取
        :param use_split: 是否使用split函数，None表示自动判断，True表示强制使用，False表示强制不使用
        :param split_interval_hours: 分割时间间隔（小时），默认1小时
        :param max_interval_per_batch: 每个批次的最大时间间隔（小时），默认12小时
        :return: DataFrame
        """
        save_format = save_format or HISTORICAL_TRADES_CONFIG['default_save_format']
        print(f"=========开始获取 {self.symbol} 历史成交数据=========")
        print(f"模式：按时间范围下载")
        
        df = self.get_historical_trades_by_time_range(start_time, end_time, use_split=use_split, 
                                                       split_interval_hours=split_interval_hours, 
                                                       max_interval_per_batch=max_interval_per_batch)
        if not df.empty:
            self.save_historical_trades_by_time(df, start_time, end_time, save_format=save_format)
        
        print(f"=========数据获取完成，共 {len(df)} 条记录=========")
        return df

    def get_yesterday_data(self, save_format=None, use_split=None):
        """
        获取昨天的历史成交数据并保存（便捷方法）
        :param save_format: 保存格式，'feather' 或 'csv'，默认从配置文件读取
        :param use_split: 是否使用split函数，None表示自动判断，True表示强制使用，False表示强制不使用
        :return: DataFrame，包含昨天的历史成交数据
        """
        save_format = save_format or HISTORICAL_TRADES_CONFIG['default_save_format']
        # 计算昨天的时间范围（香港时区）
        now = datetime.now(self.tz)
        yesterday = now - timedelta(days=1)
        
        # 昨天开始时间：00:00:00
        start_time = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        # 昨天结束时间：23:59:59.999999
        end_time = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        return self.download_by_time_range(start_time, end_time, save_format=save_format, use_split=use_split)

    def get_data_by_date(self, date, save_format=None, use_split=True, split_interval_hours=1, max_interval_per_batch=12):
        """
        获取指定日期的历史成交数据并保存（便捷方法）
        :param date: 日期，可以是datetime对象或日期字符串（格式：'YYYY-MM-DD'）
        :param save_format: 保存格式，'feather' 或 'csv'，默认从配置文件读取
        :param use_split: 是否使用split函数，None表示自动判断，True表示强制使用，False表示强制不使用
        :param split_interval_hours: 分割时间间隔（小时），默认1小时
        :param max_interval_per_batch: 每个批次的最大时间间隔（小时），默认12小时
        :return: DataFrame，包含指定日期的历史成交数据
        """
        save_format = save_format or HISTORICAL_TRADES_CONFIG['default_save_format']
        # 处理日期参数
        if isinstance(date, str):
            date = datetime.strptime(date, '%Y-%m-%d')
            date = self.tz.localize(date)
        elif isinstance(date, datetime):
            if date.tzinfo is None:
                date = self.tz.localize(date)
        
        # 指定日期的开始和结束时间
        start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        return self.download_by_time_range(start_time, end_time, save_format=save_format, use_split=use_split,
                                          split_interval_hours=split_interval_hours, 
                                          max_interval_per_batch=max_interval_per_batch)

    def update_from_local(self, limit=None, save_format=None, max_requests=None):
        """
        从本地已获取的历史数据中的最大 fromId 开始，获取最新数据并保存（不合并不删除旧文件）
        
        工作流程：
        1. 找到最大的成交ID（fromId）（优化：只读取文件名中结束时间最大的文件）
        2. 从该 ID+1 开始获取新数据直到最新
        3. 只保存新数据，不合并不删除旧文件（合并操作在读取时进行）
        
        :param limit: 每次请求返回数量，最大1000，默认从配置文件读取
        :param save_format: 保存格式，'feather' 或 'csv'，默认从配置文件读取
        :param max_requests: 最大请求次数，防止无限循环，默认从配置文件读取
        :return: DataFrame，包含新获取的数据（如需完整数据，请使用 read_all_feather_files 函数）
        """
        limit = limit or HISTORICAL_TRADES_CONFIG['default_limit']
        save_format = save_format or HISTORICAL_TRADES_CONFIG['default_save_format']
        max_requests = max_requests or HISTORICAL_TRADES_CONFIG.get('max_requests_per_batch', 100)
        
        data_dir = data_path['result_path_historical_trades']
        
        print(f"=========开始从本地数据续传 {self.symbol} 历史成交数据=========")
        
        # 步骤1：找到最大的成交ID（优化：只读取文件名中结束时间最大的文件）
        print("\n步骤1：查找本地数据中的最大成交ID...")
        max_id = get_max_from_id(data_dir, self.symbol)
        
        if max_id is None:
            print("本地没有数据或无法获取最大ID，将从最早开始获取")
            start_from_id = HISTORICAL_TRADES_CONFIG.get('default_from_id', 0)
        else:
            # 从 max_id + 1 开始获取新数据
            start_from_id = max_id + 1
            print(f"将从成交ID {start_from_id} 开始获取新数据")
        
        # 步骤2：从最大ID+1开始获取新数据直到最新
        print("\n步骤2：获取新数据...")
        
        # 获取新数据（使用已有的方法）
        new_df = self.get_historical_trades_by_fromid(from_id=start_from_id, limit=limit)
        
        if new_df.empty:
            print("未获取到新数据，可能已经是最新数据")
            print("返回空 DataFrame（如需完整数据，请使用 read_all_feather_files 函数）")
            return pd.DataFrame()
        
        print(f"获取到 {len(new_df)} 条新数据")
        
        # 步骤3：只保存新数据，不合并现有文件，不删除旧文件
        print("\n步骤3：保存新数据...")
        file_path = save_new_data_only(
            data_dir=data_dir,
            symbol=self.symbol,
            new_df=new_df,
            save_format=save_format,
            tz=self.tz
        )
        
        if file_path:
            # 返回新数据（不再合并，合并操作在读取时进行）
            print(f"=========数据更新完成，新增 {len(new_df)} 条记录=========")
            print(f"提示：如需获取完整数据，请使用 read_all_feather_files 函数读取所有文件")
            return new_df
        else:
            print("保存失败")
            return pd.DataFrame()
    
    def get_date_start(self):
        """
        获取已获取的最大时间（参考 GetMinKine.py 的实现）
        用于定时更新时确定从哪个时间点开始获取
        :return: (max_time, is_null) 元组，max_time 是 datetime 对象，is_null 表示是否为空
        """
        data_dir = data_path['result_path_historical_trades']
        
        # 获取所有 feather 文件
        file_list = []
        if data_dir.exists():
            for file in data_dir.iterdir():
                if file.suffix == '.feather' and self.symbol in file.stem:
                    file_list.append(file)
        
        if len(file_list) == 0:
            # 如果没有文件，返回 None
            return None, True
        
        # 读取所有文件，找到最新的时间
        max_time = None
        for file in file_list:
            try:
                df = pd.read_feather(file)
                if not df.empty and '成交时间' in df.columns:
                    # 转换成交时间为 datetime
                    df['成交时间'] = pd.to_datetime(df['成交时间'])
                    file_max_time = df['成交时间'].max()
                    # 确保时区正确
                    if file_max_time.tzinfo is None:
                        file_max_time = self.tz.localize(file_max_time)
                    elif file_max_time.tzinfo != self.tz:
                        file_max_time = file_max_time.astimezone(self.tz)
                    
                    if max_time is None or file_max_time > max_time:
                        max_time = file_max_time
            except Exception as e:
                print(f"读取文件 {file} 时出错：{e}")
                continue
        
        if max_time is None:
            return None, True
        
        return max_time, False
    
    def run_all_tasks(self, use_split=None):
        """
        执行所有任务（定时更新，参考 GetMinKine.py 的实现）
        :param use_split: 是否使用split函数（用于时间范围方式），None表示自动判断，True表示强制使用，False表示强制不使用
        """
        print("=========任务执行，历史成交数据更新=========")
        self.now = datetime.now(self.tz)  # 当前时间
        
        # 获取已获取的最大时间
        max_time, is_null = self.get_date_start()
        
        if is_null:
            # 如果没有本地数据，从昨天开始获取
            start_time = (self.now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            # 从上次获取的最大时间+1分钟开始（避免重复）
            start_time = max_time + timedelta(minutes=1)
        
        # 结束时间：当前时间的前1分钟（避免获取不完整的数据）
        end_time = (self.now - timedelta(minutes=1)).replace(second=0, microsecond=0)
        
        if start_time >= end_time:
            print("无可更新的历史成交数据！")
            return
        
        # 计算时间范围（小时）
        time_range_hours = (end_time - start_time).total_seconds() / 3600
        print(f"更新范围：{start_time.strftime('%Y-%m-%d %H:%M:%S')} 到 {end_time.strftime('%Y-%m-%d %H:%M:%S')}（{time_range_hours:.2f} 小时）")
        
        # 使用 update_from_local 方法更新数据（推荐方式）
        # 或者使用时间范围方式获取
        try:
            df = self.update_from_local()
            if not df.empty:
                print(f"数据更新成功，共 {len(df)} 条记录")
            else:
                print("未获取到新数据")
        except Exception as e:
            print(f"数据更新失败：{e}")
            # 如果 update_from_local 失败，尝试使用时间范围方式
            try:
                print("尝试使用时间范围方式获取数据...")
                # 如果时间范围较大（>24小时），自动使用split和并发
                df = self.download_by_time_range(start_time, end_time, use_split=use_split)
                if not df.empty:
                    print(f"数据更新成功，共 {len(df)} 条记录")
            except Exception as e2:
                print(f"时间范围方式也失败：{e2}")
        
        print("=========任务结束=========")
    
    def start_scheduler(self, is_scheduler=False, interval_minutes=1, use_split=None):
        """
        启动定时任务（参考 GetMinKine.py 的实现）
        :param is_scheduler: 是否启用定时任务，False 表示立即执行一次，True 表示启动定时任务
        :param interval_minutes: 定时任务执行间隔（分钟），默认1分钟
        :param use_split: 是否使用split函数（用于时间范围方式），None表示自动判断，True表示强制使用，False表示强制不使用
        
        关闭方法：
        - 在 PyCharm 中：点击控制台的红色停止按钮，或按 Ctrl+C
        - 在命令行中：按 Ctrl+C
        - 程序会优雅地关闭，等待当前任务完成
        """
        if not is_scheduler:
            # 立即执行一次
            self.run_all_tasks(use_split=use_split)
        else:
            print(f"定时任务已启动，每 {interval_minutes} 分钟运行一次...")
            print("提示：按 Ctrl+C 或点击停止按钮可以优雅地关闭程序")
            # 设置定时任务
            schedule.every(interval_minutes).minutes.do(self.run_all_tasks, use_split=use_split)
            
            # 使用标志变量控制循环，支持优雅关闭
            self._scheduler_running = True
            
            try:
                while self._scheduler_running:
                    schedule.run_pending()
                    time.sleep(5)  # 5秒检查一次
            except KeyboardInterrupt:
                print("\n收到停止信号，正在关闭定时任务...")
                self._scheduler_running = False
                print("定时任务已停止")
            except Exception as e:
                print(f"\n发生错误：{e}")
                print("正在关闭定时任务...")
                self._scheduler_running = False
                raise


# 如果当前文件是主程序，执行测试
if __name__ == '__main__':
    # ========== 配置参数 ==========
    # 选择是否使用split函数（用于timerange方法）
    # None: 自动判断（时间范围>24小时时自动使用split）
    # True: 强制使用split函数（即使时间范围较小也会分割）
    # False: 强制不使用split函数（即使时间范围较大也不分割）
    USE_SPLIT = True  # 可以修改为 True 或 False
    
    # 并发数设置（用于批量处理时的并发控制，避免触发API限流）
    MAX_CONCURRENT = 2  # 建议值：2-5，根据网络和API限制调整
    
    # ========== 初始化 ==========
    # 测试获取历史成交数据（使用配置文件中的默认参数）
    ghtd = GetHistoricalTradesData(max_concurrent=MAX_CONCURRENT)
    
    # ========== 模式1：按照fromId分页下载数据 ==========
    # print("\n模式1：按照fromId分页下载数据")
    # df1 = ghtd.download_by_fromid()  # 参数会从配置文件读取
    # 
    # # 也可以手动指定参数覆盖配置
    # # df1 = ghtd.download_by_fromid(from_id=1000, limit=1000, save_format='feather')
    
    # ========== 模式2：按照固定时间范围下载数据 ==========
    #print("\n模式2：按照固定时间范围下载数据")
    #from datetime import datetime
    # 设置11月2号的时间范围：0点0分0秒0毫秒 到 23点59分59秒999毫秒
    #start_time = ghtd.tz.localize(datetime(2025, 11, 3, 0, 0, 0, 0))  # 11月2号0点0分0秒0毫秒
    #end_time = ghtd.tz.localize(datetime(2025, 11, 3, 23, 59, 59, 999000))  # 11月2号23点59分59秒999毫秒
    #ghtd.download_by_time_range(start_time, end_time, use_split=USE_SPLIT)
    
    # ========== 模式3：从本地数据续传（推荐用于日常更新）==========
    print("\n模式3：从本地数据续传")
    print("从本地已获取的历史数据中的最大 fromId 开始，获取最新数据并合并保存")
    df_merged = ghtd.update_from_local()  # 参数会从配置文件读取
    #也可以手动指定参数覆盖配置
    # df_merged = ghtd.update_from_local(limit=1000, save_format='feather')
    
# ========== 模式4：定时更新（推荐用于日常自动更新）==========
    #print("\n模式4：定时更新")
    #print("立即执行一次数据更新")
    #ghtd.start_scheduler(is_scheduler=False)  # False 表示立即执行一次
    # 启用定时任务（每1分钟执行一次）
    # ghtd.start_scheduler(is_scheduler=True, interval_minutes=1)
    
    # ========== 便捷方法：获取昨天的数据 ==========
    # print("\n便捷方法：获取昨天的数据")
    # df3 = ghtd.get_yesterday_data()  # save_format从配置文件读取
    # 
    # ========== 便捷方法：获取指定日期的数据 ==========
    #print("\n便捷方法：获取指定日期的数据")
    #df4 = ghtd.get_data_by_date('2025-11-02')  # save_format从配置文件读取

