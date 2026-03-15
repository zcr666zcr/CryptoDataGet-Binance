import requests
import pytz
import pandas as pd
import os
from datetime import datetime, timedelta
from Settings import *
import time
import warnings
import schedule
from tqdm import tqdm
import hmac
import hashlib
from urllib.parse import urlencode
import re
from pathlib import Path
from feather_utils import (
    read_all_feather_files,
    get_max_from_id,
    save_new_data_only,
    merge_with_existing_and_save
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
    获取币安期权历史成交数据
    支持四种下载模式：
    1. 按照fromId分页下载数据（从指定ID开始，下载指定数量）
    2. 按照固定时间范围下载数据（通过fromId分页，根据time字段判断）
    3. 从本地数据续传（推荐用于日常更新）
    4. 定时更新（推荐用于日常自动更新）
    5. 按最大ID批量更新（根据CSV文件中的symbol列表批量更新）
    """

    def __init__(self, symbol=None, api_key=None, api_secret=None):
        """
        初始化币安期权历史成交数据获取器
        
        :param symbol: 期权合约符号（可选，如果为None则使用配置文件中的default_symbol）
        :param api_key: API密钥（可选，期权API需要密钥）
        :param api_secret: API密钥（可选，期权API需要密钥）
        """
        # 从api_dic读取API密钥（如果Settings中有配置）
        if api_key and api_secret:
            self.api_key = api_key
            self.api_secret = api_secret
        elif api_dic.get('api_key') and api_dic.get('api_secret'):
            self.api_key = api_dic['api_key']
            self.api_secret = api_dic['api_secret']
        else:
            # 尝试从环境变量读取
            import os
            api_key_env = os.environ.get('BINANCE_API_KEY')
            api_secret_env = os.environ.get('BINANCE_API_SECRET')
            if api_key_env and api_secret_env:
                self.api_key = api_key_env
                self.api_secret = api_secret_env
            else:
                print("请通过以下方式之一提供API密钥：")
                print("1. 在Settings.py的api_dic中配置")
                print("2. 通过环境变量 BINANCE_API_KEY 和 BINANCE_API_SECRET")
                print("3. 在初始化时传入 api_key 和 api_secret 参数")
                self.api_key = None
                self.api_secret = None
        
        # 期权API基础URL
        self.base_url = OPTIONS_API_BASE_URL
        
        # 使用传入的symbol或配置文件中的默认symbol
        self.symbol = symbol or HISTORICAL_TRADES_CONFIG['default_symbol']
        print(f"当前使用的期权合约: {self.symbol}")
        
        self.tz = pytz.timezone(TIMEZONE)
        
        # 确保数据目录存在
        data_path['result_path_historical_trades'].mkdir(parents=True, exist_ok=True)
        
        # API调用时间跟踪（类级别，所有实例共享）
        if not hasattr(GetHistoricalTradesData, '_last_api_call_time'):
            GetHistoricalTradesData._last_api_call_time = 0
        # API调用历史记录（用于基于时间窗口的频率限制）
        if not hasattr(GetHistoricalTradesData, '_api_call_history'):
            GetHistoricalTradesData._api_call_history = []  # 存储最近1分钟内的API调用时间戳
    
    def _call_options_api(self, endpoint, params=None):
        """
        调用币安期权API（需要签名认证）
        :param endpoint: API端点，如 '/eapi/v1/historicalTrades'
        :param params: 请求参数
        :return: API响应数据
        """
        url = f"{self.base_url}{endpoint}"
        
        # 期权API需要API密钥
        if not self.api_key or not self.api_secret:
            raise ValueError("期权API需要API密钥，请配置api_key和api_secret")
        
        # 准备参数
        if params is None:
            params = {}
        
        # 添加时间戳（毫秒）
        params['timestamp'] = int(time.time() * 1000)
        
        # 对参数进行排序并生成查询字符串
        query_string = urlencode(sorted(params.items()))
        
        # 生成签名（使用HMAC SHA256）
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        # 添加签名到参数
        params['signature'] = signature
        
        # 设置请求头
        headers = {
            'X-MBX-APIKEY': self.api_key
        }
        
        # 基于时间窗口的频率限制（币安限制：400次/分钟）
        current_time = time.time()
        max_requests_per_minute = 400
        safety_margin = 50  # 安全余量，避免刚好达到限制
        max_requests = max_requests_per_minute - safety_margin  # 实际使用350次/分钟
        
        # 清理1分钟之前的调用记录
        one_minute_ago = current_time - 60
        GetHistoricalTradesData._api_call_history = [
            t for t in GetHistoricalTradesData._api_call_history 
            if t > one_minute_ago
        ]
        
        # 检查最近1分钟内的请求数
        recent_calls = len(GetHistoricalTradesData._api_call_history)
        if recent_calls >= max_requests:
            # 计算需要等待的时间（等待最旧的请求超过1分钟）
            if GetHistoricalTradesData._api_call_history:
                oldest_call_time = min(GetHistoricalTradesData._api_call_history)
                wait_time = 60 - (current_time - oldest_call_time) + 0.1  # 额外0.1秒安全余量
                if wait_time > 0:
                    print(f"频率限制：最近1分钟内已有 {recent_calls} 次请求，等待 {wait_time:.2f} 秒...")
                    time.sleep(wait_time)
                    # 重新清理并检查
                    current_time = time.time()
                    one_minute_ago = current_time - 60
                    GetHistoricalTradesData._api_call_history = [
                        t for t in GetHistoricalTradesData._api_call_history 
                        if t > one_minute_ago
                    ]
        
        # 确保API调用之间有最小间隔（防止请求过快）
        min_interval = HISTORICAL_TRADES_CONFIG.get('api_call_min_interval', 0.25)
        current_time = time.time()  # 重新获取当前时间（可能已经等待了）
        time_since_last_call = current_time - GetHistoricalTradesData._last_api_call_time
        if time_since_last_call < min_interval:
            sleep_time = min_interval - time_since_last_call
            time.sleep(sleep_time)
            current_time = time.time()  # 等待后更新当前时间
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            # 更新最后调用时间和调用历史
            call_time = time.time()
            GetHistoricalTradesData._last_api_call_time = call_time
            GetHistoricalTradesData._api_call_history.append(call_time)
            
            # 如果响应状态码不是200，尝试解析错误信息
            if response.status_code != 200:
                try:
                    error_data = response.json()
                    error_msg = error_data.get('msg', error_data.get('message', str(error_data)))
                    raise Exception(f"API请求失败 (状态码 {response.status_code}): {error_msg}")
                except (ValueError, KeyError):
                    # 如果无法解析JSON，使用默认错误信息
                    pass
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            error_str = str(e)
            if '401' in error_str or '403' in error_str:
                raise ValueError(f"API密钥验证失败: {error_str}")
            else:
                raise Exception(f"API请求失败: {error_str}")

    def get_historical_trades_by_fromid(self, from_id=None, limit=None):
        """
        模式1：按照fromId分页下载数据
        :param from_id: 起始成交ID，如果为None则从最早的可用ID开始
        :param limit: 返回数量，最大1000，默认从配置文件读取
        :return: DataFrame，包含历史成交数据
        """
        limit = limit or HISTORICAL_TRADES_CONFIG['default_limit']
        effective_limit = min(limit, 500)  # 期权API最大limit为500（根据官方文档）
        max_consecutive_failures = HISTORICAL_TRADES_CONFIG.get('max_consecutive_failures', 10)
        stagnant_request_threshold = HISTORICAL_TRADES_CONFIG.get('stagnant_request_threshold', 3)
        
        # 如果未指定from_id，尝试获取最早的可用ID
        if from_id is None:
            from_id = HISTORICAL_TRADES_CONFIG.get('default_from_id', 0)
        
        all_data = []
        current_from_id = from_id
        request_count = 0
        max_requests = HISTORICAL_TRADES_CONFIG.get('max_requests_per_batch', 100)  # 防止无限循环
        consecutive_failures = 0
        prev_last_id = None
        stagnant_iterations = 0
        
        print(f"开始获取 {self.symbol} 从ID {current_from_id} 开始的历史成交数据...")
        
        while request_count < max_requests:
            try:
                # 构建请求参数
                # 根据API文档，limit最大值为500，默认值为100
                params = {
                    'symbol': self.symbol,
                    'limit': effective_limit
                }
                if current_from_id is not None:
                    params['fromId'] = current_from_id
                
                # 调用币安期权历史成交接口
                trades_data = self._call_options_api('/eapi/v1/historicalTrades', params=params)
                
                if not trades_data:
                    break
                try:
                    batch_min_id = min(int(item['tradeId']) for item in trades_data if 'tradeId' in item)
                except Exception:
                    batch_min_id = None

                all_data.extend(trades_data)
                
                # 终止条件：如果本批最小 tradeId 比上一批 last_id 或当前 fromId 还小，说明到头或出现回退，立即停止
                if batch_min_id is not None:
                    boundary_triggered = False
                    if prev_last_id is not None and batch_min_id < int(prev_last_id):
                        boundary_triggered = True
                    if not boundary_triggered and current_from_id is not None and batch_min_id < int(current_from_id):
                        boundary_triggered = True
                    if boundary_triggered:
                        print(f"检测到批次最小ID回退：batch_min_id={batch_min_id}, prev_last_id={prev_last_id}, current_from_id={current_from_id}，停止请求。")
                        break

                # 获取最后一条数据的ID（期权使用tradeId）
                last_id = int(trades_data[-1]['tradeId'])
                
                # 如果返回的数据少于limit，说明已经获取完所有数据
                if len(trades_data) < effective_limit:
                    break
                
                if prev_last_id is not None and last_id <= prev_last_id:
                    stagnant_iterations += 1
                    print(f"警告：检测到重复或停滞的 tradeId（last_id={last_id}），连续 {stagnant_iterations} 次未前进")
                    if stagnant_iterations >= stagnant_request_threshold:
                        print("连续多次未取得更新的 tradeId，停止进一步请求")
                        break
                else:
                    stagnant_iterations = 0
                prev_last_id = last_id
                consecutive_failures = 0
                
                # 下一批次的起始ID设为最后一条数据的ID+1
                current_from_id = last_id + 1
                request_count += 1
                
                # 打印本批次时间范围与累计进度
                try:
                    batch_times = [int(item['time']) for item in trades_data if 'time' in item]
                    if batch_times:
                        batch_min_dt = pd.to_datetime(min(batch_times), unit='ms', utc=True).tz_convert(self.tz)
                        batch_max_dt = pd.to_datetime(max(batch_times), unit='ms', utc=True).tz_convert(self.tz)
                        batch_min_str = _format_datetime_with_milliseconds(batch_min_dt)
                        batch_max_str = _format_datetime_with_milliseconds(batch_max_dt)
                        print(f"已获取 {len(all_data)} 条记录，这批次 {len(trades_data)} 条（时间范围：{batch_min_str} ~ {batch_max_str}），继续获取...")
                    else:
                        print(f"已获取 {len(all_data)} 条记录，这批次 {len(trades_data)} 条（时间范围统计失败），继续获取...")
                except Exception:
                    print(f"已获取 {len(all_data)} 条记录，这批次 {len(trades_data)} 条（时间范围统计失败），继续获取...")
                time.sleep(HISTORICAL_TRADES_CONFIG['request_interval'])  # 避免请求过快
                
            except Exception as e:
                error_str = str(e)
                # 检查是否是API密钥错误或请求过频错误
                is_api_key_error = 'API' in error_str or 'key' in error_str.lower() or '401' in error_str or '403' in error_str
                is_rate_limit_error = '429' in error_str or 'Too many requests' in error_str or 'rate limit' in error_str.lower()
                
                if is_api_key_error:
                    print(f'错误：需要API密钥才能使用historicalTrades接口')
                    print(f'请配置API密钥后重试，程序将暂停等待，直到问题解决后继续下载...')
                elif is_rate_limit_error:
                    print(f'错误：请求过于频繁（429），程序将暂停等待后继续重试...')
                else:
                    print(f'获取数据失败：{e}，暂停{RETRY_SLEEP_TIME}s后重试')
                
                # 对于请求过频错误，等待更长时间
                if is_rate_limit_error:
                    sleep_time = RETRY_SLEEP_TIME * 3  # 429错误等待更长时间
                    print(f'等待 {sleep_time}s 后重试...')
                    time.sleep(sleep_time)
                else:
                    time.sleep(RETRY_SLEEP_TIME)
                
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    print(f'连续失败次数达到上限 {max_consecutive_failures} 次，停止重试')
                    break
                
                # 持续重试，不退出循环（移除break，让程序继续尝试）
                print(f'正在重试... (当前已获取 {len(all_data)} 条记录，连续失败 {consecutive_failures} 次)')
        
        if not all_data:
            return pd.DataFrame()
        
        # 转换为DataFrame
        df = pd.DataFrame(all_data)
        
        if df.empty:
            return df
        
        # 期权API响应字段映射（根据官方API文档）
        # API返回字段：id(STRING), tradeId(STRING), price(STRING), qty(STRING), quoteQty(STRING), side(INT), time(LONG)
        # qty和quoteQty可能是负数，表示方向（负数可能是卖出，正数可能是买入），不应取绝对值
        df['ID'] = pd.to_numeric(df['id']).astype(int)  # id是字符串，需要转换（原始ID）
        df['成交ID'] = pd.to_numeric(df['tradeId']).astype(int)  # tradeId是字符串，需要转换（用于fromId索引）
        df['成交价'] = pd.to_numeric(df['price'])  # price是字符串，需要转换
        # 成交量保留原始值（包括正负号），负数可能表示卖出方向
        df['成交量'] = pd.to_numeric(df['qty'])  # qty是字符串，可能是负数
        # 成交额保留原始值（包括正负号），负数可能表示卖出方向
        df['成交额'] = pd.to_numeric(df['quoteQty'])  # quoteQty是字符串，可能是负数
        df['成交时间'] = pd.to_numeric(df['time'])  # time是数字类型
        # side字段：-1表示主动成交方方向（根据API文档）
        df['主动成交方向'] = df['side'].astype(int)  # side是数字类型，保留原始值
        
        # 处理时间戳，转换为可读时间
        df['成交时间'] = pd.to_datetime(df['成交时间'], unit='ms', utc=True).dt.tz_convert(self.tz)
        df['获取时间'] = datetime.now(self.tz).strftime('%Y-%m-%d %H:%M:%S')
        df['期权合约'] = self.symbol
        
        # 按成交ID排序（从早到晚）- fromId基于tradeId（成交ID）
        df = df.sort_values('成交ID').reset_index(drop=True)
        
        # 调整列顺序（去掉不需要的列）
        df = df[['成交时间', '获取时间', '期权合约', 'ID', '成交ID', '成交价', '成交量', '成交额', 
                 '主动成交方向']]
        
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
            params = {'symbol': self.symbol, 'limit': 1}
            latest_data = self._call_options_api('/eapi/v1/historicalTrades', params=params)
            if not latest_data:
                return None
            max_id = int(latest_data[0]['tradeId'])  # 期权使用tradeId
            latest_time = int(latest_data[0]['time'])
            
            # 如果目标时间比最新数据还新，返回None（从最新开始）
            if target_time > latest_time:
                return None
            
            # 如果目标时间很旧，从较小的ID开始
            min_id = 0
            if target_time < latest_time - 86400000 * 365:  # 如果超过1年，从0开始
                return min_id
            
            # 二分查找：在min_id和max_id之间查找
            binary_search_interval = HISTORICAL_TRADES_CONFIG.get('binary_search_interval', 0.5)
            print(f"使用二分查找定位时间 {datetime.fromtimestamp(target_time/1000, tz=self.tz)}...")
            print(f"二分查找延迟设置：每次请求间隔 {binary_search_interval} 秒（在基础延迟 {HISTORICAL_TRADES_CONFIG.get('api_call_min_interval', 0.25)} 秒基础上额外延迟）")
            # 显示当前API调用频率情况
            if hasattr(GetHistoricalTradesData, '_api_call_history'):
                current_time = time.time()
                one_minute_ago = current_time - 60
                recent_calls = len([t for t in GetHistoricalTradesData._api_call_history if t > one_minute_ago])
                print(f"当前API调用频率：最近1分钟内已有 {recent_calls} 次请求（限制：400次/分钟，安全限制：350次/分钟）")
            left_id, right_id = min_id, max_id
            best_id = None
            best_time_diff = float('inf')
            
            search_count = 0
            while search_count < max_search_requests and left_id <= right_id:
                mid_id = (left_id + right_id) // 2
                
                try:
                    params = {'symbol': self.symbol, 'fromId': mid_id, 'limit': min(limit, 500)}
                    test_data = self._call_options_api('/eapi/v1/historicalTrades', params=params)
                    if not test_data:
                        break
                    
                    # 获取这批数据的时间范围
                    first_time = int(test_data[0]['time'])
                    last_time = int(test_data[-1]['time'])
                    
                    # 如果目标时间在范围内，找到合适的ID
                    if first_time <= target_time <= last_time:
                        # 找到最接近目标时间的ID
                        for trade in test_data:
                            if int(trade['time']) >= target_time:
                                print(f"二分查找完成，共进行 {search_count + 1} 次请求")
                                return int(trade['tradeId'])  # 期权使用tradeId
                        print(f"二分查找完成，共进行 {search_count + 1} 次请求")
                        return int(test_data[0]['tradeId'])
                    
                    # 根据时间调整搜索范围
                    if target_time > last_time:
                        # 目标时间在这批数据之后，向右搜索
                        left_id = int(test_data[-1]['tradeId']) + 1
                        if abs(last_time - target_time) < best_time_diff:
                            best_id = int(test_data[-1]['tradeId']) + 1
                            best_time_diff = abs(last_time - target_time)
                    else:
                        # 目标时间在这批数据之前，向左搜索
                        right_id = int(test_data[0]['tradeId']) - 1
                        if abs(first_time - target_time) < best_time_diff:
                            best_id = int(test_data[0]['tradeId'])
                            best_time_diff = abs(first_time - target_time)
                    
                    search_count += 1
                    # 显示当前请求频率（每10次请求显示一次）
                    if search_count % 10 == 0:
                        if hasattr(GetHistoricalTradesData, '_api_call_history'):
                            current_time = time.time()
                            one_minute_ago = current_time - 60
                            recent_calls = len([t for t in GetHistoricalTradesData._api_call_history if t > one_minute_ago])
                            print(f"二分查找进度：已进行 {search_count} 次请求，当前API调用频率：最近1分钟内 {recent_calls} 次请求")
                    # 在api_call_min_interval基础上额外延迟，确保二分查找不会触发频率限制
                    time.sleep(binary_search_interval)
                    
                except Exception as e:
                    error_msg = str(e)
                    # 检查是否是API密钥错误或请求过频错误
                    is_api_key_error = 'API' in error_msg or 'key' in error_msg.lower() or '401' in error_msg or '403' in error_msg
                    is_rate_limit_error = '429' in error_msg or 'Too many requests' in error_msg or 'rate limit' in error_msg.lower()
                    
                    print(f'二分查找过程中出错（第 {search_count + 1} 次请求）：{error_msg}')
                    
                    if is_api_key_error:
                        print(f'错误：需要API密钥才能使用historicalTrades接口')
                        print(f'请配置API密钥后重试，程序将暂停等待，直到问题解决后继续...')
                    elif is_rate_limit_error:
                        print(f'错误：请求过于频繁（429），程序将暂停等待后继续重试...')
                    
                    # 对于请求过频错误，等待更长时间
                    if is_rate_limit_error:
                        sleep_time = binary_search_interval * 3  # 429错误等待更长时间
                        print(f'等待 {sleep_time}s 后重试...')
                        time.sleep(sleep_time)
                    else:
                        sleep_time = RETRY_SLEEP_TIME
                        print(f'等待 {sleep_time}s 后重试...')
                        time.sleep(sleep_time)
                    
                    # 持续重试，不退出循环（移除break，让程序继续尝试）
                    print(f'正在重试二分查找...')
            
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
                params = {
                    'symbol': self.symbol, 
                    'fromId': max(0, trade_id - limit // 2), 
                    'limit': min(limit, 500)
                }
                nearby_data = self._call_options_api('/eapi/v1/historicalTrades', params=params)
                
                if nearby_data:
                    # 转换为DataFrame以便查看
                    df = pd.DataFrame(nearby_data)
                    df['time_readable'] = pd.to_datetime(df['time'], unit='ms', utc=True).dt.tz_convert(self.tz)
                    
                    # 找到目标时间附近的记录
                    print(f"\n找到的订单ID: {trade_id}")
                    print(f"\n该ID附近的数据（显示前10条）：")
                    print(df[['tradeId', 'time', 'time_readable', 'price', 'qty']].head(10).to_string())
                    
                    # 找到最接近目标时间的记录
                    df['time_diff'] = abs(pd.to_numeric(df['time']) - target_timestamp)
                    closest = df.loc[df['time_diff'].idxmin()]
                    print(f"\n最接近目标时间的订单：")
                    print(f"  订单ID: {closest['tradeId']}")
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
        
        # 持续重试直到成功
        retry_count = 0
        max_retries = 1000000  # 设置一个很大的值，确保持续重试
        
        while retry_count < max_retries:
            try:
                params = {
                    'symbol': self.symbol, 
                    'fromId': from_id, 
                    'limit': min(limit * 2, 500)  # 期权API最大limit为500
                }
                trades_data = self._call_options_api('/eapi/v1/historicalTrades', params=params)
                
                if not trades_data:
                    return pd.DataFrame()
                
                # 过滤出目标时间前后的数据
                target_data = []
                for trade in trades_data:
                    time_diff = int(trade['time']) - target_timestamp
                    target_data.append(trade)
                
                # 按时间差排序（最接近目标时间的在前）
                target_data.sort(key=lambda x: abs(int(x['time']) - target_timestamp))
                
                # 取前N条（目标时间前后，包括目标时间本身）
                all_data = target_data[:before_count + after_count + 1]
                
                if not all_data:
                    return pd.DataFrame()
                
                # 转换为DataFrame
                df = pd.DataFrame(all_data)
                
                # 期权API响应字段映射（根据官方API文档）
                # API返回字段：id(STRING), tradeId(STRING), price(STRING), qty(STRING), quoteQty(STRING), side(INT), time(LONG)
                df['ID'] = pd.to_numeric(df['id']).astype(int)  # id是字符串，需要转换（原始ID）
                df['成交ID'] = pd.to_numeric(df['tradeId']).astype(int)  # tradeId是字符串，需要转换（用于fromId索引）
                df['成交价'] = pd.to_numeric(df['price'])  # price是字符串，需要转换
                df['成交量'] = pd.to_numeric(df['qty'])  # qty是字符串，可能是负数，保留原始值
                df['成交额'] = pd.to_numeric(df['quoteQty'])  # quoteQty是字符串，可能是负数，保留原始值
                df['成交时间'] = pd.to_numeric(df['time'])  # time是数字类型
                df['主动成交方向'] = df['side'].astype(int)  # side是数字类型，保留原始值
                
                # 处理时间戳，转换为可读时间
                df['成交时间'] = pd.to_datetime(df['成交时间'], unit='ms', utc=True).dt.tz_convert(self.tz)
                df['期权合约'] = self.symbol
                
                # 添加时间差列（用于标识哪条最接近目标时间）
                df['时间差(秒)'] = (df['成交时间'].astype('int64') / 1e9 - target_timestamp / 1000).round(2)
                
                # 按时间排序
                df = df.sort_values('成交时间').reset_index(drop=True)
                
                # 调整列顺序（去掉不需要的列）
                df = df[['成交时间', '时间差(秒)', '期权合约', 'ID', '成交ID', '成交价', '成交量', '成交额', 
                         '主动成交方向']]
                
                # 格式化时间显示（保留毫秒精度）
                df['成交时间'] = _format_datetime_with_milliseconds(df['成交时间'])
                
                return df
                
            except Exception as e:
                error_str = str(e)
                # 检查是否是API密钥错误或请求过频错误
                is_api_key_error = 'API' in error_str or 'key' in error_str.lower() or '401' in error_str or '403' in error_str
                is_rate_limit_error = '429' in error_str or 'Too many requests' in error_str or 'rate limit' in error_str.lower()
                
                retry_count += 1
                if is_api_key_error:
                    print(f'错误：需要API密钥才能使用historicalTrades接口')
                    print(f'请配置API密钥后重试，程序将暂停等待，直到问题解决后继续下载...')
                elif is_rate_limit_error:
                    print(f'错误：请求过于频繁（429），程序将暂停等待后继续重试...')
                else:
                    print(f'获取数据失败：{e}，程序将暂停等待后继续重试...')
                
                # 对于请求过频错误，等待更长时间
                if is_rate_limit_error:
                    sleep_time = RETRY_SLEEP_TIME * 3
                    print(f'等待 {sleep_time}s 后重试...')
                    time.sleep(sleep_time)
                else:
                    time.sleep(RETRY_SLEEP_TIME)
                
                print(f'正在重试... (第 {retry_count} 次重试)')
                # 继续循环，继续重试

    def get_historical_trades_by_time_range(self, start_time, end_time, limit=None, split_interval_hours=1, max_interval_per_batch=12, use_split=True):
        """
        模式2：按照固定时间范围下载数据
        策略：先找到end_time对应的订单ID，然后往前批量下载，直到time <= start_time时停止
        如果时间范围较大，会自动分割成多个小区间串行获取
        
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
                params = {'symbol': self.symbol, 'limit': 1}
                latest_data = self._call_options_api('/eapi/v1/historicalTrades', params=params)
                if latest_data:
                    end_id = int(latest_data[0]['tradeId'])  # 期权使用tradeId
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
                    'limit': min(limit, 500)  # 期权API最大limit为500
                }
                
                # 调用币安期权历史成交接口
                trades_data = self._call_options_api('/eapi/v1/historicalTrades', params=params)
                
                if not trades_data:
                    print("未获取到数据，停止下载")
                    break
                
                # 检查这批数据的时间，判断是否需要停止
                batch_data = []
                earliest_time = None
                for trade in trades_data:
                    trade_time = int(trade['time'])
                    trade_id = int(trade['tradeId'])  # 期权使用tradeId
                    
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
                first_id = int(first_trade['tradeId'])  # 期权使用tradeId
                first_time = int(first_trade['time'])
                last_trade = trades_data[-1]
                last_id = int(last_trade['tradeId'])  # 期权使用tradeId
                last_time = int(last_trade['time'])
                
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
                # 检查是否是API密钥错误或请求过频错误
                is_api_key_error = 'API' in error_str or 'key' in error_str.lower() or '401' in error_str or '403' in error_str
                is_rate_limit_error = '429' in error_str or 'Too many requests' in error_str or 'rate limit' in error_str.lower()
                
                if is_api_key_error:
                    print(f'错误：需要API密钥才能使用historicalTrades接口')
                    print(f'请配置API密钥后重试，程序将暂停等待，直到问题解决后继续下载...')
                elif is_rate_limit_error:
                    print(f'错误：请求过于频繁（429），程序将暂停等待后继续重试...')
                else:
                    print(f'获取数据失败：{e}，暂停{RETRY_SLEEP_TIME}s后重试')
                
                # 对于请求过频错误，等待更长时间
                if is_rate_limit_error:
                    sleep_time = RETRY_SLEEP_TIME * 3  # 429错误等待更长时间
                    print(f'等待 {sleep_time}s 后重试...')
                    time.sleep(sleep_time)
                else:
                    time.sleep(RETRY_SLEEP_TIME)
                
                # 持续重试，不退出循环（移除break，让程序继续尝试）
                print(f'正在重试... (当前已获取 {len(all_data)} 条记录)')
        
        if not all_data:
            print("未获取到任何数据")
            return pd.DataFrame()
        
        # 步骤3：转换为DataFrame并截断，只保留时间范围内的数据
        df = pd.DataFrame(all_data)
        
        if df.empty:
            print("未获取到任何数据")
            return pd.DataFrame()
        
        # 期权API响应字段映射（根据官方API文档）
        # API返回字段：id(STRING), tradeId(STRING), price(STRING), qty(STRING), quoteQty(STRING), side(INT), time(LONG)
        df['ID'] = pd.to_numeric(df['id']).astype(int)  # id是字符串，需要转换（原始ID）
        df['成交ID'] = pd.to_numeric(df['tradeId']).astype(int)  # tradeId是字符串，需要转换（用于fromId索引）
        df['成交价'] = pd.to_numeric(df['price'])  # price是字符串，需要转换
        df['成交量'] = pd.to_numeric(df['qty'])  # qty是字符串，可能是负数，保留原始值
        df['成交额'] = pd.to_numeric(df['quoteQty'])  # quoteQty是字符串，可能是负数，保留原始值
        df['成交时间'] = pd.to_numeric(df['time'])  # time是数字类型
        df['主动成交方向'] = df['side'].astype(int)  # side是数字类型，保留原始值
        
        # 按时间排序（从早到晚）
        df = df.sort_values('成交时间').reset_index(drop=True)
        
        # 去重（基于成交ID）- fromId基于tradeId（成交ID）
        df = df.drop_duplicates(subset=['成交ID'], keep='first')
        
        # 验证：检查是否完整覆盖了时间范围
        if len(df) > 0:
            actual_min_time = int(df['成交时间'].min())
            actual_max_time = int(df['成交时间'].max())
            
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
        df = df[(df['成交时间'] >= start_timestamp) & (df['成交时间'] <= end_timestamp)]
        
        if df.empty:
            print("截断后数据为空，可能是时间范围内没有数据")
            return pd.DataFrame()
        
        # 处理时间戳，转换为可读时间
        df['成交时间'] = pd.to_datetime(df['成交时间'], unit='ms', utc=True).dt.tz_convert(self.tz)
        df['获取时间'] = datetime.now(self.tz).strftime('%Y-%m-%d %H:%M:%S')
        df['期权合约'] = self.symbol
        
        # 调整列顺序（去掉不需要的列）
        df = df[['成交时间', '获取时间', '期权合约', 'ID', '成交ID', '成交价', '成交量', '成交额', 
                 '主动成交方向']]
        
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
        print(f"使用串行处理，按顺序获取各个批次...")
        
        # 串行获取各个批次的数据
        all_dfs = []
        batch_infos = [(i, batch_start, batch_end) for i, (batch_start, batch_end) in enumerate(merged_batches)]
        
        # 使用tqdm显示进度
        with tqdm(total=len(merged_batches), desc="获取历史成交数据") as pbar:
            for i, batch_start, batch_end in batch_infos:
                # 持续重试直到成功
                batch_df = None
                retry_count = 0
                max_retries = 1000000  # 设置一个很大的值，确保持续重试
                
                while batch_df is None and retry_count < max_retries:
                    try:
                        # 直接调用单个批次获取方法
                        batch_df = self._get_historical_trades_by_time_range_single(batch_start, batch_end, limit)
                        if not batch_df.empty:
                            all_dfs.append((i, batch_df))
                            break  # 成功获取数据，退出重试循环
                        else:
                            # 数据为空，可能是正常情况（该时间段没有数据），也退出重试循环
                            break
                    except Exception as e:
                        error_str = str(e)
                        # 检查是否是API密钥错误或请求过频错误
                        is_api_key_error = 'API' in error_str or 'key' in error_str.lower() or '401' in error_str or '403' in error_str
                        is_rate_limit_error = '429' in error_str or 'Too many requests' in error_str or 'rate limit' in error_str.lower()
                        
                        retry_count += 1
                        if is_api_key_error:
                            print(f"批次 {i+1} 获取失败：需要API密钥才能使用historicalTrades接口")
                            print(f'请配置API密钥后重试，程序将暂停等待，直到问题解决后继续下载...')
                        elif is_rate_limit_error:
                            print(f"批次 {i+1} 获取失败：请求过于频繁（429），程序将暂停等待后继续重试...")
                        else:
                            print(f"批次 {i+1} 获取失败：{e}，暂停{RETRY_SLEEP_TIME}s后重试...")
                        
                        # 对于请求过频错误，等待更长时间
                        if is_rate_limit_error:
                            sleep_time = RETRY_SLEEP_TIME * 3
                            print(f'等待 {sleep_time}s 后重试...')
                            time.sleep(sleep_time)
                        else:
                            time.sleep(RETRY_SLEEP_TIME)
                        
                        print(f'正在重试批次 {i+1}... (第 {retry_count} 次重试)')
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
    

    def save_historical_trades_by_time(self, df, start_time, end_time, save_format=None, merge_mode=True):
        """
        保存历史成交数据到文件
        :param df: 历史成交数据DataFrame
        :param start_time: 起始时间（datetime对象或时间戳毫秒）- 仅用于日志显示，实际文件名使用数据的时间范围
        :param end_time: 结束时间（datetime对象或时间戳毫秒）- 仅用于日志显示，实际文件名使用数据的时间范围
        :param save_format: 保存格式，'feather' 或 'csv'，默认从配置文件读取
        :param merge_mode: 是否合并模式，True表示合并已有文件并去重，False表示只保存新数据不合并不删除旧文件
        """
        save_format = save_format or HISTORICAL_TRADES_CONFIG['default_save_format']
        if df.empty:
            print('历史成交数据为空，跳过保存')
            return
        
        data_dir = data_path['result_path_historical_trades']
        
        if merge_mode:
            # 合并模式：合并已有文件，去重，每个symbol合并到一个feather文件
            file_path = merge_with_existing_and_save(
                data_dir=data_dir,
                symbol=self.symbol,
                new_df=df,
                save_format=save_format,
                tz=self.tz,
                delete_old_files=True  # 合并后删除旧文件
            )
        else:
            # 只保存新数据，不合并现有文件，不删除旧文件
            # 文件名将根据实际数据的时间范围自动生成
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

    def download_by_time_range(self, start_time, end_time, save_format=None, use_split=True, split_interval_hours=1, max_interval_per_batch=12, merge_mode=True):
        """
        模式2：按照固定时间范围下载并保存数据（便捷方法）
        :param start_time: 起始时间（datetime对象或时间戳毫秒）
        :param end_time: 结束时间（datetime对象或时间戳毫秒）
        :param save_format: 保存格式，默认从配置文件读取
        :param use_split: 是否使用split函数，None表示自动判断，True表示强制使用，False表示强制不使用
        :param split_interval_hours: 分割时间间隔（小时），默认1小时
        :param max_interval_per_batch: 每个批次的最大时间间隔（小时），默认12小时
        :param merge_mode: 是否合并模式，True表示合并已有文件并去重，False表示只保存新数据
        :return: DataFrame
        """
        save_format = save_format or HISTORICAL_TRADES_CONFIG['default_save_format']
        print(f"=========开始获取 {self.symbol} 历史成交数据=========")
        print(f"模式：按时间范围下载")
        
        df = self.get_historical_trades_by_time_range(start_time, end_time, use_split=use_split, 
                                                       split_interval_hours=split_interval_hours, 
                                                       max_interval_per_batch=max_interval_per_batch)
        if not df.empty:
            self.save_historical_trades_by_time(df, start_time, end_time, save_format=save_format, merge_mode=merge_mode)
        
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

    def update_from_local(self, limit=None, save_format=None, max_requests=None, merge_mode=True):
        """
        从本地已获取的历史数据中的最大 fromId 开始，获取最新数据并保存
        
        工作流程：
        1. 找到最大的成交ID（fromId）（优化：只读取文件名中结束时间最大的文件）
        2. 从该 ID+1 开始获取新数据直到最新
        3. 根据merge_mode决定是否合并已有文件并去重
        
        :param limit: 每次请求返回数量，最大1000，默认从配置文件读取
        :param save_format: 保存格式，'feather' 或 'csv'，默认从配置文件读取
        :param max_requests: 最大请求次数，防止无限循环，默认从配置文件读取
        :param merge_mode: 是否合并模式，True表示合并已有文件并去重（每个symbol合并到一个feather），False表示只保存新数据
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
        
        # 步骤3：保存数据（根据merge_mode决定是否合并）
        print("\n步骤3：保存数据...")
        if merge_mode:
            # 合并模式：合并已有文件，去重，每个symbol合并到一个feather文件
            file_path = merge_with_existing_and_save(
                data_dir=data_dir,
                symbol=self.symbol,
                new_df=new_df,
                save_format=save_format,
                tz=self.tz,
                delete_old_files=True  # 合并后删除旧文件
            )
        else:
            # 只保存新数据，不合并现有文件，不删除旧文件
            file_path = save_new_data_only(
                data_dir=data_dir,
                symbol=self.symbol,
                new_df=new_df,
                save_format=save_format,
                tz=self.tz
            )
        
        if file_path:
            print(f"=========数据更新完成，新增 {len(new_df)} 条记录=========")
            if merge_mode:
                print(f"数据已合并并去重，每个symbol合并到一个feather文件")
            else:
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
        执行所有任务（定时更新）
        从本地文件最大的fromid开始下载新数据
        
        :param use_split: 是否使用split函数（保留参数以兼容，但定时更新不使用此参数）
        """
        print("=========任务执行，历史成交数据更新=========")
        print("模式：从本地文件最大的fromid开始下载新数据")
        
        # 使用 update_from_local 方法更新数据（从本地文件最大fromid开始）
        try:
            df = self.update_from_local()
            if not df.empty:
                print(f"数据更新成功，共 {len(df)} 条记录")
            else:
                print("未获取到新数据，可能已经是最新数据")
        except Exception as e:
            print(f"数据更新失败：{e}")
            import traceback
            traceback.print_exc()
        
        print("=========任务结束=========")
    
    @staticmethod
    def get_latest_symbols_file(symbol_pool_path=None):
        """
        获取最新的 symbol 列表文件
        
        :param symbol_pool_path: symbol pool 路径，如果为None则从配置文件读取
        :return: (file_path, date_str) 元组，file_path 是文件路径，date_str 是日期字符串（YYYY-MM-DD）
        """
        if symbol_pool_path is None:
            symbol_pool_path = data_path['symbol_pool']
        
        symbol_pool_path = Path(symbol_pool_path)
        if not symbol_pool_path.exists():
            raise FileNotFoundError(f"Symbol pool 路径不存在: {symbol_pool_path}")
        
        # 查找所有符合格式的文件：options_YYYY-MM-DDsymbols.csv
        pattern = re.compile(r'options_(\d{4}-\d{2}-\d{2})symbols\.csv')
        symbol_files = []
        
        for file in symbol_pool_path.iterdir():
            if file.is_file() and file.suffix == '.csv':
                match = pattern.match(file.name)
                if match:
                    date_str = match.group(1)
                    symbol_files.append((file, date_str))
        
        if not symbol_files:
            raise FileNotFoundError(f"在 {symbol_pool_path} 中未找到符合格式的 symbol 文件（格式：options_YYYY-MM-DDsymbols.csv）")
        
        # 按日期排序，获取最新的文件
        symbol_files.sort(key=lambda x: x[1], reverse=True)
        latest_file, latest_date = symbol_files[0]
        
        print(f"找到最新的 symbol 列表文件: {latest_file.name} (日期: {latest_date})")
        return latest_file, latest_date
    
    @staticmethod
    def read_symbols_from_file(file_path):
        """
        从 CSV 文件中读取 symbol 列表
        
        :param file_path: CSV 文件路径
        :return: symbol 列表
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        try:
            # 尝试使用 utf-8 编码
            df = pd.read_csv(file_path, encoding='utf-8')
        except UnicodeDecodeError:
            # 如果失败，尝试使用 gbk 编码
            try:
                df = pd.read_csv(file_path, encoding='gbk')
            except Exception as e:
                raise Exception(f"读取文件失败: {e}")
        
        # 检查是否有 symbol 列
        if 'symbol' not in df.columns:
            # 如果没有 symbol 列，尝试使用第一列
            if len(df.columns) > 0:
                df.rename(columns={df.columns[0]: 'symbol'}, inplace=True)
            else:
                raise ValueError(f"CSV 文件中没有找到 symbol 列，且文件为空")
        
        # 提取 symbol 列表，去除空值
        symbols = df['symbol'].dropna().astype(str).str.strip().tolist()
        symbols = [s for s in symbols if s]  # 去除空字符串
        
        if not symbols:
            raise ValueError(f"CSV 文件中没有有效的 symbol")
        
        print(f"从文件 {file_path.name} 中读取到 {len(symbols)} 个 symbol")
        return symbols
    
    def download_batch_by_symbols_file(self, symbol_file_path=None, start_time=None, end_time=None,
                                       target_date=None, save_format=None, use_split=True, 
                                       split_interval_hours=1, max_interval_per_batch=12,
                                       max_workers=1, merge_mode=True):
        """
        批量下载多个 symbol 的历史成交数据
        
        :param symbol_file_path: symbol 列表文件路径，如果为None则自动查找最新的文件
        :param start_time: 开始时间（datetime对象），如果指定则使用此时间范围
        :param end_time: 结束时间（datetime对象），如果指定则使用此时间范围
        :param target_date: 目标日期（datetime对象或日期字符串'YYYY-MM-DD'），如果指定start_time和end_time则忽略此参数
        :param save_format: 保存格式，默认从配置文件读取
        :param use_split: 是否使用split函数，None表示自动判断，True表示强制使用，False表示强制不使用
        :param split_interval_hours: 分割时间间隔（小时），默认1小时
        :param max_interval_per_batch: 每个批次的最大时间间隔（小时），默认12小时
        :param max_workers: 最大并发数（暂时保留，当前版本使用串行处理）
        :param merge_mode: 是否合并模式，True表示合并已有文件并去重（每个symbol合并到一个feather），False表示只保存新数据
        :return: dict，包含每个 symbol 的下载结果
        """
        save_format = save_format or HISTORICAL_TRADES_CONFIG['default_save_format']
        
        # 步骤1：获取 symbol 列表文件
        if symbol_file_path is None:
            symbol_file_path, file_date = self.get_latest_symbols_file()
        else:
            symbol_file_path = Path(symbol_file_path)
        
        # 步骤2：读取 symbol 列表
        print(f"\n=========开始批量下载历史成交数据=========")
        print(f"Symbol 列表文件: {symbol_file_path.name}")
        symbols = self.read_symbols_from_file(symbol_file_path)
        print(f"共 {len(symbols)} 个 symbol 需要下载")
        
        # 步骤3：处理时间范围
        # 优先级：start_time/end_time > target_date > 从文件名提取日期 > 今天
        if start_time is not None and end_time is not None:
            # 使用指定的时间范围
            # 支持多种格式：字符串、datetime对象
            if isinstance(start_time, str):
                # 使用pandas解析，支持多种格式
                start_time = pd.to_datetime(start_time).to_pydatetime()
            if isinstance(end_time, str):
                # 使用pandas解析，支持多种格式
                end_time = pd.to_datetime(end_time).to_pydatetime()
            
            if isinstance(start_time, datetime):
                if start_time.tzinfo is None:
                    start_time = self.tz.localize(start_time)
            if isinstance(end_time, datetime):
                if end_time.tzinfo is None:
                    end_time = self.tz.localize(end_time)
        else:
            # 如果没有指定时间范围，则使用 target_date 或从文件名提取
            if target_date is None:
                # 尝试从文件名中提取日期
                pattern = re.compile(r'options_(\d{4}-\d{2}-\d{2})symbols\.csv')
                match = pattern.match(symbol_file_path.name)
                if match:
                    target_date = match.group(1)
                else:
                    # 如果文件名中没有日期，使用今天
                    target_date = datetime.now(self.tz).strftime('%Y-%m-%d')
            
            # 处理 target_date
            if isinstance(target_date, str):
                target_date = datetime.strptime(target_date, '%Y-%m-%d')
                target_date = self.tz.localize(target_date)
            elif isinstance(target_date, datetime):
                if target_date.tzinfo is None:
                    target_date = self.tz.localize(target_date)
            
            # 设置时间范围：目标日期的 00:00:00 到 23:59:59.999999
            start_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # 验证时间范围
        if start_time >= end_time:
            raise ValueError(f"开始时间 {start_time} 必须小于结束时间 {end_time}")
        
        # 计算时间范围的天数
        time_range_days = (end_time - start_time).total_seconds() / 86400
        
        print(f"时间范围: {start_time} 到 {end_time}")
        print(f"时间跨度: {time_range_days:.2f} 天")
        
        # 步骤4：批量下载每个 symbol 的数据
        results = {}
        failed_symbols = []
        success_count = 0
        
        print(f"\n开始批量下载...")
        with tqdm(total=len(symbols), desc="批量下载历史成交数据") as pbar:
            for i, symbol in enumerate(symbols, 1):
                try:
                    pbar.set_description(f"正在下载 {symbol} ({i}/{len(symbols)})")
                    
                    # 为每个 symbol 创建新的实例
                    downloader = GetHistoricalTradesData(symbol=symbol, 
                                                         api_key=self.api_key, 
                                                         api_secret=self.api_secret)
                    
                    # 下载数据
                    df = downloader.download_by_time_range(
                        start_time=start_time,
                        end_time=end_time,
                        save_format=save_format,
                        use_split=use_split,
                        split_interval_hours=split_interval_hours,
                        max_interval_per_batch=max_interval_per_batch,
                        merge_mode=merge_mode
                    )
                    
                    if not df.empty:
                        # 打印当前 symbol 的下载进度与时间范围
                        try:
                            min_time_str = df['成交时间'].min()
                            max_time_str = df['成交时间'].max()
                            print(f"{symbol}: 已获取 {len(df)} 条（时间范围：{min_time_str} ~ {max_time_str}）")
                        except Exception:
                            print(f"{symbol}: 已获取 {len(df)} 条（时间范围统计失败）")
                        results[symbol] = {
                            'status': 'success',
                            'count': len(df),
                            'data': df
                        }
                        success_count += 1
                    else:
                        results[symbol] = {
                            'status': 'empty',
                            'count': 0,
                            'data': pd.DataFrame()
                        }
                    
                except Exception as e:
                    error_msg = str(e)
                    print(f"\n❌ {symbol} 下载失败: {error_msg}")
                    results[symbol] = {
                        'status': 'failed',
                        'error': error_msg,
                        'count': 0,
                        'data': pd.DataFrame()
                    }
                    failed_symbols.append(symbol)
                    import traceback
                    traceback.print_exc()
                
                pbar.update(1)
                
                # 添加延迟，避免请求过快（币安API限制：每分钟最多300次）
                if i < len(symbols):
                    batch_interval = HISTORICAL_TRADES_CONFIG.get('batch_symbol_interval', 1.5)
                    time.sleep(batch_interval)
        
        # 步骤5：输出统计信息
        print(f"\n=========批量下载完成=========")
        print(f"总 symbol 数: {len(symbols)}")
        print(f"成功下载: {success_count}")
        print(f"数据为空: {sum(1 for r in results.values() if r['status'] == 'empty')}")
        print(f"下载失败: {len(failed_symbols)}")
        
        if failed_symbols:
            print(f"\n失败的 symbol 列表:")
            for symbol in failed_symbols:
                print(f"  - {symbol}: {results[symbol].get('error', 'Unknown error')}")
        
        return results
    
    def download_batch_by_symbols_file_by_max_id(self, symbol_file_path=None, limit=None, 
                                                 save_format=None, max_requests=None, merge_mode=True):
        """
        模式5扩展：按每个symbol的最大ID批量更新历史成交数据
        
        工作流程：
        1. 读取symbol列表文件
        2. 对每个symbol：
           - 查找本地数据中的最大成交ID
           - 从最大ID+1开始获取新数据直到最新
           - 合并已有文件，去重，保存到一个feather文件（按时间格式重命名）
        
        :param symbol_file_path: symbol 列表文件路径，如果为None则自动查找最新的文件
        :param limit: 每次请求返回数量，最大1000，默认从配置文件读取
        :param save_format: 保存格式，默认从配置文件读取
        :param max_requests: 最大请求次数，防止无限循环，默认从配置文件读取
        :param merge_mode: 是否合并模式，True表示合并已有文件并去重（每个symbol合并到一个feather），False表示只保存新数据
        :return: dict，包含每个 symbol 的更新结果
        """
        limit = limit or HISTORICAL_TRADES_CONFIG['default_limit']
        save_format = save_format or HISTORICAL_TRADES_CONFIG['default_save_format']
        max_requests = max_requests or HISTORICAL_TRADES_CONFIG.get('max_requests_per_batch', 100)
        
        # 步骤1：获取 symbol 列表文件
        if symbol_file_path is None:
            symbol_file_path, file_date = self.get_latest_symbols_file()
        else:
            symbol_file_path = Path(symbol_file_path)
        
        # 步骤2：读取 symbol 列表
        print(f"\n=========开始批量更新历史成交数据（按最大ID）=========")
        print(f"Symbol 列表文件: {symbol_file_path.name}")
        symbols = self.read_symbols_from_file(symbol_file_path)
        print(f"共 {len(symbols)} 个 symbol 需要更新")
        
        # 步骤3：批量更新每个 symbol 的数据
        results = {}
        failed_symbols = []
        success_count = 0
        
        print(f"\n开始批量更新...")
        with tqdm(total=len(symbols), desc="批量更新历史成交数据") as pbar:
            for i, symbol in enumerate(symbols, 1):
                try:
                    pbar.set_description(f"正在更新 {symbol} ({i}/{len(symbols)})")
                    
                    # 为每个 symbol 创建新的实例
                    downloader = GetHistoricalTradesData(symbol=symbol, 
                                                         api_key=self.api_key, 
                                                         api_secret=self.api_secret)
                    
                    # 从本地最大ID开始更新数据
                    new_df = downloader.update_from_local(
                        limit=limit,
                        save_format=save_format,
                        max_requests=max_requests,
                        merge_mode=merge_mode
                    )
                    
                    if not new_df.empty:
                        # 打印当前 symbol 的更新进度与时间范围
                        try:
                            min_time_str = new_df['成交时间'].min()
                            max_time_str = new_df['成交时间'].max()
                            print(f"{symbol}: 已获取 {len(new_df)} 条（时间范围：{min_time_str} ~ {max_time_str}）")
                        except Exception:
                            print(f"{symbol}: 已获取 {len(new_df)} 条（时间范围统计失败）")
                        results[symbol] = {
                            'status': 'success',
                            'count': len(new_df),
                            'data': new_df
                        }
                        success_count += 1
                    else:
                        results[symbol] = {
                            'status': 'no_new_data',
                            'count': 0,
                            'data': pd.DataFrame()
                        }
                    
                except Exception as e:
                    error_msg = str(e)
                    print(f"\n❌ {symbol} 更新失败: {error_msg}")
                    results[symbol] = {
                        'status': 'failed',
                        'error': error_msg,
                        'count': 0,
                        'data': pd.DataFrame()
                    }
                    failed_symbols.append(symbol)
                    import traceback
                    traceback.print_exc()
                
                pbar.update(1)
                
                # 添加延迟，避免请求过快（币安API限制：每分钟最多300次）
                if i < len(symbols):
                    batch_interval = HISTORICAL_TRADES_CONFIG.get('batch_symbol_interval', 1.5)
                    time.sleep(batch_interval)
        
        # 步骤4：输出统计信息
        print(f"\n=========批量更新完成=========")
        print(f"总 symbol 数: {len(symbols)}")
        print(f"成功更新: {success_count}")
        print(f"无新数据: {sum(1 for r in results.values() if r['status'] == 'no_new_data')}")
        print(f"更新失败: {len(failed_symbols)}")
        
        if failed_symbols:
            print(f"\n失败的 symbol 列表:")
            for symbol in failed_symbols:
                print(f"  - {symbol}: {results[symbol].get('error', 'Unknown error')}")
        
        return results
    
    def start_scheduler(self, is_scheduler=False, interval_minutes=1, use_split=None):
        """
        启动定时任务
        定时执行时从本地文件最大的fromid开始下载新数据
        
        :param is_scheduler: 是否启用定时任务，False 表示立即执行一次，True 表示启动定时任务
        :param interval_minutes: 定时任务执行间隔（分钟），默认1分钟
        :param use_split: 是否使用split函数（保留参数以兼容，但定时更新不使用此参数）
        
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
    USE_SPLIT = False # 可以修改为 True 或 False
    
    # ========== 初始化 ==========
    # 测试获取历史成交数据（使用配置文件中的默认参数）
    ghtd = GetHistoricalTradesData()
    
    # ========== 模式5：批量下载（根据CSV文件中的symbol列表批量下载）==========
    print("\n模式5：批量下载（根据CSV文件中的symbol列表批量下载）")
    
    # 方式1：自动查找最新的symbol列表文件，并从文件名中提取日期（下载单天数据）
    # results = ghtd.download_batch_by_symbols_file(
    #     symbol_file_path=None,  # None表示自动查找最新的文件
    #     target_date=None,  # None表示从文件名中提取日期
    #     use_split=USE_SPLIT,
    #     merge_mode=True  # True表示合并已有文件并去重（每个symbol合并到一个feather），False表示只保存新数据
    # )
    
    # 方式2：手动指定文件路径和日期（下载单天数据）
    # results = ghtd.download_batch_by_symbols_file(
    #     symbol_file_path=r'E:\Quant\data\symbols\options_2025-11-10symbols.csv',
    #     target_date='2025-11-10',  # 或使用 datetime 对象
    #     use_split=USE_SPLIT,
    #     merge_mode=True  # 合并模式：下载后合并已有文件并去重
    # )
    
    # 方式3：指定时间范围（可以下载多天数据，推荐）
    #from datetime import datetime
    #start_time = ghtd.tz.localize(datetime(2025, 11, 1, 0, 0, 0, 0))  # 开始时间
    #end_time = ghtd.tz.localize(datetime(2025, 11, 10, 23, 59, 59, 999000))  # 结束时间
    #results = ghtd.download_batch_by_symbols_file(
         #symbol_file_path=None,  # None表示自动查找最新的文件
         #start_time=start_time,  # 开始时间
         #end_time=end_time,  # 结束时间
         #use_split=USE_SPLIT,
         #split_interval_hours=1,
         #max_interval_per_batch=12,
         #merge_mode=True  # 合并模式：下载后合并已有文件并去重，每个symbol合并到一个feather文件
     #)
    
    # ========== 模式5扩展：按最大ID批量更新（根据CSV文件中的symbol列表批量更新）==========
    print("\n模式5扩展：按最大ID批量更新（根据CSV文件中的symbol列表批量更新）")
    
    # 按每个symbol的最大ID批量更新，合并已有文件并去重，每个symbol合并到一个feather文件
    results = ghtd.download_batch_by_symbols_file_by_max_id(
        symbol_file_path=None,  # None表示自动查找最新的文件
        limit=500,  # 每次请求返回数量
        save_format='feather',  # 保存格式
        merge_mode=True  # True表示合并已有文件并去重（每个symbol合并到一个feather），False表示只保存新数据
    )
    
    # ========== 模式1：按照fromId分页下载数据 ==========
    #print("\n模式1：按照fromId分页下载数据")
    #df1 = ghtd.download_by_fromid()  # 参数会从配置文件读取
    # 
    # # 也可以手动指定参数覆盖配置
    # # df1 = ghtd.download_by_fromid(from_id=1000, limit=1000, save_format='feather')
    
    # ========== 模式2：按照固定时间范围下载数据 ==========
    #print("\n模式2：按照固定时间范围下载数据")
    #from datetime import datetime
    # 设置11月2号的时间范围：0点0分0秒0毫秒 到 23点59分59秒999毫秒
    #start_time = ghtd.tz.localize(datetime(2025, 11, 7, 0, 0, 0, 0))  # 11月2号0点0分0秒0毫秒
    #end_time = ghtd.tz.localize(datetime(2025, 11, 7, 0, 59, 59, 999000))  # 11月2号23点59分59秒999毫秒
    #ghtd.download_by_time_range(start_time, end_time, use_split=USE_SPLIT)
    
    # ========== 模式3：从本地数据续传（推荐用于日常更新）==========
    # print("\n模式3：从本地数据续传")
    # print("从本地已获取的历史数据中的最大 fromId 开始，获取最新数据并合并保存")
    # df_merged = ghtd.update_from_local()  # 参数会从配置文件读取
    # #也可以手动指定参数覆盖配置
    # # df_merged = ghtd.update_from_local(limit=1000, save_format='feather')
    
    # ========== 模式4：定时更新（推荐用于日常自动更新）==========
    #print("\n模式4：定时更新")
    #print("立即执行一次数据更新")
    #ghtd.start_scheduler(is_scheduler=False)  # False 表示立即执行一次
    # 启用定时任务（每1分钟执行一次）
    #ghtd.start_scheduler(is_scheduler=True, interval_minutes=1)
    
    # ========== 便捷方法：获取昨天的数据 ==========
    # print("\n便捷方法：获取昨天的数据")
    # df3 = ghtd.get_yesterday_data()  # save_format从配置文件读取
    # 
    # ========== 便捷方法：获取指定日期的数据 ==========
    #print("\n便捷方法：获取指定日期的数据")
    #df4 = ghtd.get_data_by_date('2025-11-02')  # save_format从配置文件读取

