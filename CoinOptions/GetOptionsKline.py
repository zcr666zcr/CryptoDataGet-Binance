# 币安期权 API
import requests
import pytz
import schedule
from tqdm import tqdm
import pandas as pd
import os
from datetime import datetime, timedelta, timezone
from Settings import *
import time
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

class GetCoinsKline():

    def __init__(self, choice, save_csv=False):
        """
        币安期权交易所获取k线数据
        :param choice: 保留参数以兼容原有代码
        :param save_csv: 是否同时保存CSV文件，默认为False（只保存feather）
        """
        # 币安期权 API 基础 URL
        self.base_url = OPTIONS_API_BASE_URL
        self.exchange = choice
        self.tz = pytz.timezone('Asia/Hong_Kong')
        self.now = datetime.now()  # 今日时间
        self.save_csv = save_csv  # 是否同时保存CSV文件
        if self.now.hour >= 8:
            # 如果是，获取昨天的日期
            self.date_to_tackle = str(self.now.date() - timedelta(days=1))[:10]
        else:
            # 如果不是，获取前天的日期
            self.date_to_tackle = str(self.now.date() - timedelta(days=2))[:10]

    # 计算开始下载日期，根据行权日期和规则
    def calculate_start_download_date(self, expiry_date_str: str) -> str:
        """
        根据行权日期计算开始下载日期
        规则：
        1. 如果行权是季月的最后一个周五，从月份-3的第一天开始获取
        2. 如果是非季月最后一个周五，从上月第一天开始获取
        3. 周五的话，从上周一开始获取
        4. 非周五的日期从7天前开始获取
        :param expiry_date_str: 行权日期字符串，格式 'YYYY-MM-DD'
        :return: 开始下载日期字符串，格式 'YYYY-MM-DD'
        """
        if not expiry_date_str or expiry_date_str.strip() == '':
            return ''
        
        try:
            expiry_date = pd.to_datetime(expiry_date_str).date()
        except Exception:
            return ''
        
        # 判断是否是周五（weekday()返回0-6，4表示周五）
        is_friday = expiry_date.weekday() == 4
        
        if is_friday:
            # 判断是否是最后一个周五
            def get_last_friday_of_month(year, month):
                """获取指定年月的最后一个周五"""
                # 获取该月最后一天
                if month == 12:
                    last_day = datetime(year + 1, 1, 1).date() - timedelta(days=1)
                else:
                    last_day = datetime(year, month + 1, 1).date() - timedelta(days=1)
                # 从最后一天往前找，找到第一个周五
                for day_offset in range(7):
                    check_date = last_day - timedelta(days=day_offset)
                    if check_date.weekday() == 4:
                        return check_date
                return None
            
            last_friday = get_last_friday_of_month(expiry_date.year, expiry_date.month)
            is_last_friday = (last_friday and expiry_date == last_friday)
            
            # 判断是否是季月（3、6、9、12月）
            is_quarter_month = expiry_date.month in [3, 6, 9, 12]
            
            if is_quarter_month and is_last_friday:
                # 规则1：季月最后一个周五，从月份-3的第一天开始获取
                target_month = expiry_date.month - 3
                target_year = expiry_date.year
                if target_month <= 0:
                    target_month += 12
                    target_year -= 1
                start_date = datetime(target_year, target_month, 1).date()
                return start_date.strftime('%Y-%m-%d')
            elif is_last_friday:
                # 规则2：非季月最后一个周五，从上月第一天开始获取
                target_month = expiry_date.month - 1
                target_year = expiry_date.year
                if target_month <= 0:
                    target_month = 12
                    target_year -= 1
                start_date = datetime(target_year, target_month, 1).date()
                return start_date.strftime('%Y-%m-%d')
            else:
                # 规则3：其他周五，从上周一开始获取
                # 周五到上周一：往前推11天（4天到本周一，再7天到上周一）
                start_date = expiry_date - timedelta(days=11)
                return start_date.strftime('%Y-%m-%d')
        else:
            # 规则4：非周五，从7天前开始获取
            start_date = expiry_date - timedelta(days=7)
            return start_date.strftime('%Y-%m-%d')
    
    # 定义一个函数，获取期权symbol名单，并附带优化下载用的元数据。
    # 输出列：
    # - symbol：合约代码
    # - is_history：是否来自历史行权（1=历史行权，0=当前交易）
    # - exercise_expiry_date：历史行权合约的行权日期（YYYY-MM-DD），用于过期后跳过下载
    # - start_download_date：开始下载日期（YYYY-MM-DD），用于优化下载范围
    # fetch_history=True: 合并exchangeInfo当前symbol + exerciseHistory历史symbol（回溯，耗时长）
    # fetch_history=False: 读取最近CSV名单并合并当前exchangeInfo的symbol后去重，保留已有元数据
    def get_symbols(self, fetch_history: bool = False):
        # 确保symbol_pool目录存在
        data_path['symbol_pool'].mkdir(parents=True, exist_ok=True)
        symbol_file = data_path['symbol_pool'] / rf'options_{str(self.now)[:10]}symbols.csv'

        # 根据参数决定策略
        try:
            # 统一获取当前exchangeInfo的所有optionSymbols（不做状态过滤）
            url_info = f"{self.base_url}/eapi/v1/exchangeInfo"
            print(f'请求exchangeInfo：{url_info}')
            resp_info = requests.get(url_info, timeout=10)
            resp_info.raise_for_status()
            exchange_info = resp_info.json()
            options = exchange_info.get('optionSymbols', [])
            current_symbols = {opt.get('symbol') for opt in options if opt.get('symbol')}
            print(f'exchangeInfo当前合约数：{len(current_symbols)}')
            all_symbols = set()

            if fetch_history:
                # 2) 历史行权记录中出现过的symbol（按标的资产遍历并分页回溯），同时记录行权日期
                underlying_assets = ['BNBUSDT', 'BTCUSDT', 'DOGEUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT']
                hist_map = {}

                def fetch_exercise_symbols(underlying: str):
                    end_time = int(datetime.now().timestamp() * 1000)
                    start_time = int(datetime(2022, 8, 1, tzinfo=timezone.utc).timestamp() * 1000)
                    collected = {}
                    safety_rounds = 0
                    while True:
                        safety_rounds += 1
                        if safety_rounds > 20000:  # 安全停止，避免极端情况
                            break
                        if end_time <= start_time:
                            break
                        params = {'underlying': underlying, 'startTime': start_time, 'endTime': end_time, 'limit': 100}
                        url_ex = f"{self.base_url}/eapi/v1/exerciseHistory"
                        try:
                            resp = requests.get(url_ex, params=params, timeout=10)
                            resp.raise_for_status()
                            items = resp.json()
                            if not isinstance(items, list) or len(items) == 0:
                                break
                            for it in items:
                                sym = it.get('symbol')
                                exp_ms = it.get('expiryDate')
                                if sym:
                                    collected[sym] = exp_ms
                            # 进度打印：当前批次数量与行权日期范围
                            batch_count = len(items)
                            expiry_list = [it.get('expiryDate') for it in items if isinstance(it.get('expiryDate'), (int, float))]
                            if expiry_list:
                                earliest_ms = min(expiry_list)
                                latest_ms = max(expiry_list)
                                earliest_dt = pd.to_datetime(earliest_ms, unit='ms', utc=True).tz_convert(self.tz).strftime('%Y-%m-%d %H:%M:%S')
                                latest_dt = pd.to_datetime(latest_ms, unit='ms', utc=True).tz_convert(self.tz).strftime('%Y-%m-%d %H:%M:%S')
                            else:
                                earliest_dt = '-'
                                latest_dt = '-'
                            print(f'[历史抓取:{underlying}] 批次{safety_rounds}，返回{batch_count}条，行权时间范围 {earliest_dt} ~ {latest_dt}，累计{len(collected)} symbols')
                            if len(items) < 100 or len(expiry_list) == 0:
                                break
                            next_end_time = min(expiry_list) - 1
                            next_end_dt = pd.to_datetime(next_end_time, unit='ms', utc=True).tz_convert(self.tz).strftime('%Y-%m-%d %H:%M:%S')
                            print(f'[历史抓取:{underlying}] 下一页 endTime 将回溯到：{next_end_dt}')
                            end_time = next_end_time
                        except Exception as e:
                            print(f'获取{underlying}行权历史失败：{e}，稍后重试')
                            time.sleep(2)
                            continue
                    return collected

                for u in underlying_assets:
                    got_map = fetch_exercise_symbols(u)
                    print(f'{u} 历史行权出现过的symbol数：{len(got_map)}')
                    hist_map.update(got_map)

                # 构造输出DataFrame：历史行权（1）+ 当前交易（0），历史项带行权日期和开始下载日期
                rows = []
                # 历史
                for sym, exp_ms in hist_map.items():
                    exp_dt = None
                    try:
                        if isinstance(exp_ms, (int, float)):
                            exp_dt = pd.to_datetime(exp_ms, unit='ms', utc=True).tz_convert(self.tz).strftime('%Y-%m-%d')
                    except Exception:
                        exp_dt = ''
                    # 计算开始下载日期
                    start_download_dt = self.calculate_start_download_date(exp_dt or '')
                    rows.append({
                        'symbol': sym, 
                        'is_history': 1, 
                        'exercise_expiry_date': exp_dt or '',
                        'start_download_date': start_download_dt
                    })
                # 当前
                for sym in current_symbols:
                    if sym in hist_map:
                        continue  # 历史里已有，优先保留历史记录（带行权日期）
                    rows.append({
                        'symbol': sym, 
                        'is_history': 0, 
                        'exercise_expiry_date': '',
                        'start_download_date': ''
                    })
                df = pd.DataFrame(rows)
            else:
                # 快速路径：读取最近一份CSV，并与当前exchangeInfo合并去重
                df_prev = None
                try:
                    files = list(data_path['symbol_pool'].glob('options_*symbols.csv'))
                    if files:
                        latest = max(files, key=lambda p: p.stat().st_mtime)
                        df_prev = pd.read_csv(latest)
                        print(f'最近CSV：{latest.name}，读取到 {len(df_prev)} 条记录')
                    else:
                        print('未找到历史CSV，使用exchangeInfo当前合约作为基础')
                except Exception as e:
                    print(f'读取历史CSV失败：{e}，仅使用exchangeInfo当前合约')
                    df_prev = None
                # 规范化旧CSV的列，保证存在四列并保持已有信息
                if df_prev is None or df_prev.empty:
                    df_prev = pd.DataFrame(columns=['symbol', 'is_history', 'exercise_expiry_date', 'start_download_date'])
                if 'symbol' not in df_prev.columns:
                    if df_prev.shape[1] > 0:
                        df_prev.rename(columns={df_prev.columns[0]: 'symbol'}, inplace=True)
                    else:
                        df_prev['symbol'] = []
                if 'is_history' not in df_prev.columns:
                    df_prev['is_history'] = 0
                if 'exercise_expiry_date' not in df_prev.columns:
                    df_prev['exercise_expiry_date'] = ''
                if 'start_download_date' not in df_prev.columns:
                    # 对于已有历史记录，重新计算开始下载日期
                    df_prev['start_download_date'] = ''
                    for idx, row in df_prev.iterrows():
                        if row.get('is_history') == 1 and pd.notna(row.get('exercise_expiry_date')):
                            exp_date = str(row.get('exercise_expiry_date', '')).strip()
                            if exp_date:
                                df_prev.at[idx, 'start_download_date'] = self.calculate_start_download_date(exp_date)
                # 将当前exchangeInfo新增的symbol并入，默认is_history=0，无行权日期
                prev_syms = set(df_prev['symbol'].dropna().astype(str).tolist())
                add_rows = [{
                    'symbol': sym, 
                    'is_history': 0, 
                    'exercise_expiry_date': '',
                    'start_download_date': ''
                } for sym in current_symbols if sym not in prev_syms]
                df = pd.concat([df_prev, pd.DataFrame(add_rows)], ignore_index=True)
                # 去重
                df.drop_duplicates(subset=['symbol'], keep='first', inplace=True)

            # 最终保存
            df = df.sort_values('symbol').reset_index(drop=True)
            df.to_csv(symbol_file, index=False, encoding='utf-8-sig')
            print(f'已保存symbol列表（{len(df)} 条）：{symbol_file}')
        except Exception as e:
            print(f'获取symbol列表失败：{e}')
            import traceback
            traceback.print_exc()
            raise

    # 获取需要获取的日期列表,[start_date, self.date_to_tackle]
    def get_date_list(self):
        # 确保目录存在
        data_path['result_path_daily'].mkdir(parents=True, exist_ok=True)
        
        # 获取已获取的日期
        try:
            file_list = os.listdir(data_path['result_path_daily'])
            file_list = [file[:10] for file in file_list if file.endswith('.feather')]
            file_list.sort()
        except Exception as e:
            print(f'读取目录失败：{e}，使用默认起始日期')
            file_list = []

        # 若现有列表为空，按照固定默认起始时间 2022-08-01
        if len(file_list) == 0:
            start_date = '2022-08-01'
            print(f'未找到已有数据文件，使用默认起始日期：{start_date}')
        else:
            start_date = file_list[-1]
            print(f'找到已有数据，最后日期：{start_date}')

        #获取期间日期，用已有的最后一天为开始时间，当天为截止时间
        date_list = []
        start_date_dt = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=1)
        end_date_dt = datetime.strptime(self.date_to_tackle, '%Y-%m-%d')

        print(f'计算日期范围：{start_date_dt.strftime("%Y-%m-%d")} 到 {end_date_dt.strftime("%Y-%m-%d")}')

        while start_date_dt <= end_date_dt:
            date_list.append(start_date_dt.strftime('%Y-%m-%d'))
            start_date_dt += timedelta(days=1)

        print(f'已获取date_list，共 {len(date_list)} 天')
        if len(date_list) > 0:
            print(f'日期范围：{date_list[0]} 到 {date_list[-1]}')
        return date_list

    # 定义一个函数，传入期权合约代码和日期，通过币安期权API获取日度k线数据
    def get_kline(self, symbol, date, end_date, length):
        # 获取该日的起始时间戳，单位为毫秒
        start_time = pd.to_datetime(date) + timedelta(hours=8)
        start_time = self.tz.localize(start_time)  # 时间不变，加上时区信息（香港时区）
        start_time_ms = int(start_time.timestamp() * 1000)

        end_time = pd.to_datetime(end_date) + timedelta(hours=8)
        end_time = self.tz.localize(end_time)  # 时间不变，加上时区信息（香港时区）
        end_time_ms = int(end_time.timestamp() * 1000)
        klines_data = []  # 初始化 klines_data 为空列表，用于存储处理后的K线数据
        
        # 获取该日的k线数据
        while True:
            try:
                # 币安期权 API 获取 K 线数据
                url = f"{self.base_url}/eapi/v1/klines"
                params = {
                    'symbol': symbol,
                    'interval': '1d',  # 日线
                    'startTime': start_time_ms,
                    'endTime': end_time_ms,
                    'limit': 1500  # 币安期权K线接口最大limit为1500，默认500
                }
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                klines_raw = response.json()
                
                # 正常返回但数据为空：不重试，直接退出循环
                if not klines_raw:
                    klines_data = []  # 确保 klines_data 为空
                    break

                # 币安期权 API 返回的是对象数组（字典数组），需要转换为列表格式
                # 响应格式：[{open, high, low, close, volume, amount, interval, tradeCount, takerVolume, takerAmount, openTime, closeTime}, ...]
                klines_data = []
                for item in klines_raw:
                    klines_data.append([
                        item['openTime'],      # 开盘时间
                        item['open'],          # 开盘价
                        item['high'],          # 最高价
                        item['low'],           # 最低价
                        item['close'],         # 收盘价
                        item['amount'],        # 成交量
                        item['closeTime'],     # 收盘时间
                        item['volume'],        # 成交额
                        item['tradeCount'],    # 成交笔数
                        item['takerAmount'],   # 主动买入成交量
                        item['takerVolume'],   # 主动买入成交额
                        ''                     # 请忽略（占位符）
                    ])
                break
            except Exception as e:
                print(f'连接中断或发生错误：{e}，暂停10s')
                time.sleep(10)

        # 将k线数据转换为一个pandas的DataFrame，设置列名和索引
        # todo: 修改将k线数据转换为一个pandas的DataFrame，设置列名和索引
        df = pd.DataFrame(klines_data, columns=['开盘时间', '开盘价', '最高价', '最低价', '收盘价', '成交量', '收盘时间', '成交额', '成交笔数',	'主动买入成交量', '主动买入成交额', '请忽略'])
        # 在进行后续处理前检查 DataFrame 是否为空
        if df.empty:
            # print(f"DEBUG: DataFrame for {symbol} is empty after initial DataFrame creation.")
            return df # 如果是空的，直接返回

        df['开盘时间'] = pd.to_datetime(df['开盘时间'], unit='ms', utc=True).dt.tz_convert(self.tz)
        df['收盘时间'] = pd.to_datetime(df['收盘时间'], unit='ms', utc=True).dt.tz_convert(self.tz)
        # todo: 添加日期列，用于后续按天分组保存，与GetHourKine保持一致

        df['开盘时间'] = df['开盘时间'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['收盘时间'] = df['收盘时间'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['货币对'] = symbol  # 期权 symbol 格式：BTCUSDT-20251107-50000-C（标的-到期日-行权价-类型）

        # 返回日度的k线数据的df
        return df

    # 定义一个函数，获取所有symbol的行情数据，并保存到配置文件（一天一个df.to_csv）
    def get_all_kline(self):
        # 调用get_dates函数，每次取100天为一组
        date_list = self.get_date_list()
        if not date_list:
            print("没有可更新日线数据了！")
            return

        date_list = [date_list[i:i + 100] for i in range(0, len(date_list), 100)]
        print(f'日期列表已分组，共 {len(date_list)} 组，每组最多100天')
        
        # 确保symbol_pool目录存在
        data_path['symbol_pool'].mkdir(parents=True, exist_ok=True)
        
        symbol_file = data_path['symbol_pool'] / rf'options_{str(self.now)[:10]}symbols.csv'
        if not os.path.isfile(symbol_file):
            print(f'错误：期权合约列表文件不存在：{symbol_file}')
            print('请先运行 get_symbols() 获取期权合约列表')
            return
        
        try:
            df_symbols = pd.read_csv(symbol_file)
            if df_symbols.empty or 'symbol' not in df_symbols.columns:
                print(f'错误：期权合约列表文件为空或格式不正确：{symbol_file}')
                print('请先运行 get_symbols() 获取期权合约列表')
                return
            symbols = df_symbols['symbol'].to_list()
            # 过滤掉空值
            symbols = [s for s in symbols if pd.notna(s) and str(s).strip() != '']
            print(f'读取到 {len(symbols)} 个期权合约')
            # 读取元数据：行权日期、开始下载日期与来源
            has_expiry = 'exercise_expiry_date' in df_symbols.columns
            has_start_download = 'start_download_date' in df_symbols.columns
            has_history_flag = 'is_history' in df_symbols.columns
            expiry_map = {}
            start_download_map = {}
            hist_flag_map = {}
            if has_expiry:
                try:
                    for _, row in df_symbols.iterrows():
                        sym = str(row['symbol'])
                        exp = row.get('exercise_expiry_date', '')
                        if pd.notna(sym) and pd.notna(exp) and str(exp).strip() != '':
                            expiry_map[sym] = str(exp).strip()
                        if has_start_download:
                            start_dl = row.get('start_download_date', '')
                            if pd.notna(start_dl) and str(start_dl).strip() != '':
                                start_download_map[sym] = str(start_dl).strip()
                        if has_history_flag:
                            hist_flag_map[sym] = int(row.get('is_history', 0))
                except Exception:
                    expiry_map = {}
                    start_download_map = {}
                    hist_flag_map = {}
        except Exception as e:
            print(f'读取期权合约列表文件失败：{e}')
            print('请先运行 get_symbols() 获取期权合约列表')
            return
        
        # 检查是否有合约
        if len(symbols) == 0:
            print('错误：没有可用的期权合约，无法获取K线数据')
            print('请检查：')
            print('  1. 币安期权API是否正常')
            print('  2. 是否有TRADING状态的期权合约')
            return
        
        # 期权合约不需要筛选，直接使用获取到的期权 symbol 列表

        # 循环取数据
        for date in date_list:  # todo: 要看一下date_list的长度再切片
            df_date = pd.DataFrame()
            for symbol in tqdm(symbols, desc=date[0] + ' to ' + date[-1]):
                # 元数据优化1：若为历史行权合约且本批次起始日期已晚于行权日期，则跳过
                try:
                    if symbol in expiry_map and (not has_history_flag or hist_flag_map.get(symbol, 0) == 1):
                        exp_date_str = expiry_map.get(symbol)
                        if exp_date_str:
                            batch_start = pd.to_datetime(date[0])
                            exp_date = pd.to_datetime(exp_date_str)
                            if batch_start.date() > exp_date.date():
                                # 本批次起始日期晚于行权日期，跳过后续下载
                                continue
                except Exception:
                    pass
                
                # 元数据优化2：若为历史行权合约且本批次早于开始下载日期，则跳过
                # 如果批次结束日期早于开始下载日期，说明整个批次都在开始日期之前，跳过
                try:
                    if symbol in start_download_map and (not has_history_flag or hist_flag_map.get(symbol, 0) == 1):
                        start_dl_date_str = start_download_map.get(symbol)
                        if start_dl_date_str:
                            batch_end = pd.to_datetime(date[-1])
                            start_dl_date = pd.to_datetime(start_dl_date_str)
                            # 如果批次结束日期早于开始下载日期，整个批次都太早，跳过
                            if batch_end.date() < start_dl_date.date():
                                continue
                except Exception:
                    pass
                # 调用get_kline函数，获取单个symbol的K线行情数据，并赋值给kline_df
                kline_df = self.get_kline(symbol, date[0],date[-1], len(date))
                # ----------- 添加以下调试代码 -----------
                # if not kline_df.empty:
                #     print(f"为 {symbol} 获取的 kline_df 列名: {kline_df.columns.tolist()}")
                # else:
                #     print(f"为 {symbol} 获取的 kline_df 为空。")
                # ----------------------------------------
                if not kline_df.empty:
                    df_date = pd.concat([df_date, kline_df], ignore_index=True)

            # 检查是否有数据
            if df_date.empty:
                print(f'警告：日期范围 {date[0]} 到 {date[-1]} 没有获取到任何数据')
                continue

            # 检查DataFrame是否有必要的列
            if '开盘时间' not in df_date.columns:
                print(f'错误：DataFrame缺少"开盘时间"列，实际列：{df_date.columns.tolist()}')
                continue

            ## 每一天分别保存
            df_date['日期'] = df_date['开盘时间'].apply(lambda x: str(x)[:10])
            dfgroup = df_date.groupby('日期')

            for sub_date, sub_df in dfgroup:
                ## 加回时间
                del sub_df['日期']
                sub_df['开盘时间'] = sub_df['开盘时间'].astype(str)

                sub_df.reset_index(inplace=True, drop=True)
                ## 保存结果 - 默认保存为feather
                feather_path = data_path['result_path_daily'] / rf'{str(sub_date)[:10]}.feather'
                sub_df.to_feather(feather_path)
                print(f'{sub_date}已保存为feather文件')
                
                ## 如果设置了save_csv，同时保存CSV文件
                if self.save_csv:
                    csv_path = data_path['result_path_daily'] / rf'{str(sub_date)[:10]}.csv'
                    sub_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                    print(f'{sub_date}已保存为CSV文件')

    # 设置每天早上8点执行
    def run_all_tasks(self):
        print("=" * 50)
        print("=========任务执行，日线数据更新=========")
        print("=" * 50)
        self.now = datetime.now()  # 今日时间
        print(f'当前时间：{self.now.strftime("%Y-%m-%d %H:%M:%S")}')
        
        if self.now.hour >= 8:
            # 如果是，获取昨天的日期
            self.date_to_tackle = str(self.now.date() - timedelta(days=1))[:10]
        else:
            # 如果不是，获取前天的日期
            self.date_to_tackle = str(self.now.date() - timedelta(days=2))[:10]
        
        print(f'目标日期：{self.date_to_tackle}')

        try:
            print('\n步骤1：获取期权合约列表...')
            self.get_symbols()
            print('步骤1完成\n')
            
            print('步骤2：获取K线数据...')
            self.get_all_kline()
            print('步骤2完成\n')
        except Exception as e:
            print(f'\n错误：任务执行失败：{e}')
            import traceback
            traceback.print_exc()
            raise
        
        print("=" * 50)
        print("=========任务结束=========")
        print("=" * 50)

    def start_scheduler(self, is_scheduler=False):
        if not is_scheduler:
            self.run_all_tasks()
        else:
            print("定时任务已启动，等待每天8点执行...")
            schedule.every().day.at("08:00").do(self.run_all_tasks)
            while True:
                schedule.run_pending()
                time.sleep(60)


# 如果当前文件是主程序，执行main函数
if __name__ == '__main__':
    print("=" * 50)
    print("币安期权K线数据获取程序")
    print("=" * 50)
    
    try:
        # 更新币安期权数据
        choice = {0: "U本位", 1: "U本位&币本位"}  # 保留参数以兼容原有代码
        fc = GetCoinsKline(choice[0])  #
        fc.start_scheduler(is_scheduler=False)  # {True: 定时运行，False：即刻运行}，默认为False
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
    except Exception as e:
        print(f"\n\n程序执行出错：{e}")
        import traceback
        traceback.print_exc()
        print("\n程序退出")
