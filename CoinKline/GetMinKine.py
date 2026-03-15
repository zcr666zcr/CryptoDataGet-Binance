# todo: 修改导入语句，适配币安API
# import okx.MarketData as MarketData # 注释掉或删除欧易相关导入
# import okx.PublicData as PublicData # 注释掉或删除欧易相关导入
from binance.spot import Spot as Client # todo: 导入币安Spot客户端

import pytz
import schedule
from tqdm import tqdm
import pandas as pd
import os
from datetime import datetime, timedelta
from Settings import *
import time
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


class GetMinKline():

    def __init__(self, interval='1m'):
        """
        币安交易所获取k线数据
        :param interval: K线时间间隔，'1m' 表示1分钟，'15m' 表示15分钟
        """
        # todo: 修改初始化API客户端
        # self.marketDataAPI = MarketData.MarketAPI(flag="0") # 注释掉或删除欧易相关初始化
        # self.publicDataAPI = PublicData.PublicAPI(flag="0") # 注释掉或删除欧易相关初始化

        self.client = Client() # todo: 币安客户端初始化，这里不传API_KEY和SECRET_KEY

        self.tz = pytz.timezone('Asia/Hong_Kong')
        self.now = datetime.now()  # 今日时间

        # 根据时间间隔设置不同的处理时间
        if interval == '15m':
            self.interval = '15m'
            self.interval_minutes = 15
            # 15分钟K线：处理到上一个15分钟整点
            current_minute = self.now.minute
            minutes_to_subtract = current_minute % 15
            if minutes_to_subtract == 0:
                minutes_to_subtract = 15
            self.dt_to_tackle = str(self.now.replace(second=0, microsecond=0) - timedelta(minutes=minutes_to_subtract))
        else:
            self.interval = '1m'
            self.interval_minutes = 1
            self.dt_to_tackle = str(self.now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1))

    # 定义一个函数，获取欧易交易所可交易的所有币种，并保存到配置文件
    def get_symbols(self):
        # 判断是否已存在
        if os.path.isfile(data_path['symbol_pool'] / rf'{str(self.now)[:10]}symbols.csv'):
            print('已存在可交易的币种列表')

        else:
            # todo: 修改获取所有币种的接口为币安，与GetHourKine保持一致
            exchange_info = self.client.exchange_info()  # 获取交易所信息
            symbols = exchange_info['symbols']  # 提取symbols列表
            df_usdt = pd.DataFrame(symbols)
            df_usdt = df_usdt[df_usdt['symbol'].str.endswith('USDT')]
            symbols_name = df_usdt['symbol'].to_list()
            # symbols_name = []
            # for s in symbols:
            #     # todo: 确保是现货交易且状态为TRADING
            #     if s['status'] == 'TRADING' and s['isSpotTradingAllowed']:
            #         symbols_name.append(s['symbol'])

            # 创建一个空的pandas的DataFrame
            df = pd.DataFrame(columns=['symbol'])
            df['symbol'] = symbols_name

            df.to_csv(data_path['symbol_pool'] / rf'{str(self.now)[:10]}symbols.csv'
                      , index=False, encoding='utf-8-sig')
            print('获取可交易的币种列表')

    def get_date_start(self):
        # 根据时间间隔选择不同的路径
        if self.interval == '15m':
            result_path = data_path['result_path_15min']
        else:
            result_path = data_path['result_path_min']
        
        # 获取已获取的日期
        file_list = os.listdir(result_path)
        file_list = [file[:10] for file in file_list]
        file_list.sort()

        # 若现有列表为空，按照默认起始时间
        is_null = True
        if len(file_list) == 0:
            start_date = '2017-08-16'  # todo:测试用
        else:
            start_date = file_list[-1]
            is_null = False

        return start_date, is_null, result_path

    # 定义一个函数，传入标的代码和日期，通过币安API获取K线数据，返回该时间段的K线数据的df
    def get_kline(self, symbol, start_min, end_min, length=100):
        # 获取起始时间戳，单位为毫秒
        start_time = self.tz.localize(start_min)  # 时间不变，加上时区信息（香港时区）
        end_time = self.tz.localize(end_min)  # 时间不变，加上时区信息（香港时区）

        # print(f"尝试获取 {symbol} 从 {start_time} 到 {end_time} 的{self.interval} K 线数据")

        start_time = int(start_time.timestamp() * 1000)
        end_time = int(end_time.timestamp() * 1000)
        klines_data = []  # 初始化 klines_data 为空列表，用于存储处理后的K线数据
        # 获取该时间段的k线数据
        while True:
            try:
                # 使用self.interval来指定K线时间间隔
                klines_raw = self.client.klines(
                    symbol=symbol,
                    interval=self.interval,  # 使用实例变量，支持'1m'或'15m'
                    startTime=start_time,
                    endTime=end_time,
                    limit=1000  # 币安K线接口最大limit通常为1000
                )
                # 正常返回但数据为空：不重试，直接退出循环
                # 币安返回的是列表，直接判断列表是否为空
                if not klines_raw:
                    klines_data = []  # 确保 klines_data 为空
                    break

                # 正常数据，取前12项 (币安K线返回12个字段，与您的中文列名数量匹配)
                klines_data = [line[:12] for line in klines_raw]
                break
            except  Exception as e: # todo: 捕获特定异常并打印
                print(f'连接中断或发生错误：{e}，暂停5s')
                time.sleep(5)

        # 将k线数据转换为一个pandas的DataFrame，设置列名和索引
        # todo: 使用中文列名，与GetHourKine保持一致
        df = pd.DataFrame(klines_data,
                        columns=['开盘时间', '开盘价', '最高价', '最低价', '收盘价', '成交量', '收盘时间', '成交额', '成交笔数', '主动买入成交量',
                                       '主动买入成交额', '请忽略'])

        # 在进行后续处理前检查 DataFrame 是否为空
        if df.empty:
            return df  # 如果是空的，直接返回

        df['开盘时间'] = pd.to_datetime(df['开盘时间'], unit='ms', utc=True).dt.tz_convert(self.tz)
        df['收盘时间'] = pd.to_datetime(df['收盘时间'], unit='ms', utc=True).dt.tz_convert(self.tz)
        # todo: 添加日期列，用于后续按天分组保存，与GetHourKine保持一致

        df['开盘时间'] = df['开盘时间'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['收盘时间'] = df['收盘时间'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['货币对'] = symbol  # todo: 币安的symbol直接就是例如'BTCUSDT'，无需split

        # 返回日度的k线数据的df
        return df

    # 定义一个函数，获取所有symbol的行情数据，并保存到配置文件（一天一个df.to_feather）
    # 定义一个函数，获取所有symbol的行情数据，并保存到配置文件（每批次一个feather文件）
    def get_all_kline(self):
        # 调用get_dates函数
        date_start, is_null, result_path = self.get_date_start()

        symbols = pd.read_csv(data_path['symbol_pool'] / rf'{str(self.now)[:10]}symbols.csv')['symbol'].to_list()
        # 筛选仅以USDT结尾的币种
        symbols = [symbol for symbol in symbols if symbol.endswith('USDT') and not symbol.endswith(':USDT')]
        symbols = symbols[:]  # 可以根据需要修改测试数量

        if is_null:
            df = pd.DataFrame()
            min_start = pd.to_datetime(date_start)
        else:
            last_feather_str = result_path / rf'{date_start}.feather'
            df = pd.read_feather(last_feather_str)
            df["开盘时间"] = pd.to_datetime(df["开盘时间"].astype(str))
            min_start = (df["开盘时间"].max() + timedelta(minutes=self.interval_minutes)).tz_localize(None)

        min_end = pd.to_datetime(self.dt_to_tackle)

        if min_start > min_end:
            print(f"无可更新{self.interval}K线！")
            return

        # 根据时间间隔生成时间分段
        if self.interval == '15m':
            min_list = pd.date_range(min_start, min_end, freq='15min')
            min_list = [min_list[i:i + 900] for i in range(0, len(min_list), 900)]
        else:
            min_list = pd.date_range(min_start, min_end, freq='min')
            min_list = [min_list[i:i + 900] for i in range(0, len(min_list), 900)]

        # ✅ 修改点：去掉 [:3] 限制，下载全部批次，并每批保存一次
        for i, mins in enumerate(min_list, 1):
            batch_df = pd.DataFrame()
            print(f"\n===== 正在下载第 {i}/{len(min_list)} 批次："
                  f"{mins[0].strftime('%Y-%m-%d %H:%M')} → {mins[-1].strftime('%Y-%m-%d %H:%M')} =====")

            # 遍历所有币种
            for symbol in tqdm(symbols,
                               desc=f"Batch {i} {mins[0].strftime('%m-%d %H:%M')} → {mins[-1].strftime('%H:%M')}"):
                kline_df = self.get_kline(symbol, mins[0], mins[-1], len(mins))
                if not kline_df.empty:
                    batch_df = pd.concat([batch_df, kline_df], ignore_index=True)

            if batch_df.empty:
                print(f"⚠️ 第 {i} 批次数据为空，跳过保存。")
                continue

            # 添加日期列，并按天分组保存
            batch_df['日期'] = batch_df['开盘时间'].apply(lambda x: str(x)[:10])
            dfgroup = batch_df.groupby('日期')

            for sub_date, sub_df in dfgroup:
                sub_df = sub_df.drop(columns=['日期'])
                sub_df = sub_df.sort_values(['开盘时间', '货币对']).reset_index(drop=True)

                file_path = result_path / rf'{sub_date}.feather'

                # ✅ 若文件存在，则读取旧数据合并（避免覆盖）
                if os.path.exists(file_path):
                    old_df = pd.read_feather(file_path)
                    combined = pd.concat([old_df, sub_df], ignore_index=True)
                    combined = combined.drop_duplicates(subset=['开盘时间', '货币对']).sort_values(
                        ['开盘时间', '货币对'])
                    combined.reset_index(drop=True, inplace=True)
                    combined.to_feather(file_path)
                else:
                    sub_df.to_feather(file_path)

                print(f"✅ 第 {i} 批次：{sub_date} 的 {self.interval}K线 已保存至 {file_path}")

            # ✅ 批次间休眠，防止API限频（可调整）
            time.sleep(1)

        print("\n🎯 所有批次下载完成并保存。")

    def run_all_tasks(self):
        interval_name = "15分钟" if self.interval == '15m' else "1分钟"
        print(f"=========任务执行，{interval_name}K线数据更新=========")
        self.now = datetime.now()  # 今日时间
        
        # 根据时间间隔设置不同的处理时间
        if self.interval == '15m':
            # 15分钟K线：处理到上一个15分钟整点
            current_minute = self.now.minute
            minutes_to_subtract = current_minute % 15
            if minutes_to_subtract == 0:
                minutes_to_subtract = 15
            self.dt_to_tackle = str(self.now.replace(second=0, microsecond=0) - timedelta(minutes=minutes_to_subtract))
        else:
            # 1分钟K线：处理到上一分钟
            self.dt_to_tackle = str(self.now.replace(second=0, microsecond=0) - timedelta(minutes=1))

        self.get_symbols()
        self.get_all_kline()
        print("=========任务结束=========")

    def start_scheduler(self, is_scheduler=False, interval_minutes=None):
        if not is_scheduler:
            self.run_all_tasks()
        else:
            # 根据K线间隔设置定时任务间隔
            if interval_minutes is None:
                if self.interval == '15m':
                    interval_minutes = 15  # 15分钟K线每15分钟运行一次
                else:
                    interval_minutes = 1  # 1分钟K线每分钟运行一次
            
            interval_name = "15分钟" if self.interval == '15m' else "1分钟"
            print(f"定时任务已启动，每{interval_minutes}分钟运行一次（{interval_name}K线数据）...")
            schedule.every(interval_minutes).minutes.do(self.run_all_tasks)
            while True:
                schedule.run_pending()
                time.sleep(5)  # 5秒检查一次


# 如果当前文件是主程序，执行main函数
if __name__ == '__main__':
    # ========== 配置参数 ==========
    # 选择K线时间间隔：'1m' 表示1分钟K线，'15m' 表示15分钟K线
    KLINE_INTERVAL = '15m'  # 可以修改为 '1m' 或 '15m'

    # ========== 初始化 ==========
    # 根据选择的时间间隔初始化
    fc = GetMinKline(interval=KLINE_INTERVAL)
    # ========= 执行模式 ==========
    # 方式1：立即执行一次
    fc.start_scheduler(is_scheduler=False)
    
    # 方式2：启用定时任务（根据K线间隔自动设置定时间隔）
    # fc.start_scheduler(is_scheduler=True)
    
    # 方式3：启用定时任务并手动指定定时间隔（分钟）
    # fc.start_scheduler(is_scheduler=True, interval_minutes=15)
