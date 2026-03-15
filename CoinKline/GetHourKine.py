# todo: 修改导入语句
# import okx.MarketData as MarketData # 注释掉或删除欧易相关导入
# import okx.PublicData as PublicData # 注释掉或删除欧易相关导入
from binance.spot import Spot as Client # todo: 导入币安Spot客户端

import pytz
import schedule
from tqdm import tqdm
import pandas as pd
import os
from datetime import datetime, timedelta
import gc
from Settings import *
import time
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

class GetHourKline():

    def __init__(self):
        """
        欧意交易所获取k线数据
        :param start_date: 开始时间
        :param end_date: 截止时间
        """
        # todo: 修改初始化API客户端
        # self.marketDataAPI = MarketData.MarketAPI(flag="0") # 注释掉或删除欧易相关初始化
        # self.publicDataAPI = PublicData.PublicAPI(flag="0") # 注释掉或删除欧易相关初始化

        self.client = Client()  # todo: 币安客户端初始化，这里不传API_KEY和SECRET_KEY

        self.tz = pytz.timezone('Asia/Hong_Kong')
        self.now = datetime.now()  # 今日时间
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
        # 获取已获取的日期
        file_list = os.listdir(data_path['result_path_hour'])
        file_list = [file[:10] for file in file_list]
        file_list.sort()

        # 若现有列表为空，按照默认起始时间
        is_null = True
        if len(file_list) == 0:
            start_date = '2017-08-16'  # todo:测试用
        else:
            start_date = file_list[-1]
            is_null = False

        return start_date, is_null

    # 定义一个函数，传入标的代码和日期，通过ccxt获取okx交易所的数据，返回该日的小时k线数据的df
    def get_kline(self, symbol, start_hour,end_hour, length=100):
        # 获取该日的起始时间戳
        start_time = self.tz.localize(start_hour)  # 时间不变，加上时区信息（香港时区）

        end_time = self.tz.localize(end_hour)  # 时间不变，加上时区信息（香港时区）


        start_time = int(start_time.timestamp() * 1000)
        end_time = int(end_time.timestamp() * 1000)
        klines_data = []  # 初始化 klines_data 为空列表，用于存储处理后的K线数据
        # 获取该日的k线数据
        while True:
            try:
                # todo: 修改API调用为币安的klines，interval='1h'
                klines_raw = self.client.klines(
                    symbol=symbol,
                    interval='1h',  # 小时线
                    startTime=start_time,
                    endTime=end_time,
                    limit=1000  # 币安K线接口最大limit通常为1000
                )
                # 调试：打印原始API响应
                # print(f"API 原始响应: {klines_raw}")

                # 正常返回但数据为空：不重试，直接退出循环
                # 币安返回的是列表，直接判断列表是否为空
                if not klines_raw:
                    klines_data = []  # 确保 klines_data 为空
                    break

                # 正常数据，取前12项 (币安K线返回12个字段，与您的中文列名数量匹配)
                klines_data = [line[:12] for line in klines_raw]
                break
            except:
                print('连接中断，暂停5s')
                time.sleep(5)


        # 将k线数据转换为一个pandas的DataFrame，设置列名和索引
        # todo: 使用中文列名
        df = pd.DataFrame(klines_data, columns=['开盘时间', '开盘价', '最高价', '最低价', '收盘价', '成交量', '收盘时间', '成交额', '成交笔数', '主动买入成交量', '主动买入成交额', '请忽略'])

        # 在进行后续处理前检查 DataFrame 是否为空
        if df.empty:
            # print(f"DEBUG: DataFrame for {symbol} is empty after initial DataFrame creation.")
            return df # 如果是空的，直接返回

        df['开盘时间'] = pd.to_datetime(df['开盘时间'], unit='ms', utc=True).dt.tz_convert(self.tz)
        df['收盘时间'] = pd.to_datetime(df['收盘时间'], unit='ms', utc=True).dt.tz_convert(self.tz)
        # todo: 添加日期列，用于后续按天分组保存，与GetHourKine保持一致

        df['开盘时间'] = df['开盘时间'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['收盘时间'] = df['收盘时间'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['货币对'] = symbol  # todo: 币安的symbol直接就是例如'BTCUSDT'，无需split

        return df

    # 定义一个函数，获取所有symbol的行情数据，并保存到配置文件（一天一个df.to_feather）
    def get_all_kline(self):
        date_start, is_null = self.get_date_start()

        symbols = pd.read_csv(data_path['symbol_pool'] / rf'{str(self.now)[:10]}symbols.csv')['symbol'].to_list()
        symbols = [symbol for symbol in symbols if symbol[-4:] == 'USDT' and symbol[-5:] != ':USDT']

        if is_null:
            hour_start = pd.to_datetime(date_start)
        else:
            last_feather_path = data_path['result_path_hour'] / rf'{date_start}.feather'
            df_last = pd.read_feather(last_feather_path)
            df_last["开盘时间"] = df_last["开盘时间"].astype(str)
            df_last["开盘时间"] = pd.to_datetime(df_last["开盘时间"])
            hour_start = (df_last["开盘时间"].max() + timedelta(hours=1)).tz_localize(None)
            del df_last
        hour_end = pd.to_datetime(self.dt_to_tackle)
        if hour_start > hour_end:
            print("无可更新小时级K线！")
            return

        hour_list = pd.date_range(hour_start, hour_end, freq='1h')
        hour_list = [hour_list[i:i + 900] for i in range(0, len(hour_list), 900)]

        total_batches = len(hour_list)
        for b_idx, hours in enumerate(hour_list, 1):
            df_batch = pd.DataFrame()
            for symbol in tqdm(symbols, desc=hours[0].strftime('%Y-%m-%d %H:%M') + " to " + hours[-1].strftime('%Y-%m-%d %H:%M')):
                kline_df = self.get_kline(symbol, hours[0], hours[-1], len(hours))
                if not kline_df.empty:
                    df_batch = pd.concat([df_batch, kline_df])

            if df_batch.empty:
                print(f"批次 {b_idx}/{total_batches} 无数据，跳过")
                continue

            df_batch['日期'] = df_batch['开盘时间'].apply(lambda x: str(x)[:10])
            dfgroup = df_batch.groupby('日期')
            for sub_date, sub_df in dfgroup:
                del sub_df['日期']
                sub_df['开盘时间'] = sub_df['开盘时间'].astype(str)
                sub_df = sub_df.sort_values(['开盘时间', '货币对']).reset_index(drop=True)
                file_path = data_path['result_path_hour'] / rf'{sub_date}.feather'
                if os.path.isfile(file_path):
                    try:
                        old_df = pd.read_feather(file_path)
                        combined = pd.concat([old_df, sub_df], ignore_index=True)
                        combined.drop_duplicates(subset=['开盘时间', '货币对'], keep='last', inplace=True)
                        combined = combined.sort_values(['开盘时间', '货币对']).reset_index(drop=True)
                        combined.to_feather(file_path)
                        del old_df, combined
                    except Exception:
                        sub_df.to_feather(file_path)
                else:
                    sub_df.reset_index(inplace=True, drop=True)
                    sub_df.to_feather(file_path)
                print(f'{sub_date} 小时K线数据已保存。')

            print(f'批次 {b_idx}/{total_batches} 已保存')
            del df_batch
            gc.collect()

    def run_all_tasks(self):
        print("=========任务执行，小时K线数据更新=========")
        self.now = datetime.now()  # 今日时间
        self.dt_to_tackle = str(self.now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1))

        self.get_symbols()
        self.get_all_kline()
        print("=========任务结束=========")

    def start_scheduler(self, is_scheduler=False):
        if not is_scheduler:
            self.run_all_tasks()
        else:
            print("定时任务已启动，每小时运行中...")
            schedule.every(1).hours.do(self.run_all_tasks)
            while True:
                schedule.run_pending()
                time.sleep(600)  # 10分钟检查一次


# 如果当前文件是主程序，执行main函数
if __name__ == '__main__':
    fc = GetHourKline()
    fc.start_scheduler()
