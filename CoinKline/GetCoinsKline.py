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
from Settings import *
import time
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

class GetCoinsKline():

    def __init__(self, choice):
        """
        欧意交易所获取k线数据
        :param start_date: 开始时间
        :param end_date: 截止时间
        """
        # todo: 修改初始化API客户端
        # self.marketDataAPI = MarketData.MarketAPI(flag="0", debug=False) # 注释掉或删除欧易相关初始化
        # self.publicDataAPI = PublicData.PublicAPI(flag="0", debug=False) # 注释掉或删除欧易相关初始化

        # 币安API初始化，获取行情数据通常不需要API密钥，但如果后续有需要（如用户数据），可以传入
        self.client = Client()  # todo: 币安客户端初始化，这里不传API_KEY和SECRET_KEY
        # 如果get_kline需要认证，则需要这样初始化:
        # self.client = Client(api_dic['apikey'], api_dic['secretkey'])

        self.exchange = choice
        self.tz = pytz.timezone('Asia/Hong_Kong')
        self.now = datetime.now()  # 今日时间
        if self.now.hour >= 8:
            # 如果是，获取昨天的日期
            self.date_to_tackle = str(self.now.date() - timedelta(days=1))[:10]
        else:
            # 如果不是，获取前天的日期
            self.date_to_tackle = str(self.now.date() - timedelta(days=2))[:10]

    # 定义一个函数，获取欧易交易所可交易的所有币种，并保存到配置文件
    def get_symbols(self):
        #判断是否已存在
        if os.path.isfile(data_path['symbol_pool'] / rf'{str(self.now)[:10]}symbols.csv'):
            print('已存在可交易的币种列表')
            # symbols = self.okx.fetch_markets()
            # print(symbols)

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

    # 获取需要获取的日期列表,[start_date, self.date_to_tackle]
    def get_date_list(self):

        # 获取已获取的日期
        file_list = os.listdir(data_path['result_path_daily'])
        file_list = [file[:10] for file in file_list]
        file_list.sort()

        # 若现有列表为空，按照默认起始时间
        if len(file_list) == 0:
            start_date = '2017-08-16'
        else:
            start_date = file_list[-1]

        #获取期间日期，用已有的最后一天为开始时间，当天为截止时间
        date_list = []
        start_date = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=1)
        end_date = datetime.strptime(self.date_to_tackle, '%Y-%m-%d')

        while start_date <= end_date:
            date_list.append(start_date.strftime('%Y-%m-%d'))
            start_date += timedelta(days=1)

        #筛除已获取的日期
        # date_list = [date for date in date_list if date not in file_list]
        print('已获取date_list')
        return date_list

    # 定义一个函数，传入标的代码和日期，通过ccxt获取okx交易所的数据，返回该日的日度k线数据的df
    def get_kline(self, symbol, date,end_date, length):
        # print(f"尝试获取 {symbol} 从 {date} 到 {end_date} 的 K 线数据，限制数量: {length}")
        # 获取该日的起始时间戳，单位为毫秒
        start_time = pd.to_datetime(date) + timedelta(hours=8)
        start_time = self.tz.localize(start_time)  # 时间不变，加上时区信息（香港时区）
        start_time = int(start_time.timestamp() * 1000)

        end_time = pd.to_datetime(end_date) + timedelta(hours=8)
        end_time = self.tz.localize(end_time)  # 时间不变，加上时区信息（香港时区）
        end_time = int(end_time.timestamp() * 1000)
        klines_data = []  # 初始化 klines_data 为空列表，用于存储处理后的K线数据
        # 获取该日的k线数据
        while True:
            try:
                # todo: 修改API调用为币安的klines
                # 币安的klines函数需要interval参数，通常是'1d'表示日线
                # limit 通常设置为1000以获取尽可能多的数据
                # print(f"API 请求参数: symbol={symbol}, interval='1d', startTime={start_time}, endTime={end_time}, limit=1000")
                klines_raw = self.client.klines(  # 使用临时变量接收原始响应
                    symbol=symbol,
                    interval='1d',  # 日线
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
                klines_data = [line[:12] for line in klines_raw]  # 直接处理原始列表
                break


            except Exception as e:  # 捕获具体的异常，打印错误信息
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
        df['货币对'] = symbol  # todo: 币安的symbol直接就是例如'BTCUSDT'，无需split

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
        print(len(date_list))
        symbols = pd.read_csv(data_path['symbol_pool'] / rf'{str(self.now)[:10]}symbols.csv')['symbol'].to_list()

        # 筛选仅以usdt结尾的
        symbols = [symbol for symbol in symbols if symbol[-4:] == 'USDT' and symbol[-5:] != ':USDT']


        # 循环取数据
        for date in date_list:  # todo: 要看一下date_list的长度再切片
            df_date = pd.DataFrame()
            for symbol in tqdm(symbols, desc=date[0] + ' to ' + date[-1]):
                # 调用get_kline函数，获取单个symbol的K线行情数据，并赋值给kline_df
                kline_df = self.get_kline(symbol, date[0],date[-1], len(date))
                # ----------- 添加以下调试代码 -----------
                # if not kline_df.empty:
                #     print(f"为 {symbol} 获取的 kline_df 列名: {kline_df.columns.tolist()}")
                # else:
                #     print(f"为 {symbol} 获取的 kline_df 为空。")
                # ----------------------------------------
                if not kline_df.empty:
                    df_date = pd.concat([df_date, kline_df])

            ## 每一天分别保存
            df_date['日期'] = df_date['开盘时间'].apply(lambda x: str(x)[:10])
            dfgroup = df_date.groupby('日期')

            for sub_date, sub_df in dfgroup:
                ## 加回时间
                del sub_df['日期']
                sub_df['开盘时间'] = sub_df['开盘时间'].astype(str)

                sub_df.reset_index(inplace=True, drop=True)
                ## 保存结果
                sub_df.to_feather(data_path['result_path_daily'] / rf'{str(sub_date)[:10]}.feather')
                print(f'{sub_date}已保存')

    # 设置每天早上8点执行
    def run_all_tasks(self):
        print("=========任务执行，日线数据更新=========")
        self.now = datetime.now()  # 今日时间
        if self.now.hour >= 8:
            # 如果是，获取昨天的日期
            self.date_to_tackle = str(self.now.date() - timedelta(days=1))[:10]
        else:
            # 如果不是，获取前天的日期
            self.date_to_tackle = str(self.now.date() - timedelta(days=2))[:10]

        self.get_symbols()
        self.get_all_kline()
        print("=========任务结束=========")

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
    #更新欧易数据
    choice = {0: "U本位", 1: "U本位&币本位"}  # todo:后面有区分吗
    fc = GetCoinsKline(choice[0])  #
    fc.start_scheduler(is_scheduler=False)  # {Ture: 定时运行，False：即刻运行}，默认为False
