import requests
import pytz
from tqdm import tqdm
import pandas as pd
import os
from datetime import datetime, timedelta
import gc
from Settings import *
import time


class GetPerpetualHourKline:
    def __init__(self, pairs=None):
        self.pairs = pairs or ['BTCUSDT', 'ETHUSDT']
        self.tz = pytz.timezone(TIMEZONE)
        self.now = datetime.now()
        self.dt_to_tackle = str(self.now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1))

        data_path['result_path_perpetual_hour'].mkdir(parents=True, exist_ok=True)

    def get_pairs(self):
        return list(self.pairs)

    def _get_pair_last_time(self, pair: str):
        files = sorted(data_path['result_path_perpetual_hour'].glob('*.feather'))
        if not files:
            return None
        last_ts = None
        for fp in files:
            try:
                df = pd.read_feather(fp, columns=['开盘时间', '货币对'])
                if '货币对' in df.columns and '开盘时间' in df.columns:
                    sub = df[df['货币对'] == pair]
                    if not sub.empty:
                        ts = pd.to_datetime(sub['开盘时间'])
                        m = ts.max()
                        if pd.notna(m):
                            if last_ts is None or m > last_ts:
                                last_ts = m
            except Exception:
                continue
        return None if last_ts is None else last_ts.tz_localize(None)

    def get_kline(self, pair, start_hour, end_hour, length=100):
        start_time = self.tz.localize(start_hour)
        end_time = self.tz.localize(end_hour)
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        url = f"{FAPI_BASE_URL}/fapi/v1/continuousKlines"
        params = {
            'pair': pair,
            'contractType': 'PERPETUAL',
            'interval': '1h',
            'startTime': start_ms,
            'endTime': end_ms,
            'limit': 1000
        }
        klines_data = []
        while True:
            try:
                resp = requests.get(url, params=params, timeout=15)
                raw = resp.json() if resp.ok else []
                if not raw:
                    klines_data = []
                    break
                klines_data = [line[:12] for line in raw]
                break
            except:
                time.sleep(5)

        df = pd.DataFrame(klines_data, columns=['开盘时间', '开盘价', '最高价', '最低价', '收盘价', '成交量', '收盘时间', '成交额', '成交笔数', '主动买入成交量', '主动买入成交额', '请忽略'])
        if df.empty:
            return df
        df['开盘时间'] = pd.to_datetime(df['开盘时间'], unit='ms', utc=True).dt.tz_convert(self.tz)
        df['收盘时间'] = pd.to_datetime(df['收盘时间'], unit='ms', utc=True).dt.tz_convert(self.tz)
        df['开盘时间'] = df['开盘时间'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['收盘时间'] = df['收盘时间'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['货币对'] = pair
        return df

    def get_all_kline(self):
        pairs = self.get_pairs()
        hour_end = pd.to_datetime(self.dt_to_tackle)
        for pair in pairs:
            last_ts = self._get_pair_last_time(pair)
            if last_ts is None:
                hour_start = pd.to_datetime('2020-08-11')
            else:
                hour_start = (last_ts + timedelta(hours=1)).tz_localize(None)
            if hour_start > hour_end:
                continue
            hour_list = pd.date_range(hour_start, hour_end, freq='1h')
            hour_list = [hour_list[i:i + 900] for i in range(0, len(hour_list), 900)]
            for hours in tqdm(hour_list, desc=f'{pair} 批次更新'):
                df_batch = pd.DataFrame()
                kline_df = self.get_kline(pair, hours[0], hours[-1], len(hours))
                if not kline_df.empty:
                    df_batch = pd.concat([df_batch, kline_df])
                if df_batch.empty:
                    continue
                df_batch['日期'] = df_batch['开盘时间'].apply(lambda x: str(x)[:10])
                dfgroup = df_batch.groupby('日期')
                for sub_date, sub_df in dfgroup:
                    del sub_df['日期']
                    sub_df['开盘时间'] = sub_df['开盘时间'].astype(str)
                    sub_df = sub_df.sort_values(['开盘时间', '货币对']).reset_index(drop=True)
                    file_path = data_path['result_path_perpetual_hour'] / rf'{sub_date}.feather'
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
                del df_batch
                gc.collect()


if __name__ == '__main__':
    fc = GetPerpetualHourKline(pairs=['BTCUSDT', 'ETHUSDT'])
    fc.get_all_kline()
