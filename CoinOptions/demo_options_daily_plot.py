import requests
import pandas as pd
import pytz
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from Settings import OPTIONS_API_BASE_URL, TIMEZONE

def _parse_expiry(symbol: str):
    parts = symbol.split('-')
    if len(parts) >= 2 and len(parts[1]) == 6:
        yy = int(parts[1][:2])
        year = 2000 + yy
        month = int(parts[1][2:4])
        day = int(parts[1][4:6])
        return datetime(year, month, day).date()
    return None

def _fetch_klines(symbol: str, start_date: datetime.date, end_date: datetime.date):
    tz = pytz.timezone(TIMEZONE)
    start_time = tz.localize(datetime.combine(start_date, datetime.min.time()) + timedelta(hours=8))
    end_time = tz.localize(datetime.combine(end_date, datetime.min.time()) + timedelta(hours=8))
    params = {
        'symbol': symbol,
        'interval': '1d',
        'startTime': int(start_time.timestamp() * 1000),
        'endTime': int(end_time.timestamp() * 1000),
        'limit': 1500
    }
    url = f"{OPTIONS_API_BASE_URL}/eapi/v1/klines"
    resp = requests.get(url, params=params, timeout=10)
    try:
        data = resp.json()
    except Exception:
        data = []
    if isinstance(data, dict) and 'code' in data and symbol.startswith('BNB-'):
        alt = 'BNBUSDT-' + symbol.split('-', 1)[1]
        params['symbol'] = alt
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        symbol = alt
    rows = []
    for item in data:
        rows.append([
            item['openTime'],
            item['open'],
            item['high'],
            item['low'],
            item['close'],
            item['amount'],
            item['closeTime'],
            item['volume'],
            item['tradeCount'],
            item['takerAmount'],
            item['takerVolume']
        ])
    df = pd.DataFrame(rows, columns=['开盘时间', '开盘价', '最高价', '最低价', '收盘价', '成交量', '收盘时间', '成交额', '成交笔数', '主动买入成交量', '主动买入成交额'])
    if not df.empty:
        df['开盘时间'] = pd.to_datetime(df['开盘时间'], unit='ms')
        df['收盘价'] = pd.to_numeric(df['收盘价'], errors='coerce')
    return df, symbol

def main():
    input_symbol = 'BNB-251114-890-P'
    start_date = datetime(2025, 11, 1).date()
    end_date = datetime(2025, 11, 12).date()
    df, used_symbol = _fetch_klines(input_symbol, start_date, end_date)
    if df.empty:
        print('未获取到K线数据')
        return
    plt.figure(figsize=(10, 4))
    plt.plot(df['开盘时间'], df['收盘价'])
    plt.title(f'{used_symbol} 收盘价')
    plt.xlabel('日期')
    plt.ylabel('价格')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    main()