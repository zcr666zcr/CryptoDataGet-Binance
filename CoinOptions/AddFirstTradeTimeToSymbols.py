import os
import time
import hmac
import hashlib
from urllib.parse import urlencode
from pathlib import Path

import requests
import pandas as pd
import pytz

from Settings import data_path, api_dic, OPTIONS_API_BASE_URL, TIMEZONE

# 调用节奏：每次API请求前固定等待0.8秒；收到过频(429/too many/over request)时暂停30秒后重试。


def _sign_and_get(endpoint: str, params: dict, timeout: int = 10):
    """
    调用币安期权API（需要签名认证）并返回JSON响应。
    endpoint: 例如 '/eapi/v1/historicalTrades'
    params: 请求参数，将自动加入 timestamp 与 signature
    """
    if not api_dic.get('api_key') or not api_dic.get('api_secret'):
        raise ValueError("期权API需要API密钥，请在 Settings.api_dic 中配置或通过环境变量提供")

    url = f"{OPTIONS_API_BASE_URL}{endpoint}"
    params = dict(params or {})
    params['timestamp'] = int(time.time() * 1000)

    query = urlencode(sorted(params.items()))
    signature = hmac.new(
        api_dic['api_secret'].encode('utf-8'),
        query.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    params['signature'] = signature

    headers = {
        'X-MBX-APIKEY': api_dic['api_key']
    }

    # 每次请求前固定间隔，避免触发限频
    time.sleep(0.8)
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        # 如果返回429或Too many requests，主动暂停再重试一次
        if resp.status_code == 429:
            print("接口返回429(请求过频)，暂停30秒后重试...")
            time.sleep(30)
            time.sleep(0.8)
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        msg = str(e)
        # 兼容用户口述的 overleafrequest/over request 文本，包含 too many/429 时暂停
        if ('429' in msg) or ('too many' in msg.lower()) or ('over' in msg.lower() and 'request' in msg.lower()):
            print("检测到请求过频，暂停30秒后重试...")
            time.sleep(30)
            time.sleep(0.8)
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        # 其它错误抛出给上层处理
        raise


def _get_historical_trades_simple(symbol: str, from_id: int = 0, limit: int = 500, timeout: int = 10):
    """
    使用客服提供的简单URL格式调用：
    https://eapi.binance.com/eapi/v1/historicalTrades?symbol={SYMBOL}&fromId={FROM_ID}&limit={LIMIT}

    只包含这三个查询参数，不附加 timestamp/signature。
    保留 0.8s 调用间隔与 429/过频时 30s 暂停后重试一次的机制。
    """
    if not symbol:
        raise ValueError("symbol 不能为空")

    url = f"{OPTIONS_API_BASE_URL}/eapi/v1/historicalTrades"
    params = {
        'symbol': symbol,
        'fromId': int(from_id),
        'limit': int(limit),
    }
    headers = {
        # 币安部分接口需要 API KEY 头；如果不需要也不会影响
        'X-MBX-APIKEY': api_dic.get('api_key', '')
    }

    # 每次请求前固定间隔，避免触发限频
    time.sleep(0.8)
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        if resp.status_code == 429:
            print("接口返回429(请求过频)，暂停30秒后重试...")
            time.sleep(30)
            time.sleep(0.8)
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        msg = str(e)
        if ('429' in msg) or ('too many' in msg.lower()) or ('over' in msg.lower() and 'request' in msg.lower()):
            print("检测到请求过频，暂停30秒后重试...")
            time.sleep(30)
            time.sleep(0.8)
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        raise


def get_first_trade_time(symbol: str):
    """
    单次请求 fromId=0, limit=500，取返回中 tradeId 最小记录的时间。
    如果本次无数据或异常，返回 None（不继续重试）。
    """
    try:
        batch = _get_historical_trades_simple(symbol=symbol, from_id=0, limit=500)
    except Exception as e:
        print(f"[{symbol}] 请求失败：{e}")
        return None

    if not batch:
        print(f"[{symbol}] 本次未返回成交数据")
        return None

    try:
        min_item = min(batch, key=lambda x: int(x['tradeId']))
        return int(min_item['time'])
    except Exception as e:
        print(f"[{symbol}] 批次解析异常：{e}")
        return None


def annotate_symbols_with_first_trade():
    tz = pytz.timezone(TIMEZONE)

    # 读取 symbol_pool 中最新的 options_*symbols.csv
    sp = data_path['symbol_pool']
    if not sp.exists():
        raise FileNotFoundError(f"symbol_pool 路径不存在：{sp}")

    files = list(sp.glob('options_*symbols.csv'))
    if not files:
        raise FileNotFoundError("未找到 options_*symbols.csv 文件，请先运行获取symbol列表的脚本")

    latest_csv = max(files, key=lambda p: p.stat().st_mtime)
    print(f"读取最新symbol列表：{latest_csv}")
    df = pd.read_csv(latest_csv)

    # 准备新列：首笔成交时间
    if 'first_trade_time' not in df.columns:
        df['first_trade_time'] = ''

    # 历史symbol筛选（is_history==1），如无该列则全部处理
    if 'is_history' in df.columns:
        mask = df['is_history'].fillna(0).astype(int) == 1
    else:
        mask = pd.Series([True] * len(df))

    symbols = df.loc[mask, 'symbol'].dropna().astype(str).tolist()
    print(f"历史symbol数量：{len(symbols)}（将为其写入首笔成交时间）")

    for idx, sym in enumerate(symbols, start=1):
        print(f"[{idx}/{len(symbols)}] 处理 {sym} ...")
        ms = get_first_trade_time(sym)
        if ms is None:
            print(f"[{sym}] 未获取到首笔成交时间，留空")
            continue
        dt = pd.to_datetime(ms, unit='ms', utc=True).tz_convert(tz).strftime('%Y-%m-%d %H:%M:%S')
        df.loc[df['symbol'] == sym, 'first_trade_time'] = dt

    # 保存到新文件（不覆盖原文件）
    out_path = latest_csv.with_name(latest_csv.stem + '_with_first_trade_time.csv')
    df.to_csv(out_path, index=False)
    print(f"写入完成：{out_path}")


if __name__ == '__main__':
    annotate_symbols_with_first_trade()