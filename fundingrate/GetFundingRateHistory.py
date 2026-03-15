# 资金费率历史数据下载与绘图脚本
# 功能：
# 1) 从币安 USDS-M 永续合约接口分页拉取全部历史资金费率数据
# 2) 保存为 CSV 文件
# 3) 生成资金费率折线图（fundingRate 随时间变化）
#
# 接口参考：/fapi/v1/fundingRate
#
import requests
import pandas as pd
import pytz
from datetime import datetime, timezone
from pathlib import Path
import matplotlib.pyplot as plt
from Settings import COINM_FUNDING_RATE_CONFIG, TIMEZONE, CONFIG_PATHS


def _ms(dt: datetime, tz: str) -> int:
    # 将本地时间（根据配置的时区）转换为毫秒时间戳
    tzinfo = pytz.timezone(tz)
    return int(tzinfo.localize(dt).timestamp() * 1000)


def fetch_funding_rate_history(symbol: str, start_time: datetime | None = None, end_time: datetime | None = None, limit: int = 1000) -> pd.DataFrame:
    # 获取指定时间范围内的一页资金费率数据（最多 limit 条）
    url = f"{COINM_FUNDING_RATE_CONFIG['base_url']}/dapi/v1/fundingRate"
    params = {'symbol': symbol, 'limit': limit}
    if start_time is not None:
        params['startTime'] = _ms(start_time, TIMEZONE)
    if end_time is not None:
        params['endTime'] = _ms(end_time, TIMEZONE)
    resp = requests.get(url, params=params, timeout=15)
    data = resp.json() if resp.ok else []
    if not isinstance(data, list):
        return pd.DataFrame()
    df = pd.DataFrame(data)
    if df.empty:
        return df
    # fundingTime 为毫秒时间戳，接口返回为 UTC，需要转换到配置时区便于观察
    df['fundingTime'] = pd.to_datetime(df['fundingTime'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(TIMEZONE)
    df['fundingRate'] = pd.to_numeric(df['fundingRate'], errors='coerce')
    df['markPrice'] = pd.to_numeric(df.get('markPrice', pd.Series(index=df.index)), errors='coerce')
    df = df.sort_values('fundingTime').reset_index(drop=True)
    return df


def fetch_funding_rate_all(symbol: str, limit: int = 1000, sleep_sec: float = 0.5) -> pd.DataFrame:
    # 分页拉取全部历史资金费率数据（窗口式正向分页）
    # 逻辑：从 startTime=0 开始，以固定窗口（如 180 天）逐段请求，
    # 每段返回不超过 limit 条数据，逐段累积直到当前时间。
    url = f"{COINM_FUNDING_RATE_CONFIG['base_url']}/dapi/v1/fundingRate"
    # Binance USDS-M 永续约在2019-2020后才有数据，这里从 2019-01-01 起步可减少空请求
    start_ms = int(datetime(2019, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    all_rows: list[dict] = []
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    window_ms = 180 * 24 * 60 * 60 * 1000  # 每页查询窗口：180天（约540条，低于limit=1000）

    while start_ms <= now_ms:
        end_ms = min(start_ms + window_ms - 1, now_ms)
        params = {'symbol': symbol, 'limit': limit, 'startTime': start_ms, 'endTime': end_ms}
        resp = requests.get(url, params=params, timeout=15)
        data = resp.json() if resp.ok else []
        if isinstance(data, list) and data:
            all_rows.extend(data)
        start_ms = end_ms + 1
        # 简单的节流，避免过快请求
        if sleep_sec > 0:
            import time
            time.sleep(sleep_sec)

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
    # 转换时间与数据类型
    df['fundingTime'] = pd.to_datetime(df['fundingTime'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(TIMEZONE)
    df['fundingRate'] = pd.to_numeric(df['fundingRate'], errors='coerce')
    if 'markPrice' in df.columns:
        df['markPrice'] = pd.to_numeric(df['markPrice'], errors='coerce')
    df = df.sort_values('fundingTime').reset_index(drop=True)
    return df


def save_data(df: pd.DataFrame, symbol: str, fmt: str = 'csv') -> Path:
    # 保存数据到输出目录
    out_dir: Path = CONFIG_PATHS['output']
    out_dir.mkdir(parents=True, exist_ok=True)
    file = out_dir / f"{symbol}_funding_rate_history.{fmt}"
    if fmt == 'csv':
        df.to_csv(file, index=False, encoding='utf-8')
    else:
        df.to_csv(file, index=False, encoding='utf-8')
    return file


def plot_line(df: pd.DataFrame, symbol: str) -> Path:
    # 绘制资金费率随时间的折线图
    out_dir: Path = CONFIG_PATHS['output']
    out_dir.mkdir(parents=True, exist_ok=True)
    png_file = out_dir / f"{symbol}_funding_rate_line.png"
    plt.figure(figsize=(10, 4))
    plt.plot(df['fundingTime'], df['fundingRate'])
    plt.title(f"{symbol} Funding Rate")
    plt.xlabel('Time')
    plt.ylabel('Funding Rate')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(png_file)
    plt.close()
    return png_file


def main():
    # 主流程：拉取全部历史数据、保存 CSV、生成折线图
    symbol = COINM_FUNDING_RATE_CONFIG['default_symbol']
    fmt = COINM_FUNDING_RATE_CONFIG['default_save_format']
    df = fetch_funding_rate_all(symbol=symbol, limit=COINM_FUNDING_RATE_CONFIG['default_limit'])
    if df.empty:
        print('未获取到资金费率数据')
        return
    csv_path = save_data(df, symbol, fmt)
    png_path = plot_line(df, symbol)
    print(str(csv_path))
    print(str(png_path))


if __name__ == '__main__':
    main()
