import requests
import pandas as pd
import pytz
from datetime import datetime, timezone
from pathlib import Path
import matplotlib.pyplot as plt
from Settings import HYPERLIQUID_FUNDING_CONFIG, TIMEZONE, CONFIG_PATHS


def _df_from_rows(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(TIMEZONE)
    if 'fundingRate' in df.columns:
        df['fundingRate'] = pd.to_numeric(df['fundingRate'], errors='coerce')
    if 'premium' in df.columns:
        df['premium'] = pd.to_numeric(df['premium'], errors='coerce')
    df = df.sort_values('time').reset_index(drop=True)
    return df


def fetch_hl_funding_all(coin: str, window_days: int, sleep_sec: float, start_epoch_ms: int) -> pd.DataFrame:
    url = HYPERLIQUID_FUNDING_CONFIG['base_url']
    tz_now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    window_ms = window_days * 24 * 60 * 60 * 1000
    all_rows: list[dict] = []
    max_per_request = 500
    intervals = []
    s = start_epoch_ms
    while s <= tz_now_ms:
        e = min(s + window_ms - 1, tz_now_ms)
        intervals.append((s, e))
        s = e + 1
    import time
    while intervals:
        start_ms, end_ms = intervals.pop()
        payload = {
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_ms,
            "endTime": end_ms
        }
        resp = requests.post(url, json=payload, timeout=15)
        data = resp.json() if resp.ok else []
        if isinstance(data, list) and data:
            all_rows.extend(data)
            if len(data) >= max_per_request and (end_ms - start_ms) > 24 * 60 * 60 * 1000:
                mid = start_ms + (end_ms - start_ms) // 2
                intervals.append((start_ms, mid))
                intervals.append((mid + 1, end_ms))
        if sleep_sec > 0:
            time.sleep(sleep_sec)
    df = _df_from_rows(all_rows)
    if not df.empty:
        df = df.drop_duplicates(subset=['time'])
    return df


def save_data(df: pd.DataFrame, coin: str, fmt: str = 'csv') -> Path:
    out_dir: Path = CONFIG_PATHS['output']
    out_dir.mkdir(parents=True, exist_ok=True)
    file = out_dir / f"{coin}_hyperliquid_funding_rate_history.{fmt}"
    df.to_csv(file, index=False, encoding='utf-8')
    return file


def plot_line(df: pd.DataFrame, coin: str) -> Path:
    out_dir: Path = CONFIG_PATHS['output']
    out_dir.mkdir(parents=True, exist_ok=True)
    png_file = out_dir / f"{coin}_hyperliquid_funding_rate_line.png"
    plt.figure(figsize=(10, 4))
    plt.plot(df['time'], df['fundingRate'])
    plt.title(f"{coin} Funding Rate (Hyperliquid)")
    plt.xlabel('Time')
    plt.ylabel('Funding Rate')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(png_file)
    plt.close()
    return png_file


def main():
    coin = HYPERLIQUID_FUNDING_CONFIG['default_coin']
    fmt = HYPERLIQUID_FUNDING_CONFIG['default_save_format']
    window_days = HYPERLIQUID_FUNDING_CONFIG['window_days']
    sleep_sec = HYPERLIQUID_FUNDING_CONFIG['sleep_sec']
    start_epoch_ms = HYPERLIQUID_FUNDING_CONFIG['start_epoch_ms']
    df = fetch_hl_funding_all(coin, window_days, sleep_sec, start_epoch_ms)
    if df.empty:
        print('未获取到数据')
        return
    csv_path = save_data(df, coin, fmt)
    png_path = plot_line(df, coin)
    print(str(csv_path))
    print(str(png_path))


if __name__ == '__main__':
    main()
