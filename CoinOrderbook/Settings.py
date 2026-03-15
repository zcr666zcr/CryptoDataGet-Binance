"""
市场数据获取配置文件
包含API密钥、数据路径以及各种数据获取的配置参数
"""
from pathlib import Path

# ========== 项目根目录 ==========
# 获取当前文件所在目录作为项目根目录
PROJECT_ROOT = Path(__file__).parent

# ========== API配置 ==========
api_dic = {

}

# ========== 数据路径配置 ==========
data_path = {
    'symbol_pool': Path(rf'E:\Quant\data\symbols'),
    'result_path_daily': Path(rf'E:\Quant\data\CoinKline\现货行情数据1d'),
    'result_path_min': Path(rf'E:\Quant\data\CoinKline\现货行情数据1min'),
    'result_path_hour': Path(rf'E:\Quant\data\CoinKline\现货行情数据1h'),
    'result_path_depth': Path(rf'E:\Quant\data\CoinOrderbook\现货深度数据'),
    'result_path_trades': Path(rf'E:\Quant\data\CoinOrderbook\现货成交数据'),
    'result_path_aggtrades': Path(rf'E:\Quant\data\CoinOrderbook\现货成交归集数据'),
    'result_path_historical_trades': Path(rf'E:\Quant\data\CoinOrderbook\现货历史成交数据'),
    'result_path_daily_trade_count': Path(rf'E:\Quant\data\CoinOrderbook\现货交易笔数统计')
}

# ========== 运行时文件目录配置 ==========
# config 目录用于存放程序运行所需和生成的文件（如 xlsx 文件、运行日志等）
CONFIG_DIR = PROJECT_ROOT / 'config'

# 确保 config 目录存在
CONFIG_DIR.mkdir(exist_ok=True)

# config 目录下的子目录
CONFIG_PATHS = {
    'logs': CONFIG_DIR / 'logs',  # 日志文件目录
    'runtime': CONFIG_DIR / 'runtime',  # 运行时临时文件目录
    'data': CONFIG_DIR / 'data',  # 运行时数据文件目录（如 xlsx 等）
    'output': CONFIG_DIR / 'output',  # 程序输出文件目录
}

# 确保所有子目录存在
for sub_dir in CONFIG_PATHS.values():
    sub_dir.mkdir(parents=True, exist_ok=True)

# ========== 通用配置 ==========
# 默认交易对
DEFAULT_SYMBOL = 'BTCUSDT'

# 时区设置
TIMEZONE = 'Asia/Hong_Kong'

# 默认保存格式：'feather' 或 'csv'
DEFAULT_SAVE_FORMAT = 'feather'

# API请求重试配置
RETRY_SLEEP_TIME = 10  # 请求失败后重试等待时间（秒）

# ========== 深度数据配置 (GetDepthData) ==========
DEPTH_CONFIG = {
    'default_limit': 5000,  # 默认深度数量，可选值：5, 10, 20, 50, 100, 500, 1000, 5000
    'default_symbol': DEFAULT_SYMBOL,
    'default_save_format': DEFAULT_SAVE_FORMAT,
}

# ========== 成交数据配置 (GetTradesData) ==========
TRADES_CONFIG = {
    'default_limit': 1000,  # 默认返回数量，最大1000
    'default_symbol': DEFAULT_SYMBOL,
    'default_save_format': DEFAULT_SAVE_FORMAT,
    'request_interval': 0.2,  # 分页请求时的间隔时间（秒），避免请求过快
    'max_requests': 1000000,  # 按时间范围下载时的最大请求次数，防止无限循环
}

# ========== 成交归集数据配置 (GetAggTradesData) ==========
AGG_TRADES_CONFIG = {
    'default_limit': 1000,  # 默认返回数量，最大1000
    'default_symbol': DEFAULT_SYMBOL,
    'default_save_format': DEFAULT_SAVE_FORMAT,
    'request_interval': 0.2,  # 分页请求时的间隔时间（秒），避免请求过快
    'max_requests': 1000000,  # 按时间范围下载时的最大请求次数，防止无限循环
}

# ========== 历史成交数据配置 (GetHistoricalTradesData) ==========
HISTORICAL_TRADES_CONFIG = {
    'default_limit': 1000,  # 默认返回数量，最大1000
    'default_symbol': DEFAULT_SYMBOL,
    'default_save_format': DEFAULT_SAVE_FORMAT,
    'request_interval': 0.2,  # 分页请求时的间隔时间（秒），避免请求过快
    'max_requests': 1000000,  # 按时间范围下载时的最大请求次数，防止无限循环
    'max_requests_per_batch': 1000000,  # 按fromId下载时的最大请求次数，防止无限循环
    'default_from_id': 0,  # 默认起始成交ID，如果为None则从最早的可用ID开始
}