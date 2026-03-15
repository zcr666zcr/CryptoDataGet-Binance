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
           'api_key': 'nglzGY8LxLP8AGMRn6nPJcsiY26tfqm6jK5CWs7NNfZd2g46VfKVVlEoj4Bo7M3d',
           'api_secret':'RDiiMsLa4WfPPSPBT4B5kyOshIjLMbR9Pyh0ioNxaokoxHDUakfpwYb1G9esbThp'
}

# ========== 数据路径配置 ==========
data_path = {
    'symbol_pool': Path(rf'E:\Quant\data\symbols'),
    'result_path_daily': Path(rf'E:\Quant\data\CoinOptions\期权行情数据1d'),
    'result_path_min': Path(rf'E:\Quant\data\CoinOptions\期权行情数据1min'),
    'result_path_hour': Path(rf'E:\Quant\data\CoinOptions\期权行情数据1h'),
    'result_path_historical_trades': Path(rf'E:\Quant\data\CoinOptions\期权历史成交数据')
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
# 默认交易对（期权格式示例：BTCUSDT-20251107-50000-C，实际使用时需要从期权 symbol 列表获取）
DEFAULT_SYMBOL = 'BTC-251109-105000-C'
# 币安期权 API 基础 URL（根据官方文档：/eapi/v1/klines）
OPTIONS_API_BASE_URL = 'https://eapi.binance.com'

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
# 注意：历史成交数据目前只支持单个期权合约
# 期权合约格式：BTC-YYMMDD-行权价-C/P（C=看涨，P=看跌）
# 示例：BTC-251109-105000-C 表示BTC看涨期权，到期日2025-11-09，行权价105000
# 请根据实际情况修改为当前活跃的期权合约
HISTORICAL_TRADES_CONFIG = {
    'default_limit': 500,  # 默认返回数量，最大500（根据币安期权API文档）
    'default_symbol': 'BTC-251226-115000-C',  # 固定的期权合约symbol，请根据实际情况修改
    'default_save_format': DEFAULT_SAVE_FORMAT,
    'request_interval': 0.2,  # 分页请求时的间隔时间（秒），避免请求过快（币安API限制：每分钟最多400次，即每0.15秒1次，设置为0.2秒留出安全余量）
    'max_requests': 1000000,  # 按时间范围下载时的最大请求次数，防止无限循环
    'max_requests_per_batch': 1000000,  # 按fromId下载时的最大请求次数，防止无限循环
    'default_from_id': 0,  # 默认起始成交ID，如果为None则从最早的可用ID开始
    'batch_symbol_interval': 2.0,  # 批量下载时，每个symbol之间的延迟时间（秒），避免请求过快
    'binary_search_interval': 0.8,  # 二分查找时的额外请求间隔时间（秒），在api_call_min_interval基础上额外延迟，因为二分查找可能快速连续调用多次
    'api_call_min_interval': 0.25,  # API调用的最小间隔时间（秒），在_call_options_api方法中使用，确保所有请求都有延迟（400次/分钟=0.15秒/次，设置为0.25秒更安全，每分钟最多240次）
}