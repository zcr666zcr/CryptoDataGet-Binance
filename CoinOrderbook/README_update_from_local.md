# 历史成交数据工具模块使用说明

## 目录

1. [功能概述](#功能概述)
2. [从本地数据续传](#从本地数据续传)
3. [feather_utils.py 工具模块](#feather_utilspy-工具模块)
4. [csv_to_feather_utils.py 工具模块](#csv_to_feather_utilspy-工具模块)
5. [使用场景](#使用场景)
6. [注意事项](#注意事项)

---

## 功能概述

本工具集提供了完整的历史成交数据处理功能，包括：

- **数据续传**：从本地已有数据继续获取最新数据
- **文件管理**：查找、读取、合并 feather 文件
- **格式转换**：将币安官网下载的 CSV 文件转换为 feather 格式
- **数据重组**：按天重新分割 feather 文件
- **数据对比**：比较不同来源的数据一致性

---

## 从本地数据续传

### 功能说明

`update_from_local()` 方法可以从本地已获取的历史数据中找到最大的成交ID（fromId），然后从该ID开始获取最新数据，并将新旧数据合并保存。

### 工作流程

1. **读取本地数据**：自动查找并读取指定交易对的所有 feather 文件
2. **查找最大ID**：从本地数据中找到最大的成交ID（fromId）
3. **获取新数据**：从最大ID+1开始，获取最新数据直到最新
4. **合并数据**：将新旧数据合并，自动去重
5. **保存数据**：保存合并后的数据为新的 feather 文件

### 使用方法

#### 基本使用

```python
from CoinOrderbook.GetHistoricalTradesData import GetHistoricalTradesData

# 初始化
ghtd = GetHistoricalTradesData(symbol='BTCUSDT')

# 从本地数据续传
df = ghtd.update_from_local()
```

#### 自定义参数

```python
# 自定义参数
df = ghtd.update_from_local(
    limit=1000,              # 每次请求返回数量，最大1000
    save_format='feather',    # 保存格式：'feather' 或 'csv'
    max_requests=100          # 最大请求次数，防止无限循环
)
```

---

## feather_utils.py 工具模块

### 模块概述

`feather_utils.py` 提供了完整的 feather 文件操作功能，包括文件查找、读取、合并、保存和数据重组等。

### 核心函数

#### 1. 文件查找和读取

##### `find_feather_files(data_dir, symbol)`

查找指定交易对的所有 feather 文件。

**参数：**
- `data_dir` (Path): 数据目录路径
- `symbol` (str): 交易对名称，如 'BTCUSDT'

**返回：**
- `List[Path]`: feather 文件路径列表，按文件名排序

**示例：**
```python
from pathlib import Path
from CoinOrderbook.feather_utils import find_feather_files

data_dir = Path("path/to/data")
files = find_feather_files(data_dir, "BTCUSDT")
print(f"找到 {len(files)} 个文件")
```

##### `read_all_feather_files(data_dir, symbol)`

读取指定交易对的所有 feather 文件并合并为一个 DataFrame。

**参数：**
- `data_dir` (Path): 数据目录路径
- `symbol` (str): 交易对名称，如 'BTCUSDT'

**返回：**
- `pd.DataFrame`: 合并后的 DataFrame，自动去重并按成交ID排序

**示例：**
```python
from pathlib import Path
from CoinOrderbook.feather_utils import read_all_feather_files

data_dir = Path("path/to/data")
df = read_all_feather_files(data_dir, "BTCUSDT")
print(f"合并后共 {len(df)} 条记录")
```

#### 2. 数据ID管理

##### `get_max_from_id(data_dir, symbol)`

从本地 feather 文件中获取最大的成交ID（fromId）。优化版本：只读取文件名中结束时间最大的文件，避免读取和合并所有文件。

**参数：**
- `data_dir` (Path): 数据目录路径
- `symbol` (str): 交易对名称，如 'BTCUSDT'

**返回：**
- `Optional[int]`: 最大的成交ID，如果没有找到文件则返回 None

**示例：**
```python
from pathlib import Path
from CoinOrderbook.feather_utils import get_max_from_id

data_dir = Path("path/to/data")
max_id = get_max_from_id(data_dir, "BTCUSDT")
if max_id:
    print(f"本地数据中最大的成交ID: {max_id}")
    # 从 max_id + 1 开始获取新数据
```

#### 3. 数据合并和保存

##### `merge_and_save_feather(old_df, new_df, save_path, remove_duplicates=True)`

合并新旧 DataFrame 并保存为 feather 文件。

**参数：**
- `old_df` (pd.DataFrame): 旧的 DataFrame（本地已有数据）
- `new_df` (pd.DataFrame): 新的 DataFrame（刚获取的数据）
- `save_path` (Path): 保存路径
- `remove_duplicates` (bool): 是否去重，默认 True

**返回：**
- `pd.DataFrame`: 合并后的 DataFrame

**示例：**
```python
from pathlib import Path
from CoinOrderbook.feather_utils import merge_and_save_feather

save_path = Path("path/to/save/merged.feather")
merged_df = merge_and_save_feather(old_df, new_df, save_path)
```

##### `save_new_data_only(data_dir, symbol, new_df, save_format='feather', tz=None)`

只保存新数据，不合并现有文件，不删除旧文件。文件名使用实际数据的时间范围。

**参数：**
- `data_dir` (Path): 数据目录路径
- `symbol` (str): 交易对名称，如 'BTCUSDT'
- `new_df` (pd.DataFrame): 新下载的数据DataFrame
- `save_format` (str): 保存格式，'feather' 或 'csv'
- `tz`: 时区对象，用于时间格式化

**返回：**
- `Optional[Path]`: 保存的文件路径，如果数据为空则返回None

**示例：**
```python
from pathlib import Path
import pytz
from CoinOrderbook.feather_utils import save_new_data_only

data_dir = Path("path/to/data")
tz = pytz.timezone('Asia/Hong_Kong')
file_path = save_new_data_only(data_dir, "BTCUSDT", new_df, tz=tz)
```

##### `merge_with_existing_and_save(data_dir, symbol, new_df, save_format='feather', tz=None, delete_old_files=True)`

合并现有feather文件和新数据，去重后保存，文件名使用实际数据的时间范围。

**参数：**
- `data_dir` (Path): 数据目录路径
- `symbol` (str): 交易对名称，如 'BTCUSDT'
- `new_df` (pd.DataFrame): 新下载的数据DataFrame
- `save_format` (str): 保存格式，'feather' 或 'csv'
- `tz`: 时区对象，用于时间格式化
- `delete_old_files` (bool): 是否删除旧的feather文件（合并后），默认True

**返回：**
- `Optional[Path]`: 保存的文件路径，如果数据为空则返回None

**注意：** 此函数保留用于以后需要合并时使用，下载数据时请使用 `save_new_data_only`。

#### 4. 文件信息获取

##### `get_latest_feather_file(data_dir, symbol)`

获取最新的 feather 文件路径（按修改时间）。

**参数：**
- `data_dir` (Path): 数据目录路径
- `symbol` (str): 交易对名称，如 'BTCUSDT'

**返回：**
- `Optional[Path]`: 最新的文件路径，如果没有找到则返回 None

##### `get_data_time_range(df, tz=None)`

从DataFrame中获取实际的时间范围。

**参数：**
- `df` (pd.DataFrame): 历史成交数据DataFrame
- `tz`: 时区对象，如果为None则尝试从DataFrame推断

**返回：**
- `Optional[Tuple[datetime, datetime]]`: (start_time, end_time) 元组，如果DataFrame为空则返回None

#### 5. 数据重组（按天分割）

##### `reorganize_feather_files_by_day(data_dir, symbol, tz=None, delete_old_files=True)`

对目标路径文件夹内的所有feather文件进行重新分割，以一天（0点0分0秒到23点59分59秒999毫秒）为一个文件储存交易记录的跨度。

**功能特点：**
- 智能检查：在开始分割前先读取文件名，如果时间跨度已经符合要求（同一天，从00:00:00到23:59:59）就无需重排
- 自动命名：
  - 完整的一天：使用标准格式 `{symbol}_historical_trades_{YYYYMMDD}_000000_to_{YYYYMMDD}_235959.feather`
  - 不完整的一天：使用实际时间范围，如 `{symbol}_historical_trades_{YYYYMMDD}_{HHMMSS}_to_{YYYYMMDD}_{HHMMSS}.feather`
- 自动去重：合并时会自动基于成交ID去重
- 智能跳过：已按天分割的文件会被自动跳过

**参数：**
- `data_dir` (Path): 数据目录路径
- `symbol` (str): 交易对名称，如 'BTCUSDT'
- `tz`: 时区对象，用于时间处理
- `delete_old_files` (bool): 是否删除旧的feather文件（重新分割后），默认True

**返回：**
- `List[Path]`: 新创建的文件路径列表（包括已按天分割的文件）

**示例：**
```python
from pathlib import Path
import pytz
from CoinOrderbook.feather_utils import reorganize_feather_files_by_day

data_dir = Path("path/to/data")
symbol = "BTCUSDT"
tz = pytz.timezone('Asia/Hong_Kong')

# 重新分割文件
new_files = reorganize_feather_files_by_day(
    data_dir=data_dir,
    symbol=symbol,
    tz=tz,
    delete_old_files=True
)

print(f"共处理 {len(new_files)} 个文件")
```

**使用场景：**
- 将多个时间跨度的文件重新组织为按天分割的文件
- 统一文件命名格式，便于管理和查询
- 优化数据存储结构

---

## csv_to_feather_utils.py 工具模块

### 模块概述

`csv_to_feather_utils.py` 提供了将币安官网下载的 CSV 文件转换为 feather 格式的功能，并确保数据格式与 API 下载的数据完全一致。

### 核心函数

#### 1. CSV 转 Feather

##### `convert_csv_to_feather(csv_folder=None, output_folder=None, symbol=None, tz_str=None, delete_csv=False)`

将文件夹中的所有CSV文件转换为feather格式。

**功能特点：**
- 自动识别交易对：从CSV文件名中提取交易对符号（格式：`BTCUSDT-trades-2025-11-03.csv`）
- 格式一致性：完全按照API下载代码的方式处理数据，确保格式一致
- 自动命名：根据数据的时间范围生成文件名
- 批量处理：支持一次性转换文件夹中的所有CSV文件

**参数：**
- `csv_folder` (Optional[Path]): CSV文件所在的文件夹路径，如果为None则使用Settings中的`result_path_historical_trades/csv`
- `output_folder` (Optional[Path]): 输出feather文件的文件夹路径，如果为None则使用base_folder（即与CSV的父目录）
- `symbol` (Optional[str]): 交易对名称（如'BTCUSDT'），如果为None则从CSV文件名中提取
- `tz_str` (Optional[str]): 时区字符串，如果为None则使用Settings中的TIMEZONE
- `delete_csv` (bool): 转换成功后是否删除原始CSV文件，默认False

**返回：**
- `List[Path]`: 转换后的feather文件路径列表

**示例：**
```python
from pathlib import Path
from CoinOrderbook.csv_to_feather_utils import convert_csv_to_feather

# 使用默认路径（Settings中的配置）
converted_files = convert_csv_to_feather()

# 或指定自定义路径
csv_folder = Path("path/to/csv/files")
output_folder = Path("path/to/output")
converted_files = convert_csv_to_feather(
    csv_folder=csv_folder,
    output_folder=output_folder,
    delete_csv=False  # 保留原始CSV文件
)

print(f"共转换 {len(converted_files)} 个文件")
```

**数据格式说明：**

币安官网下载的CSV文件格式（无列名，第一行就是数据）：
- `id`: 成交ID
- `price`: 成交价
- `qty`: 成交量
- `quoteQty`: 成交额
- `time`: 时间戳（毫秒或微秒）
- `isBuyerMaker`: 是否为买方主动（TRUE/FALSE）
- `isBestMatch`: 是否为最优撮合（TRUE/FALSE）

转换后的feather文件格式（与API下载的数据格式完全一致）：
- `成交时间`: 格式为 'YYYY-MM-DD HH:MM:SS.mmm'（保留3位毫秒）
- `获取时间`: 格式为 'YYYY-MM-DD HH:MM:SS'
- `货币对`: 交易对名称，如 'BTCUSDT'
- `成交ID`: 成交ID
- `成交价`: 成交价
- `成交量`: 成交量
- `成交额`: 成交额
- `是否为买方主动`: 布尔值
- `是否为最优撮合`: 布尔值

#### 2. 数据对比

##### `compare_feather_files(file1_path, file2_path, start_time=None, end_time=None, tz_str='Asia/Hong_Kong')`

比较两个feather文件中固定时间段的数据是否一致。

**功能特点：**
- 支持时间范围过滤：可以只比较指定时间段的数据
- 多维度对比：比较数量、ID、数据内容
- 详细报告：输出详细的差异信息

**参数：**
- `file1_path` (Path): 第一个feather文件路径
- `file2_path` (Path): 第二个feather文件路径
- `start_time` (Optional[datetime]): 开始时间（datetime对象），如果为None则比较所有数据
- `end_time` (Optional[datetime]): 结束时间（datetime对象），如果为None则比较所有数据
- `tz_str` (str): 时区字符串，默认'Asia/Hong_Kong'

**返回：**
- `Dict`: 比较结果字典，包含以下字段：
  - `file1_path`: 文件1路径
  - `file2_path`: 文件2路径
  - `file1_count`: 文件1记录数
  - `file2_count`: 文件2记录数
  - `count_match`: 数量是否一致
  - `count_diff`: 数量差异
  - `id_match`: ID是否一致
  - `data_match`: 数据内容是否一致
  - `missing_in_file2`: 文件2中缺失的ID列表（前100个）
  - `missing_in_file1`: 文件1中缺失的ID列表（前100个）
  - `different_data`: 数据不一致的记录ID列表（前100个）
  - `overall_match`: 总体是否一致

**示例：**
```python
from pathlib import Path
from datetime import datetime
import pytz
from CoinOrderbook.csv_to_feather_utils import compare_feather_files

file1 = Path("path/to/file1.feather")
file2 = Path("path/to/file2.feather")

# 比较所有数据
result = compare_feather_files(file1, file2)

# 或比较指定时间段的数据
tz = pytz.timezone('Asia/Hong_Kong')
start_time = tz.localize(datetime(2025, 11, 4, 0, 0, 0))
end_time = tz.localize(datetime(2025, 11, 4, 23, 59, 59, 999000))

result = compare_feather_files(
    file1, 
    file2, 
    start_time=start_time,
    end_time=end_time,
    tz_str='Asia/Hong_Kong'
)

# 查看结果
print(f"数量是否一致: {result['count_match']}")
print(f"ID是否一致: {result['id_match']}")
print(f"数据是否一致: {result['data_match']}")
print(f"总体是否一致: {result['overall_match']}")
```

---

## 使用场景

### 1. 日常数据更新

定期运行 `update_from_local()` 方法，从上次获取的位置继续获取新数据。

```python
from CoinOrderbook.GetHistoricalTradesData import GetHistoricalTradesData

ghtd = GetHistoricalTradesData(symbol='BTCUSDT')
df = ghtd.update_from_local()
```

### 2. 数据补全

如果之前的数据获取中断，可以继续从断点获取。

```python
from CoinOrderbook.feather_utils import get_max_from_id, read_all_feather_files
from pathlib import Path

data_dir = Path("path/to/data")
max_id = get_max_from_id(data_dir, "BTCUSDT")
# 从 max_id + 1 开始获取新数据
```

### 3. CSV 文件转换

将币安官网下载的 CSV 文件转换为 feather 格式。

```python
from CoinOrderbook.csv_to_feather_utils import convert_csv_to_feather

converted_files = convert_csv_to_feather(delete_csv=False)
```

### 4. 数据重组

将多个时间跨度的文件重新组织为按天分割的文件。

```python
from CoinOrderbook.feather_utils import reorganize_feather_files_by_day
import pytz
from pathlib import Path

data_dir = Path("path/to/data")
tz = pytz.timezone('Asia/Hong_Kong')
new_files = reorganize_feather_files_by_day(data_dir, "BTCUSDT", tz=tz)
```

### 5. 数据验证

对比不同来源的数据，验证数据一致性。

```python
from CoinOrderbook.csv_to_feather_utils import compare_feather_files
from pathlib import Path

file1 = Path("api_downloaded.feather")
file2 = Path("csv_converted.feather")
result = compare_feather_files(file1, file2)
```

### 6. 数据合并

将多个分散的 feather 文件合并为一个完整的数据集。

```python
from CoinOrderbook.feather_utils import read_all_feather_files
from pathlib import Path

data_dir = Path("path/to/data")
df = read_all_feather_files(data_dir, "BTCUSDT")
```

---

## 注意事项

### 1. 数据目录

- 数据保存在 `Settings.py` 中配置的 `result_path_historical_trades` 目录
- CSV 文件应放在 `result_path_historical_trades/csv` 子文件夹中
- Feather 文件保存在 `result_path_historical_trades` 目录下

### 2. 文件命名

- Feather 文件命名格式：`{symbol}_historical_trades_{start_time}_to_{end_time}.feather`
- 例如：`BTCUSDT_historical_trades_20251107_000000_to_20251107_235959.feather`
- 时间格式：`YYYYMMDD_HHMMSS`（精确到秒）

### 3. 数据格式

- 所有 feather 文件使用相同的数据格式（9列）
- 成交时间格式：`YYYY-MM-DD HH:MM:SS.mmm`（保留3位毫秒）
- 数据按成交ID排序

### 4. 自动去重

- 所有合并操作都会自动基于成交ID去重
- 保留第一次出现的记录（`keep='first'`）

### 5. 时区处理

- 默认使用 `Settings.py` 中配置的 `TIMEZONE`
- 所有时间都会转换为指定时区
- 建议使用 `Asia/Hong_Kong` 时区

### 6. 首次使用

- 如果本地没有数据，`get_max_from_id()` 会返回 `None`
- `update_from_local()` 会从默认ID（0）开始获取

### 7. 性能优化

- `get_max_from_id()` 只读取最新的文件，避免读取所有文件
- `reorganize_feather_files_by_day()` 会智能跳过已按天分割的文件
- 大文件处理时建议分批处理

### 8. 数据安全

- 默认情况下，转换和重组操作不会删除原始文件
- 使用 `delete_old_files=True` 时请确保数据已正确保存
- 建议在操作前备份重要数据

---

## 完整示例

### 示例1：从CSV转换并验证数据

```python
from pathlib import Path
from datetime import datetime
import pytz
from CoinOrderbook.csv_to_feather_utils import convert_csv_to_feather, compare_feather_files
from CoinOrderbook.feather_utils import reorganize_feather_files_by_day

# 1. 转换CSV文件
print("步骤1: 转换CSV文件")
converted_files = convert_csv_to_feather(delete_csv=False)
print(f"转换完成，共 {len(converted_files)} 个文件")

# 2. 验证数据（如果有API下载的文件）
if converted_files:
    api_file = Path("path/to/api_downloaded.feather")
    if api_file.exists():
        print("\n步骤2: 验证数据一致性")
        result = compare_feather_files(converted_files[0], api_file)
        print(f"数据是否一致: {result['overall_match']}")

# 3. 按天重组文件
print("\n步骤3: 按天重组文件")
tz = pytz.timezone('Asia/Hong_Kong')
new_files = reorganize_feather_files_by_day(
    data_dir=Path("path/to/data"),
    symbol="BTCUSDT",
    tz=tz,
    delete_old_files=False  # 先不删除，确认无误后再删除
)
print(f"重组完成，共 {len(new_files)} 个文件")
```

### 示例2：日常数据更新流程

```python
from CoinOrderbook.GetHistoricalTradesData import GetHistoricalTradesData
from CoinOrderbook.feather_utils import reorganize_feather_files_by_day
from pathlib import Path
import pytz

# 1. 从本地数据续传
print("步骤1: 获取最新数据")
ghtd = GetHistoricalTradesData(symbol='BTCUSDT')
df = ghtd.update_from_local()
print(f"获取到 {len(df)} 条新记录")

# 2. 按天重组文件（可选，定期执行）
print("\n步骤2: 按天重组文件")
data_dir = Path("path/to/data")
tz = pytz.timezone('Asia/Hong_Kong')
new_files = reorganize_feather_files_by_day(
    data_dir=data_dir,
    symbol="BTCUSDT",
    tz=tz,
    delete_old_files=True
)
print(f"重组完成，共 {len(new_files)} 个文件")
```

---

## 总结

本工具集提供了完整的历史成交数据处理解决方案：

- ✅ **数据获取**：从API获取或从本地续传
- ✅ **格式转换**：CSV转Feather，确保格式一致
- ✅ **文件管理**：查找、读取、合并、重组
- ✅ **数据验证**：对比不同来源的数据一致性
- ✅ **智能优化**：自动去重、智能跳过、性能优化

通过合理使用这些工具，可以高效地管理和处理历史成交数据。
