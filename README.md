# QLTrader

A股量化回测框架，API设计参考Zipline风格。

## 功能特性

- 支持CSV格式的A股日线数据回测
- 模拟真实交易：滑点、佣金、仓位管理
- 内置`schedule`定时任务，支持日频/月频调仓
- 数据接口：`data.current()`、`data.history()`、`data.can_trade()`
- 下单接口：`order_shares()`、`order_target_percent()`、`order_percent()`、`order_target_shares()`
- 回测结果可视化与指标统计

## 项目结构

```
qltrader/
├── src/
│   └── qltrader/            # 核心框架包
│       ├── __init__.py      # 包入口，导出公共API
│       ├── config.py        # 配置常量（数据路径等）
│       ├── models.py        # 核心模型（Position, Portfolio, Context）
│       ├── data.py          # 数据访问接口（Data类）
│       ├── orders.py        # 下单函数
│       ├── scheduler.py     # 定时任务调度
│       ├── engine.py        # 回测引擎（QlTrader类）
│       ├── plotting.py      # 可视化函数
│       ├── utils.py         # 工具函数（run_backtest, get_price）
│       └── tushare_data.py  # Tushare数据下载模块
├── examples/
│   ├── tushare_example.py   # Tushare数据下载示例
│   └── strategy_example.py  # 双均线策略示例
├── data/daily/              # 股票日线数据 (CSV)
└── README.md
```

## 快速开始

### 安装

```bash
pip install qltrader
```

### 编写自己的策略

#### 方式一：纯定时任务策略（推荐）
适合定期调仓（如月度换仓）的策略，`handle_data` 参数可选：

```python
def initialize(context):
    # 设置股票池
    context.set_universe(['sh600000', 'sh600001', 'sh600004'])
    # 每月开盘调仓
    schedule(rebalance, date_rule='month', time_rule='open')

def rebalance(context, data):
    # 获取历史数据
    hist = data.history(context.universe, 20, fields='close')
    # 选股逻辑...
    # 下单
    order_shares('sh600000', 1000)

# 运行回测（不需要传 handle_data）
results = run_backtest(
    start_date='2020-01-01',
    end_date='2025-12-31',
    initialize=initialize,
    capital_base=1000000.0
)

plot_results(results)
```

#### 方式二：日频交易策略
适合需要每日判断的策略，需传入 `handle_data`：

```python
def initialize(context):
    context.set_universe(['sh600000', 'sh600001'])

def handle_data(context, data):
    # 每日收盘前判断
    for sec in context.universe:
        if data.can_trade(sec):
            # 等权持仓
            order_target_percent(context, sec, 1.0 / len(context.universe))

results = run_backtest(
    start_date='2020-01-01',
    end_date='2025-12-31',
    initialize=initialize,
    handle_data=handle_data,
    capital_base=1000000.0
)

plot_results(results)
```


## API 参考

### 数据接口

#### `data.current(securities, fields='close', as_df=False)`
获取当前交易日的行情数据。

**参数：**
- `securities`: 股票代码（字符串或列表）
- `fields`: 数据字段，可选 'open', 'high', 'low', 'close', 'volume'（字符串或列表）
- `as_df`: 是否返回 DataFrame 格式，默认为 False

**返回值：**
- `as_df=False` 时：根据参数返回标量、字典或嵌套字典
- `as_df=True` 时：返回 DataFrame，行名为股票代码，列名为字段名

**示例：**
```python
# 获取单只股票收盘价（标量）
price = data.current("000001.SZ", "close")

# 获取多只股票收盘价（字典）
prices = data.current(["000001.SZ", "000002.SZ"], "close")

# 获取多只股票多字段（DataFrame格式）
df = data.current(["000001.SZ", "000002.SZ"], ["close", "volume"], as_df=True)
```

#### `data.history(securities, bar_count, frequency='1d', fields='close', as_df=False)`
获取历史行情数据。

**参数：**
- `securities`: 股票代码（字符串或列表）
- `bar_count`: 获取的 K 线数量
- `frequency`: 频率，目前仅支持 '1d'（日线）
- `fields`: 数据字段（字符串或列表）
- `as_df`: 是否返回 DataFrame 格式，默认为 False

**返回值：**
- `as_df=False` 时：根据参数返回列表、字典或嵌套字典
- `as_df=True` 时：返回 DataFrame

**示例：**
```python
# 获取最近20天收盘价（列表）
hist = data.history("000001.SZ", bar_count=20, fields="close")

# DataFrame格式
hist_df = data.history("000001.SZ", bar_count=20, fields=["close", "volume"], as_df=True)
```

#### `data.can_trade(securities)`
检查股票在当日是否可交易（不停牌）。

**参数：**
- `securities`: 股票代码（字符串或列表）

**返回值：**
- 单股：布尔值
- 多股：{股票代码: 布尔值} 字典

#### `get_price(security, start_date, end_date, frequency='daily', fields='close', as_df=True)`
独立函数，从 CSV 文件读取历史数据（不依赖回测环境）。

**参数：**
- `security`: 股票代码
- `start_date`: 开始日期
- `end_date`: 结束日期
- `frequency`: 频率，目前仅支持 'daily'
- `fields`: 数据字段（字符串或列表）
- `as_df`: 是否返回 DataFrame 格式，默认为 True

**返回值：**
- `as_df=True` 时：返回 DataFrame
- `as_df=False` 时：返回 List[dict] 格式

**示例：**
```python
# 返回DataFrame（默认）
df = get_price("000001.SZ", "2020-01-01", "2020-12-31", fields=["close", "volume"])

# 返回字典列表
data = get_price("000001.SZ", "2020-01-01", "2020-12-31", fields="close", as_df=False)
```

### 下单接口

| 方法                                               | 说明                                    |
| -------------------------------------------------- | --------------------------------------- |
| `order_shares(sec, amount)`                        | 买入/卖出指定股数（正数买入，负数卖出） |
| `order_target_shares(context, sec, target_shares)` | 调仓到目标持仓股数                      |
| `order_percent(context, sec, percent)`             | 按账户资金比例下单（增量）              |
| `order_target_percent(context, sec, percent)`      | 调仓到目标资金比例（0-1之间）           |

**示例：**
```python
# 买入1000股
order_shares("000001.SZ", 1000)

# 卖出500股
order_shares("000001.SZ", -500)

# 调仓到持有1000股
order_target_shares(context, "000001.SZ", 1000)

# 用账户50%资金买入
order_percent(context, "000001.SZ", 0.5)

# 调仓到该股票占账户50%
order_target_percent(context, "000001.SZ", 0.5)
```

### 定时任务

```python
schedule(func, date_rule='daily/month', time_rule='open/close')
```

在 `initialize` 中设置定时调仓任务。

**参数：**
- `func`: 调仓函数，接收 (context, data) 参数
- `date_rule`: 'daily'（每天）或 'month'（每月）
- `time_rule`: 'open'（开盘）或 'close'（收盘）

**示例：**
```python
def initialize(context):
    # 每月开盘时调仓
    schedule(rebalance, date_rule='month', time_rule='open')

def rebalance(context, data):
    # 调仓逻辑
    order_target_percent(context, "000001.SZ", 0.5)
```

### 回测入口

```python
run_backtest(
    start_date='2020-01-01',      # 开始日期（YYYY-MM-DD）
    end_date='2025-12-31',        # 结束日期（YYYY-MM-DD）
    initialize=initialize,        # 初始化函数（必须）
    handle_data=None,             # 日频策略函数（可选）
    before_trading_start=None,    # 盘前函数（可选）
    capital_base=1000000.0        # 初始资金，默认100万
)
```

### 可视化

```python
plot_results(results, title='Backtest Results', save_path=None)
```

**参数：**
- `results`: `run_backtest` 返回的 DataFrame
- `title`: 图表标题
- `save_path`: 保存路径（可选），如 './output/result.png'

**示例：**
```python
results = run_backtest(...)
plot_results(results, title='My Strategy', save_path='./output/my_strategy.png')
```

## 数据格式

CSV文件位于 `data/daily/` 目录，文件命名规则：`sh600000.csv`（上证）、`sz000001.csv`（深证）

字段：`date, open, high, low, close, volume`

## License

本项目采用 [MIT](LICENSE) 协议开源。

## 下载数据

框架内置Tushare数据下载模块，支持一键下载完整A股数据。

### 设置Token

**方式一：使用 .env 文件（推荐）**

在项目根目录创建 `.env` 文件：
```
TUSHARE_TOKEN=your_tushare_token
```

**方式二：手动设置**
```python
from qltrader import set_token

# 设置Tushare Token
set_token("your_tushare_token")
```

#### 主要接口

| 接口                                          | 说明                                  |
| --------------------------------------------- | ------------------------------------- |
| `get_stock_basic(list_status='L')`            | 获取股票列表（L上市/D退市/P暂停上市） |
| `get_all_stock()`                             | 获取全部A股（上市+退市+暂停上市）     |
| `get_index_basic(market='SSE')`               | 获取指数列表                          |
| `get_stock_name(ts_code)`                     | 获取股票最新名称                      |
| `get_stock_name_history(ts_code)`             | 获取股票名称变更历史                  |
| `get_stock_industry(ts_code)`                 | 获取股票申万行业分类（一二三级）      |
| `download_data(code, start_date, end_date)`   | 下载单只证券完整数据                  |
| `download_batch(codes, start_date, end_date)` | 批量下载多只证券数据                  |

#### 一键下载全部A股

```python
from qltrader import set_token, get_all_stock, download_batch

set_token("your_tushare_token")

# 获取全部股票（包括上市、退市、暂停上市）
stock_df = get_all_stock()

# 批量下载
codes = [f"{row['ts_code'].split('.')[1].lower()}{row['ts_code'].split('.')[0]}" 
         for _, row in stock_df.iterrows()]
download_batch(codes, "2020-01-01", "2024-12-31")
```

#### 下载数据字段

CSV文件包含以下字段：

| 字段类别 | 字段名                                                                               |
| -------- | ------------------------------------------------------------------------------------ |
| 基本信息 | `code`, `ts_code`, `type`, `date`, `name`                                            |
| 行业分类 | `industry_l1_name`, `industry_l2_name`, `industry_l3_name`（申万一二三级行业）       |
| 行情数据 | `open`, `high`, `low`, `close`, `pre_close`, `change`, `pct_chg`, `volume`, `amount` |
| 估值指标 | `pe`, `pe_ttm`, `pb`, `ps`, `ps_ttm`, `dv_ratio`, `dv_ttm`                           |
| 市值数据 | `total_mv`, `circ_mv`, `free_share`, `total_share`                                   |
| 换手率   | `turnover_rate`, `turnover_rate_f`, `volume_ratio`                                   |
| 资金流向 | `buy_sm_vol`, `buy_sm_amount`, `sell_sm_vol`, ... `net_mf_vol`, `net_mf_amount`      |

#### 名称历史匹配

股票名称会根据日期自动匹配历史名称：

```python
from qltrader import get_stock_name_history, get_name_at_date

# 获取名称变更历史
history = get_stock_name_history("600000.SH")

# 获取特定日期的名称
name = get_name_at_date(history, "2024-01-01")
```

#### 示例脚本

**Tushare 数据下载示例**

运行 `examples/tushare_example.py` 查看更多使用示例：

```bash
python examples/tushare_example.py
```

**策略回测示例**

运行 `examples/strategy_example.py` 查看双均线策略示例：

```bash
python examples/strategy_example.py
```

该示例展示了：
- 如何编写双均线策略（5日均线 vs 20日均线）
- 金叉买入、死叉卖出的交易逻辑
- 回测结果统计（累计收益、年化收益、最大回撤）
- 结果可视化保存到 `output/dual_ma_strategy.png`
