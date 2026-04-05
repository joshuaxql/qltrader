"""
QlTrader 量化回测框架 - Tushare数据下载模块

提供从Tushare获取A股数据的功能，所有数据整合到单一CSV文件中，包含：
- 代码（sh600000格式）、类型（stock/index）、名称
- 申万行业（一二三级名称）
- 开高收低、前收、成交量、成交额
- 每日指标（估值、换手率、市值等）
- 个股资金流向
"""

import os
import time
from pathlib import Path
from typing import List, Optional, Dict
import pandas as pd
import tushare as ts
from dotenv import load_dotenv

# 加载.env文件中的环境变量
load_dotenv()

# Tushare API实例
_api = None


def set_token(token: str):
    """
    设置Tushare API Token

    Args:
        token: Tushare API Token
    """
    global _api
    ts.set_token(token)
    _api = ts.pro_api()


def _get_api():
    """获取Tushare API实例"""
    global _api
    if _api is None:
        # 优先从环境变量读取Token
        token = os.environ.get("TUSHARE_TOKEN", "")
        if not token:
            raise ValueError(
                "未设置Tushare Token。请通过以下方式之一设置：\n"
                "1. 在项目根目录创建.env文件，添加: TUSHARE_TOKEN=your_token\n"
                "2. 调用 set_token('your_token') 函数"
            )
        set_token(token)
    return _api


def format_code(ts_code: str) -> str:
    """
    将Tushare代码格式转换为项目格式

    Args:
        ts_code: Tushare格式代码，如 '600000.SH'

    Returns:
        项目格式代码，如 'sh600000'
    """
    if "." in ts_code:
        code, market = ts_code.split(".")
        market = market.lower()
        return f"{market}{code}"
    return ts_code


def to_ts_code(code: str) -> str:
    """
    将项目代码格式转换为Tushare格式

    Args:
        code: 项目格式代码，如 'sh600000' 或 '600000'

    Returns:
        Tushare格式代码，如 '600000.SH'
    """
    code = str(code)
    # 如果已经是Tushare格式
    if "." in code:
        return code.upper()

    # 判断前缀
    if code.lower().startswith("sh"):
        return f"{code[2:]}.SH"
    elif code.lower().startswith("sz"):
        return f"{code[2:]}.SZ"
    elif code.lower().startswith("bj"):
        return f"{code[2:]}.BJ"
    else:
        # 纯代码，默认根据代码规则判断市场
        if code.startswith("6"):
            return f"{code}.SH"
        elif code.startswith(("0", "3")):
            return f"{code}.SZ"
        elif code.startswith(("4", "8")):
            return f"{code}.BJ"
        else:
            return f"{code}.SH"


def get_sec_type(code: str) -> str:
    """
    判断证券类型

    Args:
        code: 证券代码

    Returns:
        类型：'stock' 或 'index'
    """
    code = str(code)
    # 去掉前缀
    if code.lower().startswith(("sh", "sz", "bj")):
        pure_code = code[2:]
    else:
        pure_code = code

    # 指数代码规则
    # 上海指数：000001-000999, 9xxxxx
    # 深圳指数：399001-399999
    if pure_code.startswith("000") and len(pure_code) == 6:
        # 需要进一步判断，sh000001是指数，sz000001是股票
        if code.lower().startswith("sh"):
            return "index"
        else:
            return "stock"
    elif pure_code.startswith("399"):
        return "index"
    elif pure_code.startswith("9"):
        return "index"
    else:
        return "stock"


# ============================================================
# 股票/指数基本信息
# ============================================================


def get_stock_basic(exchange: str = "", list_status: str = "L") -> pd.DataFrame:
    """
    获取股票基本信息

    Args:
        exchange: 交易所代码，SSE上交所 SZSE深交所 BSE北交所，空为全部
        list_status: 上市状态 L上市 D退市 P暂停上市

    Returns:
        DataFrame包含股票代码、名称、行业等基本信息
    """
    api = _get_api()
    df = api.stock_basic(
        exchange=exchange,
        list_status=list_status,
        fields=[
            "ts_code",
            "symbol",
            "name",
            "area",
            "industry",
            "market",
            "list_date",
            "delist_date",
            "is_hs",
        ],
    )
    return df


def get_all_stock() -> pd.DataFrame:
    """
    获取全部A股股票基本信息（包括上市、退市、暂停上市）

    Returns:
        DataFrame包含所有股票代码、名称、行业等基本信息
    """
    api = _get_api()
    all_stocks = []

    # 获取上市股票 (L)
    try:
        df_list = api.stock_basic(list_status="L")
        if not df_list.empty:
            all_stocks.append(df_list)
    except Exception as e:
        print(f"获取上市股票失败: {e}")

    # 获取退市股票 (D)
    try:
        df_delist = api.stock_basic(list_status="D")
        if not df_delist.empty:
            all_stocks.append(df_delist)
    except Exception as e:
        print(f"获取退市股票失败: {e}")

    # 获取暂停上市股票 (P)
    try:
        df_pause = api.stock_basic(list_status="P")
        if not df_pause.empty:
            all_stocks.append(df_pause)
    except Exception as e:
        print(f"获取暂停上市股票失败: {e}")

    if all_stocks:
        result = pd.concat(all_stocks, ignore_index=True)
        return result
    else:
        return pd.DataFrame()


def download_all_stock_codes(output_dir: str = None) -> pd.DataFrame:
    """
    下载全部A股股票代码列表

    将获取到的股票列表保存到CSV文件。

    Args:
        output_dir: 输出目录，默认为data/

    Returns:
        DataFrame包含所有股票代码、名称、行业等基本信息
    """
    from .config import DATA_PATH

    if output_dir is None:
        output_dir = DATA_PATH.parent
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print("正在获取全部A股股票列表...")

    # 获取全部股票（上市+退市+暂停上市）
    stock_df = get_all_stock()
    if stock_df.empty:
        print("获取股票列表失败")
        return pd.DataFrame()

    print(f"共获取到 {len(stock_df)} 只股票")

    # 添加项目格式代码
    stock_df["code"] = stock_df["ts_code"].apply(
        lambda x: f"{x.split('.')[1].lower()}{x.split('.')[0]}"
        if "." in x
        else x
    )

    # 重新排列列
    cols = ["code", "ts_code", "symbol", "name", "area", "industry", "market", "list_date", "delist_date", "is_hs"]
    existing_cols = [c for c in cols if c in stock_df.columns]
    stock_df = stock_df[existing_cols]

    # 保存到CSV
    output_file = output_path / "all_stocks.csv"
    stock_df.to_csv(output_file, index=False)
    print(f"已保存到 {output_file}")

    return stock_df


def get_index_basic(market: str = "SSE") -> pd.DataFrame:
    """
    获取指数基本信息

    Args:
        market: 市场 SSE上交所 SZSE深交所

    Returns:
        DataFrame包含指数代码、名称等基本信息
    """
    api = _get_api()
    df = api.index_basic(
        market=market,
        fields=[
            "ts_code",
            "name",
            "market",
            "publisher",
            "base_date",
            "base_point",
            "list_date",
        ],
    )
    return df


def get_stock_name(ts_code: str) -> str:
    """
    获取股票最新名称（通过namechange接口）

    Args:
        ts_code: Tushare格式股票代码

    Returns:
        股票最新名称
    """
    api = _get_api()
    try:
        df = api.namechange(
            ts_code=ts_code,
            fields=["ts_code", "name", "start_date", "end_date", "ann_date"],
        )
        if not df.empty:
            # 按开始日期降序排列，取第一个（最新名称）
            df = df.sort_values("start_date", ascending=False)
            return df.iloc[0]["name"]
    except Exception as e:
        print(f"获取名称变更失败 {ts_code}: {e}")

    # 如果namechange接口失败，尝试从stock_basic获取
    try:
        stock_df = get_stock_basic()
        mask = stock_df["ts_code"] == ts_code
        if mask.any():
            return stock_df[mask].iloc[0]["name"]
    except:
        pass

    return ""


def get_stock_name_history(ts_code: str) -> pd.DataFrame:
    """
    获取股票名称变更历史（通过namechange接口）

    Args:
        ts_code: Tushare格式股票代码

    Returns:
        DataFrame包含名称变更历史，列包括：name, start_date, end_date
        按start_date降序排列（最新在前）
    """
    api = _get_api()
    try:
        df = api.namechange(
            ts_code=ts_code,
            fields=["ts_code", "name", "start_date", "end_date", "ann_date"],
        )
        if not df.empty:
            # 按开始日期降序排列
            df = df.sort_values("start_date", ascending=False)
            return df[["name", "start_date", "end_date"]]
    except Exception as e:
        print(f"获取名称变更失败 {ts_code}: {e}")

    return pd.DataFrame()


def get_name_at_date(name_history: pd.DataFrame, date: str) -> str:
    """
    根据日期从名称变更历史中获取对应的名称

    Args:
        name_history: 名称变更历史DataFrame
        date: 日期，格式YYYY-MM-DD或YYYYMMDD

    Returns:
        该日期对应的股票名称
    """
    if name_history.empty:
        return ""

    # 统一日期格式
    date = date.replace("-", "")

    for _, row in name_history.iterrows():
        start_date = str(row.get("start_date", ""))
        end_date = str(row.get("end_date", ""))

        # 处理空值
        if not start_date:
            continue

        # 检查日期范围
        # start_date <= date 且 (end_date为空或end_date >= date)
        if start_date <= date:
            if not end_date or end_date >= date:
                return row["name"]

    # 如果没找到，返回最新的名称（第一条记录）
    return name_history.iloc[0]["name"] if len(name_history) > 0 else ""


def get_index_name(ts_code: str) -> str:
    """
    获取指数名称（通过index_basic接口）

    Args:
        ts_code: Tushare格式指数代码

    Returns:
        指数名称
    """
    try:
        # 判断交易所
        if ts_code.endswith(".SH"):
            market = "SSE"
        else:
            market = "SZSE"

        df = get_index_basic(market)
        mask = df["ts_code"] == ts_code
        if mask.any():
            return df[mask].iloc[0]["name"]
    except Exception as e:
        print(f"获取指数名称失败 {ts_code}: {e}")

    return ""


# ============================================================
# 申万行业分类缓存
# ============================================================

# 申万行业分类缓存（全局变量）
_sw_industry_cache = {
    "l1_codes": None,  # 一级行业指数代码列表
    "l2_codes": None,  # 二级行业指数代码列表
    "l3_codes": None,  # 三级行业指数代码列表
    "index_member_all": None,  # index_member_all 完整数据
    "index_basic": None,  # 指数基本信息
}


def _get_sw_industry_level(index_code: str) -> int:
    """
    根据申万行业指数代码判断行业级别

    申万行业指数代码规则：
    - 一级行业：801010-801950 (8010xx)
    - 二级行业：801011-801952 (801xxx，排除一级行业)
    - 三级行业：其他

    Args:
        index_code: 申万行业指数代码

    Returns:
        行业级别：1, 2, 3 或 0（无法判断）
    """
    try:
        # 去掉后缀
        code = index_code.split(".")[0]
        if not code.startswith("801"):
            return 0

        # 后四位判断
        suffix = code[3:]  # 去掉801
        if suffix.endswith("0") and len(suffix) == 3:
            # 一级行业结尾为0，如801010
            return 1
        elif len(suffix) == 3:
            # 二级行业，如801011
            return 2
        else:
            return 3
    except:
        return 0


def _load_sw_industry_cache():
    """
    加载申万行业分类缓存

    使用 index_member_all 接口获取所有股票的申万行业分类数据
    接口直接返回 l1_code, l1_name, l2_code, l2_name, l3_code, l3_name 等字段
    """
    global _sw_industry_cache
    api = _get_api()

    if _sw_industry_cache["index_member_all"] is not None:
        return

    try:
        print("正在加载申万行业分类数据...")

        # 获取所有股票的申万行业分类数据
        df = api.index_member_all()

        if df.empty:
            print("警告：未获取到指数成分股数据")
            return

        # 打印实际返回的列名，便于调试
        print(f"  index_member_all 返回列: {list(df.columns)}")
        print(f"  共 {len(df)} 条记录")

        _sw_industry_cache["index_member_all"] = df

        # 统计信息
        unique_stocks = df["ts_code"].nunique()
        print(f"  申万行业分类加载完成：覆盖 {unique_stocks} 只股票")

    except Exception as e:
        print(f"加载申万行业分类数据失败: {e}")
        import traceback

        traceback.print_exc()


def get_stock_industry(ts_code: str) -> Dict:
    """
    获取股票申万行业分类（一二三级）

    使用 index_member_all 接口获取股票所属的申万一二三级行业
    接口直接返回 l1_code, l1_name, l2_code, l2_name, l3_code, l3_name

    Args:
        ts_code: Tushare格式股票代码

    Returns:
        字典，包含一、二、三级行业代码和名称
    """
    global _sw_industry_cache
    result = {
        "industry_l1_code": "",
        "industry_l1_name": "",
        "industry_l2_code": "",
        "industry_l2_name": "",
        "industry_l3_code": "",
        "industry_l3_name": "",
    }

    # 确保缓存已加载
    _load_sw_industry_cache()

    if _sw_industry_cache["index_member_all"] is None:
        # 缓存加载失败，尝试从stock_basic获取一级行业
        try:
            stock_df = get_stock_basic()
            mask = stock_df["ts_code"] == ts_code
            if mask.any():
                result["industry_l1_name"] = stock_df[mask].iloc[0].get("industry", "")
        except Exception as e:
            print(f"获取股票行业分类失败 {ts_code}: {e}")
        return result

    try:
        df = _sw_industry_cache["index_member_all"]

        # 筛选该股票的记录
        stock_mask = df["ts_code"] == ts_code
        stock_industries = df[stock_mask]

        if stock_industries.empty:
            # 如果没有找到，尝试从stock_basic获取一级行业
            stock_df = get_stock_basic()
            mask = stock_df["ts_code"] == ts_code
            if mask.any():
                result["industry_l1_name"] = stock_df[mask].iloc[0].get("industry", "")
            return result

        # 筛选当前有效的行业（is_new == 'Y' 或 out_date为空/大于当前日期）
        import datetime

        today = datetime.datetime.now().strftime("%Y%m%d")

        # 优先选择 is_new == 'Y' 的记录
        if "is_new" in stock_industries.columns:
            new_record = stock_industries[stock_industries["is_new"] == "Y"]
            if not new_record.empty:
                row = new_record.iloc[0]
                result["industry_l1_code"] = row.get("l1_code", "")
                result["industry_l1_name"] = row.get("l1_name", "")
                result["industry_l2_code"] = row.get("l2_code", "")
                result["industry_l2_name"] = row.get("l2_name", "")
                result["industry_l3_code"] = row.get("l3_code", "")
                result["industry_l3_name"] = row.get("l3_name", "")
                return result

        # 如果没有 is_new == 'Y'，检查 out_date
        for _, row in stock_industries.iterrows():
            out_date = row.get("out_date", "")
            if not out_date or str(out_date) >= today:
                result["industry_l1_code"] = row.get("l1_code", "")
                result["industry_l1_name"] = row.get("l1_name", "")
                result["industry_l2_code"] = row.get("l2_code", "")
                result["industry_l2_name"] = row.get("l2_name", "")
                result["industry_l3_code"] = row.get("l3_code", "")
                result["industry_l3_name"] = row.get("l3_name", "")
                return result

        # 如果都没有，取第一条记录
        row = stock_industries.iloc[0]
        result["industry_l1_code"] = row.get("l1_code", "")
        result["industry_l1_name"] = row.get("l1_name", "")
        result["industry_l2_code"] = row.get("l2_code", "")
        result["industry_l2_name"] = row.get("l2_name", "")
        result["industry_l3_code"] = row.get("l3_code", "")
        result["industry_l3_name"] = row.get("l3_name", "")

    except Exception as e:
        print(f"获取股票行业分类失败 {ts_code}: {e}")
        # 尝试从stock_basic获取一级行业
        try:
            stock_df = get_stock_basic()
            mask = stock_df["ts_code"] == ts_code
            if mask.any():
                result["industry_l1_name"] = stock_df[mask].iloc[0].get("industry", "")
        except:
            pass

    return result


# ============================================================
# 行情数据获取
# ============================================================


def get_daily(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取日线行情数据

    Args:
        ts_code: Tushare格式证券代码
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD

    Returns:
        DataFrame包含开高低收、成交量、成交额等
    """
    api = _get_api()
    sec_type = get_sec_type(ts_code)

    if sec_type == "index":
        df = api.index_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields=[
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "vol",
                "amount",
            ],
        )
    else:
        df = api.daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields=[
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "vol",
                "amount",
            ],
        )

    return df


def get_daily_basic(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取每日指标数据（估值、换手率、市值等）

    Args:
        ts_code: Tushare格式股票代码
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD

    Returns:
        DataFrame包含PE、PB、PS、总市值、流通市值、换手率等
    """
    api = _get_api()
    try:
        df = api.daily_basic(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields=[
                "ts_code",
                "trade_date",
                "close",
                "turnover_rate",
                "turnover_rate_f",
                "volume_ratio",
                "pe",
                "pe_ttm",
                "pb",
                "ps",
                "ps_ttm",
                "dv_ratio",
                "dv_ttm",
                "total_mv",
                "circ_mv",
                "free_share",
                "total_share",
            ],
        )
        return df
    except Exception as e:
        print(f"获取每日指标失败 {ts_code}: {e}")
        return pd.DataFrame()


def get_moneyflow(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取个股资金流向数据

    Args:
        ts_code: Tushare格式股票代码
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD

    Returns:
        DataFrame包含主力、散户资金流入流出等
    """
    api = _get_api()
    try:
        df = api.moneyflow(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields=[
                "ts_code",
                "trade_date",
                "buy_sm_vol",
                "buy_sm_amount",
                "sell_sm_vol",
                "sell_sm_amount",
                "buy_md_vol",
                "buy_md_amount",
                "sell_md_vol",
                "sell_md_amount",
                "buy_lg_vol",
                "buy_lg_amount",
                "sell_lg_vol",
                "sell_lg_amount",
                "buy_elg_vol",
                "buy_elg_amount",
                "sell_elg_vol",
                "sell_elg_amount",
                "net_mf_vol",
                "net_mf_amount",
            ],
        )
        return df
    except Exception as e:
        print(f"获取资金流向失败 {ts_code}: {e}")
        return pd.DataFrame()


# ============================================================
# 分红配股数据获取
# ============================================================


def get_dividend(ts_code: str) -> pd.DataFrame:
    """
    获取分红配股数据

    Args:
        ts_code: Tushare格式股票代码

    Returns:
        DataFrame包含分红配股信息
        字段：ts_code, end_date, ann_date, div_proc, stk_div, stk_bo_rate, stk_co_rate,
              cash_div, cash_div_tax, record_date, ex_date(除权除息日), pay_date,
              div_listdate, imp_ann_date, base_date, base_share
    """
    api = _get_api()
    try:
        df = api.dividend(
            ts_code=ts_code,
            fields=[
                "ts_code",
                "end_date",
                "ann_date",
                "div_proc",
                "stk_div",
                "stk_bo_rate",
                "stk_co_rate",
                "cash_div",
                "cash_div_tax",
                "record_date",
                "ex_date",
                "pay_date",
                "div_listdate",
                "imp_ann_date",
                "base_date",
                "base_share",
            ],
        )
        return df
    except Exception as e:
        print(f"获取分红配股失败 {ts_code}: {e}")
        return pd.DataFrame()


def download_dividend(
    code: str,
    output_dir: str = None,
) -> pd.DataFrame:
    """
    下载股票分红配股数据

    Args:
        code: 证券代码（支持项目格式或Tushare格式）
        start_date: 开始日期 YYYYMMDD或YYYY-MM-DD
        end_date: 结束日期 YYYYMMDD或YYYY-MM-DD
        output_dir: 输出目录，默认为data/dividend/

    Returns:
        DataFrame包含分红配股数据
    """
    from .config import DIVIDEND_PATH

    # 转换代码格式
    ts_code = to_ts_code(code)
    proj_code = format_code(ts_code)

    # 设置输出目录
    if output_dir is None:
        output_dir = DIVIDEND_PATH
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"正在下载 {proj_code} 分红配股数据...")

    # 获取分红配股数据
    div_df = get_dividend(ts_code)
    if div_df.empty:
        print(f"  未获取到数据")
        return pd.DataFrame()

    # 格式化日期字段
    # ex_date: 除权除息日（用于回测匹配）
    div_df["ex_date"] = pd.to_datetime(
        div_df["ex_date"], format="%Y%m%d", errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    # 添加项目格式代码
    div_df["code"] = proj_code

    # 按除权除息日期排序
    div_df = div_df.sort_values("ex_date").reset_index(drop=True)

    # 保存到CSV
    output_file = output_path / f"{proj_code}.csv"
    div_df.to_csv(output_file, index=False)
    print(f"  已保存 {len(div_df)} 条记录到 {output_file}")

    return div_df


def download_batch_dividend(
    codes: List[str],
    output_dir: str = None,
) -> Dict[str, pd.DataFrame]:
    """
    批量下载多只股票分红配股数据

    Args:
        codes: 证券代码列表
        start_date: 开始日期
        end_date: 结束日期
        output_dir: 输出目录

    Returns:
        字典，键为代码，值为DataFrame
    """
    result = {}
    total = len(codes)

    for i, code in enumerate(codes):
        print(f"\n[{i + 1}/{total}] 处理 {code}...")

        try:
            df = download_dividend(code, output_dir)
            if not df.empty:
                proj_code = format_code(to_ts_code(code))
                result[proj_code] = df
        except Exception as e:
            print(f"  下载失败: {e}")

        # API限频控制
        time.sleep(0.3)

    return result


# ============================================================
# 完整数据下载（整合到一个文件）
# ============================================================


def download_data(
    code: str,
    start_date: str,
    end_date: str,
    output_dir: str = None,
    include_daily_basic: bool = True,
    include_moneyflow: bool = True,
) -> pd.DataFrame:
    """
    下载证券完整数据，整合所有信息到一个DataFrame中

    最终CSV文件包含以下列：
    - code: 代码（sh600000格式）
    - type: 类型（stock/index）
    - name: 名称
    - industry_l1_name: 申万一级行业名称
    - industry_l2_name: 申万二级行业名称
    - industry_l3_name: 申万三级行业名称
    - date: 交易日期
    - open, high, low, close: 开高低收
    - pre_close: 前收盘价
    - volume: 成交量
    - amount: 成交额
    - 每日指标: turnover_rate, pe, pb, total_mv等
    - 资金流向: net_mf_amount等

    Args:
        code: 证券代码（支持项目格式或Tushare格式）
        start_date: 开始日期 YYYYMMDD或YYYY-MM-DD
        end_date: 结束日期 YYYYMMDD或YYYY-MM-DD
        output_dir: 输出目录，默认为data/daily/
        include_daily_basic: 是否包含每日指标
        include_moneyflow: 是否包含资金流向

    Returns:
        DataFrame包含完整数据
    """
    from .config import DATA_PATH

    # 处理日期格式
    start_date = start_date.replace("-", "")
    end_date = end_date.replace("-", "")

    # 转换代码格式
    ts_code = to_ts_code(code)
    proj_code = format_code(ts_code)
    sec_type = get_sec_type(proj_code)

    # 设置输出目录
    if output_dir is None:
        output_dir = DATA_PATH
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"正在下载 {proj_code} ({sec_type}) 数据...")

    # 1. 获取名称和行业信息
    name = ""
    industry_l1 = ""
    industry_l2 = ""
    industry_l3 = ""

    # 用于存储名称变更历史
    name_history = pd.DataFrame()

    if sec_type == "stock":
        # 获取股票名称变更历史
        name_history = get_stock_name_history(ts_code)
        time.sleep(0.12)

        # 获取申万行业分类（使用 index_member_all 接口获取一二三级行业）
        try:
            industry_info = get_stock_industry(ts_code)
            industry_l1 = industry_info.get("industry_l1_name", "")
            industry_l2 = industry_info.get("industry_l2_name", "")
            industry_l3 = industry_info.get("industry_l3_name", "")
        except Exception as e:
            print(f"  获取行业分类失败: {e}")

    else:  # index
        # 获取指数名称
        name = get_index_name(ts_code)
        time.sleep(0.12)

    # 2. 获取日线行情
    print(f"  获取日线行情...")
    daily_df = get_daily(ts_code, start_date, end_date)
    if daily_df.empty:
        print(f"  未获取到数据")
        return pd.DataFrame()

    time.sleep(0.12)

    # 3. 获取每日指标（仅股票）
    daily_basic_df = pd.DataFrame()
    if include_daily_basic and sec_type == "stock":
        print(f"  获取每日指标...")
        daily_basic_df = get_daily_basic(ts_code, start_date, end_date)
        if not daily_basic_df.empty:
            time.sleep(0.12)

    # 4. 获取资金流向（仅股票）
    moneyflow_df = pd.DataFrame()
    if include_moneyflow and sec_type == "stock":
        print("  获取资金流向...")
        moneyflow_df = get_moneyflow(ts_code, start_date, end_date)
        if not moneyflow_df.empty:
            time.sleep(0.12)

    # 5. 合并所有数据
    print("  合并数据...")
    # 重命名列
    daily_df = daily_df.rename(columns={"trade_date": "date", "vol": "volume"})

    # 尝试合并每日指标
    if not daily_basic_df.empty:
        daily_basic_df = daily_basic_df.rename(columns={"trade_date": "date"})
        # 去除重复列（保留ts_code用于后续处理）
        cols_to_drop = [c for c in ["close"] if c in daily_basic_df.columns]
        daily_basic_df = daily_basic_df.drop(columns=cols_to_drop, errors="ignore")
        daily_df = daily_df.merge(daily_basic_df, on=["ts_code", "date"], how="left")

    # 尝试合并资金流向
    if not moneyflow_df.empty:
        moneyflow_df = moneyflow_df.rename(columns={"trade_date": "date"})
        cols_to_drop = [c for c in ["ts_code"] if c in moneyflow_df.columns]
        moneyflow_df = moneyflow_df.drop(columns=cols_to_drop, errors="ignore")
        daily_df = daily_df.merge(moneyflow_df, on="date", how="left")

    # 6. 格式化日期（先格式化以便处理名称）
    daily_df["date"] = pd.to_datetime(daily_df["date"], format="%Y%m%d").dt.strftime(
        "%Y-%m-%d"
    )

    # 7. 按日期排序
    daily_df = daily_df.sort_values("date").reset_index(drop=True)

    # 8. 根据日期填充名称
    if sec_type == "stock" and not name_history.empty:
        # 使用名称变更历史按日期填充名称
        daily_df["name"] = daily_df["date"].apply(
            lambda d: get_name_at_date(name_history, d)
        )
    else:
        # 使用固定名称
        daily_df["name"] = name

    # 9. 按照指定顺序排列列
    columns_order = [
        "code",
        "ts_code",
        "type",
        "date",
        "name",
        "industry_l1_name",
        "industry_l2_name",
        "industry_l3_name",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "volume",
        "amount",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "pe",
        "pe_ttm",
        "pb",
        "ps",
        "ps_ttm",
        "dv_ratio",
        "dv_ttm",
        "total_mv",
        "circ_mv",
        "free_share",
        "total_share",
        "buy_sm_vol",
        "buy_sm_amount",
        "sell_sm_vol",
        "sell_sm_amount",
        "buy_md_vol",
        "buy_md_amount",
        "sell_md_vol",
        "sell_md_amount",
        "buy_lg_vol",
        "buy_lg_amount",
        "sell_lg_vol",
        "sell_lg_amount",
        "buy_elg_vol",
        "buy_elg_amount",
        "sell_elg_vol",
        "sell_elg_amount",
        "net_mf_vol",
        "net_mf_amount",
    ]

    # 添加code和type列
    daily_df["code"] = proj_code
    daily_df["type"] = sec_type
    daily_df["industry_l1_name"] = industry_l1
    daily_df["industry_l2_name"] = industry_l2
    daily_df["industry_l3_name"] = industry_l3

    # 只保留存在的列
    existing_columns = [col for col in columns_order if col in daily_df.columns]
    daily_df = daily_df[existing_columns]

    # 10. 保存到CSV
    output_file = output_path / f"{proj_code}.csv"
    daily_df.to_csv(output_file, index=False)
    print(f"  已保存 {len(daily_df)} 条记录到 {output_file}")

    return daily_df


def download_batch(
    codes: List[str],
    start_date: str,
    end_date: str,
    output_dir: str = None,
    include_daily_basic: bool = True,
    include_moneyflow: bool = True,
) -> Dict[str, pd.DataFrame]:
    """
    批量下载多只证券数据

    Args:
        codes: 证券代码列表
        start_date: 开始日期
        end_date: 结束日期
        output_dir: 输出目录
        include_daily_basic: 是否包含每日指标
        include_moneyflow: 是否包含资金流向

    Returns:
        字典，键为代码，值为DataFrame
    """
    result = {}
    total = len(codes)

    for i, code in enumerate(codes):
        print(f"\n[{i + 1}/{total}] 处理 {code}...")

        try:
            df = download_data(
                code,
                start_date,
                end_date,
                output_dir,
                include_daily_basic,
                include_moneyflow,
            )
            if not df.empty:
                proj_code = format_code(to_ts_code(code))
                result[proj_code] = df
        except Exception as e:
            print(f"  下载失败: {e}")

        # API限频控制
        time.sleep(0.3)

    return result


# ============================================================
# 数据读取辅助函数
# ============================================================


def load_daily_data(
    code: str, start_date: str = None, end_date: str = None, data_path: Path = None
) -> pd.DataFrame:
    """
    加载日线数据

    Args:
        code: 证券代码（项目格式，如sh600000）
        start_date: 开始日期 YYYY-MM-DD
        end_date: 结束日期 YYYY-MM-DD
        data_path: 数据目录

    Returns:
        DataFrame日线数据
    """
    from .config import DATA_PATH

    if data_path is None:
        data_path = DATA_PATH

    file_path = Path(data_path) / f"{code}.csv"
    if not file_path.exists():
        raise FileNotFoundError(f"数据文件不存在: {file_path}")

    df = pd.read_csv(file_path)
    df["date"] = pd.to_datetime(df["date"])

    # 过滤日期
    if start_date:
        start_date = pd.to_datetime(start_date)
        df = df[df["date"] >= start_date]
    if end_date:
        end_date = pd.to_datetime(end_date)
        df = df[df["date"] <= end_date]

    return df


def get_securities_info(codes: List[str] = None) -> pd.DataFrame:
    """
    获取证券信息汇总

    Args:
        codes: 指定代码列表，为None则获取全部

    Returns:
        DataFrame包含代码、类型、名称、行业等信息
    """
    from .config import DATA_PATH

    daily_path = DATA_PATH
    all_info = []

    for csv_file in daily_path.glob("*.csv"):
        code = csv_file.stem
        df = pd.read_csv(csv_file, nrows=1)

        info = {"code": code}

        # 从数据中获取元信息
        if "type" in df.columns:
            info["type"] = df.iloc[0]["type"]
        else:
            info["type"] = get_sec_type(code)

        if "name" in df.columns:
            info["name"] = df.iloc[0]["name"]

        if "industry_l1_name" in df.columns:
            info["industry_l1_name"] = df.iloc[0].get("industry_l1_name", "")
        if "industry_l2_name" in df.columns:
            info["industry_l2_name"] = df.iloc[0].get("industry_l2_name", "")
        if "industry_l3_name" in df.columns:
            info["industry_l3_name"] = df.iloc[0].get("industry_l3_name", "")

        all_info.append(info)

    # 过滤指定代码
    if codes:
        all_info = [info for info in all_info if info["code"] in codes]

    return pd.DataFrame(all_info)
