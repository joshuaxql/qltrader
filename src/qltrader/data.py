"""
QlTrader 量化回测框架 - 数据访问模块

包含 Data 类，提供行情数据查询功能。
支持日线行情、每日指标、资金流向等多维度数据访问。
"""

from datetime import datetime
from typing import Dict, List, Union, Optional
from pathlib import Path
import numpy as np
import pandas as pd


class Data:
    """
    数据访问类

    提供回测过程中的数据查询功能，包括获取当前价格、
    历史数据、判断股票是否可交易等。在handle_data和定时函数中通过data访问。

    支持的数据类型：
    - 日线行情（stored in _data_cache）：开高低收、成交量、成交额等
    - 每日指标（stored in _daily_basic_cache）：PE、PB、市值、换手率等
    - 资金流向（stored in _moneyflow_cache）：主力资金、散户资金等
    """

    def __init__(
        self,
        data_cache: Dict[str, pd.DataFrame],
        current_dt: datetime,
        daily_basic_cache: Optional[Dict[str, pd.DataFrame]] = None,
        moneyflow_cache: Optional[Dict[str, pd.DataFrame]] = None,
    ):
        self._data_cache = data_cache  # 日线行情数据缓存
        self.current_dt = current_dt  # 当前日期时间
        self._daily_basic_cache = daily_basic_cache or {}  # 每日指标缓存
        self._moneyflow_cache = moneyflow_cache or {}  # 资金流向缓存

    def current(
        self,
        securities: Union[str, List[str]],
        fields: Union[str, List[str]] = "close",
        as_df: bool = False,
    ):
        """
        获取当前时刻的行情数据

        获取指定股票在当前日期的指定字段值（如收盘价、开盘价等）。

        Args:
            securities: 股票代码（字符串或列表）
            fields: 数据字段（字符串或列表），默认"close"
                   可选值包括：open, high, low, close, volume等
            as_df: 是否返回DataFrame格式，默认为False（返回字典或标量）

        Returns:
            根据参数不同返回不同格式：
            - as_df=False时:
                - 单股单字段: 标量值（float）
                - 单股多字段: {field: value} 字典
                - 多股单字段: {sec: value} 字典
                - 多股多字段: {sec: {field: value}} 嵌套字典
            - as_df=True时:
                - 返回DataFrame格式，行名为股票代码，列名为字段名

        Example:
            price = data.current("000001.SZ", "close")  # 获取收盘价
            prices = data.current(["000001.SZ", "000002.SZ"], "close")  # 获取多只股票收盘价
            df = data.current(["000001.SZ", "000002.SZ"], ["close", "volume"], as_df=True)  # DataFrame格式
        """
        # 统一转换为列表
        if isinstance(securities, str):
            securities = [securities]
        if isinstance(fields, str):
            fields = [fields]

        result = {}
        for sec in securities:
            # 检查数据是否存在
            if sec not in self._data_cache:
                result[sec] = {f: np.nan for f in fields}
                continue

            df = self._data_cache[sec]
            date_str = self.current_dt.strftime("%Y-%m-%d")

            try:
                row = df[df["date"] == date_str]
                if len(row) == 0:
                    # 当日无数据
                    result[sec] = {f: np.nan for f in fields}
                else:
                    result[sec] = {}
                    for f in fields:
                        result[sec][f] = row.iloc[0][f]
            except Exception:
                result[sec] = {f: np.nan for f in fields}

        # 如果as_df=True，返回DataFrame格式
        if as_df:
            if len(securities) == 1 and len(fields) == 1:
                # 单股单字段：返回Series
                return pd.Series(
                    [result.get(securities[0], {}).get(fields[0], np.nan)],
                    index=[securities[0]],
                    name=fields[0],
                )
            elif len(fields) == 1:
                # 多股单字段
                return pd.Series(
                    {
                        sec: result.get(sec, {}).get(fields[0], np.nan)
                        for sec in securities
                    },
                    name=fields[0],
                )
            else:
                # 多股多字段
                df_data = {}
                for sec in securities:
                    df_data[sec] = result.get(sec, {f: np.nan for f in fields})
                return pd.DataFrame(df_data).T

        # 根据参数数量返回不同格式（默认）
        if len(securities) == 1 and len(fields) == 1:
            return result.get(securities[0], {}).get(fields[0], np.nan)
        elif len(securities) == 1:
            return result.get(securities[0], {f: np.nan for f in fields})
        elif len(fields) == 1:
            return {
                sec: result.get(sec, {}).get(fields[0], np.nan) for sec in securities
            }
        return result

    def history(
        self,
        securities: Union[str, List[str]],
        bar_count: int,
        frequency: str = "1d",
        fields: Union[str, List[str]] = "close",
        as_df: bool = False,
    ):
        """
        获取历史行情数据

        获取指定股票的历史数据，用于技术指标计算等。

        Args:
            securities: 股票代码（字符串或列表）
            bar_count: 获取的K线数量
            frequency: 频率，目前仅支持"1d"（日线）
            fields: 数据字段（字符串或列表），默认"close"
            as_df: 是否返回DataFrame格式，默认为False（返回列表或字典）

        Returns:
            根据参数不同返回不同格式：
            - df=False时:
                - 单股单字段: [value1, value2, ...] 列表
                - 单股多字段: {field: [values]} 字典
                - 多股单字段: {sec: [values]} 字典
                - 多股多字段: {sec: {field: [values]}} 嵌套字典
            - df=True时:
                - 返回DataFrame格式，行为时间序列，列为股票代码或字段名

        Example:
            # 获取最近20天的收盘价
            hist = data.history("000001.SZ", bar_count=20, fields="close")
            # DataFrame格式
            hist_df = data.history("000001.SZ", bar_count=20, fields=["close", "volume"], df=True)
        """
        if isinstance(securities, str):
            securities = [securities]
        if isinstance(fields, str):
            fields = [fields]

        result = {}
        for sec in securities:
            if sec not in self._data_cache:
                result[sec] = {f: [np.nan] * bar_count for f in fields}
                continue

            data_df = self._data_cache[sec].copy()

            try:
                # 设置日期索引并排序
                date_index = pd.to_datetime(data_df["date"])
                data_df = data_df.set_index(date_index)
                data_df = data_df.sort_index()

                # 定位当前日期在数据中的位置
                end_loc = data_df.index.get_indexer([self.current_dt], method="pad")[0]
                start_loc = max(0, end_loc - bar_count + 1)

                result[sec] = {}
                for f in fields:
                    values = data_df.iloc[start_loc : end_loc + 1][f].tolist()
                    # 数据不足时用NaN填充
                    if len(values) < bar_count:
                        values = [np.nan] * (bar_count - len(values)) + values
                    result[sec][f] = values
            except Exception:
                result[sec] = {f: [np.nan] * bar_count for f in fields}

        # 如果as_df=True，返回DataFrame格式
        if as_df:
            if len(securities) == 1 and len(fields) == 1:
                # 单股单字段：返回Series
                return pd.Series(result[securities[0]][fields[0]], name=fields[0])
            elif len(securities) == 1:
                # 单股多字段：返回DataFrame，行为字段，列为日期索引
                data = result[securities[0]]
                return pd.DataFrame(data)
            elif len(fields) == 1:
                # 多股单字段：返回DataFrame，行为股票，列为历史数据
                data = {sec: result[sec][fields[0]] for sec in securities}
                return pd.DataFrame(data).T
            else:
                # 多股多字段：返回Panel格式（使用字典）
                return result

        # 格式化返回值（默认）
        if len(securities) == 1 and len(fields) == 1:
            return result[securities[0]][fields[0]]
        elif len(securities) == 1:
            return result[securities[0]]
        elif len(fields) == 1:
            return {sec: result[sec][fields[0]] for sec in securities}
        return result

    def can_trade(
        self, securities: Union[str, List[str]]
    ) -> Union[bool, Dict[str, bool]]:
        """
        判断股票是否可以交易

        检查股票在当日是否可交易（有数据且成交量>0，不停牌）。

        Args:
            securities: 股票代码（字符串或列表）

        Returns:
            bool或Dict[str, bool]: 单股返回bool，多股返回字典

        Example:
            if data.can_trade("000001.SZ"):
                order_shares("000001.SZ", 1000)
        """
        if isinstance(securities, str):
            return self._can_trade_single(securities)

        return {sec: self._can_trade_single(sec) for sec in securities}

    def _can_trade_single(self, sec: str) -> bool:
        """单只股票交易可行性检查的内部实现"""
        if sec not in self._data_cache:
            return False

        df = self._data_cache[sec]
        date_str = self.current_dt.strftime("%Y-%m-%d")

        try:
            row = df[df["date"] == date_str]
            if len(row) == 0:
                return False
            volume = row.iloc[0]["volume"]
            # 成交量大于0表示可交易（不停牌）
            return volume > 0
        except Exception:
            return False

    def get_daily_basic(
        self,
        securities: Union[str, List[str]],
        fields: Union[str, List[str]] = "pe",
        as_df: bool = False,
    ):
        """
        获取每日指标数据

        获取指定股票在当前日期的估值指标，如PE、PB、市值等。

        Args:
            securities: 股票代码（字符串或列表）
            fields: 指标字段（字符串或列表），默认"pe"
                   常用字段：pe, pe_ttm, pb, ps, ps_ttm,
                            total_mv（总市值）, circ_mv（流通市值）,
                            turnover_rate（换手率）, volume_ratio（量比）等
            as_df: 是否返回DataFrame格式，默认为False

        Returns:
            格式与current()方法类似

        Example:
            pe = data.get_daily_basic("sh600000", "pe")  # 获取PE
            valuation = data.get_daily_basic("sh600000", ["pe", "pb", "total_mv"])
        """
        if isinstance(securities, str):
            securities = [securities]
        if isinstance(fields, str):
            fields = [fields]

        result = {}
        for sec in securities:
            if sec not in self._daily_basic_cache:
                result[sec] = {f: np.nan for f in fields}
                continue

            df = self._daily_basic_cache[sec]
            date_str = self.current_dt.strftime("%Y-%m-%d")

            try:
                row = df[df["trade_date"] == date_str]
                if len(row) == 0:
                    result[sec] = {f: np.nan for f in fields}
                else:
                    result[sec] = {}
                    for f in fields:
                        result[sec][f] = row.iloc[0].get(f, np.nan)
            except Exception:
                result[sec] = {f: np.nan for f in fields}

        if as_df:
            df_data = {}
            for sec in securities:
                df_data[sec] = result.get(sec, {f: np.nan for f in fields})
            return pd.DataFrame(df_data).T

        if len(securities) == 1 and len(fields) == 1:
            return result.get(securities[0], {}).get(fields[0], np.nan)
        elif len(securities) == 1:
            return result.get(securities[0], {f: np.nan for f in fields})
        elif len(fields) == 1:
            return {
                sec: result.get(sec, {}).get(fields[0], np.nan) for sec in securities
            }
        return result

    def get_moneyflow(
        self,
        securities: Union[str, List[str]],
        fields: Union[str, List[str]] = "net_mf_amount",
        as_df: bool = False,
    ):
        """
        获取资金流向数据

        获取指定股票在当前日期的资金流向数据，如主力净流入、散户净流入等。

        Args:
            securities: 股票代码（字符串或列表）
            fields: 资金字段（字符串或列表），默认"net_mf_amount"
                   常用字段：buy_elg_vol（特大单买入量）, buy_lg_vol（大单买入量）,
                            sell_elg_vol（特大单卖出量）, sell_lg_vol（大单卖出量）,
                            net_mf_vol（净流入量）, net_mf_amount（净流入额）等
            as_df: 是否返回DataFrame格式，默认为False

        Returns:
            格式与current()方法类似

        Example:
            net_mf = data.get_moneyflow("sh600000", "net_mf_amount")  # 获取净流入额
            flow = data.get_moneyflow("sh600000", ["buy_lg_vol", "sell_lg_vol", "net_mf_amount"])
        """
        if isinstance(securities, str):
            securities = [securities]
        if isinstance(fields, str):
            fields = [fields]

        result = {}
        for sec in securities:
            if sec not in self._moneyflow_cache:
                result[sec] = {f: np.nan for f in fields}
                continue

            df = self._moneyflow_cache[sec]
            date_str = self.current_dt.strftime("%Y-%m-%d")

            try:
                row = df[df["trade_date"] == date_str]
                if len(row) == 0:
                    result[sec] = {f: np.nan for f in fields}
                else:
                    result[sec] = {}
                    for f in fields:
                        result[sec][f] = row.iloc[0].get(f, np.nan)
            except Exception:
                result[sec] = {f: np.nan for f in fields}

        if as_df:
            df_data = {}
            for sec in securities:
                df_data[sec] = result.get(sec, {f: np.nan for f in fields})
            return pd.DataFrame(df_data).T

        if len(securities) == 1 and len(fields) == 1:
            return result.get(securities[0], {}).get(fields[0], np.nan)
        elif len(securities) == 1:
            return result.get(securities[0], {f: np.nan for f in fields})
        elif len(fields) == 1:
            return {
                sec: result.get(sec, {}).get(fields[0], np.nan) for sec in securities
            }
        return result

    def history_daily_basic(
        self,
        securities: Union[str, List[str]],
        bar_count: int,
        fields: Union[str, List[str]] = "pe",
        as_df: bool = False,
    ):
        """
        获取历史每日指标数据

        Args:
            securities: 股票代码（字符串或列表）
            bar_count: 获取的K线数量
            fields: 指标字段（字符串或列表）
            as_df: 是否返回DataFrame格式

        Returns:
            格式与history()方法类似
        """
        if isinstance(securities, str):
            securities = [securities]
        if isinstance(fields, str):
            fields = [fields]

        result = {}
        for sec in securities:
            if sec not in self._daily_basic_cache:
                result[sec] = {f: [np.nan] * bar_count for f in fields}
                continue

            data_df = self._daily_basic_cache[sec].copy()

            try:
                date_index = pd.to_datetime(data_df["trade_date"])
                data_df = data_df.set_index(date_index)
                data_df = data_df.sort_index()

                end_loc = data_df.index.get_indexer([self.current_dt], method="pad")[0]
                start_loc = max(0, end_loc - bar_count + 1)

                result[sec] = {}
                for f in fields:
                    values = (
                        data_df.iloc[start_loc : end_loc + 1]
                        .get(f, [np.nan] * bar_count)
                        .tolist()
                    )
                    if len(values) < bar_count:
                        values = [np.nan] * (bar_count - len(values)) + values
                    result[sec][f] = values
            except Exception:
                result[sec] = {f: [np.nan] * bar_count for f in fields}

        if as_df:
            if len(securities) == 1 and len(fields) == 1:
                return pd.Series(result[securities[0]][fields[0]], name=fields[0])
            elif len(securities) == 1:
                data = result[securities[0]]
                return pd.DataFrame(data)
            elif len(fields) == 1:
                data = {sec: result[sec][fields[0]] for sec in securities}
                return pd.DataFrame(data).T
            else:
                return result

        if len(securities) == 1 and len(fields) == 1:
            return result[securities[0]][fields[0]]
        elif len(securities) == 1:
            return result[securities[0]]
        elif len(fields) == 1:
            return {sec: result[sec][fields[0]] for sec in securities}
        return result

    def history_moneyflow(
        self,
        securities: Union[str, List[str]],
        bar_count: int,
        fields: Union[str, List[str]] = "net_mf_amount",
        as_df: bool = False,
    ):
        """
        获取历史资金流向数据

        Args:
            securities: 股票代码（字符串或列表）
            bar_count: 获取的K线数量
            fields: 资金字段（字符串或列表）
            as_df: 是否返回DataFrame格式

        Returns:
            格式与history()方法类似
        """
        if isinstance(securities, str):
            securities = [securities]
        if isinstance(fields, str):
            fields = [fields]

        result = {}
        for sec in securities:
            if sec not in self._moneyflow_cache:
                result[sec] = {f: [np.nan] * bar_count for f in fields}
                continue

            data_df = self._moneyflow_cache[sec].copy()

            try:
                date_index = pd.to_datetime(data_df["trade_date"])
                data_df = data_df.set_index(date_index)
                data_df = data_df.sort_index()

                end_loc = data_df.index.get_indexer([self.current_dt], method="pad")[0]
                start_loc = max(0, end_loc - bar_count + 1)

                result[sec] = {}
                for f in fields:
                    values = (
                        data_df.iloc[start_loc : end_loc + 1]
                        .get(f, [np.nan] * bar_count)
                        .tolist()
                    )
                    if len(values) < bar_count:
                        values = [np.nan] * (bar_count - len(values)) + values
                    result[sec][f] = values
            except Exception:
                result[sec] = {f: [np.nan] * bar_count for f in fields}

        if as_df:
            if len(securities) == 1 and len(fields) == 1:
                return pd.Series(result[securities[0]][fields[0]], name=fields[0])
            elif len(securities) == 1:
                data = result[securities[0]]
                return pd.DataFrame(data)
            elif len(fields) == 1:
                data = {sec: result[sec][fields[0]] for sec in securities}
                return pd.DataFrame(data).T
            else:
                return result

        if len(securities) == 1 and len(fields) == 1:
            return result[securities[0]][fields[0]]
        elif len(securities) == 1:
            return result[securities[0]]
        elif len(fields) == 1:
            return {sec: result[sec][fields[0]] for sec in securities}
        return result
