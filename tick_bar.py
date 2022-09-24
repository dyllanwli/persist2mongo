from typing import List, Optional
from datetime import timedelta

from tqdm import tqdm
from pytz import timezone

from rqdatac.services.future import get_dominant_price, get_dominant

from vnpy.trader.object import HistoryRequest, Interval, Exchange
from vnpy.trader.setting import SETTINGS
from vnpy.trader.utility import load_json

import vnpy_rqdata
from vnpy_mongodb import Database as MongodbDatabase
from vnpy.trader.object import BarData, TickData

from pymongo import InsertOne, DeleteOne, ReplaceOne
from pymongo import ASCENDING
from pymongo.database import Database
from pymongo.collection import Collection


class TickDatabase(MongodbDatabase):
    def __init__(
        self,
        database_name: str = None,
        tick_collection_name: str = "tick_data",
        bar_collection_name: str = "bar_data",
    ):
        SETTING_FILENAME: str = "vt_setting.json"
        SETTINGS.update(load_json(SETTING_FILENAME))

        super().__init__()

        if database_name:
            # Initialize database
            self.db: Database = self.client[database_name]

        self.progress_collection = self.db["progress"]
        self.progress_collection.create_index(
            [
                ("symbol", ASCENDING),
                ("datetime", ASCENDING),
            ],
            unique=True,
        )

        if tick_collection_name:
            # Initialize tick data collection
            self.tick_collection: Collection = self.db[tick_collection_name]
            self.tick_collection.create_index(
                [
                    ("exchange", ASCENDING),
                    ("symbol", ASCENDING),
                    ("datetime", ASCENDING),
                ],
                unique=True,
            )
            self.tick_collection.create_index([("symbol", ASCENDING)])
            try:
                self.tick_collection.create_index([("datetime", ASCENDING)])
            except:
                self.tick_collection.drop_index("datetime_1")
                self.tick_collection.create_index([("datetime", ASCENDING)])
        if bar_collection_name:
            # Initialize bar data collection
            self.bar_collection: Collection = self.db[bar_collection_name]
            self.bar_collection.create_index(
                [
                    ("exchange", ASCENDING),
                    ("symbol", ASCENDING),
                    ("interval", ASCENDING),
                    ("datetime", ASCENDING),
                ],
                unique=True,
            )
            self.tick_collection.create_index([("symbol", ASCENDING)])
            try:
                self.tick_collection.create_index([("datetime", ASCENDING)])
            except:
                self.bar_collection.drop_index("datetime_1")
                self.tick_collection.create_index([("datetime", ASCENDING)])

    def save_tick_data_bulk(self, ticks: List[TickData]) -> bool:
        """
        Method override: 保存TICK数据
        """
        requests = []
        print("Start saving tick data to mongodb...")
        for tick in ticks:
            filter = {
                "symbol": tick.symbol,
                "exchange": tick.exchange.value,
                "datetime": tick.datetime,
            }

            d = {
                "symbol": tick.symbol,
                "exchange": tick.exchange.value,
                "datetime": tick.datetime,
                "name": tick.name,
                "volume": tick.volume,
                "turnover": tick.turnover,
                "open_interest": tick.open_interest,
                "last_price": tick.last_price,
                "last_volume": tick.last_volume,
                "limit_up": tick.limit_up,
                "limit_down": tick.limit_down,
                "open_price": tick.open_price,
                "high_price": tick.high_price,
                "low_price": tick.low_price,
                "pre_close": tick.pre_close,
                "bid_price_1": tick.bid_price_1,
                "bid_price_2": tick.bid_price_2,
                "bid_price_3": tick.bid_price_3,
                "bid_price_4": tick.bid_price_4,
                "bid_price_5": tick.bid_price_5,
                "ask_price_1": tick.ask_price_1,
                "ask_price_2": tick.ask_price_2,
                "ask_price_3": tick.ask_price_3,
                "ask_price_4": tick.ask_price_4,
                "ask_price_5": tick.ask_price_5,
                "bid_volume_1": tick.bid_volume_1,
                "bid_volume_2": tick.bid_volume_2,
                "bid_volume_3": tick.bid_volume_3,
                "bid_volume_4": tick.bid_volume_4,
                "bid_volume_5": tick.bid_volume_5,
                "ask_volume_1": tick.ask_volume_1,
                "ask_volume_2": tick.ask_volume_2,
                "ask_volume_3": tick.ask_volume_3,
                "ask_volume_4": tick.ask_volume_4,
                "ask_volume_5": tick.ask_volume_5,
                "localtime": tick.localtime,
            }
            requests.append(ReplaceOne(filter, d, upsert=True))

        result = self.tick_collection.bulk_write(requests, ordered=False)
        return True


class DominantDatafeed(vnpy_rqdata.Datafeed):
    def __init__(self):
        SETTING_FILENAME: str = "/h/diya.li/quant/rqscripts/vt_setting.json"
        SETTINGS.update(load_json(SETTING_FILENAME))
        super().__init__()

    def query_dominant_tick_history(
        self, req: HistoryRequest
    ) -> Optional[List[TickData]]:
        """
        Method Override: 查询Tick数据
        """
        if not self.inited:
            n = self.init()
            if not n:
                return []

        symbol = req.symbol
        exchange = req.exchange
        start = req.start
        end = req.end

        # 为了查询夜盘数据
        end += timedelta(1)

        # 只对衍生品合约才查询持仓量数据
        fields = [
            "open",
            "high",
            "low",
            "last",
            "prev_close",
            "volume",
            "total_turnover",
            "limit_up",
            "limit_down",
            "b1",
            "b2",
            "b3",
            "b4",
            "b5",
            "a1",
            "a2",
            "a3",
            "a4",
            "a5",
            "b1_v",
            "b2_v",
            "b3_v",
            "b4_v",
            "b5_v",
            "a1_v",
            "a2_v",
            "a3_v",
            "a4_v",
            "a5_v",
        ]
        if not symbol.isdigit():
            fields.append("open_interest")

        df = get_dominant_price(
            symbol,
            frequency="tick",
            fields=fields,
            start_date=start,
            end_date=end,
            adjust_type="pre",
            adjust_method="prev_close_ratio",
        )

        data: List[TickData] = []
        print("Start packaging tick data...")
        if df is not None:
            for ix, row in df.iterrows():
                dt = row.name[1].to_pydatetime()
                dt = timezone("Asia/Shanghai").localize(dt)

                tick = TickData(
                    symbol=symbol,
                    exchange=exchange,
                    datetime=dt,
                    open_price=row["open"],
                    high_price=row["high"],
                    low_price=row["low"],
                    pre_close=row["prev_close"],
                    last_price=row["last"],
                    volume=row["volume"],
                    turnover=row["total_turnover"],
                    open_interest=row.get("open_interest", 0),
                    limit_up=row["limit_up"],
                    limit_down=row["limit_down"],
                    bid_price_1=row["b1"],
                    bid_price_2=row["b2"],
                    bid_price_3=row["b3"],
                    bid_price_4=row["b4"],
                    bid_price_5=row["b5"],
                    ask_price_1=row["a1"],
                    ask_price_2=row["a2"],
                    ask_price_3=row["a3"],
                    ask_price_4=row["a4"],
                    ask_price_5=row["a5"],
                    bid_volume_1=row["b1_v"],
                    bid_volume_2=row["b2_v"],
                    bid_volume_3=row["b3_v"],
                    bid_volume_4=row["b4_v"],
                    bid_volume_5=row["b5_v"],
                    ask_volume_1=row["a1_v"],
                    ask_volume_2=row["a2_v"],
                    ask_volume_3=row["a3_v"],
                    ask_volume_4=row["a4_v"],
                    ask_volume_5=row["a5_v"],
                    gateway_name="RQ",
                )

                data.append(tick)

        return data


class TickBarData:
    """
    Query and save
    """

    def __init__(self, datafeed = None):
        if datafeed is None:
            self.datafeed = DominantDatafeed()
            self.datafeed.init()
        else:
            self.datafeed = datafeed

    def query_save_tick_bar_data(
        self, req, vndb: TickDatabase, verbose=True, ratio=False
    ):
        """
        Query and save tick and bar data to mongo
        To init datafeed:
                datafeed = vnpy_rqdata.Datafeed()
                datafeed.init()

        Req example:
            req = HistoryRequest(
                symbol="SC2203",
                exchange=Exchange.INE,
                start=pd.to_datetime("2019-03-02"),
                end=pd.to_datetime("2022-02-28"),
                interval=Interval.MINUTE,
            )
        """
        data = None
        if verbose:
            print("Querying and saving {} data".format(req.interval))
        if req.interval == Interval.TICK:
            if ratio:
                data = self.datafeed.query_dominant_tick_history(req)
                print("Getting {} tick data".format(len(data)))
                vndb.save_tick_data_bulk(data)
            else:
                data = self.datafeed.query_tick_history(req)
                vndb.save_tick_data(data)
        else:
            data = self.datafeed.query_bar_history(req)
            vndb.save_bar_data(data)

        return True
