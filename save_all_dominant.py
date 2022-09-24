import time
from datetime import timedelta, datetime

import pandas as pd
from rqdatac import futures
import rqdatac
from rqdatac.services.basic import instruments
from rqdatac.services.future import get_dominant

from vnpy.trader.object import HistoryRequest, Interval, Exchange

from tick_bar import TickBarData, TickDatabase

class SaveDominant:
    def __init__(self):
        self.td = TickBarData()

    def get_start_end_date(
        self, dominant: str, tickDatabase: TickDatabase, window: int = 100, default_start_date: str = None
    ):
        # Get last checkpoint datetime
        filter = {"symbol": dominant}
        sort = list({"datetime": -1}.items())
        last_checkpoint = tickDatabase.progress_collection.find_one(filter, sort=sort)

        window = timedelta(days=window)
        start_date = None
        if last_checkpoint:
            start_date = last_checkpoint["datetime"]
        elif default_start_date == None:
            start_date = pd.to_datetime(self.dominant_list.index.values[0])
        else:
            print("Using default start date.")
            start_date = pd.to_datetime(default_start_date)
        end_date = start_date + window
        return start_date, end_date

    def save_all_continue_dominant(
        self,
        dominant: str,
        window: int = 10,
        interval: Interval = Interval.TICK,
        ratio: bool = False,
        default_start_date: str = None,
    ):
        """
        Save all continue dominant futures to mongodb.
        """

        tickDatabase = TickDatabase(
            database_name="pre_ratio_futures" if ratio else "dominant_futures",
            tick_collection_name="tick_data",
            bar_collection_name="bar_data",
        )

        self.dominant_list = get_dominant(underlying_symbol=dominant, market="cn")

        start_date, end_date = self.get_start_end_date(dominant, tickDatabase, window, default_start_date)
        print(
            "Start saving {} futures from {} to {}".format(
                dominant, start_date, end_date
            )
        )

        inst = instruments(dominant + "888", market="cn")
        exchange = Exchange[inst.exchange]
        req = HistoryRequest(
            symbol=dominant if ratio else dominant + "888",
            exchange=exchange,
            start=start_date,
            end=end_date,
            interval=interval,
        )
        if self.td.query_save_tick_bar_data(req, tickDatabase, ratio=ratio):
            tickDatabase.progress_collection.insert_one(
                {
                    "symbol": dominant,
                    "datetime": end_date,
                    "updatedAt": datetime.utcnow(),
                }
            )
        return "done."

    def save_all_dominant(self, dominant: str):
        """
        Save all dominant futures to mongodb.
        Dominant e.g.
            database_name = "dominant_futures
            dominant = "lh".lower()  # use lower case for database
        """

        database_name = "vnpy_futures"
        tick_collection_name = "{}_tick_data".format(dominant)
        bar_collection_name = "{}_bar_data".format(dominant)

        tickDatabase = TickDatabase(
            database_name=database_name,
            tick_collection_name=tick_collection_name,
            bar_collection_name=bar_collection_name,
        )

        td = TickBarData()

        dominant_future = futures.get_dominant(
            dominant.upper()
        )  # use upper case for rqdatac
        print(
            "Start saving {} futures to mongodb, num: {}".format(
                dominant, len(dominant_future)
            )
        )

        for symbol, date in zip(dominant_future, dominant_future.index):

            progress_filter = {"symbol": symbol, "datetime": date}
            if tickDatabase.progress_collection.count_documents(progress_filter) > 0:
                print("{} found on {}, skipping.".format(symbol, date))
                tickDatabase.progress_collection.find_one_and_update(
                    progress_filter, {"$set": {"updatedAt": datetime.utcnow()}}
                )
                continue

            inst = rqdatac.instruments(symbol, market="cn")
            exchange = Exchange[inst.exchange]
            req = HistoryRequest(
                symbol=symbol.upper(),
                exchange=exchange,
                start=date,
                end=date + timedelta(days=1),
                interval=Interval.TICK,
            )
            try:
                td.query_save_tick_bar_data(req, tickDatabase, bulk=True)
            except rqdatac.share.errors.QuotaExceeded:
                t = datetime.today()
                future = datetime(t.year, t.month, t.day, 11, 00, 5)
                if t.timestamp() > future.timestamp():
                    future += timedelta(days=1)
                print("Quota exceeded, sleeping until {}".format(future))
                time.sleep((future - t).total_seconds())
            tickDatabase.progress_collection.insert_one(
                {"symbol": symbol, "datetime": date, "updatedAt": datetime.utcnow()}
            )

        return "done."
