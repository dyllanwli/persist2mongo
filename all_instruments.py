from .utils import Config

import pymongo
import pandas as pd

# examples
# config = Config()
# persist_db = config.persist_db

# coll = persist_db.option_instruments
# instruments_type = "Option"

def save_all_instruments(df, coll):
    df["de_listed_date"] = pd.to_datetime(df["de_listed_date"], errors="coerce")
    df["listed_date"] = pd.to_datetime(df["listed_date"], errors="coerce")

    df.reset_index(inplace=True)
    # get id column
    data_dict = df.to_dict("records")

    dummy_time = pd.to_datetime("1900-01-01")
    # some dominant future has no de_listed_date in rqdatac, it shows "0000-00-00"

    try: 

        nat = type(df[df["listed_date"].isnull()]["listed_date"].iloc[0])

        df["de_listed_date"] = df["de_listed_date"].apply(
            lambda x: dummy_time if type(x) == nat else x
        )
        df["listed_date"] = df["listed_date"].apply(
            lambda x: dummy_time if type(x) == nat else x
        )
    except:
        print("Didn't find any NaT")

    data_dict = df.to_dict("records")

    coll.create_index(
        [
            ("order_book_id", pymongo.ASCENDING),
            ("de_listed_date", pymongo.ASCENDING),
            ("trading_code", pymongo.ASCENDING),
            ("listed_date", pymongo.ASCENDING),
        ]
    )
    coll.insert_many(data_dict)
