from .utils import Config

import pymongo
import pandas as pd


from tqdm import tqdm

# examples
# config = Config()
# persist_db = config.persist_db

# coll = persist_db.option_instruments
# instruments_type = "Option"

# df = rqdatac.all_instruments(type="Future", market='cn', date=None)

def save_instruments(instruments, coll):
    data_dicts = []
    for x in tqdm(instruments):
        # process instruments
        data_dict = dict((key, value) for key, value in x.__dict__.items() if not callable(value) and not key.startswith('__'))
        data_dict["de_listed_date"] = pd.to_datetime(data_dict["de_listed_date"], errors="coerce")
        data_dict["listed_date"] = pd.to_datetime(data_dict["listed_date"], errors="coerce")

        try: 
            dummy_time = pd.to_datetime("1900-01-01")
            # some dominant future has no de_listed_date in rqdatac, it shows "0000-00-00"

            if data_dict['de_listed_date'] == "0000-00-00":
                data_dict['de_listed_date'] = dummy_time
            if data_dict['listed_date'] == "0000-00-00":
                data_dict['listed_date'] = dummy_time

        except:
            print("Didn't find any NaT")
        data_dicts.append(data_dict)

    coll.create_index(
        [
            ("order_book_id", pymongo.ASCENDING),
            ("trading_code", pymongo.ASCENDING),
            ("type", pymongo.TEXT),
            ("underlying_order_book_id", pymongo.ASCENDING),
            ("de_listed_date", pymongo.ASCENDING),
            ("trading_code", pymongo.ASCENDING),
            ("listed_date", pymongo.ASCENDING),
        ]
    )
    coll.insert_many(data_dicts)
    print("done")