
from . import Config
import pandas as pd


db  = Config().persist_db
# pd.DataFrame(db["portfolio"].find_one({"strategy_id": '1'}, {"data": 1, "_id": 0})["data"])
# pd.DataFrame(list(db["trade"].find({"strategy_id": '1'}, {"_id": 0})))
import gridfs
_fs = gridfs.GridFS(db)
value = b",".join([bytes(x) for x in range(100000)])
import datetime
update_time = datetime.datetime.now()
strategy_id = "0"
key = 0
_fs.put(value, strategy_id=strategy_id, key=key, update_time=update_time)
for grid_out in _fs.find({"strategy_id": strategy_id, "key": key, "update_time": {"$lt": update_time}}):
    _fs.delete(grid_out._id)