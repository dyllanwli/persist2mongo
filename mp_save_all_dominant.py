import ray
import time
from utils.constant import DISTINCT_DOMINANT_SYMBOLS

import rqdatac

from save_all_dominant import SaveDominant

# ['HC', 'JM', 'RB', 'SF', 'SM', 'SS', 'WR']


@ray.remote
def run(future):
    sd = SaveDominant()
    sd.save_all_continue_dominant(future, window=100, ratio=True)
    sd.save_all_continue_dominant(future, window=100, ratio=True)
    sd.save_all_continue_dominant(future, window=100, ratio=True)


def run_parallel():
    ref3 = run.remote("RB")
    time.sleep(20)
    ref1 = run.remote("HC")
    time.sleep(20)
    ref2 = run.remote("JM")
    time.sleep(20)
    refs = [ref1, ref2, ref3]
    while True:
        ready_refs, remining_refs = ray.wait(refs)
        if len(remining_refs) == 0:
            break
        refs = remining_refs
        time.sleep(10)


parallel = True
if parallel:
    ray.init(include_dashboard=False)
    run_parallel()
else:
    sd = SaveDominant()
    # sd.save_all_continue_dominant("RB", window = 100, ratio = True, default_start_date = "20100104")
    sd.save_all_continue_dominant("SF", window=10, ratio=True, default_start_date = "20210201")

print("Done")

# finished 
# I J SF