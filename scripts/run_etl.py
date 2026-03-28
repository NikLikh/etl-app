import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from etl.pipeline import run_incremental


result = run_incremental()
print(result)
