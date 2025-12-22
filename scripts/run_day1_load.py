import sys
from pathlib import Path
import json
from datetime import datetime, timezone
import logging

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
logger = logging.getLogger(__name__)

from data_workflow.io import read_orders_csv, read_users_csv, write_parquet, read_parquet
from data_workflow.config import make_paths
from data_workflow.transforms import enforce_schema

log = logging.getLogger(__name__)



def main():
    p = make_paths(ROOT)
    df = read_orders_csv(p.raw / "orders.csv")
    df = enforce_schema(df) 
    write_parquet(df, p.processed / "orders.parquet")   


if __name__ == '__main__':
    main()

