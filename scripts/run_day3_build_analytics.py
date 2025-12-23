import sys
from pathlib import Path
import json
from datetime import datetime, timezone
import logging

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from data_workflow.io import write_parquet, read_parquet
from data_workflow.config import make_paths
from data_workflow.transforms import (parse_datetime, add_time_parts,
 iqr_bounds, winsorize, add_outlier_flag
 )
from data_workflow.quality import assert_non_empty, assert_unique_key, assert_in_range, require_columns
from data_workflow.joins import safe_left_join

def main():
    p = make_paths(ROOT)
    orders = read_parquet(p.processed / "orders_clean.parquet")
    users = read_parquet(p.processed / "users.parquet")

    assert_unique_key(users, "user_id")

    joined = safe_left_join(
        orders, users, on="user_id", validate="many_to_one", suffixes=(" ", "_user"))

    joined = parse_datetime(joined, "created_at", utc = True)
    joined = add_time_parts(joined, "created_at")

    orders2 = joined.assign(amount_winsor=winsorize(joined["amount"]))
    

    joined = add_outlier_flag(joined, col="amount", k=1.5)
    write_parquet(
        joined.assign(
            amount_winsorized = orders2["amount_winsor"],
        ),
        p.processed / "orders_analytics.parquet"
    )

if __name__ == '__main__':
    main()