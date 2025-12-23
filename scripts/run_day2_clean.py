import sys
from pathlib import Path
import json
from datetime import datetime, timezone
import logging

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from data_workflow.io import read_orders_csv, read_users_csv, write_parquet, read_parquet
from data_workflow.config import make_paths
from data_workflow.transforms import enforce_schema, dedupe_keep_latest, missingness_report, add_missing_flags, normalize_text, apply_mapping
from data_workflow.quality import assert_non_empty, assert_unique_key, assert_in_range, require_columns



def main():
    p = make_paths(ROOT)
    orders_df = read_orders_csv(p.raw / "orders.csv")
    users_df = read_users_csv(p.raw / "users.csv")

    require_columns(orders_df, ["order_id","user_id","amount","quantity","created_at","status"])
    require_columns(users_df, ["user_id","country","signup_date"])
    assert_non_empty(orders_df, "orders_df")
    assert_non_empty(users_df, "users_df")

    orders = enforce_schema(orders_df)

    rep = missingness_report(orders)
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    rep_path = reports_dir / "missingness_orders.csv"
    rep.to_csv(rep_path, index=True)

    status_norm = normalize_text(orders["status"])
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    status_clean = apply_mapping(status_norm, mapping)

    orders_clean = (
        orders.assign(status_clean=status_clean)
              .pipe(add_missing_flags, cols=["amount", "quantity"])
    )

    write_parquet(orders_clean, p.processed / "orders_clean.parquet")
    write_parquet(users_df, p.processed / "users.parquet")

if __name__ == '__main__':
    main()


