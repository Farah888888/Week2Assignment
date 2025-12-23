import pandas as pd


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame: 
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )


def missingness_report(df: pd.DataFrame) -> pd.DataFrame: 
    total = len(df)
    return(
        df.isna()
        .sum()
        .rename("missing_count")
        .to_frame()
        .assign(p_missing=lambda t: t["missing_count"] / total)
        .sort_values("p_missing", ascending=False)
    )


def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame: 
    out = df.copy()
    for col in cols:
        out[f"{col}__isna"] = out[col].isna()
    return out


def normalize_text(s: pd.Series) -> pd.Series: 
    return s.str.strip().str.casefold().str.replace(r"\s+", " ", regex=True)

def apply_mapping(s: pd.Series, mapping: dict[str, str]) -> pd.Series: 
    return s.map(lambda x: mapping.get(x, x))

def dedupe_keep_latest(df: pd.DataFrame, key_cols: list[str], ts_col: str) -> pd.DataFrame: 
    df_sorted = df.sort_values(by=ts_col, ascending=False)
    deduped_df = df_sorted.drop_duplicates(subset=key_cols, keep='last')
    return deduped_df.sort_index()  

def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame: 
    df[col] = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: df[col]})

def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame: 
    ts = df[ts_col]
    return df.assign(
        **{
            f"{ts_col}_year": ts.dt.year,
            f"{ts_col}_month": ts.dt.month,
            f"{ts_col}_day": ts.dt.day,
            f"{ts_col}_weekday": ts.dt.weekday,
            f"{ts_col}_hour": ts.dt.hour,
        }
    )

def iqr_bounds(s: pd.Series, k: float = 1.5) -> tuple[float, float]: 
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - k * iqr
    upper_bound = q3 + k * iqr
    return lower_bound, upper_bound   

def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series: 
    lower_bound = s.quantile(lo)
    upper_bound = s.quantile(hi)
    return s.clip(lower=lower_bound, upper=upper_bound)

def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame: 
    lower_bound, upper_bound = iqr_bounds(df[col], k=k)
    return df.assign(**{f"{col}__is_outlier": (df[col] < lower_bound) | (df[col] > upper_bound)})

