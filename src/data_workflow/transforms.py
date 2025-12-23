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