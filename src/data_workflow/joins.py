import pandas as pd


def safe_left_join(left, right, on, validate, suffixes=...): 
    return pd.merge(
        left,
        right,
        how="left",
        on=on,
        validate=validate,
        suffixes=suffixes,
    )   


