import pandas as pd

def split_valid_and_quarantine(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:

    valid = df[df["dq_is_valid"]].copy()

    quarantine = df[~df["dq_is_valid"]].copy()

    return valid, quarantine