import pandas as pd
import polars as pl

def get_path(state, filename):
    return f"./.{state}_property_parts/{filename}.parquet"

def read_parquet_file(state, filename):
    return pd.read_parquet(get_path(state, filename))

def save_as_csv(state, filename):
    df = read_parquet_file(state, filename)
    df.to_csv(f"{state}_{filename}.csv", index = False)

save_as_csv("tx", "75001")