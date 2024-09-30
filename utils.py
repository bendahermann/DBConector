from pyspark.sql import (functions as F, types as T, 
    Row, Window as W, DataFrame as spk_DF, Column) 
from datetime import timedelta, datetime
import pandas as pd

#def format_date(df, cols: list, dt_format: str):
#    for c in cols:
#        df = df.withColumn(c, F.to_date(c, dt_format))
#    return df

def format_date(df, cols: list, dt_format: str):
    for c in cols:
        df[c] = pd.to_datetime(df[c], format=dt_format)  # Formatear a fecha
    return df