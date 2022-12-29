import pandas as pd
from dagster import op, In

from practical_data_engineering_shared.process_transactions.market import transform_market_transactions


@op(name="transform_market_transactions_op", ins={"df": In(pd.DataFrame)})
def transform_market_transactions_op(context, df):
    df, errors = transform_market_transactions(df)
    return df
