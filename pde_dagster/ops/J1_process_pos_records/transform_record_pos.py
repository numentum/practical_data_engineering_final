import pandas as pd
from dagster import op, In

from practical_data_engineering_shared.process_transactions.pos import transform_pos_transactions


@op(name="transform_record_pos_op", ins={"df": In(pd.DataFrame)}, description="Transforms a record")
def transform_record_pos_op(context, df):
    df = transform_pos_transactions(df)
    return df
