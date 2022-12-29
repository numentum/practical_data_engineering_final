import pandas as pd
from dagster import op, In

from practical_data_engineering_shared.process_transactions.pos import transform_pos_transactions
from practical_data_engineering_shared.process_transactions.online import transform_online_transactions
from practical_data_engineering_shared.process_transactions.crypto import transform_crypto_transactions


@op(name="transform_record_op", ins={"df": In(pd.DataFrame)}, required_resource_keys={"job_config"}, description="Transforms a record")
def transform_record_op(context, df):
    table_name = context.resources.job_config["table_name"]

    if table_name == "pos_transactions":
        df = transform_pos_transactions(df)
    elif table_name == "online_transactions":
        df = transform_online_transactions(df)
    elif table_name == "crypto_transactions":
        df = transform_crypto_transactions(df)
    else:
        raise ValueError(f"Table {table_name} is not supported.")

    return df
