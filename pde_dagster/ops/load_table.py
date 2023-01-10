import pandas as pd
from dagster import op, In

from practical_data_engineering_shared.io.load import load_dataframe


@op(name="load_table_op", ins={"df": In(pd.DataFrame)}, required_resource_keys={"postgres_resource"})
def load_table_op(context, df):
    """
    Loads the transformed records to the final destination.
    """
    _, _, conn_str = context.resources.postgres_resource
    load_dataframe(df, pg_conn=conn_str, context=context)
