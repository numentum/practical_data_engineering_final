from dagster import op
from practical_data_engineering_shared.io.drive import load_files_to_df


@op(name="load_files_op")
def load_files_op(context):
    df = load_files_to_df(verbose=True, context=context)

    return df
