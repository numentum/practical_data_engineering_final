from dagster import op
from practical_data_engineering_shared.io.drive import parallel_load_files_to_df


@op(name="parallel_load_files_op")
def parallel_load_files_op(context):
    df = parallel_load_files_to_df(context=context)

    return df
