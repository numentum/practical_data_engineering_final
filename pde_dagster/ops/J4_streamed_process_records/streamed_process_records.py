import json
from dagster import op
import psycopg2
import pandas as pd
from practical_data_engineering_shared.io.extract import extract_table
from practical_data_engineering_shared.io.load import load_dataframe
from practical_data_engineering_shared.process_transactions.pos import transform_pos_transactions
from practical_data_engineering_shared.process_transactions.online import transform_online_transactions
from practical_data_engineering_shared.process_transactions.crypto import transform_crypto_transactions


@op(name="streamed_process_records_op", required_resource_keys={"postgres_resource"})
def streamed_process_records_op(context):
    _, _, conn_str = context.resources.postgres_resource
    query_str = context.op_config["query_str"]

    products_df = extract_table("products")

    cursor_factory = psycopg2.extras.RealDictCursor

    if "online_transactions" in query_str:
        cursor_factory = psycopg2.extras.DictCursor

    with psycopg2.connect(conn_str) as conn:
        with conn.cursor(name="server_side_cursor", cursor_factory=cursor_factory) as cursor:
            cursor.itersize = 500
            write_batch_size = 500

            cursor.execute(query_str)

            dfs = []
            i = 0

            for record in cursor:
                if "online_transactions" in query_str:
                    record = [json.dumps(r) for r in record]

                df_of_record = pd.DataFrame(record, index=[0], columns=[desc[0] for desc in cursor.description])

                if "pos_transactions" in query_str:
                    transformed_df = transform_pos_transactions(df_of_record, products_df=products_df)
                elif "online_transactions" in query_str:
                    transformed_df = transform_online_transactions(df_of_record, products_df=products_df)
                elif "crypto_transactions" in query_str:
                    transformed_df = transform_crypto_transactions(df_of_record, products_df=products_df)
                else:
                    raise ValueError(f"Table name in query string '{query_str}' is not supported.")

                dfs.append(transformed_df)

                if len(dfs) > write_batch_size:
                    df = pd.concat(dfs)
                    load_dataframe(df, conn_str)
                    i += len(df)
                    context.log.info(f"Loaded {i} records so far")
                    dfs = []

            df = pd.concat(dfs)
            load_dataframe(df, conn_str)
            i += len(df)
