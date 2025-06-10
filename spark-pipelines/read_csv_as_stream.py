import csv
from typing import Iterable

import pyarrow as pa
from delta.tables import DeltaTable
from deltalake import DeltaTable, write_deltalake

import config
from common import get_pyspark_session
from generate_csv_stream import Order


def read_csv(csv_path: str) -> Iterable[Order]:
    with open(csv_path, newline="") as csvfile:
        order_reader = csv.reader(csvfile, delimiter=",", quotechar="|")
        for row in order_reader:
            print(row)
            yield Order(row[0], row[1], row[2])


def create_df_from_stream(order_iterable):
    df = pa.table(order_iterable).add_column()
    return df


def upsert_csv_to_bronze(df_updates, bronze_dt):
    # api docs here: https://delta-io.github.io/delta-rs/api/delta_table/delta_table_merger/#deltalake.table.TableMerger.when_not_matched_insert
    bronze_dt.merge(
        source=df_updates,
        predicate="target.uuid = source.uuid",
        source_alias="source",
        target_alias="target",
        partition_filter="year = 2025 and month = 6"
    ).when_not_matched_insert(
        updates={
            "ts": "source.ts",
            "uuid": "source.uuid",
            "customer_name": "source.customer_name",
        }
    ).execute()


def get_bronze_delta_table():
    bronze_table = DeltaTable(config.ORDERS_BRONZE_TABLE)
    return bronze_table


def create_delta_table_if_not_exists(spark):

    write_deltalake(config.ORDERS_BRONZE_TABLE, schema={

    })

def run():
    spark = get_pyspark_session()
    create_delta_table_if_not_exists(spark)
    order_iter = read_csv(config.CSV_STREAM_PATH)

    df = create_df_from_stream(order_iter, spark)
    df.printSchema()
    bronze_table = get_bronze_delta_table(spark)
    upsert_csv_to_bronze(order_iter, bronze_table)


if __name__ == "__main__":
    run()
