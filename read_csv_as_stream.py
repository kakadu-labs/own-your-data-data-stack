import csv
from typing import Iterable

from delta.tables import DeltaTable

import config
from common import get_pyspark_session
from generate_csv_stream import Order


def read_csv(csv_path: str) -> Iterable[Order]:
    with open(csv_path, newline="") as csvfile:
        order_reader = csv.reader(csvfile, delimiter=",", quotechar="|")
        for row in order_reader:
            print(row)
            yield Order(row[0], row[1], row[2])


def create_df_from_stream(order_iterable, spark):
    df = spark.createDataFrame(order_iterable)
    return df


def upsert_csv_to_bronze(df_updates, delta_table):
    delta_table.alias("target").merge(
        df_updates.alias("source"), "source.uuid = target.uuid"
    ).whenNotMatchedInsert(
        values={
            "id": "source.uuid",
            "ts": "source.ts",
            "customer_name": "source.customer_name",
        }
    ).execute()


def get_bronze_delta_table(spark):
    bronze_table = DeltaTable.forPath(spark, config.ORDERS_BRONZE_TABLE)
    return bronze_table


def create_delta_table_if_not_exists(spark):
    DeltaTable.createIfNotExists(spark).location(config.ORDERS_BRONZE_TABLE).execute()


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
