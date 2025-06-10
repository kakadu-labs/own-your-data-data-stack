import logging
import time
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from common import get_pyspark_session
from config import BRONZE_INGEST_PATH, N_SECONDS_SLEEP

logger = logging.getLogger(__name__)


def create_new_df(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(
        [
            (
                1,
                "foo",
                datetime.now(),
            ),  # create your data here, be consistent in the types.
            (2, "bar", datetime.now()),
        ],
        ["id", "label", "ts"],  # add your column names here
    )
    return df


def append_dataframe(df, path):
    logging.info(f"Appending new Data to Dataframe. with Path: {path}")
    # merge upsert etc. here!
    # Dont overwrite -> no streaming.
    df.write.format("delta").mode("append").save(path)


def run():
    spark = get_pyspark_session()
    # every n seconds append random rows to dataframe.
    while True:
        new_df = create_new_df(spark)
        append_dataframe(new_df, BRONZE_INGEST_PATH)
        time.sleep(N_SECONDS_SLEEP)


if __name__ == "__main__":
    run()
