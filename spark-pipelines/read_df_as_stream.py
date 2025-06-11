from pyspark.sql import functions as F

from config import BRONZE_INGEST_PATH
from data_generator import get_pyspark_session


def foreach_batch_function(batch_df, batch_id):
    # business logic here
    batch_df.show(10)
    # merge, append, insert to data consumers.


def main():
    spark = get_pyspark_session()
    bronze_df = spark.readStream.format("delta").load(BRONZE_INGEST_PATH)
    df_transformed = bronze_df.where(F.col("label") == "bar")
    df_transformed.writeStream.format("console").option(
        "checkpointLocation", "ckpts/console_writer"
    ).foreachBatch(foreach_batch_function).trigger(once=True).start().awaitTermination()


if __name__ == "__main__":
    main()
