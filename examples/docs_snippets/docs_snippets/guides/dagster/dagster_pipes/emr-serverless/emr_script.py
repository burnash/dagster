import os
import sys

import boto3
import pyspark.sql.functions as F
from dagster_pipes import (
    PipesCliArgsParamsLoader,
    PipesS3ContextLoader,
    open_dagster_pipes,
)
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    output_path = None

    if len(sys.argv) > 1:
        output_path = sys.argv[1]
    else:
        print(  # noqa
            "S3 output location not specified printing top 10 results to output stream"
        )

    region = os.getenv("AWS_REGION")
    text_file = spark.sparkContext.textFile(
        "s3://" + region + ".elasticmapreduce/emr-containers/samples/wordcount/input"
    )
    counts = (
        text_file.flatMap(lambda line: line.split(" "))
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], False)
    )
    counts_df = counts.toDF(["word", "count"])

    if output_path:
        counts_df.write.mode("overwrite").csv(output_path)
        print(  # noqa
            "WordCount job completed successfully. Refer output at S3 path: "
            + output_path
        )
    else:
        counts_df.show(10, False)
        print("WordCount job completed successfully.")  # noqa

    spark.stop()


if __name__ == "__main__":
    """
        Usage: wordcount [destination path]
    """

    with open_dagster_pipes() as pipes:
        pipes.log.info("Hello from AWS EMR Serverless job!")
        pipes.report_asset_materialization(
            metadata={"some_metric": {"raw_value": 0, "type": "int"}},
            data_version="alpha",
        )
        main()
