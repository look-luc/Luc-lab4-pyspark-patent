from pyspark.sql import SparkSession
from typing import Literal
from rdd import RDD


def DataFrame(citations:Literal[RDD[str]]):
    spark = (
        SparkSession.builder.appName("Lab4-Dataframe").master("local[*]").getOrCreate()
    )
    citations = spark.read.load(
        "cite75_99.txt.gz",
        format="csv",
        sep=",",
        header=True,
        compression="gzip",
        inferSchema="true",
    )
    patents = spark.read.load(
        "apat63_99.txt.gz",
        format="csv",
        sep=",",
        header=True,
        compression="gzip",
        inferSchema="true",
    )
