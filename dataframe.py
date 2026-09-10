from pyspark.sql import SparkSession
from pyspark.sql.functions import count, coalesce, lit, col


def patent_DataFrame():
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
    p_cited = patents.alias("P_CITED")
    p_citing = patents.alias("P_CITING")

    matched = citations.join(
        p_cited,
        citations["CITED"] == p_cited["PATENT"],
    ).join(
        p_citing,
        citations["CITING"] == p_citing["PATENT"],
    )

    filtered = matched.filter(
        col("P_CITED.POSTATE").isNotNull() & col("P_CITING.POSTATE").isNotNull() & \
        (col("P_CITED.POSTATE") == col("P_CITING.POSTATE"))
    )

    same_State_counts = filtered\
        .groupBy("CITING")\
        .agg(count(p_cited["PATENT"]).alias("same_State_count"))
    result = patents.join(
        same_State_counts,
        citations["CITING"] == same_State_counts["CITING"],
        how="left"
    ).select(
        patents["*"],
        coalesce(
            same_State_counts["CO_STATE_COUNT"],
            lit(0)
        ).alias("CO_STATE"),
    ).orderBy(col("CO_STATE").desc())
    return result