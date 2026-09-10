from pyspark.sql import SparkSession


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
    citations.createOrReplaceTempView("citations")

    patents = spark.read.load(
        "apat63_99.txt.gz",
        format="csv",
        sep=",",
        header=True,
        compression="gzip",
        inferSchema="true",
    )
    patents.createOrReplaceTempView("patents")

    summary_df = spark.sql(
        """
        WITH SAME_CITING_COUNT AS(
            SELECT
                CITED,
                P_CITED.POSTATE AS CITED_POSTATE,
                CITING,
                P_CITING.POSTATE AS CITING_POSTATE,
                COUNT (P_CITED.PATENT) AS CO_STATE_COUNT
            FROM citations
            JOIN PATENTS P_CITED ON CITED = P_CITED.PATENT
            JOIN PATENTS P_CITING ON CITING = P_CITING.PATENT
            WHERE P_CITING.POSTATE IS NOT NULL
                  AND P_CITING.POSTATE != ''
                  AND P_CITED.POSTATE IS NOT NULL
                  AND P_CITED.POSTATE != ''
                  AND P_CITED.POSTATE = P_CITING.POSTATE
            GROUP BY P_CITING.PATENT
        )
        SELECT
            P.*,
            COALESCE(S.CO_STATE_COUNT, 0) AS CO_STATE
        FROM PATENTS P
        LEFT JOIN SAME_CITING_COUNT S ON P.PATENT = S.CITING
        ORDER BY CO_STATE DESC
        """
    )
