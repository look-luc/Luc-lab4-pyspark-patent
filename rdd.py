from pyspark import SparkConf, SparkContext


def patent_RDD(
    citing_name='"CITING"',
    cited_name='"CITED"',
    patent_name='"PATENT"',
    co_state='"POSTATE"',
    takes: int | None = None,
):
    conf = SparkConf().setAppName("Lab4-rdd").setMaster("local[*]")
    sc = SparkContext.getOrCreate(conf=conf)

    rddCitations = sc.textFile("cite75_99.txt.gz")
    rddPatents = sc.textFile("apat63_99.txt.gz")

    citations_header = rddCitations.first().split(",")
    patents_header = rddPatents.first().split(",")

    citing_idx = citations_header.index(citing_name)
    cited_idx = citations_header.index(cited_name)
    patent_idx = patents_header.index(patent_name)
    state_idx = patents_header.index(co_state)

    citations_clean = rddCitations.filter(lambda l: not l.startswith(citing_name))
    patents_clean = rddPatents.filter(lambda l: not l.startswith(patent_name))

    citations_by_cited = citations_clean.map(lambda l: l.split(",")).map(
        lambda p: (p[cited_idx], p[citing_idx])
    )
    patents_state = patents_clean.map(lambda l: l.split(",")).map(
        lambda p: (p[patent_idx], p[state_idx].replace('"', ""))
    )

    cited_joined = citations_by_cited.join(patents_state)

    citing_keyed = cited_joined.map(lambda x: (x[1][0], x[1][1]))

    citing_joined = citing_keyed.join(patents_state)

    filtered_matches = citing_joined.filter(
        lambda x: x[1][0] != "" and x[1][1] != "" and x[1][0] == x[1][1]
    )

    same_state_counts = filtered_matches.map(lambda x: (x[0], 1)).reduceByKey(
        lambda a, b: a + b
    )

    patents_by_id = patents_clean.map(lambda l: (l.split(",")[patent_idx], l))
    result = patents_by_id.leftOuterJoin(same_state_counts)

    final_rdd = result.map(lambda x: (x[1][0], x[1][1] if x[1][1] is not None else 0))\
      .sortBy(lambda x: x[1], ascending=False)\
      .map(lambda x: f"{x[0]},{x[1]}")

    header_string = f'{rddPatents.first()}, "CO_STATE"'
    header_rdd = sc.parallelize([header_string])

    combined_rdd = header_rdd.union(final_rdd)
    if takes is not None:
        return combined_rdd.take(takes)
    return combined_rdd