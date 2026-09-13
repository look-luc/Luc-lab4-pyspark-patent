from pyspark import SparkContext, SparkConf


def get_idx(csv, idx_name):
    return csv.zipWithIndex().lookup(idx_name)


def patent_RDD(
    citing_name="citing",
    cited_name="cited",
    patent_name="patent",
    co_state="POSTATE",
    takes: int | None = None,
):
    conf = SparkConf().setAppName("Lab4-rdd").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rddCitations = sc.textFile("cite75_99.txt.gz")
    rdd_citations_sample = rddCitations.sample(False, 0.1).cache()

    citations_header = rdd_citations_sample.first().split(",")

    citing_idx = citations_header.index(citing_name)
    cited_idx = citations_header.index(cited_name)

    citing_idx = get_idx(rdd_citations_sample, citing_name)
    cited_idx = get_idx(rdd_citations_sample, cited_name)

    rddPatents = sc.textFile("apat63_99.txt.gz")
    rdd_patents_sample = rddPatents.sample(False, 0.1).cache()
    patents_header = rdd_patents_sample.first().split(",")

    patent_idx = patents_header.index(patent_name)
    state_idx = patents_header.index(co_state)

    citations_by_cited = rdd_citations_sample.map(lambda l: l.split(",")).map(
        lambda p: (p[cited_idx], p[citing_idx])
    )
    patents_state = rddPatents.map(lambda l: l.split(",")).map(
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

    patents_by_id = rdd_patents_sample.map(lambda l: (l.split(",")[patent_idx], l))
    result = patents_by_id.leftOuterJoin(same_state_counts)

    final_rdd = result.map(
        lambda x: (x[0], x[1][0], x[1][1] if x[1][1] is not None else 0)
    ).sortBy(lambda x: x[2], ascending=False)

    if takes is not None:
        return final_rdd.take(takes)
    return final_rdd
