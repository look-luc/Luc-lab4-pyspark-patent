from pyspark import SparkContext, SparkConf

def get_idx(csv, idx_name):
    return csv.zipWithIndex().lookup(idx_name)

def patent_RDD(
        citing_name="citing",
        cited_name="cited",
        patent_name="patent",
        co_state="POSTATE",
        takes:int|None=None
):
    citing_name = citing_name.upper()
    cited_name = cited_name.upper()
    patent_name = patent_name.upper()
    co_state = co_state.upper()

    conf = SparkConf().setAppName("Lab4-rdd").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rddCitations = sc.textFile("cite75_99.txt.gz")
    rdd_citations_sample = rddCitations.sample(False, 0.1).cache()

    citing_idx = get_idx(rdd_citations_sample, citing_name)
    cited_idx = get_idx(rdd_citations_sample, cited_name)

    rddPatents = sc.textFile("apat63_99.txt.gz")
    rdd_patents_sample = rddPatents.sample(False, 0.1).cache()

    patent_idx = get_idx(rdd_patents_sample, patent_name)
    costate_idx = get_idx(rdd_patents_sample, co_state)

    citations_kv = rdd_citations_sample.map(lambda line: line.split(",")) \
              .map(lambda parts: (citing_idx, cited_idx))
    patents_kv = rdd_patents_sample.map(lambda line: line.split(",")) \
              .map(lambda parts: (patent_idx, (parts[1:])))

    citation_join = citations_kv.join(patents_kv)
    cited_join = citation_join.map(lambda line: line.split(",")) \
        .map(lambda parts: (citing_idx, cited_idx))\
        .join(patents_kv)

    filtered_matches = cited_join.filter(
        lambda x: x[1][0] != "" and x[1][1] != "" and x[1][0] == x[1][1]
    )

    same_state_counts = filtered_matches.map(lambda x: (x[0], 1)).reduceByKey(
        lambda a, b: a + b
    )

    patents_by_id = rdd_patents_sample.map(lambda l: (l.split(",")[0], l))
    result = patents_by_id.leftOuterJoin(same_state_counts)

    final_rdd = result.map(
        lambda x: (x[0], x[1][0], x[1][1] if x[1][1] is not None else 0)
    ).sortBy(lambda x: x[2], ascending=False)
    if takes is not None:
        return final_rdd.take(takes)
    else:
        return final_rdd
