from pyspark import SparkContext, SparkConf
from pyspark import RDD

def parse(csv:RDD[str], sc:SparkContext):
    header_string = csv.first()
    data_rows = csv.filter(lambda line: line != header_string)

    processed_data = data_rows.map(lambda line: line.split(",")).map(
        lambda parts: (int(parts[0]), parts[1:])
    )

    csv_data_rows = processed_data.map(lambda kv: f"{kv[0]},{','.join(kv[1])}")

    header_rdd = sc.parallelize([header_string])
    return header_rdd.union(csv_data_rows)

def patent_RDD():
    conf = SparkConf().setAppName("Lab4-rdd").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rddCitations = sc.textFile("cite75_99.txt.gz")
    rdd_citations_sample = rddCitations.sample(False, 0.1).cache()
    
    rddPatents = sc.textFile("apat63_99.txt.gz")
    rdd_patents_sample = rddPatents.sample(False, 0.1).cache()

    citations = parse(csv=rdd_citations_sample, sc=sc).cache().take(5)
    patents = parse(csv=rdd_patents_sample, sc=sc).cache().take(5)

    return citations, patents
