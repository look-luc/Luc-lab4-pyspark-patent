from pyspark import SparkContext, SparkConf
from pyspark import RDD

def parse(csv:RDD[str]):
    header = csv.first()
    return (
        csv.filter(lambda line: line != header)
        .map(lambda line: line.split(","))
        .map(lambda parts: (int(parts[0]), parts[1:]))
    )

def patent_RDD():
    conf = SparkConf().setAppName("Lab4-rdd").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rddCitations = sc.textFile("cite75_99.txt.gz")
    rdd_citations_sample = rddCitations.sample(False, 0.1).cache()
    
    rddPatents = sc.textFile("apat63_99.txt.gz")
    rdd_patents_sample = rddPatents.sample(False, 0.1).cache()

    citations = parse(rdd_citations_sample).cache().take(5)
    patents = parse(rdd_patents_sample).cache().take(5)

    return citations, patents
