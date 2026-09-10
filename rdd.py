from pyspark import SparkContext, SparkConf


def patent_RDD():
    conf = SparkConf().setAppName("Lab4-rdd").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rddCitations = sc.textFile("cite75_99.txt.gz")
    rddPatents = sc.textFile("apat63_99.txt.gz")
    return rddCitations, rddPatents

if __name__ == "__main__":
    citations, patents = RDD()
    print(f"citation type: {type(citations)}\npatent type: {type(patents)}")