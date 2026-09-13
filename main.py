from dataframe import patent_DataFrame
from rdd import patent_RDD
def main():
    # df = patent_DataFrame()
    # df.show()
    citation, patent = patent_RDD()


if __name__ == "__main__":
    main()