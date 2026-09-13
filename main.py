from dataframe import patent_DataFrame
from rdd import patent_RDD
def main():
    # df = patent_DataFrame()
    # df.show()
    results = patent_RDD(takes=5)
    print(f"results: {results}")

if __name__ == "__main__":
    main()