from dataframe import patent_DataFrame
from rdd import patent_RDD
def main():
    df = patent_DataFrame()
    df.show(5)
    # print(df)

if __name__ == "__main__":
    main()