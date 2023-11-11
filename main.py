from pyspark.sql import SparkSession

S3_DATA_INPUT_PATH="<<S3 bucket link to dataset>>/wikiticker-2015-09-12-sampled.json"
S3_DATA_OUTPUT_PATH_FILTERED="<<s3 bucket link to output>>/output-destination/filtered"

def main():
    spark = SparkSession.builder.appName('EMRBathcProcessing').getOrCreate()
    df = spark.read.json(S3_DATA_INPUT_PATH)
    print(f'The total number of records in the source data set is {df.count()}')
    filtered_df = df.filter((df.isRobot == False) & (df.countryName == 'United States'))
    print(f'The total number of records in the filtered data set is {filtered_df.count()}')
    filtered_df.show(10)
    filtered_df.printSchema()
    filtered_df.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_FILTERED)
    print('The filtered output is uploaded successfully')

if __name__ == '__main__':
    main()
