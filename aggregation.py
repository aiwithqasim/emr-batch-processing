from pyspark.sql import SparkSession

S3_DATA_INPUT_PATH="s3://emr-batchprocessing-raw-useast1-274614244619-dev/input-source/wikiticker-2015-09-12-sampled.json"
S3_DATA_OUTPUT_PATH_AGGREGATED="s3://emr-batchprocessing-raw-useast1-274614244619-dev/output-destination/aggregated"

def main():
    spark = SparkSession.builder.appName('EMRBathcProcessing1').getOrCreate()
    df = spark.read.json(S3_DATA_INPUT_PATH)
    print(f'The total number of records in the input data set is {df.count()}')
    aggregated_df = df.groupBy(df.channel).count()
    print(f'The total number of records in the aggregated data set is {aggregated_df.count()}')
    aggregated_df.show(10)
    aggregated_df.printSchema()
    aggregated_df.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_AGGREGATED)
    print('The aggregated data has been uploaded successfully')

if __name__ == '__main__':
    main()