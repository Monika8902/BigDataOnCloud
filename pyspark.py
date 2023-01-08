from pyspark.sql import SparkSession
from pyspark.sql.functions import col 

S3_DATA_SOURCE_PATH = 's3://demo8902/input/survey_results_public.csv'
S3_DATA_OUTPUT_PATH ='s3://demo8902/output/'

def main():
    spark = SparkSession.builder.appName('MonikaProject').getOrCreate()
    all_data = spark.read.csv(S3_DATA_SOURCE_PATH,header=True)
    print('Total number of record: %s' % all_data.count())
    selected_data = all_data.where((col('Country')=='United States') & (col('WorkWeekHrs')>45))
    print('The engineer from US who works more than 45hrs in a week are : %s' % selected_data_count())
    selected_data.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH)
    print('Selected Data was sucessfully saved to S3 output folder: %s' %S3_DATA_OUTPUT_PATH)


if _name=='__main_':
    main()
