from pyspark.sql import SparkSession
import pyspark.sql.functions as fc

spark = SparkSession \
    .builder \
    .appName('football_dataset_cleaner') \
    .getOrCreate()

int_football_result = spark.read.csv('gs://int_football_bucket/data_lake/football/results.csv', inferSchema=True,
                                     header=True, mode='DROPMALFORMED')
int_football_result = int_football_result.dropna('any',
                                                 subset=['date', 'home_team', 'away_team', 'home_score', 'away_score'])
int_football_result = int_football_result.dropDuplicates().dropna('all', subset=['country', 'city'])
int_football_result.write.parquet('gs://int_football_bucket/staging/football/results.parquet', mode='overwrite')
