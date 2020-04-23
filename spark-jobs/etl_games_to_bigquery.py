from pyspark.sql import SparkSession
import pyspark.sql.types as tp

spark = SparkSession \
    .builder \
    .appName('football_dataset_cleaner') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'int_football_bucket')

cleaned_games = spark.read.parquet('gs://int_football_bucket/staging/football/results.parquet')
games_dt = cleaned_games.selectExpr("date", "home_team as team_1", "away_team as team_2", "home_score as team_1_score",
                                    "away_score as team_2_score", "tournament", "city", "country",
                                    "CASE WHEN neutral == FALSE THEN home_team ELSE NULL END as home_team")

games_table_schema = tp.StructType([tp.StructField('date', tp.TimestampType(), False),
                                    tp.StructField('team_1', tp.StringType(), False),
                                    tp.StructField('team_2', tp.StringType(), False),
                                    tp.StructField('team_1_score', tp.IntegerType(), False),
                                    tp.StructField('team_2_score', tp.IntegerType(), False),
                                    tp.StructField('tournament', tp.StringType(), True),
                                    tp.StructField('city', tp.StringType(), True),
                                    tp.StructField('country', tp.StringType(), True),
                                    tp.StructField('home_team', tp.StringType(), True)
                                    ])

final_table = spark.createDataFrame(games_dt.rdd, schema=games_table_schema)

final_table.write.format('bigquery').mode('Append').option('table', 'football_matches.games').save()
