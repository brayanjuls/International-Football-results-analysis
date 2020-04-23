from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName('football_dataset_cleaner') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'int_football_bucket')

cleaned_games = spark.read.parquet('gs://int_football_bucket/staging/football/results.parquet')
games_dt = cleaned_games.selectExpr("date", "home_team as team_1", "away_team as team_2", "home_score as team_1_score",
                                    "away_score as team_2_score", "tournament", "city", "country",
                                    "CASE WHEN neutral == FALSE THEN home_team ELSE NULL END as home_team")

games_dt.write.format('bigquery').mode('Append').option('table', 'football_matches.games').save()
