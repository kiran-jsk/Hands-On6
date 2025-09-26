# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, desc, row_number, to_timestamp, hour
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets
listening_logs_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("listening_logs.csv")
)

songs_metadata_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("songs_metadata.csv")
)

# Task 1: User Favorite Genres
logs_with_genre_df = listening_logs_df.join(songs_metadata_df.select("song_id", "genre"), on="song_id", how="left")

user_genre_counts_df = (
    logs_with_genre_df.groupBy("user_id", "genre").agg(count("*").alias("play_count"))
)

favorite_genre_window = Window.partitionBy("user_id").orderBy(desc("play_count"), col("genre"))

user_favorite_genre_df = (
    user_genre_counts_df
    .withColumn("rank", row_number().over(favorite_genre_window))
    .filter(col("rank") == 1)
    .drop("rank")
    .withColumnRenamed("genre", "favorite_genre")
)

user_favorite_genre_df.select(
    "user_id", "favorite_genre", "play_count"
).write.mode("overwrite").format("csv").option("header", True).save(
    "output/user_favorite_genres/"
)

# Task 2: Average Listen Time
avg_listen_time_df = (
    listening_logs_df.groupBy("song_id").agg(avg("duration_sec").alias("avg_duration_sec"))
).join(songs_metadata_df.select("song_id", "title", "artist"), on="song_id", how="left")

avg_listen_time_df.select(
    "song_id", "title", "artist", "avg_duration_sec"
).write.mode("overwrite").format("csv").option("header", True).save(
    "output/avg_listen_time_per_song/"
)



# Task 3: Genre Loyalty Scores
total_user_plays_df = listening_logs_df.groupBy("user_id").agg(count("*").alias("total_plays"))

genre_loyalty_df = (
    user_favorite_genre_df.join(total_user_plays_df, on="user_id", how="inner")
    .withColumn("loyalty_score", col("play_count") / col("total_plays"))
    .filter(col("loyalty_score") <= 0.75)
)

genre_loyalty_df.select(
    "user_id", "favorite_genre", "play_count", "total_plays", "loyalty_score"
).write.mode("overwrite").format("csv").option("header", True).save(
    "output/genre_loyalty_scores/"
)


# Task 4: Identify users who listen between 12 AM and 5 AM
logs_with_hour_df = listening_logs_df.withColumn(
    "timestamp_ts", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")
).withColumn("hour", hour(col("timestamp_ts")))

night_owl_df = (
    logs_with_hour_df.filter((col("hour") >= 0) & (col("hour") < 5))
    .groupBy("user_id")
    .agg(count("*").alias("night_plays"))
    .join(total_user_plays_df, on="user_id", how="left")
    .withColumn("night_play_ratio", col("night_plays") / col("total_plays"))
    .filter(col("night_plays") > 0)
)

night_owl_df.select(
    "user_id", "night_plays", "total_plays", "night_play_ratio"
).write.mode("overwrite").format("csv").option("header", True).save(
    "output/night_owl_users/"
)

spark.stop()
