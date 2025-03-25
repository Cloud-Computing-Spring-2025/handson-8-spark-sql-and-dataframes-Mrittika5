from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# Split the Hashtags column into individual hashtags and explode the list into rows
hashtags_df = posts_df.withColumn("Hashtag", explode(split(col("Hashtags"), ",")))

# Count the frequency of each hashtag
hashtag_counts = hashtags_df.groupBy("Hashtag").count()

# Sort by count in descending order and take the top 10 hashtags
top_hashtags = hashtag_counts.orderBy(col("count").desc()).limit(10)

# Save result
top_hashtags.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)
