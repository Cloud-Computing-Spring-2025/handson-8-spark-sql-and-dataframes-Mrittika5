from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Initialize Spark Session
spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Join datasets on UserID and filter for verified users
verified_users_df = posts_df.join(users_df.filter(col("Verified") == True), "UserID")

# Calculate total reach as Likes + Retweets
verified_users_df = verified_users_df.withColumn("TotalReach", col("Likes") + col("Retweets"))

# Group by Username and calculate total reach, then sort by reach in descending order
top_verified = verified_users_df.groupBy("Username") \
    .agg(_sum("TotalReach").alias("TotalReach")) \
    .orderBy(col("TotalReach").desc()) \
    .limit(5)

# Save result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)
