from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Load Data to PostgreSQL") \
    .config("spark.jars", "/home/jovyan/drivers/postgresql-42.7.3.jar") \
    .config("spark.memory.fraction", "0.8") \
    .getOrCreate()

# PostgreSQL connection properties
url = "jdbc:postgresql://postgres_warehouse:5432/warehouse"
properties = {
    "user": "warehouse",
    "password": "warehouse",
    "driver": "org.postgresql.Driver"
}

# Read CSV data
df = spark.read.csv("/home/jovyan/data/Social_Valid.csv", header=True, inferSchema=True)

# Rename column to avoid reserved keyword conflict
df = df.withColumnRenamed("like", "like_count")

# Create dimension DataFrames and add surrogate keys
dim_users_df = df.select("user_id", "username", "age", "email", "gender", "name", "date_created").dropDuplicates()

# Add user_sk as surrogate key
dim_users_df = dim_users_df.withColumn("user_sk", monotonically_increasing_id())

# Debugging step: Print schema to check if user_sk is added
dim_users_df.printSchema()

dim_posts_df = df.select("post_id", "post_text", "location", "post_timestamp", "tags").dropDuplicates()
dim_posts_df = dim_posts_df.withColumn("post_sk", monotonically_increasing_id())

# Write the DataFrames to the PostgreSQL table 'dim_users'
dim_users_df.coalesce(1).write \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", "dim_users") \
    .option("user", "warehouse") \
    .option("password", "warehouse") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()
print("dim_users write complete.")

# Write the DataFrames to the PostgreSQL table 'dim_posts'
dim_posts_df.coalesce(1).write \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", "dim_posts") \
    .option("user", "warehouse") \
    .option("password", "warehouse") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()
print("dim_posts write complete.")


# Create fact table by joining with dimension DataFrames directly (no need to read from PostgreSQL)
fact_post_interactions_df = df \
    .join(dim_users_df.alias("main_user"), df["user_id"] == col("main_user.user_id"), "inner") \
    .join(dim_posts_df.alias("post"), df["post_id"] == col("post.post_id"), "inner") \
    .join(dim_users_df.alias("comment_user"), df["comment_user_id"] == col("comment_user.user_id"), "left") \
    .select(col("post.post_sk").alias("post_sk"),  # Use post_sk as the surrogate key
            col("main_user.user_sk").alias("user_sk"),  # Ensure user_sk is present
            "shares", "angry", "haha", "like_count", "love", "sad", "wow", 
            col("comment_user.user_sk").alias("comment_user_sk"), 
            "comment_text", "comment_timestamp")

# Write the fact table to PostgreSQL
fact_post_interactions_df.coalesce(1).write \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", "fact_post_interactions") \
    .option("user", "warehouse") \
    .option("password", "warehouse") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()
print("fact_post_interactions write complete.")
