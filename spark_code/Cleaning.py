from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, to_timestamp

# Initialize Spark session
spark = SparkSession.builder.appName("Clean CSV Data with Age Handling").getOrCreate()

# Read the CSV file
df = spark.read.option("header", True).csv("/home/jovyan/data/Social.csv")

df = df.withColumn("age", df["age"].cast("integer"))
df = df.withColumn("date_created",to_timestamp("date_created", "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("shares", df["shares"].cast("integer"))
df = df.withColumn("angry", df["angry"].cast("integer"))
df = df.withColumn("haha", df["haha"].cast("integer"))
df = df.withColumn("like", df["like"].cast("integer"))
df = df.withColumn("love", df["love"].cast("integer"))
df = df.withColumn("sad", df["sad"].cast("integer"))
df = df.withColumn("wow", df["wow"].cast("integer"))
df = df.withColumn("post_timestamp", to_timestamp("post_timestamp", "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("comment_timestamp", to_timestamp("comment_timestamp", "yyyy-MM-dd HH:mm:ss"))

# Drop rows with missing critical fields like user_id, post_id
df_cleaned = df.na.drop(subset=["user_id", "post_id"])

# Fill null values in reaction columns with 0
df_cleaned = df_cleaned.fillna({
    "shares": 0,
    "angry": 0,
    "haha": 0,
    "like": 0,
    "love": 0,
    "sad": 0,
    "wow": 0
})

# Fill missing comments with an empty string
df_cleaned = df_cleaned.fillna({
    "comment_text": "",
    "comment_user_id": ""
})

# Calculate the median age
age_median = df.approxQuantile("age", [0.5], 0.01)[0]

# Fill missing ages with the median age
df_cleaned = df_cleaned.fillna({"age": age_median})

df_cleaned.write.mode("overwrite").option("header", True).csv("/home/jovyan/data/Social_cleaned.csv")