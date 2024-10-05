from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, concat_ws

# Initialize Spark session
spark = SparkSession.builder.appName("Flatten JSON to CSV").getOrCreate()

file_path="/home/jovyan/data/social_media_info.json"

# Read the JSON file
df = spark.read.option("multiline", "true").json(file_path)


df_flattened_posts = df.withColumn("post", explode(df.posts)).drop("posts")
# Step 1: Flatten the 'post' struct fields into individual columns
df_flattened_posts = df_flattened_posts.select(
    "user_id",  # Main user_id
    "username", 
    "age", 
    "email", 
    "gender", 
    "name", 
    "date_created",
    col("post.post_id").alias("post_id"), 
    col("post.post_text").alias("post_text"), 
    col("post.location").alias("location"), 
    col("post.timestamp").alias("post_timestamp"), 
    col("post.shares").alias("shares"),
    col("post.reactions.angry").alias("angry"), 
    col("post.reactions.haha").alias("haha"), 
    col("post.reactions.like").alias("like"), 
    col("post.reactions.love").alias("love"), 
    col("post.reactions.sad").alias("sad"), 
    col("post.reactions.wow").alias("wow"),
    concat_ws(",", col("post.tags")).alias("tags"),  # Convert ARRAY<STRING> to STRING using a comma separator
    col("post.comments").alias("comments")  # Alias the 'comments' array for easier handling
)

# Step 2: Explode the 'comments' array
df_flattened_comments = df_flattened_posts.withColumn("comment", explode(col("comments")))

# Step 3: Select relevant fields and rename 'user_id' from 'comments' to avoid conflicts
df_final = df_flattened_comments.select(
    "user_id",  # Keep the original user_id
    "username", 
    "age", 
    "email", 
    "gender", 
    "name", 
    "date_created",
    "post_id", 
    "post_text", 
    "location", 
    "post_timestamp", 
    "shares",
    "angry", 
    "haha", 
    "like", 
    "love", 
    "sad", 
    "wow", 
    "tags",  # Flattened 'tags' array as a string
    col("comment.comment").alias("comment_text"),  # Rename the 'comment' field for clarity
    col("comment.timestamp").alias("comment_timestamp"), 
    col("comment.user_id").alias("comment_user_id")  # Rename this to avoid conflict with original 'user_id'
)

# Step 4: Write the final flattened DataFrame to a CSV file
df_final.write.mode("overwrite").option("header", True).csv("/home/jovyan/data/Social.csv")

