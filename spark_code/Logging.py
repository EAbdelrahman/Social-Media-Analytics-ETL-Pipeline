import logging

# Configure logging
logging.basicConfig(level=logging.INFO, filename='/home/jovyan/work/error_log.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_not_null(df, columns, logger):
    """
    Function to check for null values in critical columns.
    Args:
    - df: DataFrame to check.
    - columns: List of columns that should not have null values.
    - logger: Logger to log errors.
    
    Returns:
    - valid_df: DataFrame with valid records.
    - invalid_df: DataFrame with invalid records (null values in critical columns).
    """
    invalid_df = df.filter(" OR ".join([f"{col} IS NULL" for col in columns]))
    valid_df = df.filter(" AND ".join([f"{col} IS NOT NULL" for col in columns]))

    if invalid_df.count() > 0:
        logger.error(f"Null value found in critical columns: {columns}")
        invalid_df.show(truncate=False)

    return valid_df, invalid_df

def validate_age_range(df, logger):
    """
    Function to check that age is within a valid range (e.g., 0 to 120).
    Args:
    - df: DataFrame to check.
    - logger: Logger to log errors.
    
    Returns:
    - valid_df: DataFrame with valid records.
    - invalid_df: DataFrame with invalid age values.
    """
    invalid_df = df.filter((df["age"] < 0) | (df["age"] > 120))
    valid_df = df.filter((df["age"] >= 0) & (df["age"] <= 120))

    if invalid_df.count() > 0:
        logger.error(f"Invalid age values found (out of range):")
        invalid_df.show(truncate=False)

    return valid_df, invalid_df

import re
from pyspark.sql.functions import col

def validate_email_format(df, logger):
    """
    Function to validate the format of the email column.
    Args:
    - df: DataFrame to check.
    - logger: Logger to log errors.
    
    Returns:
    - valid_df: DataFrame with valid email format.
    - invalid_df: DataFrame with invalid email format.
    """
    email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    
    invalid_df = df.filter(~df["email"].rlike(email_regex))
    valid_df = df.filter(df["email"].rlike(email_regex))

    if invalid_df.count() > 0:
        logger.error(f"Invalid email format detected")
        invalid_df.show(truncate=False)

    return valid_df, invalid_df

from pyspark.sql import SparkSession
import logging

# Initialize Spark session
spark = SparkSession.builder.appName("Data Cleaning with Validation and Logging").getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO, filename='/home/jovyan/work/error_log.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Read the CSV file
df = spark.read.csv("/home/jovyan/data/Social_cleaned.csv", header=True, inferSchema=True)


# Step 1: Validation for critical columns (e.g., user_id, post_id)
df_valid, df_invalid_nulls = validate_not_null(df, ["user_id", "post_id"], logger)

# Step 2: Age range validation (0 to 120)
df_valid_age, df_invalid_age = validate_age_range(df_valid, logger)

# Step 3: Email format validation
df_valid_email, df_invalid_email = validate_email_format(df_valid_age, logger)

# Log any records that failed validation (optional)
logger.info(f"Total invalid records (nulls, age, email): {df_invalid_nulls.count() + df_invalid_age.count() + df_invalid_email.count()}")


# Coalesce to 1 partition
df_valid_email.coalesce(1).write.mode("overwrite").option("header", True).csv("/home/jovyan/data/Social_Valid.csv")

