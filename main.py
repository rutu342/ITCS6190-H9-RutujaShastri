
"""Hands-on L9: Social Media Analysis with Spark SQL and DataFrames.

Usage:
    spark-submit main.py <input directory> <output directory>
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as ssum, avg, round as sround, desc,
    explode, split, lower, trim
)

if len(sys.argv) != 3:
    print(__doc__)
    sys.exit(2)

in_dir, out_dir = sys.argv[1].rstrip("/"), sys.argv[2].rstrip("/")
spark = SparkSession.builder.appName("SocialMediaAnalysis").getOrCreate()


def save(df, name):
    """Print a DataFrame and write it as a single CSV file with a header."""
    if df is None:
        print(f"\n=== {name}: not implemented yet ===")
        return

    print(f"\n=== {name} ===")
    df.show(20, truncate=False)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{out_dir}/{name}"
    )


# Step 0: Load and register the data.
posts_schema = (
    "post_id STRING, user_id STRING, content STRING, timestamp TIMESTAMP, "
    "likes INT, retweets INT, hashtags STRING, sentiment_score DOUBLE"
)
users_schema = (
    "user_id STRING, username STRING, age_group STRING, country STRING, "
    "verified BOOLEAN"
)

posts = spark.read.csv(
    f"{in_dir}/posts.csv", header=True, schema=posts_schema
)
users = spark.read.csv(
    f"{in_dir}/users.csv", header=True, schema=users_schema
)

posts.printSchema()
print(f"{posts.count()} posts, {users.count()} users")

posts.createOrReplaceTempView("posts")
users.createOrReplaceTempView("users")


# Task 1: Hashtag trends (DataFrame API).
def task1_hashtag_trends():
    """Return the ten most used normalized hashtags."""
    return (
        posts
        .select(explode(split(col("hashtags"), ",")).alias("hashtag"))
        .select(lower(trim(col("hashtag"))).alias("hashtag"))
        .groupBy("hashtag")
        .agg(count("*").alias("count"))
        .orderBy(desc("count"), col("hashtag").asc())
        .limit(10)
    )


# Task 2: Engagement by age group (Spark SQL).
def task2_engagement_by_age_sql():
    """Average engagement per age group using one SQL query."""
    return spark.sql("""
        SELECT
            u.age_group AS age_group,
            COUNT(*) AS posts,
            ROUND(AVG(p.likes), 2) AS avg_likes,
            ROUND(AVG(p.retweets), 2) AS avg_retweets
        FROM posts p
        JOIN users u ON p.user_id = u.user_id
        GROUP BY u.age_group
        ORDER BY avg_likes DESC
    """)


def task2_engagement_by_age_df():
    """The same result using the DataFrame API."""
    return (
        posts
        .join(users, "user_id")
        .groupBy("age_group")
        .agg(
            count("*").alias("posts"),
            sround(avg("likes"), 2).alias("avg_likes"),
            sround(avg("retweets"), 2).alias("avg_retweets")
        )
        .orderBy(desc("avg_likes"))
    )


# Task 3: Sentiment versus engagement (Spark SQL).
def task3_sentiment_vs_engagement_sql():
    """Summarize post engagement by sentiment category."""
    return spark.sql("""
        SELECT
            CASE
                WHEN sentiment_score > 0.3 THEN 'Positive'
                WHEN sentiment_score < -0.3 THEN 'Negative'
                ELSE 'Neutral'
            END AS sentiment,
            COUNT(*) AS posts,
            ROUND(AVG(likes), 2) AS avg_likes,
            ROUND(AVG(retweets), 2) AS avg_retweets
        FROM posts
        GROUP BY
            CASE
                WHEN sentiment_score > 0.3 THEN 'Positive'
                WHEN sentiment_score < -0.3 THEN 'Negative'
                ELSE 'Neutral'
            END
        ORDER BY sentiment
    """)


# Task 4: Top verified users (DataFrame API).
def task4_top_verified_users():
    """Return the five verified users with the largest reach."""
    verified_posts = (
        posts
        .join(users, "user_id")
        .filter(col("verified") == True)
    )

    return (
        verified_posts
        .groupBy("username")
        .agg(
            ssum("likes").alias("total_likes"),
            ssum("retweets").alias("total_retweets")
        )
        .withColumn(
            "reach", col("total_likes") + col("total_retweets")
        )
        .select("username", "total_likes", "total_retweets", "reach")
        .orderBy(desc("reach"), col("username").asc())
        .limit(5)
    )


# Run all four tasks and save their outputs.
save(task1_hashtag_trends(), "task1")

t2_sql = task2_engagement_by_age_sql()
save(t2_sql, "task2")

t2_df = task2_engagement_by_age_df()
if t2_sql is not None and t2_df is not None:
    print("\n=== task2: physical plan of the SQL version ===")
    t2_sql.explain()
    print("\n=== task2: physical plan of the DataFrame version ===")
    t2_df.explain()

save(task3_sentiment_vs_engagement_sql(), "task3")
save(task4_top_verified_users(), "task4")

spark.stop()
