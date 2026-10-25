# Hands-on L9: Social Media Analysis with Spark SQL and DataFrames

**Course:** ITCS 6190/8190 — Cloud Computing for Data Analysis  
**Student:** Rutuja Hemant Shastri  
**Student ID / Data seed:** `801484366`

---

## 1. Seed and commands

I generated the dataset using my student ID as the seed.

```bash
python3 datagen.py 801484366
docker compose up -d
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output
docker compose down
```

After editing `main.py`, I copied the updated file to the Spark master and reran the `spark-submit` command.

## 2. Results

### Task 1 — Hashtag trends (DataFrame API)

| Hashtag | Count |
|---|---:|
| `#ai` | 1548 |
| `#social` | 1514 |
| `#ux` | 1499 |
| `#love` | 1009 |
| `#darkmode` | 978 |
| `#update` | 920 |
| `#tech` | 595 |
| `#cleanui` | 425 |
| `#bug` | 421 |
| `#design` | 421 |

`#ai` was the most frequent hashtag, appearing 1,548 times, followed by `#social` and `#ux`. Normalizing hashtags with `lower` and `trim` combines differently capitalized spellings into the same category. Before normalization, `#ai` appeared in two distinct spellings: `#AI` (638 occurrences) and `#ai` (910 occurrences).

### Task 2 — Engagement by age group (Spark SQL and DataFrame API)

| Age group | Posts | Average likes | Average retweets |
|---|---:|---:|---:|
| Adult | 1938 | 56.20 | 13.52 |
| Teen | 1803 | 50.96 | 12.44 |
| Senior | 1259 | 47.68 | 11.93 |

Adults had the highest average engagement, with 56.20 likes and 13.52 retweets per post. Seniors had the lowest averages in both measures. The DataFrame API version produced the same results as the SQL version.

### Task 3 — Sentiment versus engagement (Spark SQL)

| Sentiment | Posts | Average likes | Average retweets |
|---|---:|---:|---:|
| Negative | 1072 | 25.64 | 6.41 |
| Neutral | 2224 | 47.48 | 11.67 |
| Positive | 1704 | 74.96 | 18.09 |

Positive posts received the highest average engagement, with 74.96 likes and 18.09 retweets. Negative posts had the lowest averages, at 25.64 likes and 6.41 retweets. Neutral posts were the most common sentiment category, with 2,224 posts.

### Task 4 — Top verified users (DataFrame API)

| Username | Total likes | Total retweets | Reach |
|---|---:|---:|---:|
| `@designer14` | 16912 | 4191 | 21103 |
| `@coffee_first6` | 16681 | 4056 | 20737 |
| `@designer34` | 8989 | 2246 | 11235 |
| `@calm_mind47` | 8892 | 2157 | 11049 |
| `@pixel_pusher1` | 8257 | 2134 | 10391 |

`@designer14` had the highest combined reach among the top five verified users, with a reach of 21,103. `@coffee_first6` ranked second, with a reach of 20,737.

## 3. Two languages, one plan

### Spark SQL physical plan

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#54 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#54 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=697]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=694]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=689]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>
```

### DataFrame API physical plan

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#134 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#134 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=747]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=744]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=739]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>
```

### Comparison

The two plans are equivalent in their main execution operators. Differences in expression IDs and plan IDs do not change the execution strategy.

- **Join:** Spark uses `BroadcastHashJoin` with an `Inner` join and `BuildRight`. The `users` side is broadcast to support the join with `posts`.
- **First `Exchange`:** `Exchange hashpartitioning(age_group, 200)` redistributes rows by age group so rows belonging to the same group can be combined in the final aggregation. Spark performs partial aggregation before this exchange and final aggregation afterward.
- **Second `Exchange`:** `Exchange rangepartitioning(avg_likes DESC, 200)` prepares the data for sorting by average likes in descending order.
- **Scans and filters:** Spark reads the required columns from both CSV files and filters null join keys.
- **Conclusion:** Spark SQL and the DataFrame API are both translated into a physical execution plan by Spark. Since these versions use equivalent operations, their physical plans are effectively the same. The choice between SQL and the DataFrame API can be based on readability, familiarity, and maintainability.

## 4. Views

`createOrReplaceTempView` registered the `posts` and `users` DataFrames under names that could be referenced by Spark SQL during the Spark session. It did not copy the CSV data into a new persistent table or create a permanent catalog entry. The views were temporary and session-scoped, so they were no longer available after the Spark session and program ended.

## 5. Problems and fixes

When starting the Docker cluster, I encountered a container-name conflict because a container named `spark-master` already existed:

```text
Error response from daemon: Conflict. The container name "/spark-master" is already in use by container "2cb2fad64693ab09be0234e730f54aa62641604d3f2c4e2d5d5442d214c1dd3f". You have to remove (or rename) that container to be able to reuse that name.
```

The existing Spark containers conflicted with the names required by this Compose setup. I removed the conflicting containers:

```bash
docker rm -f spark-master spark-worker-1 spark-worker-2
```

I then reran `docker compose up -d`, and the cluster started successfully. I copied the updated `main.py` into the master container and reran the Spark job to generate the task outputs.
