import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
import os
import shutil
import time

def broadcast_join_maps(spark):
    matches_df = spark.read.csv("../../data/matches.csv", header=True, inferSchema=True)
    maps_df = spark.read.csv("../../data/maps.csv", header=True, inferSchema=True)
    matches_maps_df = matches_df.join(broadcast(maps_df), "mapid")

    return matches_maps_df

def broadcast_join_medals(spark):
    medals_df = spark.read.csv("../../data/medals.csv", header=True, inferSchema=True)
    medals_matches_players_df = spark.read.csv("../../data/medals_matches_players.csv", header=True, inferSchema=True)
    medals_players_df = medals_matches_players_df.join(broadcast(medals_df), "medal_id")

    return medals_players_df

def bucket_join_matches(spark):
    matches_df = spark.read.csv("../../data/matches.csv", header=True, inferSchema=True)

    spark.sql("DROP TABLE IF EXISTS matches_bucketed")

    warehouse_path = "spark-warehouse/matches_bucketed"
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)

    matches_df = matches_df.repartition(16, "match_id").sortWithinPartitions("match_id")
    matches_df.write.format("parquet").mode("overwrite").bucketBy(16, "match_id").saveAsTable("matches_bucketed")

    matches_bucketed_df = spark.table("matches_bucketed")
    return matches_bucketed_df

def bucket_join_match_details(spark):
    match_details_df = spark.read.csv("../../data/match_details.csv", header=True, inferSchema=True)
    
    spark.sql("DROP TABLE IF EXISTS match_details_bucketed")

    warehouse_path = "spark-warehouse/match_details_bucketed"
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)

    match_details_df = match_details_df.repartition(16, "match_id").sortWithinPartitions("match_id")
    match_details_df.write.format("parquet").mode("overwrite").bucketBy(16, "match_id").saveAsTable("match_details_bucketed")

    match_details_bucketed_df = spark.table("match_details_bucketed")
    return match_details_bucketed_df

def bucket_join_medal_matches_players(spark):
    medal_matches_players_df = spark.read.csv("../../data/medals_matches_players.csv", header=True, inferSchema=True)
    
    spark.sql("DROP TABLE IF EXISTS medal_matches_players_bucketed")

    warehouse_path = "spark-warehouse/medal_matches_players_bucketed"
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)

    medal_matches_players_df = medal_matches_players_df.repartition(16, "match_id").sortWithinPartitions("match_id")
    medal_matches_players_df.write.format("parquet").mode("overwrite").bucketBy(16, "match_id").saveAsTable("medal_matches_players_bucketed")

    medal_matches_players_bucketed_df = spark.table("medal_matches_players_bucketed")
    return medal_matches_players_bucketed_df

def bucket_join_everything(spark, matches_df, match_details_df, medal_matches_players_df):
    bucketed_df = matches_df.join(match_details_df, "match_id").join(medal_matches_players_df, ["match_id", "player_gamertag"])
    return bucketed_df

def aggregate_data(spark, df):
    start_time = time.time()

    #Average kills per game for each player
    df.groupBy("match_id","player_gamertag").avg("player_total_kills").alias("average_kills_per_game").show()
    #print(f"The player that averages the most kills per game is {highest_kill_average}")

    #Get the most common playlist
    most_common_playlist = df.groupBy("playlist_id").count().orderBy("count", ascending=False).take(1)
    print(f"THe most common playlist is {most_common_playlist}")

    #Get the most common map
    most_common_map = df.groupBy("mapid").count().orderBy("count", ascending=False).take(1)
    print(f"The most common map is {most_common_map}")

    #Get the most common medal for medal classification KillingSpree
    medal_df = spark.read.csv("../../data/medals.csv", header=True, inferSchema=True)
    df = df.join(broadcast(medal_df), "medal_id")

    killing_sprees = df.filter(df.classification == "KillingSpree").groupBy("mapid").count().orderBy("count", ascending=False).take(1)
    print(f"The most common map for KillingSpree is {killing_sprees}")

    end_time = time.time()
    print(f"Time taken to aggregate data: {end_time - start_time}")



def bucket_join_matches_v2(spark):
    matches_df = spark.read.csv("../../data/matches.csv", header=True, inferSchema=True)

    spark.sql("DROP TABLE IF EXISTS matches_bucketed_v2")

    warehouse_path = "spark-warehouse/matches_bucketed_v2"
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)

    matches_df = matches_df.repartition(16, "match_id").sortWithinPartitions("match_id","mapid")
    matches_df.write.format("parquet").mode("overwrite").bucketBy(16, "match_id").saveAsTable("matches_bucketed_v2")

    matches_bucketed_df = spark.table("matches_bucketed_v2")
    return matches_bucketed_df

def bucket_join_matches_v3(spark):
    matches_df = spark.read.csv("../../data/matches.csv", header=True, inferSchema=True)

    spark.sql("DROP TABLE IF EXISTS matches_bucketed_v3")

    warehouse_path = "spark-warehouse/matches_bucketed_v3"
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)

    matches_df = matches_df.repartition(16, "match_id").sortWithinPartitions("match_id","playlist_id")
    matches_df.write.format("parquet").mode("overwrite").bucketBy(16, "match_id").saveAsTable("matches_bucketed_v3")

    matches_bucketed_df = spark.table("matches_bucketed_v3")
    return matches_bucketed_df

def compare_file_sizes():
    #Compare the file sizes of the three bucketed DataFrames
    files = ["matches_bucketed", "matches_bucketed_v2", "matches_bucketed_v3"]

    for file in files:
        file_path = f"spark-warehouse/{file}"
        file_sizes = []

        if os.path.exists(file_path):
            for file in os.listdir(file_path):
                file_sizes.append(os.path.getsize(f"{file_path}/{file}"))
            print(f"File: {file}, File sizes: {sorted(file_sizes)}")

        
def main():
    spark = SparkSession.builder.appName("MatchStatistics").getOrCreate()

    # 1. Disable automatic broadcast join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    # 2. Explicitly broadcast JOINS medals and maps
    broadcast_map = broadcast_join_maps(spark)
    broadcast_map.show()

    broadcast_medals = broadcast_join_medals(spark)
    broadcast_medals.show()

    #3. Bucket join match_details, matches, medal_matches_players on match_id with 16 buckets
    bucketed_matches = bucket_join_matches(spark)
    bucketed_matches.show()

    bucketed_match_details = bucket_join_match_details(spark)
    bucketed_match_details.show()

    bucketed_medal_matches_players = bucket_join_medal_matches_players(spark)
    bucketed_medal_matches_players.show()

    bucketed_df = bucket_join_everything(spark, bucketed_matches, bucketed_match_details, bucketed_medal_matches_players)
    bucketed_df.show()
    
    # 4. Aggregate data and find the answers to the questions
    # 4.1 Which player averages the most kills per game?
    # 4.2. Which playlist gets played the most?
    # 4.3. Which map gets played the most?
    # 4.4. Which map do players get the most KillingSpree medals on?

    aggregate_data(spark, bucketed_df)

    # 5. With aggregated data set, find the smallest data size using .sortWithinPartitions

    # Bucket join matches DataFrame v2
    bucketed_matches_v2 = bucket_join_matches_v2(spark)
    bucketed_df_v2 = bucket_join_everything(spark, bucketed_matches_v2, bucketed_match_details, bucketed_medal_matches_players)
    
    #Get aggregated stats for bucketed_df_v2
    aggregate_data(spark, bucketed_df_v2)

    # Bucket join matches DataFrame v3
    bucketed_matches_v3= bucket_join_matches_v3(spark)
    bucketed_df_v3 = bucket_join_everything(spark, bucketed_matches_v3, bucketed_match_details, bucketed_medal_matches_players)

    #Get aggregated stats for bucketed_df_v3
    aggregate_data(spark, bucketed_df_v3)

    #Compare file sizes of the three bucketed DataFrames
    compare_file_sizes()


if __name__ == "__main__":
    main()
