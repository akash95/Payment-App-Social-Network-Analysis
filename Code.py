!apt-get install openjdk-8-jdk-headless -qq > /dev/null

!wget -q https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz

!tar xf spark-3.1.2-bin-hadoop3.2.tgz

!pip install -q findspark

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.2-bin-hadoop3.2"

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from datetime import datetime, date, timedelta
from dateutil import relativedelta
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.functions import to_timestamp, to_date
from pyspark.sql import functions as F  
from pyspark.sql.functions import collect_list, collect_set, concat, first, array_distinct, col, size, expr
from pyspark.sql import DataFrame 
import random
import pandas as pd

from collections import defaultdict

from pyspark.sql.functions import col

spark = SparkSession\
        .builder\
        .getOrCreate()

from google.colab import drive
drive.mount('/content/gdrive')

"""Social Network Analytics"""

#reading the venmo dataset from google drive
df = spark.read.parquet("/content/gdrive/MyDrive/BigData/VenmoSample.snappy.parquet")

#subsetting fraction of dataset of shorter computation time
# Set the seed for reproducibility 
seed = 42

# Calculate the fraction for sampling (0.1% = 0.001)
fraction = 0.001

# Use the sample method to select random samples
venmo = df.sample(withReplacement=False, fraction=fraction, seed=seed)

#  want to display 10 rows
venmo.show(10)

""" Write a script to find a user’s friends and friends of friends (Friend definition: A user’s friend is someone who has transacted with the user, either sending money to the user or receiving money from the user). Describe your algorithm and calculate its computational complexity"""

#script to find a user’s friends

# Friend relationships from user1's perspective
friends1 = venmo.select(col("user1").alias("user"), col("user2").alias("friend"))
# Friend relationships from user2's perspective
friends2 = venmo.select(col("user2").alias("user"), col("user1").alias("friend"))

# Union two DataFrames and remove duplicates
friends = friends1.union(friends2).distinct()

#First, we constructed a DataFrame that represents all friend relationships by unioning transactions from both 
#perspectives (user1 to user2 and user2 to user1), and then removing duplicates

friends.show(10)

#script to find friends of friends

# Find friends of friends
friends_of_friends = friends.alias("f1").join(friends.alias("f2"), col("f1.friend") == col("f2.user"))

# Filter out relationships where the friend of a friend is the original user
friends_of_friends = friends_of_friends.filter(col("f1.user") != col("f2.friend"))

# Select only relevant columns and remove duplicates
friends_of_friends = friends_of_friends.select(col("f1.user").alias("user"), col("f2.friend").alias("friend_of_friend")).distinct()

#Now, that we have a DataFrame of unique friend relationships. To get the friends of friends, we joined the DataFrame with itself 
#on the "friend" column, and then filtered out any relationships where the friend of a friend is the original user.

friends_of_friends.show(10)

"""Now, that we have the list of each user’s friends and friends of friends, we are in
position to calculate many social network variables. We use the dynamic analysis from before, and
calculate the following social network metrics across a user’s lifetime in the app (from 0 up to 12
months).
Metrics: 
i) Number of friends and number of friends of friends 
ii) Clustering coefficient of a user's network (Hint: the easiest way to calculate this
is to program it yourselves. Alternatively, you can use “NetworKit” or “networkX” python
package. The latter approach will slow down your script significantly).
iii) Calculate the page rank of each user. (Hint: First of all, you need to use
GraphFrames to do this. Moreover, notice that page rank is a global social network metric. If
we go ahead and calculate the page rank for each user at each of her lifetime points, we will
soon realize it will be a dead end)
"""

#i) Number of friends and number of friends of friends:
#To calculate the number of friends and friends of friends for each user, we can use the friends and friends_of_friends 
#DataFrames obtained earlier. We would need to group the DataFrames by the "user" column and count the distinct values in the 
#"friend" and "friend_of_friend" columns, respectively, within the desired time frame (from 0 up to 12 months).

from pyspark.sql.functions import col, expr, countDistinct
from pyspark.sql.types import TimestampType
from datetime import datetime, timedelta

# Define the time frame
start_date = venmo.selectExpr("min(datetime)").first()[0]
end_date = venmo.selectExpr("max(datetime)").first()[0]
time_frame = timedelta(days=365)  # 12 months

# Calculate the start and end timestamps for the time frame
start_timestamp = start_date
end_timestamp = start_date + time_frame

# Convert start and end timestamps to TimestampType
start_timestamp = datetime.strptime(str(start_timestamp), "%Y-%m-%d %H:%M:%S")
start_timestamp = start_timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
start_timestamp = start_timestamp.strftime("%Y-%m-%d %H:%M:%S")
start_timestamp = datetime.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S")
start_timestamp = start_timestamp.replace(tzinfo=None).timestamp()

end_timestamp = datetime.strptime(str(end_timestamp), "%Y-%m-%d %H:%M:%S")
end_timestamp = end_timestamp.replace(hour=23, minute=59, second=59, microsecond=999999)
end_timestamp = end_timestamp.strftime("%Y-%m-%d %H:%M:%S")
end_timestamp = datetime.strptime(end_timestamp, "%Y-%m-%d %H:%M:%S")
end_timestamp = end_timestamp.replace(tzinfo=None).timestamp()

# Filter the transactions within the time frame
filtered_venmo = venmo.filter((col("datetime") >= expr(f"timestamp({start_timestamp})")) & (col("datetime") <= expr(f"timestamp({end_timestamp})")))

# Calculate number of friends
num_friends = friends.groupBy("user").agg(countDistinct("friend").alias("num_friends"))

# Calculate number of friends of friends
num_friends_of_friends = friends_of_friends.groupBy("user").agg(countDistinct("friend_of_friend").alias("num_friends_of_friends"))

num_friends_of_friends.show(10)

#ii) Clustering coefficient of a user's network:
#The clustering coefficient measures the degree to which a user's friends are connected to each other. To calculate the clustering 
#coefficient for each user, we would need to define a function that computes the clustering coefficient based on the user's friends 
#and friends of friends. The formula for the clustering coefficient involves counting the number of triangles in the network.

from pyspark.sql.functions import col

def calculate_clustering_coefficient(user_id):
    user_friends = friends.filter(col("user") == user_id).select("friend").distinct().collect()
    num_friends = len(user_friends)
    
    if num_friends < 2:
        return 0.0

    num_triangles = 0
    for i in range(num_friends):
        friend_i = user_friends[i][0]
        for j in range(i + 1, num_friends):
            friend_j = user_friends[j][0]
            if friends_of_friends.filter((col("user") == friend_i) & (col("friend_of_friend") == friend_j)).count() > 0:
                num_triangles += 1

    clustering_coefficient = (2.0 * num_triangles) / (num_friends * (num_friends - 1))
    return clustering_coefficient

!pip install graphframes

#iii) Calculating the page rank of each user:
#To calculate the page rank of each user, instead of calculating the page rank for each user at each lifetime point, 
#we can calculate the page rank once for the entire network using the GraphFrames API.
from graphframes import GraphFrame

from pyspark.sql.functions import col

# Create the graph from the friends DataFrame
graph = GraphFrame(friends, friends_of_friends)

# Initialize the PageRank column for each user
graph = graph.vertices.withColumn("pagerank", lit(1.0)).select("id", "pagerank")
graph.persist()

# Perform incremental updates to the PageRank
for interval in intervals:
    # Filter transactions for the specific interval
    interval_start_time = start_time + (30 * interval)
    interval_end_time = start_time + (30 * (interval + 1))
    interval_transactions = venmo.filter(
        (col("datetime") >= interval_start_time) & (col("datetime") < interval_end_time)
    )

    # Update the graph with new transactions
    interval_graph = GraphFrame(interval_transactions, friends_of_friends)
    graph = graph.union(interval_graph.vertices.select("id", "pagerank"))

    # Run PageRank on the updated graph
    graph = graph.pageRank(resetProbability=0.15, tol=0.01)

# Display the PageRank for each user
graph.select("id", "pagerank").show()
