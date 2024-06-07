from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round, regexp_extract, avg, min, max, floor, expr, split, explode

spark = SparkSession.builder.appName("NetflixAnalysis").getOrCreate()
df = spark.read.csv("netflix_titles.csv", header=True, multiLine=True, escape="\"")
df.printSchema()
df.show()

# Question 1:
directors_df = df.filter(col("director").isNotNull())
directors_count_df = directors_df.groupBy("director").agg(count("title").alias("number_of_movies"))
directors_count_df = directors_count_df.orderBy(col("number_of_movies").desc())
print("Question 1: Liste des réalisateurs les plus prolifiques :")
directors_count_df.show()

# Question 2:
countries_df = df.filter(col("country").isNotNull())
countries_count_df = countries_df.groupBy("country").agg(count("title").alias("number_of_titles"))
total_titles = df.count()
countries_percentage_df = countries_count_df.withColumn("percentage", round((col("number_of_titles") / total_titles) * 100, 2))
countries_percentage_df = countries_percentage_df.orderBy(col("percentage").desc())
print("Question 2: Pourcentages des pays dans lesquels les films/séries ont été produits :")
countries_percentage_df.show(truncate=False)

# Question 3:
movies_df = df.filter(col("type") == "Movie")
movies_df = movies_df.withColumn("duration_minutes", regexp_extract(col("duration"), "(\d+)", 1).cast("int"))
print("Question 3: Premières lignes avec les durées en minutes :")
movies_df.select("title", "duration", "duration_minutes").show()
average_duration = movies_df.select(avg("duration_minutes").alias("average_duration")).collect()[0]["average_duration"]
longest_movie = movies_df.orderBy(col("duration_minutes").desc()).select("title", "duration_minutes").first()
shortest_movie = movies_df.filter(col("duration_minutes").isNotNull()).orderBy(col("duration_minutes")).select("title", "duration_minutes").first()

print(f"Question 3: Durée moyenne des films sur Netflix: {average_duration} minutes")
print(f"Question 3: Le film le plus long: {longest_movie['title']} avec une durée de {longest_movie['duration_minutes']} minutes")
print(f"Question 3: Le film le plus court: {shortest_movie['title']} avec une durée de {shortest_movie['duration_minutes']} minutes")

# Question 4:
max_year = df.select(max("release_year")).collect()[0][0]
movies_df = movies_df.filter(col("duration_minutes").isNotNull())
movies_df = movies_df.withColumn("year_interval", floor(col("release_year") / 2) * 2)
average_duration_by_interval = (
    movies_df
    .groupBy("year_interval")
    .agg(round(avg("duration_minutes"), 2).alias("average_duration"))
    .orderBy("year_interval", ascending=False)
)
print("Question 4: Durée moyenne des films par intervalles de 2 ans :")
average_duration_by_interval.withColumn(
    "interval_range",
    expr("concat_ws('-', year_interval + 2, year_interval)")
).select(
    "interval_range",
    expr("concat(average_duration, ' minutes')").alias("average_duration")
).orderBy("year_interval").show(truncate=False)

# Question 5:
movies_with_director_and_actor = df.filter(col("director").isNotNull() & col("cast").isNotNull())
movies_with_director_and_actor = movies_with_director_and_actor.withColumn("directors", split(col("director"), ", ")).withColumn("actors", split(col("cast"), ", "))
movies_with_director_and_actor = movies_with_director_and_actor.withColumn("director_actor_pairs", explode(col("directors")))
collaboration_count = movies_with_director_and_actor.groupBy("director_actor_pairs").agg(count("*").alias("movie_count"))
most_common_collaboration = collaboration_count.orderBy(col("movie_count").desc()).first()

print("Question 5: Duo réalisateur-acteur qui a collaboré dans le plus de films :")
print(f"Réalisateur : {most_common_collaboration['director_actor_pairs']}, Nombre de films : {most_common_collaboration['movie_count']}")
