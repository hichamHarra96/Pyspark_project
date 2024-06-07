from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, round, when, month, year, lit, concat

spark = SparkSession.builder.appName("Analyse de Tweets de Trump").getOrCreate()

tweets_df = spark.read.csv("trump_insult_tweets_2014_to_2021.csv", header=True, inferSchema=True, multiLine=True, escape="\"")

# Question 1: Afficher les 7 comptes que Donald Trump insulte le plus (en nombres et en pourcentages)
# ----------------------------------------------------------------------------------------
top_accounts = (
    tweets_df.groupBy("target")
    .agg(count("insult").alias("nombre_insultes"))
    .sort(col("nombre_insultes").desc())
    .limit(7)
)
total_insults = tweets_df.count()
top_accounts_with_percentages = (
    top_accounts.withColumn("pourcentage", round((col("nombre_insultes") / total_insults) * 100, 2))
)
print("Question 1: Les 7 comptes que Donald Trump insulte le plus (en nombres et en pourcentages):")
top_accounts_with_percentages.show()

# Question 2: Afficher les insultes que Donald Trump utilise le plus (en nombres et en pourcentages)
# ----------------------------------------------------------------------------------------
top_insults = (
    tweets_df.groupBy("insult")
    .agg(count("insult").alias("nombre_utilisations"))
    .sort(col("nombre_utilisations").desc())
    .limit(7)
)
total_insults = tweets_df.count()
top_insults_with_percentages = (
    top_insults.withColumn("pourcentage", round((col("nombre_utilisations") / total_insults) * 100, 2))
)
print("Question 2: Les insultes que Donald Trump utilise le plus (en nombres et en pourcentages):")
top_insults_with_percentages.show()

# Question 3: Trouver l'insulte que Donald Trump utilise le plus contre Joe Biden
# ----------------------------------------------------------------------------------------
biden_insults = tweets_df.filter(col("target") == "joe-biden")
most_used_insult = (
    biden_insults.groupBy("insult")
    .agg(count("insult").alias("nombre_utilisations"))
    .sort(col("nombre_utilisations").desc())
    .limit(1)
)
print("Question 3: L'insulte que Donald Trump utilise le plus contre Joe Biden:")
most_used_insult.show()

# Question 4: Compter le nombre de tweets contenant les mots "Mexico", "China" et "coronavirus"
# ----------------------------------------------------------------------------------------
mexico_tweets = tweets_df.filter(col("tweet").contains("Mexico")).count()
china_tweets = tweets_df.filter(col("tweet").contains("China")).count()
coronavirus_tweets = tweets_df.filter(col("tweet").contains("Coronavirus")).count()
print("Question 4:")
print(f"Nombre de tweets contenant 'Mexico': {mexico_tweets}")
print(f"Nombre de tweets contenant 'China': {china_tweets}")
print(f"Nombre de tweets contenant 'Coronavirus': {coronavirus_tweets}")

# Question 5: Classer le nombre de tweets par période de 6 mois
# ----------------------------------------------------------------------------------------
tweets_df = tweets_df.withColumn(
    "periode",
    when(
        month(col("date")).between(1, 6),
        concat(year(col("date")), lit("/01"), lit(" - "), year(col("date")), lit("/06")),
    ).otherwise(
        concat(year(col("date")), lit("/07"), lit(" - "), year(col("date")), lit("/12"))
    ),
)
tweets_by_period = (
    tweets_df.groupBy("periode")
    .agg(count("tweet").alias("nombre_tweets"))
    .sort(col("periode"))
)
print("Question 5: Le nombre de tweets par période de 6 mois:")
tweets_by_period.show()
