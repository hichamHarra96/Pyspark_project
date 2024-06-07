from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, expr, when, round, regexp_replace

spark = SparkSession.builder.appName("WordCount").getOrCreate()
# Lire le fichier CSV
df = spark.read.csv("listings.csv", header=True, multiLine=True, escape="\"")
# Afficher le schéma du DataFrame
df.printSchema()
# Afficher les premières lignes du DataFrame
df.show()

nombre_total = df.count()
print("Le nombre Total est :", nombre_total)
nbr_by_type =df.groupBy("room_type").count()
# Calculer les pourcentages
pourcentages = nbr_by_type.withColumn("percentage", round(col("count") / nombre_total * 100, 1))
print("pourcentages by type: ")
pourcentages.show()


# Créer une nouvelle colonne qui calcule la formule
df = df.withColumn("estimated_nights_booked", col("number_of_reviews") * 0.5 * col("minimum_nights_avg_ntm"))

# Calculer la moyenne de la nouvelle colonne
average_nights_booked = df.select(avg("estimated_nights_booked")).collect()[0][0]

print("average_nights_booked :", average_nights_booked)

# Supprimer le symbole "$" de la colonne "price" et convertir en type numérique
df = df.withColumn("price_cleaned", regexp_replace(col("price"), "\$", "").cast("float"))

# Calculer la somme du champ "price_cleaned"
total_price = df.selectExpr("sum(price_cleaned) as total_price").collect()[0]["total_price"]
average_price= total_price/nombre_total
# Filtrer les lignes où le prix est différent de zéro et n'est pas
print("Price/nigh en dollar avec conversion en trouve la bonne valeur:", average_price)

# Créer une nouvelle colonne pour le revenu estimé
df = df.withColumn("estimated_income", col("price_cleaned") * col("estimated_nights_booked"))

# Calculer la moyenne du revenu estimé
average_income = df.select(avg("estimated_income")).collect()[0][0]

print("Revenu moyen estimé :", average_income)
