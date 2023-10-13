from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Customers") \
    .config("spark.jars", "/opt/spark-3.5.0/jars/postgresql-42.6.0.jar") \
    .config("spark.jars", "/opt/spark-3.5.0/jars/mariadb-java-client-3.2.0.jar") \
    .getOrCreate()
    # .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/people.customers?readPreference=primaryPreferred")\
    # .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/people.customers")\

df_customers = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/shop?permitMysqlScheme") \
    .option("driver", "org.mariadb.jdbc.Driver") \
    .option("dbtable", "customers") \
    .option("user", "spark") \
    .option("password", "spark") \
    .load()

df_customers.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/galeria_anatomica") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "shop.customers") \
    .option("user", "spark") \
    .option("password", "spark") \
    .mode("overwrite") \
    .save()    

# df_customers.write.format("mongo").mode("append").save()
# df_balances.write.format("mongo").mode("append").option("database","people").option("collection", "balances").save()
# df_balances.write.format("mongo").option("uri","mongodb://127.0.0.1/people.balances").mode("append").save()
# df = spark.read \
#      .format("mongo") \
#      .option("uri","mongodb://127.0.0.1/people.balances") \
#      .load() \
#      .createOrReplaceTempView("balances")
# spark.sql("""SELECT COUNT(*) FROM balances WHERE entity_id='3295'""").show(100)
# df.show()
