# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8ebe1319-241d-4cd2-add6-4c97c366dcee",
# META       "default_lakehouse_name": "credit_bronze",
# META       "default_lakehouse_workspace_id": "460f6e60-af50-42e2-aaa2-56514df94542",
# META       "known_lakehouses": [
# META         {
# META           "id": "8ebe1319-241d-4cd2-add6-4c97c366dcee"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import current_timestamp, lit, year, month, to_timestamp

txn = spark.read.option("header",True).csv("Files/raw/transactions_data.csv")
txn = txn.withColumn("date_parsed", to_timestamp("date","yyyy-MM-dd HH:mm:ss")) \
         .withColumn("year", year("date_parsed")) \
         .withColumn("month", month("date_parsed")) \
         .withColumn("_ingested_at", current_timestamp())
txn.write.format("delta").mode("overwrite") \
   .partitionBy("year","month").saveAsTable("bronze_transactions")

for name in ["cards","users"]:
    df = spark.read.option("header",True).csv(f"Files/raw/{name}_data.csv") \
             .withColumn("_ingested_at", current_timestamp())
    df.write.format("delta").mode("overwrite").saveAsTable(f"bronze_{name}")

mcc = spark.read.option("multiline",True).json("Files/raw/mcc_codes.json")
mcc.write.format("delta").mode("overwrite").saveAsTable("bronze_mcc_codes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
