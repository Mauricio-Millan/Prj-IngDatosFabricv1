# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1c4c22d9-7174-4ea3-b80a-8f33d4835d62",
# META       "default_lakehouse_name": "credit_gold",
# META       "default_lakehouse_workspace_id": "460f6e60-af50-42e2-aaa2-56514df94542",
# META       "known_lakehouses": [
# META         {
# META           "id": "652ebbd0-ad01-4405-a4c9-97cd145127f6"
# META         },
# META         {
# META           "id": "1c4c22d9-7174-4ea3-b80a-8f33d4835d62"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ================================================================
# NOTEBOOK: 03_silver_to_gold
# CAPA: Gold — Construcción del modelo estrella
# ================================================================

from pyspark.sql.functions import (
    col, when, lit, date_format, to_date, year, month, quarter,
    add_months, sequence, explode, expr, create_map
)
from itertools import chain

# Cargar tablas Silver (Lakehouse fuente — se referencia por nombre)
cards   = spark.table("credit_silver.dbo.silver_cards")
users   = spark.table("credit_silver.dbo.silver_users")
mcc     = spark.table("credit_silver.dbo.silver_mcc")
monthly = spark.table("credit_silver.dbo.silver_monthly_spend")
txn     = spark.table("credit_silver.dbo.silver_transactions")

print("Tablas Silver cargadas:")
print(f"  silver_cards:         {cards.count():,}")
print(f"  silver_users:         {users.count():,}")
print(f"  silver_mcc:           {mcc.count():,}")
print(f"  silver_monthly_spend: {monthly.count():,}")
print(f"  silver_transactions:  {txn.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calendario mensual 2010-2020 (132 meses) — granularidad del hecho mensual
dim_date_month = (
    spark.sql("""
        SELECT add_months(to_date('2010-01-01'), seq) AS first_of_month
        FROM (SELECT explode(sequence(0, 131)) AS seq)
    """)
    .withColumn("month_key",
        date_format(col("first_of_month"), "yyyyMM").cast("int"))
    .withColumn("year_month",
        date_format(col("first_of_month"), "yyyy-MM"))
    .withColumn("year",       year(col("first_of_month")))
    .withColumn("quarter",    quarter(col("first_of_month")))
    .withColumn("month",      month(col("first_of_month")))
    .withColumn("month_name", date_format(col("first_of_month"), "MMMM"))
    .withColumn("month_label",date_format(col("first_of_month"), "MMM yyyy"))
    .select("month_key", "year_month", "year", "quarter",
            "month", "month_name", "month_label")
)

dim_date_month.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("dim_date_month")

print(f"dim_date_month: {dim_date_month.count()} meses")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dimensión de tarjeta — atributos del producto
dim_card = cards.select(
    "card_id", "user_id", "card_brand", "card_type",
    "credit_limit", "credit_limit_band", "product_tier",
    "has_chip", "card_on_dark_web", "card_age_months",
    "num_cards_issued", "acct_open_date"
)

dim_card.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("dim_card")

print(f"dim_card: {dim_card.count():,} tarjetas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dimensión de cliente — perfil financiero y de riesgo
dim_user_profile = users.select(
    "user_id", "current_age", "age_band", "gender",
    "per_capita_income", "yearly_income", "income_band",
    "total_debt", "debt_to_income_ratio",
    "credit_score", "credit_score_band",
    "num_credit_cards", "customer_risk_index"
)

dim_user_profile.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("dim_user_profile")

print(f"dim_user_profile: {dim_user_profile.count():,} clientes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dimensión de categoría mercantil
dim_mcc = mcc.select("mcc", "mcc_description", "mcc_super_category")

dim_mcc.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("dim_mcc")

print(f"dim_mcc: {dim_mcc.count():,} códigos")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dimensión estática — bandas de utilización con acción recomendada
utilization_ref = [
    (0, "Dormant (<10%)",   0.00, 0.10, "Low",     "Incrementar límite o activar"),
    (1, "Low (10-30%)",     0.10, 0.30, "Low",     "Oportunidad de crecimiento"),
    (2, "Optimal (30-70%)", 0.30, 0.70, "Optimal", "Zona objetivo — maximizar"),
    (3, "High (70-90%)",    0.70, 0.90, "Elevated","Candidata a upgrade — monitorear"),
    (4, "Critical (>90%)",  0.90, 9.99, "High",    "Intervención de riesgo requerida"),
]

dim_utilization_band = spark.createDataFrame(
    utilization_ref,
    ["band_key", "band_label", "min_util", "max_util",
     "risk_level", "recommended_action"]
)

dim_utilization_band.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("dim_utilization_band")

print(f"dim_utilization_band: {dim_utilization_band.count()} bandas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_card_utilization = (
    monthly
    .withColumn("month_key",
        date_format(to_date(col("txn_year_month"), "yyyy-MM"), "yyyyMM").cast("int"))
    .select(
        "card_id", "user_id", "month_key",
        col("txn_year_month").alias("year_month"),
        col("txn_year").alias("year"),
        col("txn_month").alias("month"),
        "monthly_spend", "txn_count", "avg_ticket", "max_ticket",
        "online_spend", "chip_spend", "swipe_spend", "error_txn_count",
        "credit_limit", "utilization_ratio", "utilization_pct",
        "utilization_band", "interchange_revenue_proxy",
        "net_revenue_proxy", "limit_efficiency",
        "upgrade_candidate", "overutilization_flag",
        "product_tier", "credit_limit_band"
    )
)

fact_card_utilization.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_card_utilization")

print(f"fact_card_utilization: {fact_card_utilization.count():,} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_transactions_gold = (
    txn
    .withColumn("month_key",
        date_format(col("txn_date"), "yyyyMM").cast("int"))
    .select(
        "transaction_id", "txn_date", "month_key",
        "card_id", "user_id", "mcc", "channel", "amount",
        "merchant_city", "merchant_state", "error_type",
        col("txn_year").alias("year"),
        col("txn_month").alias("month")
    )
)

fact_transactions_gold.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_transactions_gold")

print(f"fact_transactions_gold: {fact_transactions_gold.count():,} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verificar que las claves del hecho existen en las dimensiones
fact = spark.table("fact_card_utilization")
d_card = spark.table("dim_card")
d_user = spark.table("dim_user_profile")
d_date = spark.table("dim_date_month")

# Claves huérfanas (deben ser 0)
orphan_cards = fact.join(d_card, "card_id", "left_anti").count()
orphan_users = fact.join(d_user, "user_id", "left_anti").count()
orphan_dates = fact.join(d_date, "month_key", "left_anti").count()

print(f"Filas del hecho:            {fact.count():,}")
print(f"card_id huérfanos:          {orphan_cards}")
print(f"user_id huérfanos:          {orphan_users}")
print(f"month_key huérfanos:        {orphan_dates}")

assert orphan_cards == 0, "Hay card_id sin tarjeta en dim_card"
assert orphan_users == 0, "Hay user_id sin cliente en dim_user_profile"
assert orphan_dates == 0, "Hay month_key fuera del rango de dim_date_month"
print("\nIntegridad referencial verificada. Modelo estrella listo.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
