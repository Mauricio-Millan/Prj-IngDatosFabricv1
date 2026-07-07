# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "652ebbd0-ad01-4405-a4c9-97cd145127f6",
# META       "default_lakehouse_name": "credit_silver",
# META       "default_lakehouse_workspace_id": "460f6e60-af50-42e2-aaa2-56514df94542",
# META       "known_lakehouses": [
# META         {
# META           "id": "8ebe1319-241d-4cd2-add6-4c97c366dcee"
# META         },
# META         {
# META           "id": "652ebbd0-ad01-4405-a4c9-97cd145127f6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ================================================================
# NOTEBOOK: 02_bronze_to_silver
# CAPA: Silver — Limpieza, enriquecimiento y agregación mensual
# ================================================================

from pyspark.sql.functions import (
    col, to_timestamp, to_date, year, month, date_format,
    when, regexp_replace, trim, upper, coalesce, lit,
    round as spark_round, sum as spark_sum, count as spark_count,
    avg as spark_avg, max as spark_max, months_between, current_date
)
from pyspark.sql.types import DoubleType, IntegerType

# Optimizaciones de Spark para el trial (capacidad compartida)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

print("Configuración Spark aplicada. Sesión lista.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leer las 4 tablas crudas desde el Lakehouse Bronze
txn_raw   = spark.table("credit_bronze.dbo.bronze_transactions")
cards_raw = spark.table("credit_bronze.dbo.bronze_cards")
users_raw = spark.table("credit_bronze.dbo.bronze_users")
mcc_raw   = spark.table("credit_bronze.dbo.bronze_mcc_codes")

print(f"Transactions Bronze: {txn_raw.count():,} filas")
print(f"Cards Bronze:        {cards_raw.count():,} filas")
print(f"Users Bronze:        {users_raw.count():,} filas")
print(f"MCC Bronze:          {mcc_raw.count():,} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Limpieza y normalización de la tabla transaccional
txn_clean = (
    txn_raw
    # 1. Parsear fecha y derivar componentes temporales
    .withColumn("txn_datetime", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("txn_date", to_date(col("txn_datetime")))
    .withColumn("txn_year",  year(col("txn_datetime")))
    .withColumn("txn_month", month(col("txn_datetime")))
    .withColumn("txn_year_month", date_format(col("txn_datetime"), "yyyy-MM"))

    # 2. Limpiar monto: quitar "$", comas y espacios → double
    .withColumn("amount",
        regexp_replace(col("amount"), r"[\$,\s]", "").cast(DoubleType()))

    # 3. Normalizar canal a 3 valores
    .withColumn("channel",
        when(upper(col("use_chip")).contains("CHIP"), "Chip")
        .when(upper(col("use_chip")).contains("SWIPE"), "Swipe")
        .otherwise("Online"))

    # 4. Normalizar geografía
    .withColumn("merchant_state", upper(trim(col("merchant_state"))))
    .withColumn("merchant_city",  trim(col("merchant_city")))

    # 5. Clasificar errores (nulo o vacío → "None")
    .withColumn("error_type",
        when(col("errors").isNull() | (col("errors") == ""), "None")
        .otherwise(trim(col("errors"))))
    .withColumn("has_error",
        when(col("error_type") == "None", False).otherwise(True))

    # 6. Tipar el MCC
    .withColumn("mcc", col("mcc").cast(IntegerType()))

    # 7. Renombrar claves para consistencia del modelo
    .withColumnRenamed("id", "transaction_id")
    .withColumnRenamed("client_id", "user_id")

    # 8. Filtros de calidad
    .filter(col("amount").isNotNull() & (col("amount") > 0))
    .filter(col("txn_datetime").isNotNull())
    .filter(col("card_id").isNotNull())
    .dropDuplicates(["transaction_id"])
)

print(f"Transactions tras limpieza: {txn_clean.count():,} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Limpieza de la dimensión de tarjetas — credit_limit es la columna clave
cards_clean = (
    cards_raw
    .withColumnRenamed("id", "card_id")
    .withColumnRenamed("client_id", "user_id")

    # Limpiar credit_limit: "$10,500" → 10500.0
    .withColumn("credit_limit",
        regexp_replace(col("credit_limit"), r"[\$,\s]", "").cast(DoubleType()))

    # Convertir booleanos textuales
    .withColumn("has_chip",
        when(upper(col("has_chip")) == "YES", True).otherwise(False))
    .withColumn("card_on_dark_web",
        when(upper(col("card_on_dark_web")) == "YES", True).otherwise(False))

    # Normalizar strings
    .withColumn("card_type",  upper(trim(col("card_type"))))
    .withColumn("card_brand", upper(trim(col("card_brand"))))

    # Parsear fecha de apertura (formato MM/YYYY) y calcular antigüedad
    .withColumn("acct_open_date", to_date(col("acct_open_date"), "MM/yyyy"))
    .withColumn("card_age_months",
        months_between(current_date(), col("acct_open_date")).cast(IntegerType()))

    # Banda de límite de crédito (regla de negocio)
    .withColumn("credit_limit_band",
        when(col("credit_limit") < 3000,  "Basic (<$3K)")
        .when(col("credit_limit") < 8000,  "Standard ($3K-$8K)")
        .when(col("credit_limit") < 20000, "Gold ($8K-$20K)")
        .when(col("credit_limit") < 40000, "Platinum ($20K-$40K)")
        .otherwise("Premium (>$40K)"))

    # Tier de producto (usado luego para RLS en Power BI)
    .withColumn("product_tier",
        when(col("card_type").contains("DEBIT"), "Debit")
        .when(col("credit_limit") < 5000,  "Entry")
        .when(col("credit_limit") < 20000, "Standard")
        .otherwise("Premium"))

    .filter(col("credit_limit").isNotNull() & (col("credit_limit") > 0))
    .dropDuplicates(["card_id"])
)

print(f"Cards tras limpieza: {cards_clean.count():,} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Limpieza de la dimensión de clientes + derivaciones de riesgo
users_clean = (
    users_raw
    .withColumnRenamed("id", "user_id")

    # Limpiar campos monetarios
    .withColumn("yearly_income",
        regexp_replace(col("yearly_income"), r"[\$,\s]", "").cast(DoubleType()))
    .withColumn("total_debt",
        regexp_replace(col("total_debt"), r"[\$,\s]", "").cast(DoubleType()))
    .withColumn("per_capita_income",
        regexp_replace(col("per_capita_income"), r"[\$,\s]", "").cast(DoubleType()))

    # Ratio deuda/ingreso (DTI)
    .withColumn("debt_to_income_ratio",
        spark_round(col("total_debt") / col("yearly_income"), 4))

    # Normalizar género
    .withColumn("gender", upper(trim(col("gender"))))

    # Banda de edad
    .withColumn("age_band",
        when(col("current_age") < 25, "18-24")
        .when(col("current_age") < 35, "25-34")
        .when(col("current_age") < 50, "35-49")
        .when(col("current_age") < 65, "50-64")
        .otherwise("65+"))

    # Banda de ingreso anual
    .withColumn("income_band",
        when(col("yearly_income") < 30000,  "Low (<$30K)")
        .when(col("yearly_income") < 70000,  "Mid ($30K-$70K)")
        .when(col("yearly_income") < 120000, "High ($70K-$120K)")
        .otherwise("Premium (>$120K)"))

    # Banda de score crediticio (escala FICO)
    .withColumn("credit_score_band",
        when(col("credit_score") < 580, "Poor (<580)")
        .when(col("credit_score") < 670, "Fair (580-669)")
        .when(col("credit_score") < 740, "Good (670-739)")
        .when(col("credit_score") < 800, "Very Good (740-799)")
        .otherwise("Exceptional (800+)"))

    # Índice compuesto de riesgo del cliente
    .withColumn("customer_risk_index",
        spark_round(
            (col("total_debt") / col("yearly_income")) * 100 /
            (col("credit_score") / 850), 4))

    .filter(col("yearly_income").isNotNull() & (col("yearly_income") > 0))
    .dropDuplicates(["user_id"])
)

print(f"Users tras limpieza: {users_clean.count():,} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# Catálogo MCC — el JSON es {codigo: descripcion}, hay que pivotarlo
# ================================================================
import json

# Ruta al archivo crudo en el Lakehouse Bronze.
# Ajusta si lo subiste a otra carpeta (revisa Files/ en credit_bronze).
mcc_path = "abfss://9d122fb1-a2c7-48e3-a1f4-fb6273d3031b@onelake.dfs.fabric.microsoft.com/471e40d9-8235-4974-b4af-ea96c9e7093b/Files/raw/mcc_codes.json"

# Leer el JSON como texto y parsearlo con Python
mcc_text = spark.read.text(mcc_path).agg({"value": "collect_list"}).collect()[0][0]
mcc_dict = json.loads("".join(mcc_text))

# Convertir {codigo: descripcion} en filas
mcc_rows = [
    {"mcc": int(codigo), "mcc_description": desc}
    for codigo, desc in mcc_dict.items()
]
mcc_raw_fixed = spark.createDataFrame(mcc_rows)

# Categorización en super-categorías de negocio
from pyspark.sql.functions import col, when

mcc_clean = (
    mcc_raw_fixed
    .withColumn("mcc_super_category",
        when(col("mcc").between(5411, 5499), "Grocery & Food")
        .when(col("mcc").between(5912, 5999), "Retail & Drug")
        .when(col("mcc").between(4111, 4789), "Travel & Transport")
        .when(col("mcc").between(7011, 7999), "Leisure & Entertainment")
        .when(col("mcc").between(8000, 8099), "Healthcare")
        .when(col("mcc").between(6010, 6099), "Financial Services")
        .when(col("mcc").between(5812, 5814), "Restaurants & Bars")
        .otherwise("Other"))
    .dropDuplicates(["mcc"])
)

print(f"MCC tras limpieza: {mcc_clean.count():,} filas")
mcc_clean.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Escribir las dimensiones limpias como tablas Delta en credit_silver
txn_clean.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("txn_year", "txn_month") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_transactions")

cards_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_cards")

users_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_users")

mcc_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_mcc")

print("4 tablas Silver base persistidas correctamente.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# TABLA PIVOTE — corazón analítico del proyecto
# Convierte ~13M transacciones diarias en gasto mensual por tarjeta
# y calcula utilización + proxies de rentabilidad
# ================================================================

INTERCHANGE_RATE = 0.0235   # 2.35% — tasa promedio Visa/Mastercard 2024
COST_OF_FUNDS    = 0.055    # 5.5%  — costo de fondos anual estimado 2024

# 1. Agregación mensual por tarjeta
monthly = (
    txn_clean
    .groupBy("card_id", "txn_year_month", "txn_year", "txn_month")
    .agg(
        spark_sum("amount").alias("monthly_spend"),
        spark_count("transaction_id").alias("txn_count"),
        spark_avg("amount").alias("avg_ticket"),
        spark_max("amount").alias("max_ticket"),
        spark_sum(when(col("channel") == "Online", col("amount")).otherwise(0)).alias("online_spend"),
        spark_sum(when(col("channel") == "Chip",   col("amount")).otherwise(0)).alias("chip_spend"),
        spark_sum(when(col("channel") == "Swipe",  col("amount")).otherwise(0)).alias("swipe_spend"),
        spark_count(when(col("has_error") == True, 1)).alias("error_txn_count")
    )
)

# 2. Unir con el límite de crédito y atributos de la tarjeta
monthly = monthly.join(
    cards_clean.select("card_id", "user_id", "credit_limit",
                       "card_type", "card_brand", "product_tier",
                       "credit_limit_band"),
    on="card_id", how="inner"
)

# 3. Calcular utilización + rentabilidad + flags de negocio
silver_monthly = (
    monthly
    .withColumn("utilization_ratio",
        spark_round(col("monthly_spend") / col("credit_limit"), 4))
    .withColumn("utilization_pct",
        spark_round(col("monthly_spend") / col("credit_limit") * 100, 2))

    .withColumn("utilization_band",
        when(col("utilization_ratio") < 0.10, "Dormant (<10%)")
        .when(col("utilization_ratio") < 0.30, "Low (10-30%)")
        .when(col("utilization_ratio") < 0.70, "Optimal (30-70%)")
        .when(col("utilization_ratio") < 0.90, "High (70-90%)")
        .otherwise("Critical (>90%)"))

    .withColumn("interchange_revenue_proxy",
        spark_round(col("monthly_spend") * INTERCHANGE_RATE, 2))
    .withColumn("net_revenue_proxy",
        spark_round(col("monthly_spend") * INTERCHANGE_RATE
                    - col("credit_limit") * COST_OF_FUNDS / 12, 2))
    .withColumn("limit_efficiency",
        spark_round(col("monthly_spend") * INTERCHANGE_RATE / col("credit_limit"), 6))

    # Flag: candidata a incremento de límite (alta utilización, sin errores)
    .withColumn("upgrade_candidate",
        when((col("utilization_ratio").between(0.65, 0.90)) &
             (col("error_txn_count") == 0), True).otherwise(False))

    # Flag: zona crítica
    .withColumn("overutilization_flag",
        when(col("utilization_ratio") > 0.90, True).otherwise(False))
)

# 4. Persistir la tabla pivote
silver_monthly.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("txn_year", "txn_month") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_monthly_spend")

print(f"silver_monthly_spend persistida: {silver_monthly.count():,} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Controles de calidad antes de pasar a Gold
from pyspark.sql.functions import count as c, when as w

checks = silver_monthly.agg(
    c("*").alias("total_filas"),
    c(w(col("utilization_ratio").isNull(), 1)).alias("util_nulos"),
    c(w(col("credit_limit") <= 0, 1)).alias("limite_invalido"),
    c(w(col("utilization_ratio") > 5, 1)).alias("util_extremos")
).collect()[0]

print(f"Total filas:          {checks['total_filas']:,}")
print(f"Utilización nula:     {checks['util_nulos']}")
print(f"Límite inválido:      {checks['limite_invalido']}")
print(f"Utilización > 500%:   {checks['util_extremos']}  (revisar outliers)")

assert checks["util_nulos"] == 0, "Hay utilizaciones nulas — revisar joins"
print("\nValidación de calidad superada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
