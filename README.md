# Credit Card Profitability — Microsoft Fabric Data Engineering Project

Pipeline de datos end-to-end sobre **Microsoft Fabric** que transforma ~13M de transacciones de tarjetas de crédito en un modelo estrella y un reporte de **Power BI** para analizar utilización de límite y rentabilidad del portafolio.

## Arquitectura (Medallion)

```
Files/raw (CSV/JSON)
      │  01_bronze_ingestion
      ▼
 credit_bronze (Lakehouse)   → bronze_transactions, bronze_cards, bronze_users, bronze_mcc_codes
      │  02_bronze_to_silver
      ▼
 credit_silver (Lakehouse)   → silver_transactions, silver_cards, silver_users, silver_mcc, silver_monthly_spend
      │  03_silver_to_gold
      ▼
 credit_gold (Lakehouse) / credit_gold_dw (Warehouse)   → modelo estrella
      │
      ▼
 sm_card_profitability (Semantic Model) → report_card_profit (Power BI)
```

La orquestación la hace el pipeline `pl_credit_etl`: **raw → silver → gold**, en secuencia (cada paso depende del éxito del anterior).

## Contenido del repositorio

Todo vive en [Credit-Project/](Credit-Project/) (formato Fabric Git integration):

| Item | Descripción |
|---|---|
| `01_bronze_ingestion.Notebook` | Lee CSV/JSON crudos y los guarda como Delta; transacciones particionadas por año/mes, con `_ingested_at`. |
| `02_bronze_to_silver.Notebook` | Limpieza y tipado (montos `$`, fechas, canal Chip/Swipe/Online, errores), bandas de edad/ingreso/score/límite, índice de riesgo del cliente, catálogo MCC en súper-categorías y tabla pivote `silver_monthly_spend` con controles de calidad. |
| `03_silver_to_gold.Notebook` | Construye las dimensiones y hechos del modelo estrella y valida integridad referencial (sin claves huérfanas). |
| `pl_credit_etl.DataPipeline` | Pipeline que ejecuta los 3 notebooks en orden. |
| `credit_bronze / credit_silver / credit_gold .Lakehouse` | Lakehouses de cada capa. |
| `credit_gold_dw.Warehouse` | Warehouse sobre la capa Gold. |
| `sm_card_profitability.SemanticModel` | Modelo semántico (TMDL): relaciones y medidas DAX. |
| `report_card_profit.Report` | Reporte Power BI de 4 páginas: *Portfolio_Executive*, *Utilization Bands*, *Card Detail*, *Profitability Deep Dive*. |
| `CopyJob_1.CopyJob` | Copy Job de Fabric (vacío, sin actividades definidas). |

## Modelo de datos (Gold)

- **Hechos:** `fact_card_utilization` (tarjeta × mes), `fact_transactions_gold` (transacción).
- **Dimensiones:** `dim_card`, `dim_user_profile`, `dim_mcc`, `dim_date_month` (2010–2020), `dim_utilization_band`.

## Reglas de negocio destacadas

- **Utilización** = gasto mensual / límite de crédito, en bandas: Dormant (<10%), Low, Optimal (30–70%), High, Critical (>90%).
- **Ingreso por intercambio (proxy)** = gasto × 2.35 %; **ingreso neto (proxy)** = intercambio − límite × 5.5 % / 12 (costo de fondos).
- **Candidata a upgrade:** utilización 65–90 % y sin errores en el mes. **Sobreutilización:** > 90 %.
- **Riesgo del cliente:** `debt_to_income_ratio` e índice compuesto con score tipo FICO.

## Tecnologías

Microsoft Fabric · PySpark · Delta Lake · Data Pipelines · Warehouse · Power BI (TMDL / PBIR) · DAX

## Cómo reproducirlo

1. Crear un workspace en Fabric y conectarlo a este repo (Git integration) o importar los items.
2. Subir los datos crudos a `credit_bronze` en `Files/raw/`: `transactions_data.csv`, `cards_data.csv`, `users_data.csv`, `mcc_codes.json`.
3. Ajustar las rutas/IDs específicos del workspace (p. ej. la ruta `abfss://...` en el notebook 02 y la conexión del modelo semántico).
4. Ejecutar `pl_credit_etl` y luego refrescar el modelo semántico y el reporte.

> Los datos crudos no se incluyen en el repositorio.

## Autor

Mauricio Millan
