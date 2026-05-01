# Actividad 01 - PySpark (Databricks)
# Tareas a realizar
# 1. Cargar el dataset movielens.
# 2. Filtrar películas de género "Action" con rating ≥ 4.
# 3. Calcular media y desviación estándar de ratings por usuario.
# 4. Ordenar películas por rating promedio.
# 5. Convertir rating timestamp a fecha.
# 6. Medir tiempos de ejecución.

# Este fichero es el que subo a mi workspace en DATABRICKS

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, from_unixtime
from pyspark import StorageLevel
import time


def try_cache_dataframe(df_to_cache, use_disk=True, label="DataFrame"):
    """Intenta cachear/persistir; en Serverless (Comunity me sale error que no está soportado) puede no estar soportado."""
    try:
        if use_disk:
            cached_df = df_to_cache.persist(StorageLevel.MEMORY_AND_DISK)
        else:
            cached_df = df_to_cache.cache()
        return cached_df, True
    except Exception as err:
        err_msg = str(err)
        if "NOT_SUPPORTED_WITH_SERVERLESS" in err_msg:
            print(f"[WARN] {label}: cache/persist no soportado en serverless. Continuo sin cache.")
            return df_to_cache, False
        raise


def try_unpersist_dataframe(df_to_unpersist, was_cached):
    """Libera cache solo si realmente se pudo cachear."""
    if not was_cached:
        return
    try:
        df_to_unpersist.unpersist()
    except Exception:
        # No bloqueamos el script por un fallo al liberar cache.
        pass

# En Databricks, spark ya está disponible por eso dice el profe qeu es tan rapido
# spark = SparkSession.builder.appName("Actividad01").getOrCreate()

# Ruta a los datos - ajustar según tu ubicación en DBFS
# Path del volumen que he usado para subir los dos CSV
data_path = "/Volumes/workspace/default/actividad01"

################# Tarea-1. Cargar el dataset movielens. #################
start = time.time()
df_movies = spark.read.csv(f"{data_path}/movies.csv", header=True, inferSchema=True)
df_ratings = spark.read.csv(f"{data_path}/ratings.csv", header=True, inferSchema=True)
# En Community Edition suele haber memoria limitada: mejor MEMORY_AND_DISK
df = df_ratings.join(df_movies, on="movieId")
df, df_cached = try_cache_dataframe(df, use_disk=True, label="df base")
df.count()  # Forzar materialización
t1_spark = time.time() - start
print(f"PySpark - Cargar dataset: {t1_spark:.4f} sec")

################# Tarea-2. Filtrar películas de género "Action" con rating ≥ 4. #################
start = time.time()
df_filtered = df.filter(col("genres").contains("Action") & (col("rating") >= 4))
count_filtered = df_filtered.count()
t2_spark = time.time() - start
print(f"PySpark - Filtrar películas Action con rating >= 4: {t2_spark:.4f} sec")
print(f"Resultados: {count_filtered} filas")

################# Tarea-3. Calcular media y desviación estándar de ratings por usuario. #################
start = time.time()
df_stats = df.groupBy("userId").agg(
    mean("rating").alias("mean"),
    stddev("rating").alias("std")
)
df_stats.count()  # Forzar ejecución
t3_spark = time.time() - start
print(f"PySpark - Media y desviación estándar por usuario: {t3_spark:.4f} sec")
df_stats.show(5)

################# Tarea-4. Ordenar películas por rating promedio. #################
start = time.time()
df_avg = df.groupBy("title").agg(
    mean("rating").alias("avg_rating")
).orderBy(col("avg_rating").desc())
df_avg.count()  # Forzar ejecución
t4_spark = time.time() - start
print(f"PySpark - Ordenar películas por rating promedio: {t4_spark:.4f} sec")
df_avg.show(10)

################# Tarea-5. Convertir rating timestamp a fecha. #################
start = time.time()
df_with_date = df.withColumn("date", from_unixtime(col("timestamp")))
df_with_date_selected = df_with_date.select("userId", "movieId", "rating", "timestamp", "date")
df_with_date_selected, date_cached = try_cache_dataframe(
    df_with_date_selected,
    use_disk=False,
    label="df_with_date_selected"
)
df_with_date_selected.count()
t5_spark = time.time() - start
print(f"PySpark - Convertir timestamp a fecha: {t5_spark:.4f} sec")
df_with_date_selected.show(5)

# Liberar memoria cacheada al terminar
try_unpersist_dataframe(df_with_date_selected, date_cached)
try_unpersist_dataframe(df, df_cached)

################# Tarea-6. Resumen de tiempos de ejecución #################
print("\n" + "="*60)
print("RESUMEN DE TIEMPOS - PySpark")
print("="*60)
print(f"Tarea 1 - Cargar dataset:              {t1_spark:.4f} sec")
print(f"Tarea 2 - Filtrar Action rating>=4:    {t2_spark:.4f} sec")
print(f"Tarea 3 - Media y std por usuario:     {t3_spark:.4f} sec")
print(f"Tarea 4 - Ordenar por rating promedio: {t4_spark:.4f} sec")
print(f"Tarea 5 - Timestamp a fecha:           {t5_spark:.4f} sec")
print("="*60)

