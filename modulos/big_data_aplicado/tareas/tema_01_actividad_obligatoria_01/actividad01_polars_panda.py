# Tareas a realizar
# 1. Cargar el dataset movielens superdataset.csv.
# 2. Filtrar películas de género”Action” con rating ≥ 4.
# 3. Calcular media y desviación estándar de ratings por usuario.
# 4. Ordenar películas por rating promedio.
# 5. Convertir rating timestamp a fecha.
# 6. Medir tiempos de ejecución.
# 2.5 Polars: LazyFrame
# • Modo normal: ejecución inmediata.
# • LazyFrame: optimiza antes de ejecutar.
import pandas as pandas
import polars as polars
import time

data_path = '/modulos/big_data_aplicado/tareas/tema_01_M05_ACT_OBL_01/ml-20m'

################# Tarea-1. Cargar el dataset movielens superdataset.csv. #################

## Ejecución con Panda:
start_time = time.time()
df_pandas_movies = pandas.read_csv(f'{data_path}/movies.csv')
df_pandas_ratings = pandas.read_csv(f'{data_path}/ratings.csv')
df_pandas = pandas.merge(df_pandas_ratings, df_pandas_movies, on='movieId')
end_time = time.time()
pandas_time = end_time - start_time
print(f"Cargar el dataset movielens con Panda: {pandas_time:.4f} seconds")

## Ejecución con Polars:
start_time = time.time()
df_polars_movies = polars.read_csv(f'{data_path}/movies.csv')
df_polars_ratings = polars.read_csv(f'{data_path}/ratings.csv')
df_polars = df_polars_ratings.join(df_polars_movies, on='movieId')
end_time = time.time()
polars_time = end_time - start_time

print(f"Cargar el dataset movielens con Polars: {polars_time:.4f} seconds")

## Ejecución con Polars LazyFrame:
start_time = time.time()
df_polars_lazy = (
    polars.scan_csv(f'{data_path}/ratings.csv')
    .join(polars.scan_csv(f'{data_path}/movies.csv'), on='movieId')
    .collect()
)
end_time = time.time()
lazy_time = end_time - start_time
print(f"Cargar el dataset movielens con Polars LazyFrame: {lazy_time:.4f} seconds")





################# Tarea-2. Filtrar películas de género”Action” con rating ≥ 4. #################

## Ejecución con Panda:
start = time.time()
df_pandas_filtered = df_pandas[(df_pandas["genres"].str.contains("Action", na=False)) & (df_pandas["rating"] >= 4)]
t2_pandas = time.time() - start
print(f"Pandas - Filtrar películas Action con rating >= 4: {t2_pandas:.4f} sec")
print(f"Resultados: {len(df_pandas_filtered)} filas")

## Ejecución con Polars:
start = time.time()
df_polars_filtered = df_polars.filter(
    (polars.col("genres").str.contains("Action")) & (polars.col("rating") >= 4)
)
t2_polars = time.time() - start
print(f"Polars - Filtrar películas Action con rating >= 4: {t2_polars:.4f} sec")
print(f"Resultados: {len(df_polars_filtered)} filas")

## Ejecución con Polars LazyFrame:
start = time.time()
df_lazy_filtered = (
    df_polars_lazy.lazy()
    .filter((polars.col("genres").str.contains("Action")) & (polars.col("rating") >= 4))
    .collect()
)
t2_lazy = time.time() - start
print(f"Polars LazyFrame - Filtrar películas Action con rating >= 4: {t2_lazy:.4f} sec")
print(f"Resultados: {len(df_lazy_filtered)} filas")

################# Tarea-3. Calcular media y desviación estándar de ratings por usuario. #################

## Ejecución con Pandas:
start = time.time()
df_pandas_stats = df_pandas.groupby('userId')['rating'].agg(['mean', 'std'])
t3_pandas = time.time() - start
print(f"Pandas - Media y desviación estándar por usuario: {t3_pandas:.4f} sec")
print(df_pandas_stats.head())

## Ejecución con Polars:
start = time.time()
df_polars_stats = df_polars.group_by('userId').agg([
    polars.col('rating').mean().alias('mean'),
    polars.col('rating').std().alias('std')
])
t3_polars = time.time() - start
print(f"Polars - Media y desviación estándar por usuario: {t3_polars:.4f} sec")
print(df_polars_stats.head())

## Ejecución con Polars LazyFrame:
start = time.time()
df_lazy_stats = (
    df_polars_lazy.lazy()
    .group_by('userId').agg([
        polars.col('rating').mean().alias('mean'),
        polars.col('rating').std().alias('std')
    ])
    .collect()
)
t3_lazy = time.time() - start
print(f"Polars LazyFrame - Media y desviación estándar por usuario: {t3_lazy:.4f} sec")
print(df_lazy_stats.head())

################# Tarea-4. Ordenar películas por rating promedio. #################

## Ejecución con Pandas:
start = time.time()
df_pandas_avg = df_pandas.groupby('title')['rating'].mean().sort_values(ascending=False)
t4_pandas = time.time() - start
print(f"Pandas - Ordenar películas por rating promedio: {t4_pandas:.4f} sec")
print(df_pandas_avg.head(10))

## Ejecución con Polars:
start = time.time()
df_polars_avg = df_polars.group_by('title').agg(
    polars.col('rating').mean().alias('avg_rating')
).sort('avg_rating', descending=True)
t4_polars = time.time() - start
print(f"Polars - Ordenar películas por rating promedio: {t4_polars:.4f} sec")
print(df_polars_avg.head(10))

## Ejecución con Polars LazyFrame:
start = time.time()
df_lazy_avg = (
    df_polars_lazy.lazy()
    .group_by('title').agg(
        polars.col('rating').mean().alias('avg_rating')
    )
    .sort('avg_rating', descending=True)
    .collect()
)
t4_lazy = time.time() - start
print(f"Polars LazyFrame - Ordenar películas por rating promedio: {t4_lazy:.4f} sec")
print(df_lazy_avg.head(10))

################# Tarea-5. Convertir rating timestamp a fecha. #################

## Ejecución con Pandas:
start = time.time()
df_pandas['date'] = pandas.to_datetime(df_pandas['timestamp'], unit='s')
t5_pandas = time.time() - start
print(f"Pandas - Convertir timestamp a fecha: {t5_pandas:.4f} sec")
print(df_pandas[['userId', 'movieId', 'rating', 'timestamp', 'date']].head())

## Ejecución con Polars:
start = time.time()
df_polars = df_polars.with_columns(
    polars.from_epoch(polars.col('timestamp'), time_unit='s').alias('date')
)
t5_polars = time.time() - start
print(f"Polars - Convertir timestamp a fecha: {t5_polars:.4f} sec")
print(df_polars.select(['userId', 'movieId', 'rating', 'timestamp', 'date']).head())

## Ejecución con Polars LazyFrame:
start = time.time()
df_lazy_date = (
    df_polars_lazy.lazy()
    .with_columns(
        polars.from_epoch(polars.col('timestamp'), time_unit='s').alias('date')
    )
    .collect()
)
t5_lazy = time.time() - start
print(f"Polars LazyFrame - Convertir timestamp a fecha: {t5_lazy:.4f} sec")
print(df_lazy_date.select(['userId', 'movieId', 'rating', 'timestamp', 'date']).head())

################# Tarea-6. Medir tiempos de ejecución  #################

################# Generación de gráfica comparativa de tiempos de ejecución #################
import matplotlib.pyplot as plt
import numpy as np

tareas = ['Tarea 1: Cargar dataset', 'Tarea 2: Filtrar Action rating>=4', 'Tarea 3: Media y std por usuario', 'Tarea 4: Ordenar por rating promedio', 'Tarea 5: Timestamp a fecha']

# Invertir orden para que Tarea 1 quede arriba y las siguientes abajo
tareas = tareas[::-1]
tiempos_pandas = [pandas_time, t2_pandas, t3_pandas, t4_pandas, t5_pandas][::-1]
tiempos_polars = [polars_time, t2_polars, t3_polars, t4_polars, t5_polars][::-1]
tiempos_lazy = [lazy_time, t2_lazy, t3_lazy, t4_lazy, t5_lazy][::-1]

y = np.arange(len(tareas))
height = 0.25

fig, ax = plt.subplots(figsize=(12, 7))
bars1 = ax.barh(y - height, tiempos_pandas, height, label='Pandas', color='blue')
bars2 = ax.barh(y, tiempos_polars, height, label='Polars', color='green')
bars3 = ax.barh(y + height, tiempos_lazy, height, label='Polars LazyFrame', color='orange')

ax.set_xlabel('Tiempo de ejecución (segundos)')
ax.set_title('Comparación de rendimiento: Pandas vs. Polars vs. Polars LazyFrame')
ax.set_yticks(y)
ax.set_yticklabels(tareas)
ax.legend()

for bar in list(bars1) + list(bars2) + list(bars3):
    width = bar.get_width()
    ax.text(width + 0.001, bar.get_y() + bar.get_height()/2, f"{width:.4f} s", va='center', fontsize=9)

plt.tight_layout()
plt.show()
