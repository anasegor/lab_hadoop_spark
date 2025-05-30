# coding: utf-8
import time
import logging
import psutil
import sys
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import min, max, col
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler, StringIndexer

OPTIMIZED = True if sys.argv[1] == "True" else False

# --- Настройка логгера ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# --- Создание SparkSession с минимальными логами ---
conf = SparkConf()
conf.set("spark.ui.showConsoleProgress", "false")
conf.set("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
conf.set("spark.executor.memory", "1g")
conf.set("spark.driver.memory", "2g")

# --- SparkSession ---
spark = (
    SparkSession.builder.appName("SparkPerformanceApp")
    .master("spark://spark-master:7077")
    .config(conf=conf)
    .getOrCreate()
)

# Уровень логирования
spark.sparkContext.setLogLevel("ERROR")  # Убираем WARN, FATAL и прочее

# --- Замер времени ---
start_time = time.time()
logger.info("Загрузка данных началась.")

# --- Чтение данных ---
df = spark.read.csv(
    "hdfs:///data/spotify-tracks-dataset.csv", header=True, inferSchema=True
)
logger.info("Данные загружены: {} строк.".format(df.count()))

# --- Обработка данных ---
df = df.fillna(0)
df = df.withColumn("popularity", col("popularity").cast("int"))
df = df.withColumn("danceability", col("danceability").cast("float"))
df = df.withColumn("energy", col("energy").cast("float"))
df = df.withColumn("key", col("key").cast("float"))
df = df.withColumn("loudness", col("loudness").cast("float"))
df = df.withColumn("mode", col("mode").cast("float"))
df = df.withColumn("speechiness", col("speechiness").cast("float"))
df = df.withColumn("acousticness", col("acousticness").cast("float"))
df = df.withColumn("liveness", col("liveness").cast("float"))
df = df.withColumn("valence", col("valence").cast("float"))
df = df.withColumn("duration_ms", col("duration_ms").cast("float"))

# if OPTIMIZED:
#     df = df.repartition(5).cache()

logger.info("Начата фильтрация данных.")
filtered = df.filter(df["popularity"] > 0)

logger.info("После фильтрации: {} строк.".format(df.count()))

logger.info("Начата агрегация.")
agg = df.groupBy("track_genre").count()
agg1 = df.groupBy("time_signature").count()

agg.show(5)
agg1.show(5)
df.show(5)

logger.info("Поиск самых коротких и длинных треков.")
df1 = df.withColumn("duration_min", spark_round(df["duration_ms"] / 60000, 2))
shortest = df1.orderBy("duration_min").select("track_name", "duration_min").limit(5)
longest = (
    df1.orderBy(df1["duration_min"].desc())
    .select("track_name", "duration_min")
    .limit(5)
)
logger.info("Самые короткие треки:")
shortest.show(truncate=False)
logger.info("Самые длинные треки:")
longest.show(truncate=False)

logger.info("Подсчёт средней популярности по жанрам.")
avg_popularity = df.groupBy("track_genre").avg("popularity")
avg_popularity = avg_popularity.withColumnRenamed("avg(popularity)", "avg_popularity")
avg_popularity.orderBy("avg_popularity", ascending=False).show(5)

logger.info("Начата нормализация значений popularity.")
pop_min = df.select(min("popularity")).first()[0]
pop_max = df.select(max("popularity")).first()[0]
normalized = df.withColumn(
    "popularity_norm", (col("popularity") - pop_min) / (pop_max - pop_min)
)
normalized.select("popularity", "popularity_norm").show(5)

indexer = StringIndexer(inputCol="track_genre", outputCol="genre")
df = indexer.fit(df).transform(df)
numeric_cols = [
    "popularity",
    "duration_ms",
    "danceability",
    "energy",
    "key",
    "loudness",
    "mode",
    "speechiness",
    "acousticness",
    "instrumentalness",
    "liveness",
    "valence",
    "tempo",
    "time_signature",
]
assembler = VectorAssembler(
    inputCols=numeric_cols, outputCol="vectorized_data", handleInvalid="skip"
)
df = assembler.transform(df)

data_train, data_test = df.randomSplit([0.75, 0.25])
if OPTIMIZED:
    data_train.cache()
    data_train = data_train.repartition(5)
    data_test.cache()
    data_test = data_test.repartition(5)


model = LogisticRegression(featuresCol="vectorized_data", labelCol="genre")
model = model.fit(data_train)
pred_test = model.transform(data_test)

# --- Замер времени и RAM ---
end_time = time.time()
elapsed = end_time - start_time
spark.stop()
logger.info("Время выполнения Spark-пайплайна: {:.2f} секунд".format(elapsed))

process = psutil.Process(os.getpid())
ram_usage_mb = process.memory_info().rss / (1024 * 1024)
logger.info("Использование памяти драйвера: {:.2f} MB".format(ram_usage_mb))

# --- Завершение ---
logger.info("Завершение приложения.")
