import time
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when, count, avg, unix_timestamp, to_timestamp
import os

def run_spark_analysis(data_path: str, num_workers: int) -> tuple[float, list]:
    """
    Executa a análise completa de dados meteorológicos usando Apache Spark.
    Retorna o tempo total de execução e a lista de anomalias detectadas.
    """
    start_time = time.perf_counter()

    # 1. Iniciar a SparkSession
    spark = SparkSession.builder \
        .appName(f"Analysis_Workers_{num_workers}") \
        .master(f"local[{num_workers}]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Carregar os dados
    df = spark.read.csv(data_path, header=True, inferSchema=False) \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("temperature", col("temperature").cast("float")) \
        .withColumn("humidity", col("humidity").cast("float")) \
        .withColumn("pressure", col("pressure").cast("float")) \
        .withColumn("station_id", col("station_id").cast("integer"))


    # 3. Identificar anomalias e enriquecer o DataFrame
    anomaly_conditions = (
        (col("temperature") < 5.0) | (col("temperature") > 45.0) |
        (col("humidity") < 0.0) | (col("humidity") > 100.0) |
        (col("pressure") < 980.0) | (col("pressure") > 1040.0)
    )
    
    df_with_anomalies = df.withColumn("is_anomaly", when(anomaly_conditions, 1).otherwise(0)) \
        .withColumn("anomaly_sensor", 
            when((col("temperature") < 5.0) | (col("temperature") > 45.0), "temperature")
            .when((col("humidity") < 0.0) | (col("humidity") > 100.0), "humidity")
            .when((col("pressure") < 980.0) | (col("pressure") > 1040.0), "pressure")
            .otherwise(None)
        )
    
    df_with_anomalies.cache()

    
    # Métrica 1: Relatório de anomalias por estação
    station_anomaly_report = df_with_anomalies.groupBy("station_id") \
        .agg(
            count("*").alias("total_events"),
            count(when(col("is_anomaly") == 1, True)).alias("anomaly_events")
        )

    # Métrica 2: Média móvel por região
    window_region = Window.partitionBy("region").orderBy("timestamp").rowsBetween(-49, 0)
    df_no_anomalies = df_with_anomalies.filter(col("is_anomaly") == 0)
    region_moving_avg_report = df_no_anomalies.withColumn("temp_mov_avg", avg("temperature").over(window_region)) \
        .groupBy("region") \
        .agg(avg("temp_mov_avg").alias("avg_temperature"))

    # Métrica 3: Períodos de anomalias múltiplas
    window_station_10min = Window.partitionBy("station_id").orderBy(unix_timestamp("timestamp")).rangeBetween(-600, 0)
    multi_anomaly_periods = df_with_anomalies.filter(col("is_anomaly") == 1) \
        .withColumn("distinct_anomaly_sensors_in_window", count(col("anomaly_sensor")).over(window_station_10min)) \
        .filter(col("distinct_anomaly_sensors_in_window") > 1) \
        .groupBy("station_id") \
        .count()
    found_anomalies_df = df_with_anomalies \
        .filter(col("is_anomaly") == 1) \
        .select("timestamp", "station_id", "anomaly_sensor")

    station_anomaly_report.collect()
    region_moving_avg_report.collect()
    multi_anomaly_periods.collect()
    
    found_anomalies_rows = found_anomalies_df.collect()
    found_anomalies_list = [
        {
            "timestamp": row.timestamp.isoformat(), 
            "station_id": row.station_id, 
            "sensor": row.anomaly_sensor
        } 
        for row in found_anomalies_rows
    ]

    df_with_anomalies.unpersist()
    
    spark.stop()
    end_time = time.perf_counter()
    
    return (end_time - start_time), found_anomalies_list

if __name__ == '__main__':
    DATA_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'synthetic_data.csv')
    print("Iniciando teste direto do processador Spark com 4 workers...")
    
    execution_time, anomalies_found = run_spark_analysis(DATA_FILE, num_workers=4)
    
    print(f"Tempo de execução do teste direto: {execution_time:.4f} segundos.")
    print(f"Total de anomalias encontradas pelo Spark: {len(anomalies_found)}")
    if anomalies_found:
        print("Exemplo de anomalia encontrada:", anomalies_found[0])