# # solution_spark/processor.py
# from pyspark.sql import SparkSession, Window
# from pyspark.sql.functions import col, when, count, avg, lag, unix_timestamp

# def run_spark_analysis(data_path):
#     """
#     Executa a análise completa de dados meteorológicos usando Apache Spark.
#     """
#     # 1. Iniciar a SparkSession
#     spark = SparkSession.builder \
#         .appName("MeteorologicalAnalysis") \
#         .master("local[*]") \
#         .getOrCreate()

#     print("Sessão Spark iniciada.")

#     # 2. Carregar os dados
#     df = spark.read.csv(data_path, header=True, inferSchema=True)
#     print("Dados carregados em um DataFrame Spark.")
    
#     # --- Métrica 1: Percentual de anomalias por estação (como antes) ---
#     anomaly_conditions = (
#         (col("temperature") < 5.0) | (col("temperature") > 45.0) |
#         (col("humidity") < 0.0) | (col("humidity") > 100.0) |
#         (col("pressure") < 980.0) | (col("pressure") > 1040.0)
#     )
#     df_with_anomalies = df.withColumn("is_anomaly", when(anomaly_conditions, 1).otherwise(0)) \
#                           .withColumn("anomaly_sensor", 
#                               when(col("temperature") < 5.0, "temperature").when(col("temperature") > 45.0, "temperature")
#                               .when(col("humidity") < 0.0, "humidity").when(col("humidity") > 100.0, "humidity")
#                               .when(col("pressure") < 980.0, "pressure").when(col("pressure") > 1040.0, "pressure")
#                           )

#     station_anomaly_report = df_with_anomalies.groupBy("station_id") \
#         .agg(
#             count("*").alias("total_events"),
#             count(when(col("is_anomaly") == 1, True)).alias("anomaly_events")
#         ) \
#         .withColumn("anomaly_percentage", (col("anomaly_events") / col("total_events")) * 100) \
#         .orderBy("station_id")

#     # --- Métrica 2: Média móvel por região (excluindo anomalias) ---
#     # Define uma "janela" que particiona os dados por região e os ordena por tempo
#     window_region = Window.partitionBy("region").orderBy("timestamp").rowsBetween(-49, 0) # Janela de 50 eventos

#     # Filtra as anomalias antes de calcular
#     df_no_anomalies = df_with_anomalies.filter(col("is_anomaly") == 0)

#     # Aplica a função de média sobre a janela
#     df_moving_avg = df_no_anomalies.withColumn("temp_mov_avg", avg("temperature").over(window_region)) \
#                                    .withColumn("hum_mov_avg", avg("humidity").over(window_region)) \
#                                    .withColumn("press_mov_avg", avg("pressure").over(window_region))
    
#     # Pega o último valor calculado para cada região como representativo
#     region_moving_avg_report = df_moving_avg.groupBy("region") \
#         .agg(
#             avg("temp_mov_avg").alias("avg_temperature"),
#             avg("hum_mov_avg").alias("avg_humidity"),
#             avg("press_mov_avg").alias("avg_pressure")
#         )

#     # --- Métrica 3: Períodos com anomalias em sensores distintos ---
#     # Define uma janela por estação, ordenada por tempo, cobrindo um intervalo de 10 minutos
#     window_station_10min = Window.partitionBy("station_id") \
#                                  .orderBy(unix_timestamp("timestamp")) \
#                                  .rangeBetween(-600, 0) # 10 minutos = 600 segundos

#     # Conta o número de sensores distintos com anomalias na janela
#     df_multi_anomaly = df_with_anomalies.filter(col("is_anomaly") == 1) \
#         .withColumn("distinct_anomaly_sensors_in_window", 
#             count(col("anomaly_sensor")).over(window_station_10min)
#         )

#     # Marca os períodos que têm mais de 1 tipo de anomalia
#     multi_anomaly_periods = df_multi_anomaly.filter(col("distinct_anomaly_sensors_in_window") > 1) \
#         .groupBy("station_id") \
#         .count() \
#         .withColumnRenamed("count", "multi_anomaly_periods")

#     # --- Exibir todos os resultados ---
#     print("\n--- [Spark] Relatório de Anomalias por Estação ---")
#     station_anomaly_report.show(truncate=False)

#     print("\n--- [Spark] Relatório de Média Móvel por Região ---")
#     region_moving_avg_report.show(truncate=False)

#     print("\n--- [Spark] Relatório de Períodos com Múltiplas Anomalias por Estação ---")
#     multi_anomaly_periods.show(truncate=False)

#     # 5. Encerrar a sessão Spark
#     spark.stop()
#     print("Sessão Spark encerrada.")

# if __name__ == '__main__':
#     DATA_FILE_PATH = "data/synthetic_data.csv"
#     run_spark_analysis(DATA_FILE_PATH)



# solution_spark/processor.py
import time
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when, count, avg, unix_timestamp

def run_spark_analysis(data_path: str, num_workers: int) -> float:
    """
    Executa a análise completa de dados meteorológicos usando Apache Spark.
    Retorna o tempo total de execução.
    """
    start_time = time.perf_counter()

    # 1. Iniciar a SparkSession, controlando o paralelismo com .master()
    spark = SparkSession.builder \
        .appName(f"Analysis_Workers_{num_workers}") \
        .master(f"local[{num_workers}]") \
        .getOrCreate()
    
    # Reduz o nível de log para manter o output mais limpo
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Carregar os dados
    df = spark.read.csv(data_path, header=True, inferSchema=True)
    
    # 3. Executar as métricas
    
    # Métrica 1: Percentual de anomalias
    anomaly_conditions = (
        (col("temperature") < 5.0) | (col("temperature") > 45.0) |
        (col("humidity") < 0.0) | (col("humidity") > 100.0) |
        (col("pressure") < 980.0) | (col("pressure") > 1040.0)
    )
    df_with_anomalies = df.withColumn("is_anomaly", when(anomaly_conditions, 1).otherwise(0)) \
                          .withColumn("anomaly_sensor", 
                              when(col("temperature") < 5.0, "temperature").when(col("temperature") > 45.0, "temperature")
                              .when(col("humidity") < 0.0, "humidity").when(col("humidity") > 100.0, "humidity")
                              .when(col("pressure") < 980.0, "pressure").when(col("pressure") > 1040.0, "pressure")
                          )

    station_anomaly_report = df_with_anomalies.groupBy("station_id") \
        .agg(
            count("*").alias("total_events"),
            count(when(col("is_anomaly") == 1, True)).alias("anomaly_events")
        )

    # Métrica 2: Média móvel
    window_region = Window.partitionBy("region").orderBy("timestamp").rowsBetween(-49, 0)
    df_no_anomalies = df_with_anomalies.filter(col("is_anomaly") == 0)
    region_moving_avg_report = df_no_anomalies.withColumn("temp_mov_avg", avg("temperature").over(window_region)) \
                                   .groupBy("region") \
                                   .agg(avg("temp_mov_avg").alias("avg_temperature"))

    # Métrica 3: Anomalias múltiplas
    window_station_10min = Window.partitionBy("station_id").orderBy(unix_timestamp("timestamp")).rangeBetween(-600, 0)
    multi_anomaly_periods = df_with_anomalies.filter(col("is_anomaly") == 1) \
        .withColumn("distinct_anomaly_sensors_in_window", count(col("anomaly_sensor")).over(window_station_10min)) \
        .filter(col("distinct_anomaly_sensors_in_window") > 1) \
        .groupBy("station_id") \
        .count()

    # 4. Forçar a execução de todas as ações para garantir que o trabalho foi feito
    station_anomaly_report.collect()
    region_moving_avg_report.collect()
    multi_anomaly_periods.collect()

    # 5. Encerrar a sessão e o tempo
    spark.stop()
    end_time = time.perf_counter()
    
    return end_time - start_time

if __name__ == '__main__':
    # Bloco para teste direto do script
    DATA_FILE = "../data/synthetic_data.csv"
    print("Iniciando teste direto do processador Spark com 4 workers...")
    execution_time = run_spark_analysis(DATA_FILE, num_workers=4)
    print(f"Tempo de execução do teste direto: {execution_time:.4f} segundos.")