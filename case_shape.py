from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_utc_timestamp, to_timestamp, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import functions as F

def load_log_data(spark, file_path):
    return spark.read.text(file_path)

def load_sensor_equipment_data(spark, file_path):
    return spark.read.csv(file_path, header=True)

def load_equipment_data(spark, file_path):
    equipment_schema = StructType([
        StructField("equipment_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("group_name", StringType(), True)
    ])
    return spark.read.option("multiline", "true").schema(equipment_schema).json(file_path)

def process_log_data(log_df):
    extracted_df = log_df.select(
        regexp_extract("value", r"\[(.*?)\]", 1).alias("data_hora"),
        regexp_extract("value", r"sensor\[(\d+)\]", 1).alias("sensor_id"),
        regexp_extract("value", r"temperature\s+(-?\d+\.\d+)", 1).alias("temp_sensor"),
        regexp_extract("value", r"vibration\s+(-?\d+\.\d+)", 1).alias("vibration_sensor")
    )
    processed_df = extracted_df.withColumn("data_hora", to_timestamp("data_hora", "yyyy-MM-dd HH:mm:ss"))
    return processed_df.withColumn("timestamp", from_utc_timestamp(col("data_hora"), "GMT"))

def process_sensor_equipment_data(sensor_equipment_df, equipment_df):
    return sensor_equipment_df.join(equipment_df, sensor_equipment_df.equipment_id == equipment_df.equipment_id, "left") \
        .select(sensor_equipment_df["*"], equipment_df["name"], equipment_df["group_name"])

def total_failures(log_df):
    return log_df.count()

def most_failed_equipment(log_df, sensor_equipment_df):
    return log_df.join(sensor_equipment_df, log_df.log_sensor_id == sensor_equipment_df.sensor_id) \
        .groupBy("name") \
        .count() \
        .orderBy(col("count").desc()) \
        .select("name") \
        .first()[0]

def average_failures_per_group(log_df, sensor_equipment_df, total_failures):
    return log_df.join(sensor_equipment_df, log_df.log_sensor_id == sensor_equipment_df.sensor_id) \
        .groupBy("group_name") \
        .count() \
        .orderBy(col("count").asc()) \
        .select("group_name", (F.col("count") / total_failures).alias("qt_media_falhas"))

def sensors_with_most_errors(log_df, sensor_equipment_df):
    return log_df.join(sensor_equipment_df, log_df.log_sensor_id == sensor_equipment_df.sensor_id) \
        .groupBy("group_name", "name", "sensor_id") \
        .count() \
        .orderBy(col("count").desc()) \
        .select("group_name", "name", "sensor_id", "count")

def main():
    # Inicialize a sessão do Spark
    spark = SparkSession.builder.appName("Equipment Failure Analysis").getOrCreate()

    # Carregar os dados
    log_df = load_log_data(spark, "/content/equipment_failure_sensors.txt")
    sensor_equipment_df = load_sensor_equipment_data(spark, "/content/equipment_sensors.csv")
    equipment_df = load_equipment_data(spark, "/content/equipment.json")

    # Processar os dados
    log_df = process_log_data(log_df)
    sensor_equipment_df = process_sensor_equipment_data(sensor_equipment_df, equipment_df)

    # Aplicar as consultas
    total = total_failures(log_df)
    most_failed = most_failed_equipment(log_df, sensor_equipment_df)
    avg_per_group = average_failures_per_group(log_df, sensor_equipment_df, total)
    sensors_most_errors = sensors_with_most_errors(log_df, sensor_equipment_df)

    # Mostrar os resultados
    print("1. Falhas totais de equipamentos que aconteceram:", total)
    print("2. Nome do equipamento com mais falhas:", most_failed)
    print("3. Quantidade média de falhas em todos os grupos de equipamentos:")
    avg_per_group.show()
    print("4. Sensores que apresentam maior número de erros por nome de equipamento em um grupo de equipamentos:")
    sensors_most_errors.show()

    # Encerrar a sessão do Spark
    spark.stop()

if __name__ == "__main__":
    main()
