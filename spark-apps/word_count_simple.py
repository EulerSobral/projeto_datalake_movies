from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, count, desc

def main():
    print("🚀 Iniciando WordCount PySpark...")
    
    spark = SparkSession.builder \
        .appName("WordCount-Simple") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Dados de exemplo
    data = [
        "Apache Spark é incrível",
        "Airflow orquestra workflows",
        "Docker simplifica deployment"
    ]
    
    # Processar
    df = spark.createDataFrame(data, "string").toDF("text")
    
    df.write.mode('overwrite').parquet("opt/spark-data/teste.parquet")

if __name__ == "__main__":
    main()
