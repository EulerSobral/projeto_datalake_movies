from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode 
from pyspark.sql.types import IntegerType


spark = SparkSession.builder.appName('silver_spark').getOrCreate()

def retirando_dados_nan(df):
    return df.na.drop(how="all")

def modificando_boleanos(df, col_name):
      return df.withColumn(col_name, col(col_name).cast(IntegerType()))

def abrindo_arrays(df):

    return df.select(
        'adult',
        'backdrop_path',
        'id',
        'original_language',
        'original_title',
        'overview',
        'popularity',
        'poster_path',
        'release_date',
        'title',
        'video',
        'vote_average',
        'vote_count',
        explode(col('genre_ids')).alias('genre_ids'))


def tratando_dados():
    df = spark.read.parquet('/opt/spark-data/bronze')
    df = retirando_dados_nan(df)
    df = abrindo_arrays(df)
    df = modificando_boleanos(df, 'adult') 
    df.write.mode('overwrite').parquet('/opt/spark-data/silver')

if __name__ == "__main__": 
     tratando_dados() 
     spark.stop()