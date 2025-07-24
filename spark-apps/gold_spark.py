from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('gold_spark').getOrCreate()

def modificando_generos(df): 
    df_genres = spark.read.parquet('/opt/spark-data/data_genres')  
    df = df.join(df_genres, df.genre_ids == df_genres.id, 'left') 
    df = df.drop('genre_ids') 
    df = df.drop('id')
    df = df.withColumnRenamed('name', 'genres_names')
    return df

def select_data(): 
    df = spark.read.parquet('/opt/spark-data/silver')
    df = df.select('adult', 'id', 'original_language',  'original_title', 'popularity', 'release_date', 'vote_average', 'vote_count', 'genre_ids')
    df = modificando_generos(df) 
    df.write.mode('overwrite').parquet('/opt/spark-data/gold')

if __name__ == "__main__": 
    select_data() 
    spark.stop()