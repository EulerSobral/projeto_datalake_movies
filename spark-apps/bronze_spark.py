import requests

from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, ArrayType, StructType, StructField, StringType, IntegerType, BooleanType

spark = SparkSession.builder.appName('bronze_spark').getOrCreate()

def genre_names():
      headers = {
      "accept": "application/json",
      "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJjODlhN2E4ZTM2NzdjMjAyMDQ0OWY1MzNmYzBkNGUxZiIsIm5iZiI6MTczNjcyMDQ1NS44NzUsInN1YiI6IjY3ODQ0MDQ3YWJhYmJiYTA0MGJiNjM3MiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.N1_ZwXeQiwoHDtpGWFwqjWuibB_we0iFHnVRpr7IZhQ"
      }

      url = "https://api.themoviedb.org/3/genre/movie/list"

      response = requests.get(url, headers=headers)
      genres = response.json()
      data = genres['genres']


      schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType())
      ])

      df_genres = spark.createDataFrame(data, schema=schema)

      df_genres.write.mode('overwrite').parquet('/opt/spark-data/data_genres')


def movie_lists():
    results = []
    for pag in range(1,20):
        url = f"https://api.themoviedb.org/3/movie/now_playing?language=en-US&page={pag}"

        headers = {
                      "accept": "application/json",
                      "Authorization": "Bearer SUA_CHAVE_DE_API_AQUI"
                  }

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
              raise Exception(f"Erro na chamada da api: {response.status_code} - {response.text}")

        data = response.json()

        results.extend(data['results'])

    results_json_dict = {
          "filmes": results
      } 

    data = results_json_dict['filmes']

    schema = StructType([
        StructField("adult", BooleanType()),
        StructField("backdrop_path", StringType()),
        StructField("genre_ids", ArrayType(IntegerType())),
        StructField("id", IntegerType()),
        StructField("original_language", StringType()),
        StructField("original_title", StringType()),
        StructField("overview", StringType()),
        StructField("popularity", FloatType()),
        StructField("poster_path", StringType()),
        StructField("release_date", StringType()),
        StructField("title", StringType()),
        StructField("video", BooleanType()),
        StructField("vote_average", FloatType()),
        StructField("vote_count", IntegerType())
    ])

    df = spark.createDataFrame(data, schema=schema)

    df.write.mode('overwrite').parquet('/opt/spark-data/bronze')


if __name__ == "__main__":  
    try: 
        genre_names() 
        movie_lists() 
    finally:
        spark.stop() 