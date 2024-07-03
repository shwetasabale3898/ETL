from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,regexp_extract,explode,avg


spark = SparkSession.builder.appName("assignment").getOrCreate()

def convert_to_df(file_path,columns):
    df = spark.read.text(file_path)
    split_col=split(df['value'],'::')
    df_split=df.select([split_col.getItem(i).alias(columns[i]) for i in range(len(columns))])
    return df_split

file_path_movies='s3://ngap2-user-data/dsmimengineeringnonprod/ssabal/movies.dat'
columns_movies = ["MovieID","Title","Genres"]

file_path_user='s3://ngap2-user-data/dsmimengineeringnonprod/ssabal/users.dat'
columns_user = ["UserID","Gender","Age","Occupation","Zip-code"]

file_path_ratings='s3://ngap2-user-data/dsmimengineeringnonprod/ssabal/ratings.dat'
columns_ratings = ["UserID","MovieID","Rating","Timestamp"]

df_movies=convert_to_df(file_path_movies,columns_movies)
df_users=convert_to_df(file_path_user,columns_user)
df_ratings=convert_to_df(file_path_ratings,columns_ratings)


df_movies_YOR=df_movies.withColumn("YOR",regexp_extract(df_movies['Title'], r'\((\d{4})\)',1))
df_movies_1989=df_movies_YOR.withColumn("YOR",col("YOR").cast('int')).filter(col("YOR") > 1989)


df_split_genre=df_movies_1989.withColumn("Genres", explode(split("Genres", "\\|")))


df_join=df_ratings.join(df_users,df_ratings["UserID"]==df_users["UserID"],"inner")
df_user_filter=df_join.filter((col("Age")> 17) & (col("Age")< 50) )


df_final=df_user_filter.join(df_split_genre,df_user_filter["MovieID"]==df_split_genre["MovieID"],"inner")


df_avg=df_final.groupBy("Genres","YOR").agg(avg('rating').alias("Average_Rating"))
df_avg.show()