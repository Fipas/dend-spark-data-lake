import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek,  from_unixtime
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data_table")

    # extract columns to create songs table
    songs_select_sql = """
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM song_data_table
        WHERE song_id IS NOT NULL
    """
    songs_table = spark.sql(songs_select_sql)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_select_sql = """
        SELECT DISTINCT artist_id, artist_name as name, artist_location as location, 
                        artist_latitude as latitude, artist_longitude as longitude
        FROM song_data_table
        WHERE artist_id IS NOT NULL
    """
    artists_table = spark.sql(artists_select_sql)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : x // 1000)
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    df = df.withColumn("datetime", from_unixtime(df.timestamp))
    
    df.createOrReplaceTempView("log_data_table")

    # extract columns for users table    
    users_select_sql = """
        SELECT DISTINCT userId AS user_id, firstName AS first_name, lastName AS last_name, gender, level
        FROM log_data_table
        WHERE userId IS NOT NULL
    """
    users_table = spark.sql(users_select_sql)
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users")
    
    # extract columns to create time table
    time_table_sql = """
        SELECT DISTINCT timestamp as start_time, hour(datetime) as hour, dayofmonth(datetime) as day, weekofyear(datetime) as week,
                           month(datetime) as month, year(datetime) as year, dayofweek(datetime) as weekday
        FROM log_data_table
        WHERE timestamp IS NOT NULL
    """
    time_table = spark.sql(time_table_sql)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs")
    song_df.createOrReplaceTempView("songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_select_sql = """
        SELECT DISTINCT se.timestamp AS start_time, se.userId AS user_id, se.level, ss.song_id, ss.artist_id, 
                        se.sessionId as session_id, se.location, se.userAgent AS user_agent, month(se.datetime) as month, year(se.datetime) as year
        FROM log_data_table AS se
        JOIN songs AS ss
        ON se.song = ss.title AND se.length = ss.duration
    """
    songplays_table = spark.sql(songplays_select_sql)
    songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-spark-data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
