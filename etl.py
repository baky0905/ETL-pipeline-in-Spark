import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import functions as F
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    process_song_data() function takes created spark session, input data path and
    output data path on S3. Loads the json files from input data path into a dataframe
    and then creates analytical tables - song_table and artist_table. These tables are
    then written in parquet files based on the output data path.
    
    Parameters
    ----------
    spark
        Created spark session by create_spark_session()
    input_data
        Input data path on S3.
    output_data
        Output data path on S3.
        
    """
    # get filepath to song data file
    input_data = input_data
    song_path = "song_data/*/*/*"
    song_path = os.path.join(input_data, song_path)
    
    # read song data file
    df_song = spark.read.format("json").load(song_path)
    
    # extract columns to create songs table
    song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df_song.select(song_columns)
    # drop duplicates
    songs_table = songs_table.dropDuplicates(["song_id"])
    # drop na's
    songs_table = songs_table.dropna(subset=("song_id"))
    song_parq_path = "songs_table_parquet"
                                         
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("artist_id","year").parquet(os.path.join(output_data, song_parq_path))
    
    # extract columns to create artists table
    artist_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = df_song.select(artist_columns)
    # drop duplicates
    artists_table = artists_table.dropDuplicates(["artist_id"])
    # drop na's
    artists_table = artists_table.dropna(subset=("artist_id"))
    artist_parq_path = "artist_table_parquet"
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, artist_parq_path))


def process_log_data(spark, input_data, output_data):
    """
    process_log_data() function takes created spark session, input data path and
    output data path on S3. Loads the json files from input data path into a dataframe
    and then creates analytical tables - songplays_table, user_table and time table . These tables are
    then written in parquet files based on the output data path.
    
    Parameters
    ----------
    spark
        Created spark session by create_spark_session()
    input_data
        Input data path on S3.
    output_data
        Output data path on S3.
        
    """
    input_data = input_data
    # get filepath to log data file
    log_data = "log_data/*/*"
    log_data = os.path.join(input_data, log_data)
    # read log data file
    df =  spark.read.format("json").load(log_data)
    # filter by actions for song plays
    df = df.filter(df["page"] == 'NextSong')
    # extract columns for users table    
    user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_table = df.select(user_columns)
    # drop duplicates
    user_table = user_table.dropDuplicates(["userId"]) 
    # drop na's
    user_table = user_table.dropna(subset=("userId"))
    user_parq_path = "user_table_parquet"
    # write users table to parquet files
    user_table.write.parquet(os.path.join(output_data, user_parq_path))

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    df = df.withColumn("ts", get_timestamp(df.ts))
    df = df.withColumnRenamed('ts', 'start_time')
    
    # extract columns to create time table

    time_table =  (df.select('start_time', date_format('start_time', 'H').alias('hour'), 
                              date_format('start_time', 'D').alias('day'),
                              date_format('start_time', 'w').alias('week'),
                              date_format('start_time', 'M').alias('month'),
                              date_format('start_time', 'y').alias('year'),
                              date_format('start_time', 'W').alias('weekday')))
    # write time table to parquet files partitioned by year and month 
    time_parq_path = "time_table_parquet"
    # write users table to parquet files
    time_table.write.parquet(os.path.join(output_data, time_parq_path))

    # read in song data to use for songplays table
    song_path = "song_data/*/*/*"
    song_path = os.path.join(input_data,song_path)

    song_df = spark.read.format("json").load(song_path)
    # extract columns from joined song and log datasets to create songplays table 
    log_df = df.alias('log_df')
    song_df = song_df.alias('song_df')
    songplays_table = log_df.join(song_df, log_df.song == song_df.title)
    songplays_table = songplays_table.select('start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent', date_format('start_time', 'y').alias('year'), date_format('start_time', 'M').alias('month'))
    # This will return a new DF with all the columns + id
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    # write songplays table to parquet files partitioned by year and month
    songplay_parq_path = "songplay_table_parquet"
    songplays_table.write.partitionBy("year","month").parquet(os.path.join(output_data, songplay_parq_path))


def main():
    """
    main() function initiates create_spark_session, process_song_data and process_log_data.
    Also input and output file paths were hardcoded and are inputs in consequitve functions
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "analytics"
    
    process_log_data(spark, input_data, output_data)
    process_song_data(spark, input_data, output_data)    
    


if __name__ == "__main__":
    main()
