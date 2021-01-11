import configparser
from datetime import datetime
import os
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, monotonically_increasing_id, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import TimestampType, DateType


#config = configparser.ConfigParser()
#config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']='<INSERT_ACCESS_KEY_ID>'
os.environ['AWS_SECRET_ACCESS_KEY']='<INSERT_SECRET_ACCESS_KEY>'


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def format_datetime(ts):
    '''
    Returns integer as datetime object.
    '''
    return datetime.fromtimestamp(ts / 1000.0)

def process_song_data(spark, input_data, output_data):
    '''
    Reads in song/artists data from AWS S3.
    Queries data to create songs_tables and artist_tables.
    Writes/loads schematized tables as parquet to AWS S3.
    Partitions songs_table by "year" and "artist_id".
    '''
    # get filepath to song data file
    # Currently set for local testing
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').mode('overwrite').parquet(os.path.join(output_data, 'songs'))
    
    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    '''
    Reads in the log of songs played by users and song library from AWS S3.
    Queries data to create users_table, time_table, and songplays_table.
    Writes/loads schematized tables as parquet to AWS S3.
    Partitions time_table and songplays_table by "year" and "month".
    '''
    # get filepath to log data file
    # Currently set for local testing.
    log_data = input_data + '/log_data'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df[df['page'] == 'NextSong']

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = users_table.dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda dt: format_datetime(int(dt)), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda dt: format_datetime(int(dt)), DateType())
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    df = df.withColumn('day_of_week', date_format(col('timestamp'), 'E'))
    time_table = df.select(col('timestamp').alias('start_time'),
                           hour(df.timestamp).alias('hour'),
                           dayofmonth(df.timestamp).alias('day'),
                           weekofyear(df.timestamp).alias('week'),
                           month(df.timestamp).alias('month'),
                           year(df.timestamp).alias('year'),
                           'day_of_week')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet(os.path.join(output_data, 'time'))
    
  
    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # create SQL tables.
    df.createOrReplaceTempView('log_data_table')
    song_df.createOrReplaceTempView('song_data_table')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
                            SELECT
                                l.timestamp AS start_time,
                                l.userId,
                                l.level,
                                s.song_id,
                                s.artist_id,
                                l.sessionId,
                                l.location,
                                l.userAgent
                            FROM log_data_table l
                                LEFT JOIN song_data_table s
                                    ON l.song = s.title
                            ''')
    
    # add sequential ID for songplay_id
    songplays_table = songplays_table.withColumn('mono_increasing_id', monotonically_increasing_id())
    window = Window.orderBy(col('mono_increasing_id'))
    songplays_table = songplays_table.withColumn('songplay_id', row_number().over(window))
    songplays_table = songplays_table.drop('mono_increasing_id')

    # create "month" and "year" columns for partitioning.
    songplays_table = songplays_table.withColumn('month', month(col('start_time')))
    songplays_table = songplays_table.withColumn('year', year(col('start_time')))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet(os.path.join(output_data, 'songplays'))


def main():
    spark = create_spark_session()
    # change for local testing.
    # use "s3a://udacity-dend/" when connecting to AWS.
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-data-lake-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
