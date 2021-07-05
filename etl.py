import configparser
from datetime import datetime
import os

from pexpect import spawn
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, spark_partition_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .appName("Sparkify") \
        .getOrCreate()
    return spark


def get_date_unit_function(unit):
    """
      Return udf to get correct date base on unit
      Keywork argument:
      unit -- key to return udf to get correct date
    """
    switcher = {
        'start_time': udf(lambda x: datetime.fromtimestamp(x / 1000).strftime("%H:%M:%S")),
        'hour': udf(lambda x: datetime.fromtimestamp(x / 1000).hour),
        'day': udf(lambda x: datetime.fromtimestamp(x / 1000).day),
        'week': udf(lambda x: datetime.fromtimestamp(x / 1000).isocalendar()[1]),
        'month': udf(lambda x: datetime.fromtimestamp(x / 1000).month),
        'year': udf(lambda x: datetime.fromtimestamp(x / 1000).year),
        'weekday': udf(lambda x: datetime.fromtimestamp(x / 1000).weekday())
    }
    return switcher.get(unit)


def process_song_data(spark, input_data, output_data):
    """
          Extract song data and transform them into parquet files
          Keywork argument:
          spark -- key to return udf to get correct date
          input_data -- s3 uri to get the data
          output_data -- s3 uri to store result files
        """
    # get filepath to song data file
    song_log_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    # read song data file
    df = spark.read.json(song_log_data)
    # song schema
    song_schema = StructType([
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("year", LongType(), True),
        StructField("duration", DoubleType(), True)
    ])

    song_data = df.select("song_id", "title", "artist_id", col("year").cast('int').alias("year"),
                          col("duration").cast('double').alias("duration")). \
        where(col("song_id").isNotNull()).dropDuplicates().collect()
    # extract columns to create songs table
    songs_table = spark.createDataFrame(data=song_data, schema=song_schema)

    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(os.path.join(output_data, "songs-table"))

    # define artist schema
    artist_schema = StructType([
        StructField("artist_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ])
    artist_data = df.select("artist_id", col("artist_name").alias("name"), col("artist_location").alias("location"),
                            col("artist_latitude").alias("latitude"),
                            col("artist_longitude").alias("longitude")).where(
        col("artist_id").isNotNull()).dropDuplicates().collect()
    # extract columns to create artists table
    artists_table = spark.createDataFrame(data=artist_data, schema=artist_schema)

    # write artists table to parquet files
    artists_table = artists_table.write.mode("overwrite").parquet(os.path.join(output_data, "artists-table"))


def process_log_data(spark, input_data, output_data):
    """
              Extract log data from user activity log and transform them into parquet files
              Keywork argument:
              spark -- key to return udf to get correct date
              input_data -- s3 uri to get the data
              output_data -- s3 uri to store result files
            """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    user_schema = StructType(
        [StructField("user_id", StringType(), False), StructField("first_name", StringType(), True),
         StructField("last_name", StringType(), True), StructField("gender", StringType(), True),
         StructField("level", StringType(), True)
         ])
    users_data = df.select("userId", "firstName", "lastName", "gender", "level").where(
        col("userId").isNotNull()).dropDuplicates().collect()
    # extract columns for users table    
    users_table = spark.createDataFrame(data=users_data, schema=user_schema)

    # write users table to parquet files
    users_table = users_table.write.mode("overwrite").parquet(os.path.join(output_data, "users-table"))

    # create timestamp column from original timestamp column
    df = df.withColumn("start_time", get_date_unit_function('start_time')(df.ts)). \
        withColumn("hour", get_date_unit_function('hour')(df.ts)). \
        withColumn("day", get_date_unit_function('day')(df.ts)). \
        withColumn("week", get_date_unit_function('week')(df.ts)). \
        withColumn("month", get_date_unit_function('month')(df.ts)). \
        withColumn("year", get_date_unit_function('year')(df.ts)). \
        withColumn("weekday", get_date_unit_function('weekday')(df.ts))

    time_schema = StructType([
        StructField("start_time", StringType(), True),
        StructField("hour", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("week", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("weekday", IntegerType(), True)
    ])
    time_data = df.select("start_time", col("hour").cast('int').alias("hour"),
                          col("day").cast('int').alias("day"),
                          col("week").cast('int').alias("week"), col("month").cast('int').alias("month"),
                          col("year").cast('int').alias("year"),
                          col("weekday").cast('int').alias("weekday")).collect()

    # extract columns to create time table
    time_table = spark.createDataFrame(data=time_data, schema=time_schema)

    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data, "time-table"))

    # read in song data to use for songplays table
    song_df = spark.read.load(output_data + "songs-table")
    artist_df = spark.read.load(output_data + "artists-table")

    songplay_schema = StructType([
        StructField("songplay_id", LongType(), False),
        StructField("start_time", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("level", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False)
    ])
    df = df.join(artist_df, df.artist == artist_df.name, how='left'). \
        join(song_df, df.song == song_df.title, how='left'). \
        drop(artist_df.name).drop(song_df.title).drop(artist_df.location).drop(artist_df.artist_id). \
        drop(song_df.artist_id). \
        withColumn("songplay_id", monotonically_increasing_id())
    # extract columns from joined song and log datasets to create songplays table

    songplays_data = df.select("songplay_id", col("ts").alias("start_time"), col("userId").alias("user_id"), "level",
                               "song_id", "artist_id", col("sessionId").alias("session_id"),
                               "location", col("userAgent").alias("user_agent"),
                               get_date_unit_function("year")(df.ts).cast('int'),
                               get_date_unit_function("month")(df.ts).cast('int')). \
        dropDuplicates().collect()
    songplays_table = spark.createDataFrame(data=songplays_data, schema=songplay_schema)

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data, "songplays-table"))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://sparkify-stored-tabled/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
