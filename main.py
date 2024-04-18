import sys


from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
        .builder \
        .appName("Oracle_to_snowflake_via_S3") \
        .getOrCreate()



def main():
    s3_bucket=sys.argv[1];
    s3_file=sys.argv[2];
    s3_location="s3://{}/{}".format(s3_bucket,s3_file);
    #s3_location= "s3://ljakka-emr-1/spotify_raw_data_20240412_074159.json"
    df = spark.read.format("json").load(s3_location)
    def convert_nested_json_csv(df):
  #Let's first focus on complex datatypes like arrays and Structure Types
      complex_fields = dict([(field.name, field.dataType)
                                for field in df.schema.fields
                                if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
      while len(complex_fields)!=0:
          #iterate over every complex Structure type
          col_name=list(complex_fields.keys())[0]

          #flatten structs
          if (type(complex_fields[col_name]) == StructType):
            expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
            df=df.select("*", *expanded).drop(col_name)

          # if ArrayType then add the Array Elements as Rows using the explode function
          # i.e. explode Arrays
          elif (type(complex_fields[col_name]) == ArrayType):
            df=df.withColumn(col_name,explode_outer(col_name))

          # recompute remaining Complex Fields in Schema
          complex_fields = dict([(field.name, field.dataType)
                                for field in df.schema.fields
                                if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
      return df
    df_flatted = convert_nested_json_csv(df)

    #Creating the tables
    album_df = df_flatted.select("items_track_album_id","items_track_album_name","items_track_album_release_date","items_track_album_total_tracks","items_track_album_uri","items_track_album_available_markets").dropDuplicates()
    album_df = album_df.withColumnRenamed("items_track_album_id","items_album_id").withColumnRenamed("items_track_album_name","items_album_name").withColumnRenamed("items_track_album_uri","items_album_uri").\
    withColumnRenamed("items_track_album_available_markets","items_available_markets")
    album_df = album_df.withColumn("items_release_date",to_date("items_track_album_release_date","yyyy-MM-dd")).withColumn("items_total_tracks",col("items_track_album_total_tracks").cast(IntegerType())).\
    drop("items_track_album_release_date","items_track_album_total_tracks")
    #Since we are restricted within USA, India and Canada, let's filter further down
    interested_countries =["IN","US","CA"]
    album_df = album_df.filter(album_df.items_available_markets.isin(interested_countries))

    #table song_list
    track_df = df_flatted.select("items_track_id","items_track_name","items_track_duration_ms","items_track_uri","items_track_popularity","items_track_album_id","items_track_artists_id","items_added_at").dropDuplicates()
    track_df = track_df.withColumnRenamed("items_track_album_id","items_album_id").withColumnRenamed("items_track_artists_id","items_artist_id")
    track_df = track_df.withColumn("items_track_duration",(round(col("items_track_duration_ms")/60000,2)).cast(DoubleType())).withColumn("items_track_popularity",col("items_track_popularity").cast(IntegerType())).\
    drop("items_track_duration_ms")
    track_df = track_df.withColumn("items_track_added",substring(track_df.items_added_at,1,10).cast(DateType())).drop("items_added_at")

    #Artist Table
    artist_df = df_flatted.select("items_track_artists_id","items_track_artists_name","items_track_artists_uri").dropDuplicates()
    artist_df = artist_df.withColumnRenamed("items_track_artists_id","items_artist_id").withColumnRenamed("items_track_artists_name","items_artist_name").withColumnRenamed("items_track_artists_uri","items_artist_uri")
    album_df.show()
    album_df.coalesce(1).write.format("csv").option("header", "true").save("s3://spotify-project-ljakka2/transformed_data/album/"+ datetime.now().strftime("%Y%m%d_%H%M%S"))
    track_df.coalesce(1).write.format("csv").option("header", "true").save("s3://spotify-project-ljakka2/transformed_data/tracks/"+ datetime.now().strftime("%Y%m%d_%H%M%S"))
    artist_df.coalesce(1).write.format("csv").option("header", "true").save("s3://spotify-project-ljakka2/transformed_data/artist/"+ datetime.now().strftime("%Y%m%d_%H%M%S"))
    print("Hello")
main()
