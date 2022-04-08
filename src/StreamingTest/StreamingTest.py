from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import time 
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType, StringType, StructType, StructField, IntegerType, LongType



def streamingWorking():
    spark = SparkSession.builder \
            .appName('fuckingStreaming') \
            .getOrCreate()
    
    sc = spark._sc
    #lines =spark.readStream \
    #    .format('socket') \
    #    .option('host', 'localhost') \
    #    .option('port', 9999) \
    #    .load()
    
    lines =spark.readStream \
        .format('text') \
        .option('path', '/home/sscd/tmp/') \
        .load()
    
    words = lines.select(
            explode(
                split(lines.value, ' ')
            ).alias('word')
    )
    wordCounts = words.groupBy('word').count()
    
    query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
    
    query.awaitTermination()
    sc.stop()


def streamingFromAndToCSV():

    spark = SparkSession.builder \
        .appName('fuckingStreaming') \
        .getOrCreate()
    sc = spark._sc
    # Explicitly set schema
    schema = StructType([ StructField("ctr", IntegerType(), True),
    StructField("Datefirstseen", TimestampType(), True),
    StructField("SrcIPAddr", StringType(), True),
    StructField("DstIPAddr", StringType(), True),
    StructField("Datefirstseenunix", LongType(), True),
    StructField("Duration", IntegerType(), True),
    StructField("Proto", StringType(), True),
    StructField("SrcPt", IntegerType(), True),
    StructField("DstPt", IntegerType(), True),
    StructField("Packets", IntegerType(), True),
    StructField("Bytes", IntegerType(), True),
    ])

    # read from tcp socket
    #lines = spark.readStream \
    #    .format('socket') \
    #    .option('host', 'localhost') \
    #    .option('port', 9999) \
    #    .option('includeTimestamp', 'true') \
    #    .load()
    
    # read from file
    #lines =spark.readStream \
    #    .schema(schema) \
    #    .format('csv') \
    #    .option('path', 'file:///home/sscd/Programme/zeppelin-0.10.0-bin-netinst/bin/tmp/') \
    #    .load()
    
    
    inputPath = 'file:///home/sscd/Programme/zeppelin-0.10.0-bin-netinst/bin/tmp/'
    streamingDF = (
      spark
        .readStream
        .schema(schema)
        .option("maxFilesPerTrigger", 1)
        .csv(inputPath)
    )
    
    #display(streamingDF)
    print('\n')
    print(streamingDF.isStreaming)
    
    
    query=streamingDF.writeStream.format("csv").outputMode("append").option("checkpointLocation","/home/sscd/tmp").option("path", "file:///home/sscd/outstream/").start()
    query.awaitTermination()



def streamingFromFileWithAggregationToConsole():

    spark = SparkSession.builder \
        .appName('aggregationStreaming') \
        .getOrCreate()
    sc = spark._sc
    # Explicitly set schema
    schema = StructType([ StructField("ctr", IntegerType(), True),
    StructField("Datefirstseen", TimestampType(), True),
    StructField("SrcIPAddr", StringType(), True),
    StructField("DstIPAddr", StringType(), True),
    StructField("Datefirstseenunix", LongType(), True),
    StructField("Duration", IntegerType(), True),
    StructField("Proto", StringType(), True),
    StructField("SrcPt", IntegerType(), True),
    StructField("DstPt", IntegerType(), True),
    StructField("Packets", IntegerType(), True),
    StructField("Bytes", IntegerType(), True),
    ])
   
    # read from file
    inputPath = 'file:///home/sscd/Programme/zeppelin-0.10.0-bin-netinst/bin/tmp/'
    flows_streaming = (
      spark
        .readStream
        .schema(schema)
        .option("maxFilesPerTrigger", 1)
        .csv(inputPath)
    )
    
    # das funktioniert yeah!!
    #avgSignalDF = flows_streaming.groupBy("SrcIPAddr").sum("Bytes")
    avgSignalDF = flows_streaming.withWatermark("Datefirstseen", "5 seconds").groupBy(window("Datefirstseen","30 seconds"),"SrcIPAddr").sum("Duration","Packets","Bytes")
    #print(avgSignalDF.isStreaming)
    print(avgSignalDF.printSchema())

    print('is this widowedCounts streaming?')
    print(avgSignalDF.isStreaming)
    query = avgSignalDF.writeStream.format("console").option("truncate",False).outputMode("update").option("checkpointLocation","/home/sscd/tmp").start()
    query.awaitTermination()

streamingFromFileWithAggregationToConsole()
#remember that you must clean folder /home/sscd/tmp before restart
