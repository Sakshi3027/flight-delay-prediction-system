"""
Spark Streaming Consumer - Processes flight data from Kafka in real-time
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# Define schema for flight data
flight_schema = StructType([
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("baro_altitude", DoubleType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("vertical_rate", DoubleType(), True),
    StructField("true_track", DoubleType(), True),
    StructField("on_ground", BooleanType(), True),
    StructField("timestamp", LongType(), True),
])

# Define schema for enriched data (with weather)
enriched_schema = StructType([
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("baro_altitude", DoubleType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("weather", StructType([
        StructField("temperature", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("clouds", IntegerType(), True),
        StructField("weather_main", StringType(), True),
    ]), True),
    StructField("delay_assessment", StructType([
        StructField("risk_level", StringType(), True),
        StructField("delay_probability", DoubleType(), True),
    ]), True),
])

def create_spark_session():
    """Create Spark session with Kafka support"""
    spark = SparkSession.builder \
        .appName("FlightDelayPrediction") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_flight_stream(spark):
    """Process raw flight data stream"""
    print("="*80)
    print("PROCESSING RAW FLIGHT DATA STREAM")
    print("="*80)
    
    # Read from Kafka
    flight_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "flight-data") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON and extract fields
    parsed_flights = flight_stream \
        .select(from_json(col("value").cast("string"), flight_schema).alias("data")) \
        .select("data.*")
    
    # Filter only airborne flights
    airborne = parsed_flights.filter(col("on_ground") == False)
    
    # Calculate additional features
    processed = airborne \
        .withColumn("altitude_ft", col("baro_altitude") * 3.28084) \
        .withColumn("speed_mph", col("velocity") * 2.23694) \
        .withColumn("altitude_category", 
                   when(col("baro_altitude") < 3000, "Low")
                   .when(col("baro_altitude") < 10000, "Medium")
                   .otherwise("High"))
    
    # Aggregations per country
    country_stats = processed \
        .groupBy("origin_country", 
                 window(current_timestamp(), "30 seconds")) \
        .agg(
            count("*").alias("flight_count"),
            avg("altitude_ft").alias("avg_altitude_ft"),
            avg("speed_mph").alias("avg_speed_mph"),
            max("altitude_ft").alias("max_altitude_ft")
        ) \
        .orderBy(col("flight_count").desc())
    
    # Write to console
    query1 = country_stats \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .start()
    
    return query1

def process_enriched_stream(spark):
    """Process enriched flight data with weather"""
    print("="*80)
    print("PROCESSING ENRICHED FLIGHT DATA STREAM (WITH WEATHER)")
    print("="*80)
    
    # Read from Kafka
    enriched_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "enriched-flight-data") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON
    parsed_enriched = enriched_stream \
        .select(from_json(col("value").cast("string"), enriched_schema).alias("data")) \
        .select("data.*")
    
    # Extract weather and delay info
    with_weather = parsed_enriched \
        .withColumn("temperature", col("weather.temperature")) \
        .withColumn("wind_speed", col("weather.wind_speed")) \
        .withColumn("clouds", col("weather.clouds")) \
        .withColumn("weather_condition", col("weather.weather_main")) \
        .withColumn("risk_level", col("delay_assessment.risk_level")) \
        .withColumn("delay_probability", col("delay_assessment.delay_probability"))
    
    # Risk analysis - count flights by risk level
    risk_analysis = with_weather \
        .groupBy("risk_level",
                 window(current_timestamp(), "30 seconds")) \
        .agg(
            count("*").alias("flight_count"),
            avg("delay_probability").alias("avg_delay_prob"),
            avg("temperature").alias("avg_temp"),
            avg("wind_speed").alias("avg_wind_speed")
        ) \
        .orderBy("risk_level")
    
    # High risk flights alert
    high_risk = with_weather \
        .filter(col("risk_level") == "HIGH") \
        .select("callsign", "origin_country", "latitude", "longitude",
                "temperature", "wind_speed", "weather_condition", 
                "delay_probability")
    
    # Write risk analysis to console
    query2 = risk_analysis \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # Write high risk alerts to console
    query3 = high_risk \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="5 seconds") \
        .start()
    
    return query2, query3

def main():
    print("\n" + "="*80)
    print("SPARK STREAMING - REAL-TIME FLIGHT DELAY PREDICTION")
    print("="*80 + "\n")
    
    # Create Spark session
    spark = create_spark_session()
    
    print("✅ Spark session created")
    print(f"✅ Spark version: {spark.version}")
    print(f"✅ Connected to Kafka at kafka:29092")
    print("\nStarting stream processing...\n")
    
    # Start processing streams
    query1 = process_flight_stream(spark)
    query2, query3 = process_enriched_stream(spark)
    
    print("\n" + "="*80)
    print("STREAMS ACTIVE - Processing real-time data...")
    print("="*80)
    print("Press Ctrl+C to stop")
    print("="*80 + "\n")
    
    # Wait for termination
    try:
        query1.awaitTermination()
        query2.awaitTermination()
        query3.awaitTermination()
    except KeyboardInterrupt:
        print("\n⚠️  Stopping streams...")
        query1.stop()
        query2.stop()
        query3.stop()
        spark.stop()
        print("✅ Streams stopped")

if __name__ == "__main__":
    main()