from pyspark import pipelines as dp
from pyspark.sql.functions import *

# bookings data
rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}

@dp.table
def stage_bookings():
    df = spark.readStream.format('delta')\
              .load('/Volumes/flight_catalog/bronze/bronzevolume/bookings/data/')
    
    return df

@dp.table
def trans_bookings():
    df = spark.readStream.table('stage_bookings')
    df = df.withColumn('amount', col('amount').cast('double'))\
            .withColumn('modified_date', current_timestamp())\
            .withColumn('booking_date', to_date(col('booking_date')))\
            .drop('_rescued_data')

    return df

@dp.table
@dp.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table('trans_bookings')

    return df

# flight data
@dp.table
def trans_flights():
    df = spark.readStream.format('delta')\
              .load('/Volumes/flight_catalog/bronze/bronzevolume/flights/data/')

    df = df.drop('_rescued_data')\
            .withColumn('modified_date', current_timestamp())

    return df

dp.create_streaming_table('silver_flights')

dp.create_auto_cdc_flow(
    target = 'silver_flights',
    source = 'trans_flights',
    keys = ['flight_id'],
    sequence_by = 'flight_id',
    stored_as_scd_type = 1
)

# custommer data
@dp.table
def trans_passengers():
    df = spark.readStream.format('delta')\
              .load('/Volumes/flight_catalog/bronze/bronzevolume/passengers/data/')

    df = df.drop('_rescued_data')\
            .withColumn('modified_date', current_timestamp())

    return df

dp.create_streaming_table('silver_passengers')

dp.create_auto_cdc_flow(
    target = 'silver_passengers',
    source = 'trans_passengers',
    keys = ['passenger_id'],
    sequence_by = 'passenger_id',
    stored_as_scd_type = 1
)

# airport data
@dp.table
def trans_airports():
    df = spark.readStream.format('delta')\
              .load('/Volumes/flight_catalog/bronze/bronzevolume/airports/data/')

    df = df.drop('_rescued_data')\
            .withColumn('modified_date', current_timestamp())

    return df

dp.create_streaming_table('silver_airports')

dp.create_auto_cdc_flow(
    target = 'silver_airports',
    source = 'trans_airports',
    keys = ['airport_id'],
    sequence_by = 'airport_id',
    stored_as_scd_type = 1
)

# silver business
@dp.table
def silver_business():
    df = spark.readStream.table('silver_bookings')\
            .join(spark.readStream.table('silver_flights'), 'flight_id')\
            .join(spark.readStream.table('silver_passengers'), 'passenger_id')\
            .join(spark.readStream.table('silver_airports'), 'airport_id')\
            .drop('modified_date')
    
    return df


























