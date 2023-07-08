import configparser
import os

from time import time
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql.functions import udf, col, desc, substring

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Create Spark Session
    """
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()
    return spark



def process_immigration_data(spark, input_data, output_data):
    """
    Args:
        spark: spark session
        input_data: Path to input data
        output_data: Path to output data
    Returns:
        Outputs the fact immigration table
    """
    # get filepath to immigration data file
    path = input_data + "immigration_data_sample.csv" # sas_data for local mode
    
    # read data file
    df=spark.read.parquet(path)
    
    # Add Visa Categories (Business - Pleasure - Student)
    sql_expr = """
        CASE WHEN i94visa = 1.0 THEN 'Business' 
             WHEN i94visa = 2.0 THEN 'Pleasure'
             WHEN i94visa = 3.0 THEN 'Student'
             ELSE 'N/A' 
        END              
        """
    df = df.withColumn('i94visa', F.expr(sql_expr))
    
    
    df.createOrReplaceTempView('immigration')

    # extract columns to create immigration table
    fact_immigration = spark.sql("""
        SELECT
            cicid    AS cicid,
            i94yr    AS arrival_year,
            i94mon   AS arrival_month,
            i94cit   AS citizinship,
            i94res   AS residence,
            i94port  AS port,
            arrdate  AS arrival_date,
            i94mode  AS travel_mode,
            i94addr  AS us_state,
            depdate  AS departure_date,
            i94bir   AS age,
            i94visa  AS visa_category,
            visapost AS dep_issued_visa,
            dtaddto  AS visa_expiration_date,
            gender   AS gender,
            airline  AS airline,
            admnum   AS admission_number,
            fltno    AS flight_number,
            visatype AS visa_type
        FROM immigration 
    """) 
    
    # write table to parquet files 
    fact_immigration.write\
    .partitionBy("us_state")\
    .mode('overwrite')\
    .parquet(os.path.join(output_data, 'immigration'))
    
def process_demographics_data(spark, input_data, output_data):
    """
    Args:
        spark: spark session
        input_data: Path to input data
        output_data: Path to output data
    Returns:
        Outputs the demographics table
    """  
    # get filepath to immigration data file
    path = input_data + "us-cities-demographics.csv" 
    
    # read data file
    df = spark.read.csv(path, inferSchema=True, header=True, sep=';') 
    
    # Rename Columns
    df = df.withColumnRenamed('Median Age', 'median_age') \
        .withColumnRenamed('Male Population', 'male_population') \
        .withColumnRenamed('Female Population', 'female_population') \
        .withColumnRenamed('Total Population', 'total_population') \
        .withColumnRenamed('Number of Veterans', 'n_veterans') \
        .withColumnRenamed('Foreign-born', 'foreign_born') \
        .withColumnRenamed('Average Household Size', 'avg_household_size') \
        .withColumnRenamed('State Code', 'state_code')
    
    
    df.write.parquet(output_data + "demographics", mode="overwrite")    


def main():
    spark = create_spark_session()
    
    input_data = config.get('IO', 'INPUT_DATA')
    output_data = config.get('IO', 'OUTPUT_DATA')
    
    #input_data = "s3a://udacity-dend/"
    #output_data = "./Results/"

    #input_data, output_data = './input_data/', './output_data/'  
    
    process_immigration_data(spark, input_data, output_data)    
    process_demographics_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
