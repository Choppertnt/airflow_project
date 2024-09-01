from pyspark.sql import SparkSession
from google.cloud import bigquery
from google.oauth2 import service_account
import argparse
import os
import pandas as pd



def process_input(input_path1, output_folder1,input_path2,output_folder2):
    spark = SparkSession.builder \
        .appName("example_spark_job") \
        .config("spark.master", "spark://spark:7077") \
        .config("spark.jars", "/opt/airflow/spark/jars/gcs-connector-hadoop3-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/spark/rapid-idiom-432113-m4-8bf13c095a8f.json") \
        .getOrCreate()

    if input_path1:
        df1 = spark.read.csv(input_path1, header=True, inferSchema=True)
        df1.createOrReplaceTempView("data_sales")
        sql_query = """
                    SELECT
                        *,
                        CASE 
                            WHEN shop = 'AAA' THEN 'Shop1'
                            WHEN shop = 'BBB' THEN 'Shop2'
                            WHEN shop = 'CCC' THEN 'Shop3'
                            ELSE 'Old'
                        END AS new_shop
                    FROM data_sales
                    """
        result_df1 = spark.sql(sql_query)
        result_df1 = result_df1.drop("shop")
        result_df1 = result_df1.withColumnRenamed("new_shop", "shop")
        result_df1.show()
        output_path1 = os.path.join(output_folder1, "data_sales_output.csv")
        result_df1 = result_df1.toPandas()
        result_df1.to_csv(output_path1, index=False)

    if input_path2:
        df2 = spark.read.csv(input_path2, header=True, inferSchema=True)
        df2.show()
        output_path2 = os.path.join(output_folder2, "customer_sales_output.csv")
        df2 = df2.toPandas()
        df2.to_csv(output_path2, index=False)
        
    spark.stop()

if __name__ == "__main__":
    # ใช้ argparse เพื่อรับพารามิเตอร์จาก command line
    parser = argparse.ArgumentParser()
    parser.add_argument('--input1', type=str, help='Path to the first input CSV file in GCS')
    parser.add_argument('--input2', type=str, help='Path to the second input CSV file in GCS')
    parser.add_argument('--output1', type=str, help='Table ID for the first CSV file')
    parser.add_argument('--output2', type=str, help='Table ID for the second CSV file')
    args = parser.parse_args()
    
    spark = SparkSession.builder \
    .appName("example_spark_job") \
    .config("spark.master", "spark://spark:7077") \
    .config("spark.jars", "/opt/airflow/spark/jars/gcs-connector-hadoop3-latest.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/spark/rapid-idiom-432113-m4-8bf13c095a8f.json") \
    .getOrCreate()
    
    process_input(args.input1,args.output1,args.input2,args.output2)
    

    spark.stop()