from pyspark.sql import SparkSession
from google.cloud import bigquery
from google.oauth2 import service_account
import argparse
import os
import pandas as pd



def upload_to_bigquery(spark,df_path, table_id):
    spark_df = spark.read.csv(df_path,header=True, inferSchema=True)
    pandas_df = spark_df.toPandas()
    if 'data_sales' in table_id:
        pandas_df['createtime'] = pd.to_datetime(pandas_df['createtime'], format='%d/%m/%Y', errors='coerce')
        pass
    credentials = service_account.Credentials.from_service_account_file(
        '/opt/airflow/spark/rapid-idiom-432113-m4-8bf13c095a8f.json'
    )
    client = bigquery.Client(credentials=credentials, project='rapid-idiom-432113-m4')
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND  
    )
    job = client.load_table_from_dataframe(pandas_df, table_id, job_config=job_config)
    job.result()
    print(f"Data loaded into {table_id}")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--output1', type=str, help='Path for the first CSV file output')
    parser.add_argument('--output2', type=str, help='Path for the second CSV file output')
    parser.add_argument('--table1', type=str, help='Table ID for the first CSV file')
    parser.add_argument('--table2', type=str, help='Table ID for the second CSV file')
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("example_spark_job") \
        .config("spark.master", "spark://spark:7077") \
        .config("spark.jars", "/opt/airflow/spark/jars/gcs-connector-hadoop3-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/spark/rapid-idiom-432113-m4-8bf13c095a8f.json") \
        .getOrCreate()
    
    if args.table1:
        upload_to_bigquery(spark, os.path.join(args.output1, "data_sales_output.csv"), args.table1)
    if args.table2:
        upload_to_bigquery(spark, os.path.join(args.output2, "customer_sales_output.csv"), args.table2)
    
    spark.stop()