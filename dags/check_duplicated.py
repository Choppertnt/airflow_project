from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from google.cloud import bigquery
from google.oauth2 import service_account
import os
def check_duplicates(input_path):
    spark = SparkSession.builder \
        .appName("check_duplicated_1") \
        .config("spark.master", "spark://spark:7077") \
        .config("spark.jars", "/opt/airflow/spark/jars/gcs-connector-hadoop3-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/spark/rapid-idiom-432113-m4-8bf13c095a8f.json") \
        .getOrCreate()
    
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    original_count = df.count()
    df_deduplicated = df.dropDuplicates()
    deduplicated_count = df_deduplicated.count()
    
    if 'data_sales' in input_path:
        return 'handle_sales_duplicates' if original_count > deduplicated_count else 'process_sales_duplicates'
    else:
        return 'handle_customers_duplicates' if original_count > deduplicated_count else 'process_customers_duplicates'


def process_duplicates():
    pass


def check_duplicates_again(input_path, bigquery_table):
    spark = SparkSession.builder \
        .appName("check_duplicated_2") \
        .config("spark.master", "spark://spark:7077") \
        .config("spark.jars", "/opt/airflow/spark/jars/gcs-connector-hadoop3-latest.jar,/opt/airflow/spark/jars/spark-3.5-bigquery-0.40.0.jar") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/spark/rapid-idiom-432113-m4-8bf13c095a8f.json") \
        .getOrCreate()
    
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    if 'data_sales' in input_path:
        df = df.withColumn("createtime", to_date(col("createtime"), "dd/MM/yyyy"))
    else:
        pass
    bigquery_df = spark.read.format("bigquery").option("table", bigquery_table).load()
    df_combined = df.union(bigquery_df)
    df_deduplicated = df_combined.dropDuplicates()
    df_combined.show()
    df_deduplicated.show()
    duplicates_count = df_combined.count() - df_deduplicated.count()
    
    if 'data_sales' in input_path:
        return 'handle_sales_duplicates_again' if duplicates_count > 0 else 'process_sales_duplicates_again'
    else:
        return 'handle_customers_duplicates_again' if duplicates_count > 0 else 'process_customers_duplicates_again'
    
    
def handle_duplicates(input_path,output_folder1):
    spark = SparkSession.builder \
        .appName("check_duplicated_1") \
        .config("spark.master", "spark://spark:7077") \
        .config("spark.jars", "/opt/airflow/spark/jars/gcs-connector-hadoop3-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/spark/rapid-idiom-432113-m4-8bf13c095a8f.json") \
        .getOrCreate()
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df_deduplicated = df.dropDuplicates()
    if 'data_sales' in input_path:
        output_path1 = os.path.join(output_folder1, "data_sales_output.csv")
    else:
        output_path1 = os.path.join(output_folder1, "customer_sales_output.csv")     
    df_deduplicated = df_deduplicated.toPandas()
    df_deduplicated.to_csv(output_path1, index=False)
    spark.stop()

def handle_duplicates_again(input_path, bigquery_table, output_path_final):
    spark = SparkSession.builder \
        .appName("handle_duplicated_again") \
        .config("spark.master", "spark://spark:7077") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.jars", "/opt/airflow/spark/jars/gcs-connector-hadoop3-latest.jar,/opt/airflow/spark/jars/spark-3.5-bigquery-0.40.0.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/spark/rapid-idiom-432113-m4-8bf13c095a8f.json") \
        .getOrCreate()
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    if 'data_sales' in input_path:
        df = df.withColumn("createtime", to_date(col("createtime"), "dd/MM/yyyy"))
    bigquery_df = spark.read.format("bigquery").option("table", bigquery_table).load()
    df_deduplicated = df.join(bigquery_df, df.columns, "left_anti")
    if 'data_sales' in input_path:
        output_path1 = os.path.join(output_path_final, "data_sales_output.csv")
    else:
        output_path1 = os.path.join(output_path_final, "customer_sales_output.csv")
    df_deduplicated = df_deduplicated.toPandas()
    df_deduplicated.to_csv(output_path1, index=False)
    spark.stop()

def process_duplicates_again():
    pass


def data_from_bigquery(customers, datasales,new_tables):
    spark = SparkSession.builder \
        .appName("connect data and relationship of data") \
        .config("spark.master", "spark://spark:7077") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.jars", "/opt/airflow/spark/jars/gcs-connector-hadoop3-latest.jar,/opt/airflow/spark/jars/spark-3.5-bigquery-0.40.0.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/spark/rapid-idiom-432113-m4-8bf13c095a8f.json") \
        .getOrCreate()
    bigquery_customer = spark.read.format("bigquery").option("table", customers).load()
    bigquery_sales = spark.read.format("bigquery").option("table", datasales).load()
    bigquery_customer.createOrReplaceTempView("customers")
    bigquery_sales.createOrReplaceTempView("sales")
    query = """
    select sales.*,customername,gender,age from sales
    inner join customers ON
    sales.id = customers.id
    """
    joined_df = spark.sql(query)
    joined_df.show()
    joined_df_pd = joined_df.toPandas()
    credentials = service_account.Credentials.from_service_account_file(
    '/opt/airflow/spark/rapid-idiom-432113-m4-8bf13c095a8f.json'
    )
    client = bigquery.Client(credentials=credentials, project='rapid-idiom-432113-m4')
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = client.load_table_from_dataframe(joined_df_pd, new_tables, job_config=job_config)
    job.result()
    print("Data loaded to BigQuery successfully.")
    spark.stop()
