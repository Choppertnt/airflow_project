from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, date
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from check_duplicated import check_duplicates,check_duplicates_again,\
     handle_duplicates,process_duplicates,process_duplicates_again,handle_duplicates_again,data_from_bigquery

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 19),
    'retries': 1,
}



date_day = date.today().strftime('%Y-%m-%d')
days = date.today().day
bucket_name = "pyspark_demo01"
path1 = f"gs://{bucket_name}/{date_day}/data_sales_{days}.csv"
path2 = f"gs://{bucket_name}/{date_day}/customer_infomation_{days}.csv"

with DAG('airflow_project_intern', default_args=default_args, schedule_interval=None) as dag:


    read_csv_in1 = SparkSubmitOperator(
        task_id='read_sales_data',
        application='/opt/airflow/dags/pyspark_readcsv.py',
        conn_id='spark_default',
        application_args=['--input1', path1,'--output1','/opt/airflow/spark/data'],
        jars='/opt/airflow/spark/jars/gcs-connector-hadoop3-latest.jar',
        conf={
            'spark.eventLog.enabled': 'true',
            'spark.eventLog.dir': '/opt/spark/spark-events',
            'spark.history.fs.logDirectory': '/opt/spark/spark-events',
        },
        name='read_sales_data',
    )

    read_csv_in2 = SparkSubmitOperator(
        task_id='read_customer_data',
        application='/opt/airflow/dags/pyspark_readcsv.py',
        conn_id='spark_default',
        application_args=['--input2', path2,'--output2','/opt/airflow/spark/data'],
        jars='/opt/airflow/spark/jars/gcs-connector-hadoop3-latest.jar',
        conf={
            'spark.eventLog.enabled': 'true',
            'spark.eventLog.dir': '/opt/spark/spark-events',
            'spark.history.fs.logDirectory': '/opt/spark/spark-events',
        },
        name='read_customers_data',
    )

    check_sales_duplicates_task = BranchPythonOperator(
        task_id='check_sales_duplicates',
        python_callable=check_duplicates,
        op_args=['/opt/airflow/spark/data/data_sales_output.csv'],
    )

    check_customer_duplicates_task = BranchPythonOperator(
        task_id='check_customer_duplicates',
        python_callable=check_duplicates,
        op_args=['/opt/airflow/spark/data/customer_sales_output.csv'],
    )    



    handle_sales_duplicates = PythonOperator(
        task_id='handle_sales_duplicates',
        python_callable=handle_duplicates,
        op_args=['/opt/airflow/spark/data/data_sales_output.csv','/opt/airflow/spark/data'],
    )


    process_sales_duplicates = PythonOperator(
        task_id='process_sales_duplicates',
        python_callable=process_duplicates,
    )

    process_customers_duplicates = PythonOperator(
        task_id='process_customers_duplicates',
        python_callable=process_duplicates,
    )

    handle_customers_duplicates = PythonOperator(
        task_id='handle_customers_duplicates',
        python_callable=handle_duplicates,
        op_args=['/opt/airflow/spark/data/customer_sales_output.csv','/opt/airflow/spark/data'],
    )


    check_sales_duplicates_again_task = BranchPythonOperator(
    task_id='check_sales_duplicates_again',
    python_callable=check_duplicates_again,
    op_args=['/opt/airflow/spark/data/data_sales_output.csv', 'test_project.data_sales'],
    trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    handle_sales_duplicates_again = PythonOperator(
        task_id='handle_sales_duplicates_again',
        python_callable=handle_duplicates_again,
        op_args=['/opt/airflow/spark/data/data_sales_output.csv', 'test_project.data_sales' , '/opt/airflow/spark/data'],
    )

    check_customer_duplicates_again_task = BranchPythonOperator(
        task_id='check_customer_duplicates_again',
        python_callable=check_duplicates_again,
        op_args=['/opt/airflow/spark/data/customer_sales_output.csv', 'test_project.customer_sales'],
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    handle_customers_duplicates_again = PythonOperator(
        task_id='handle_customers_duplicates_again',
        python_callable=handle_duplicates_again,
        op_args=['/opt/airflow/spark/data/customer_sales_output.csv', 'test_project.customer_sales' , '/opt/airflow/spark/data'],
    )

    process_sales_duplicates_again = PythonOperator(
        task_id='process_sales_duplicates_again',
        python_callable=process_duplicates_again,
    )

    process_customers_duplicates_again = PythonOperator(
        task_id='process_customers_duplicates_again',
        python_callable=process_duplicates_again,
    )


    upload_bigquery1  = SparkSubmitOperator(
        task_id='upload_spark_csv_job1_to_bigquery',
        application='/opt/airflow/dags/dag_upload_bigquery.py',
        conn_id='spark_default',
        application_args=['--output1', '/opt/airflow/spark/data', '--table1', 'test_project.data_sales'],
        jars='/opt/airflow/spark/jars/gcs-connector-hadoop3-latest.jar',
        conf={
            'spark.eventLog.enabled': 'true',
            'spark.eventLog.dir': '/opt/spark/spark-events',
            'spark.history.fs.logDirectory': '/opt/spark/spark-events',
        },
        name='upload_datasales',
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    upload_bigquery2  = SparkSubmitOperator(
        task_id='upload_spark_csv_job2_to_bigquery',
        application='/opt/airflow/dags/dag_upload_bigquery.py',
        conn_id='spark_default',
        application_args=['--output2', '/opt/airflow/spark/data', '--table2', 'test_project.customer_sales'],
        jars='/opt/airflow/spark/jars/gcs-connector-hadoop3-latest.jar',
        conf={
            'spark.eventLog.enabled': 'true',
            'spark.eventLog.dir': '/opt/spark/spark-events',
            'spark.history.fs.logDirectory': '/opt/spark/spark-events',
        },
        name='upload_customersales',
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    get_dataformbigquery = PythonOperator(
        task_id='data_from_bigquery',
        python_callable=data_from_bigquery,
        op_args=[ 'test_project.customer_sales', 'test_project.data_sales' , 'test_project.final_']
    )
    


    read_csv_in1 >> check_sales_duplicates_task
    check_sales_duplicates_task >> handle_sales_duplicates >> check_sales_duplicates_again_task
    check_sales_duplicates_task >> process_sales_duplicates >> check_sales_duplicates_again_task 
    check_sales_duplicates_again_task >> handle_sales_duplicates_again >> upload_bigquery1
    check_sales_duplicates_again_task >> process_sales_duplicates_again >> upload_bigquery1


    read_csv_in2 >> check_customer_duplicates_task 
    check_customer_duplicates_task >> handle_customers_duplicates >> check_customer_duplicates_again_task
    check_customer_duplicates_task >> process_customers_duplicates >> check_customer_duplicates_again_task
    check_customer_duplicates_again_task >> handle_customers_duplicates_again >> upload_bigquery2
    check_customer_duplicates_again_task >> process_customers_duplicates_again >> upload_bigquery2

    upload_bigquery1 >> get_dataformbigquery
    upload_bigquery2 >> get_dataformbigquery







    
