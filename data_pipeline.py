from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests


MYSQL_CONNECTION = "mysql_default"   # ชื่อของ connection ใน Airflow ที่เซ็ตเอาไว้
CONVERSION_RATE_URL = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"

# ตัวแปรของ output_path ที่จะเซฟ
mysql_output_path = "/home/airflow/gcs/data/transaction_data_merged.parquet"
conversion_rate_output_path = "/home/airflow/gcs/data/conversion_rate.parquet"
final_output_path = "/home/airflow/gcs/data/workshop4_output.parquet"

default_args = {
    'owner': 'datath',
}


# สร้าง task แรก เพื่อ Query ข้อมูลจาก MySQL
@task()
def get_data_from_mysql(output_path):
    # รับ output_path มาจาก task ที่เรียกใช้

    # เรียกใช้ MySqlHook เพื่อต่อไปยัง MySQL จาก connection ที่สร้างไว้ใน Airflow
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    
    # Query จาก database โดยใช้ Hook ที่สร้าง ผลลัพธ์ได้ pandas DataFrame
    product = mysqlserver.get_pandas_df(sql="SELECT * FROM r2de3.product")
    customer = mysqlserver.get_pandas_df(sql="SELECT * FROM r2de3.customer")
    transaction = mysqlserver.get_pandas_df(sql="SELECT * FROM r2de3.transaction")
    merged_transaction = transaction.merge(product, how="left", left_on="ProductNo", right_on="ProductNo").merge(customer, how="left", left_on="CustomerNo", right_on="CustomerNo")
    
    # Save ไฟล์ parquet ไปที่ output_path ที่รับเข้ามา
    # จะไปอยู่ที่ GCS โดยอัตโนมัติ
    merged_transaction.to_parquet(output_path, index=False)
    print(f"Output to {output_path}")


# สร้าง task ที่ 2 (ทำงานพร้อม task แรก) เพื่อเรียกข้อมูล conversion rate
@task()
def get_conversion_rate(output_path):
    r = requests.get(CONVERSION_RATE_URL)
    result_conversion_rate = r.json()
    df = pd.DataFrame(result_conversion_rate)
    df = df.drop(columns=['id'])
    df['date'] = pd.to_datetime(df['date'])
    df.to_parquet(output_path, index=False)
    print(f"Output to {output_path}")


# สร้าง task ที่ 3 เพื่อ merge data (ต้องทำงานหลัง task 1 และ 2 เท่านั้น)
@task()
def merge_data(transaction_path, conversion_rate_path, output_path):
    # อ่านจากไฟล์ สังเกตว่าใช้ path จากที่รับ parameter มา
    transaction = pd.read_parquet(transaction_path)
    conversion_rate = pd.read_parquet(conversion_rate_path)

    # merge 2 DataFrame
    final_df = transaction.merge(conversion_rate, how="left", left_on="Date", right_on="date")
    
    # แปลงราคา ให้เป็น total_amount และ THB_price_total
    final_df["total_amount"] = final_df["Price"] * final_df["Quantity"]
    final_df["THB_price_total"] = final_df["total_amount"] * final_df["gbp_thb"]

    # drop column ที่ไม่ใช้ และเปลี่ยนชื่อ column
    final_df = final_df.drop(["date", "gbp_thb"], axis=1)

    final_df.columns = ['transaction_id', 'date', 'product_id', 'price', 'quantity', 'customer_id',
        'product_name', 'customer_country', 'customer_name', 'total_amount','THB_price_total']

    # save ไฟล์ Parquet
    final_df.to_parquet(output_path, index=False)
    print(f"Output to {output_path}")
    print("== End of Workshop ==")


# สร้าง dag
@dag(default_args=default_args,schedule_interval="@once",catchup=False,start_date=days_ago(1))
def workshop_pipeline():

    # สร้าง task จาก function ด้านบน และใส่ parameter ให้ถูกต้อง
    t1 = get_data_from_mysql(output_path = mysql_output_path)
    t2 = get_conversion_rate(output_path = conversion_rate_output_path)
    t3 = merge_data(
        transaction_path = mysql_output_path, 
        conversion_rate_path = conversion_rate_output_path, 
        output_path = final_output_path
        )
    # option 1: สร้าง t4 ที่เป็น GCSToBigQueryOperator เพื่อใช้งานกับ BigQuery แบบ Airflow และใส่ dependencies
    t4 = GCSToBigQueryOperator(
        task_id = "gcs_to_bq_example",
        bucket="us-central1-workshop5-7ac892d3-bucket",
        source_objects=["data/workshop4_output.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table="workshop.transaction1",
        write_disposition="WRITE_TRUNCATE"
    )
    # option 2: สร้าง t4 ที่เป็น BashOperator เพื่อใช้งานกับ BigQuery และใส่ dependencies
    # t4 = BashOperator(
    #     task_id = "bq_load",
    #     bash_command="bq load --source_format=PARQUET workshop.transaction gs://us-central1-workshop5-7ac892d3-bucket/data/workshop4_output.parquet"
    # ) 

    # สร้าง dependency
    [t1,t2] >> t3 >> t4

workshop_pipeline()