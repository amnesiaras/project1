# -*- coding: utf-8 -*-
! pip install pymysql

from google.colab import userdata # import function ให้สามารถใช้ค่าจาก Colab Secret
import sqlalchemy
import pandas as pd
import requests

# ใช้ userdata.get("key") เพื่อเข้าถึง Colab Secrets
class Config:
  MYSQL_HOST = userdata.get("MYSQL_HOST")
  MYSQL_PORT = userdata.get("MYSQL_PORT")
  MYSQL_USER = userdata.get("MYSQL_USER")
  MYSQL_PASSWORD = userdata.get("MYSQL_PASSWORD")
  MYSQL_DB = 'r2de3'
  MYSQL_CHARSET = 'utf8mb4'

# สร้าง engine โดย engine เป็น object ของ database ที่เอาไว้ใช้ในการเข้าถึง data
engine = sqlalchemy.create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(
        user=Config.MYSQL_USER,
        password=Config.MYSQL_PASSWORD,
        host=Config.MYSQL_HOST,
        port=Config.MYSQL_PORT,
        db=Config.MYSQL_DB,
    )
)

# --version 1-- ดึงข้อมูลจาก database โดยใช้ Pandas 
customer = pd.read_sql("SELECT * FROM r2de3.customer",engine)
transaction = pd.read_sql("SELECT * FROM r2de3.transaction",engine)
product = pd.read_sql("SELECT * FROM r2de3.product",engine)
# Merge Table customer transaction product เข้าด้วยกัน 
merged_transaction = transaction.merge(customer,how="left",left_on="CustomerNo",right_on="CustomerNo").merge(product,how="left",left_on="ProductNo",right_on="ProductNo")


# --version 2-- ดึงข้อมูลจาก database โดยใช้ sqlalchemy
with engine.connect() as connection:
  customert_result = connection.execute(sqlalchemy.text(f"SELECT * FROM r2de3.customer;")).fetchall()
  transaction_result = connection.execute(sqlalchemy.text(f"SELECT * FROM r2de3.transaction;")).fetchall()
  product_result = connection.execute(sqlalchemy.text(f"SELECT * FROM r2de3.product;")).fetchall()
# นำข้อมูลมาใส่ใน DataFrame
customer = pd.DataFrame(customert_result)
transaction = pd.DataFrame(transaction_result)
product = pd.DataFrame(product_result)
# Merge Table customer transaction product เข้าด้วยกัน 
merged_transaction = transaction.merge(customer,how="left",left_on="CustomerNo",right_on="CustomerNo").merge(product,how="left",left_on="ProductNo",right_on="ProductNo")


#ดึงข้อมูลจาก API
url = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"
r = requests.get(url)
result_conversion_rate = r.json()
conversion_rate = pd.DataFrame(result_conversion_rate)
conversion_rate["date"] = pd.to_datetime(conversion_rate["date"])
conversion_rate.drop("id",axis=1,inplace=True)


final_merge = merged_transaction.merge(conversion_rate,how="left",left_on="Date",right_on="date")
final_merge["THB_price_total"] = final_merge["Price"] * final_merge["Quantity"] * final_merge["gbp_thb"]
final_merge.drop(["date","gbp_thb"] ,axis=1,inplace=True)
final_merge


