import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from faker import Faker
import pandas as pd
import numpy as np

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# ====================== Utility Functions ============================
def generate_employee_data(fake, num_records):
    data = []
    for _ in range(num_records):
        data.append({
            'employee_id': fake.uuid4(),
            'name': fake.name(),
            'department': fake.job(),
            'email': fake.email(),
            'phone_number': fake.phone_number()
        })
    return pd.DataFrame(data)


def generate_sales_data(fake, num_records,employee_df):
    data = []
    for _ in range(num_records):
        data.append({
            'sales_id': fake.uuid4(),
            'employee_id': np.random.choice(employee_df['employee_id']),
            'product_id': fake.uuid4(),
            'quantity': np.random.randint(1, 10),
            'amount': round(np.random.uniform(10, 1000), 2),
            'sale_date': fake.date_between(start_date='-2y', end_date='today')
        })
    return pd.DataFrame(data)


def generate_products_data(fake, num_records):
    data = []
    for _ in range(num_records):
        data.append({
            'product_id': fake.uuid4(),
            'product_name': fake.word(),
            'category': fake.word(),
            'price': round(np.random.uniform(10, 500), 2)
        })
    return pd.DataFrame(data)


def generate_customers_data(fake, num_records):
    data = []
    for _ in range(num_records):
        data.append({
            'customer_id': fake.uuid4(),
            'customer_name': fake.name(),
            'email': fake.email(),
            'address': fake.address(),
            'phone_number': fake.phone_number()
        })
    return pd.DataFrame(data)


def generate_transactions_data(fake, num_records, customers_df, products_df):
    data = []
    for _ in range(num_records):
        data.append({
            'transaction_id': fake.uuid4(),
            'customer_id': np.random.choice(customers_df['customer_id']),
            'product_id': np.random.choice(products_df['product_id']),
            'quantity': np.random.randint(1, 5),
            'amount': round(np.random.uniform(10, 500), 2),
            'transaction_date': fake.date_between(start_date='-2y', end_date='today')
        })
    return pd.DataFrame(data)


def load_dataframe_redshit(df, dbname, table_name, s3_bucket, rs_host, rs_port, rs_username, rs_password):
    (df.write
     .format("io.github.spark_redshift_community.spark.redshift")
     .option("url", f"jdbc:redshift://{rs_host}:{rs_port}/{dbname}?user={rs_username}&password={rs_password}")
     .option("dbtable", table_name)
     .option("tempdir", f"{s3_bucket}/temp/{table_name}")
     .option("forward_spark_s3_credentials", "true")
     .mode("error")
     .save()
     )


# ============================================
fake = Faker()
s3_bucket='s3://redsdlcbucket/fin6-data'

# Generate employee data
num_records = 1000000
employee_df = generate_employee_data(fake, num_records)
spark.createDataFrame(employee_df).repartition(1).write.parquet(f'{s3_bucket}/employee')
employee = spark.read.parquet(f'{s3_bucket}/employee').toPandas()
print(f'Employee count = {employee.shape[0]}')

# Generate sales data
sales_df = generate_sales_data(fake, num_records, employee)
spark.createDataFrame(sales_df).repartition(1).write.parquet(f'{s3_bucket}/sales_raw')
sales_raw = spark.read.parquet(f'{s3_bucket}/employee').toPandas()
print(f'sales count = {sales_raw.shape[0]}')

# Generate products data
products_df = generate_products_data(fake,1000)
spark.createDataFrame(sales_df).repartition(1).write.parquet(f'{s3_bucket}/products')
products = spark.read.parquet(f'{s3_bucket}/products').toPandas()
print(f'products count = {products.shape[0]}')

# Generate customers data
customers_df = generate_customers_data(fake, 5000)
spark.createDataFrame(customers_df).repartition(1).write.parquet(f'{s3_bucket}/customers')
customers = spark.read.parquet(f'{s3_bucket}/customers').toPandas()
print(f'customers count = {customers.shape[0]}')

# Generate transactions data
transactions_df = generate_transactions_data(fake, 10000000, customers_df = customers, products_df=products)
spark.createDataFrame(transactions_df).repartition(100).write.parquet(f'{s3_bucket}/transactions_raw')
transactions_raw = spark.read.parquet(f'{s3_bucket}/transactions_raw').toPandas()
print(f'transactions_raw count = {transactions_raw.shape[0]}')

final_sales = sales_sdf.join(emp_sdf, ['employee_id', 'name'], 'left')
# final_transactions = transactions_sdf.join(customers_sdf.select('customer_id', 'customer_name'), ['customer_id'], 'left')
# final_transactions = final_transactions.join(products_sdf.select('product_id', 'product_name'), ['product_id'], 'left')


job.commit()