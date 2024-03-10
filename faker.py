import pandas as pd
import numpy as np
from faker import Faker
import os
import boto3

# Generate fake data
fake = Faker()

def generate_employee_data(num_records):
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

def generate_sales_data(num_records):
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

def generate_products_data(num_records):
    data = []
    for _ in range(num_records):
        data.append({
            'product_id': fake.uuid4(),
            'product_name': fake.word(),
            'category': fake.word(),
            'price': round(np.random.uniform(10, 500), 2)
        })
    return pd.DataFrame(data)

def generate_customers_data(num_records):
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

def generate_transactions_data(num_records):
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

# Generate employee data
num_records = 1000000
employee_df = generate_employee_data(num_records)

# Generate sales data
sales_df = generate_sales_data(num_records)

# Generate products data
products_df = generate_products_data(1000)

# Generate customers data
customers_df = generate_customers_data(5000)

# Generate transactions data
transactions_df = generate_transactions_data(10000000)

# Connect tables using foreign keys
sales_df = pd.merge(sales_df, employee_df[['employee_id', 'name']], on='employee_id', how='left')
transactions_df = pd.merge(transactions_df, customers_df[['customer_id', 'customer_name']], on='customer_id', how='left')
transactions_df = pd.merge(transactions_df, products_df[['product_id', 'product_name']], on='product_id', how='left')

# Create directory to store CSV files
output_dir = '/tmp/finance_data'
os.makedirs(output_dir, exist_ok=True)

# Write dataframes to CSV files
employee_df.to_csv(os.path.join(output_dir, 'employee.csv'), index=False)
sales_df.to_csv(os.path.join(output_dir, 'sales.csv'), index=False)
products_df.to_csv(os.path.join(output_dir, 'products.csv'), index=False)
customers_df.to_csv(os.path.join(output_dir, 'customers.csv'), index=False)
transactions_df.to_csv(os.path.join(output_dir, 'transactions.csv'), index=False)

# Upload CSV files to S3
bucket_name = 'your-bucket-name'
s3_client = boto3.client('s3')

for file_name in os.listdir(output_dir):
    s3_client.upload_file(os.path.join(output_dir, file_name), bucket_name, f'finance_data/{file_name}')

print("Files uploaded to S3 successfully!")
