import boto3
import csv
import random
import string

# Set up S3 client
s3_client = boto3.client('s3')

# Define parameters
bucket_name = 'your-bucket-name'
folder_path = 'dummy-data/'
file_size_mb = 1024  # Target file size in MB

# Function to generate realistic names
def generate_realistic_name():
    first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Daniel', 'Jessica', 'James', 'Elizabeth']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Garcia', 'Rodriguez', 'Martinez']
    return random.choice(first_names), random.choice(last_names)

# Function to generate random email address
def generate_email(first_name, last_name):
    domain = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'company.com']
    return f"{first_name.lower()}.{last_name.lower()}@{random.choice(domain)}"

# Function to generate dummy employees data
def generate_employees_data(file_name):
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Employee ID', 'First Name', 'Last Name', 'Email', 'Transaction ID', 'Account ID'])
        for _ in range(file_size_mb * 1024):
            first_name, last_name = generate_realistic_name()
            email = generate_email(first_name, last_name)
            transaction_id = random.randint(100000, 999999)
            account_id = random.randint(1000, 9999)
            writer.writerow([_ + 1, first_name, last_name, email, transaction_id, account_id])

# Function to generate dummy transactions data
def generate_transactions_data(file_name):
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Transaction ID', 'Amount', 'Description'])
        for _ in range(file_size_mb * 1024):
            transaction_id = random.randint(100000, 999999)
            amount = round(random.uniform(10.00, 1000.00), 2)
            description = ''.join(random.choices(string.ascii_letters + string.digits, k=20))
            writer.writerow([transaction_id, amount, description])

# Function to generate dummy accounts data
def generate_accounts_data(file_name):
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Account ID', 'Balance', 'Account Type'])
        for _ in range(file_size_mb * 1024):
            account_id = random.randint(1000, 9999)
            balance = round(random.uniform(1000.00, 100000.00), 2)
            account_type = random.choice(['Checking', 'Savings', 'Investment', 'Credit'])
            writer.writerow([account_id, balance, account_type])

# Function to generate dummy department data
def generate_department_data(file_name):
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Department ID', 'Department Name'])
        for department_id, department_name in enumerate(['HR', 'Finance', 'IT', 'Marketing', 'Operations'], start=1):
            writer.writerow([department_id, department_name])

# Function to generate dummy position data
def generate_position_data(file_name):
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Position ID', 'Position Name'])
        for position_id, position_name in enumerate(['Manager', 'Team Lead', 'Developer', 'Analyst', 'Coordinator'], start=1):
            writer.writerow([position_id, position_name])

# Upload file to S3
def upload_to_s3(file_name):
    s3_key = folder_path + file_name
    s3_client.upload_file(file_name, bucket_name, s3_key)
    print(f"Uploaded {file_name} to S3 at s3://{bucket_name}/{s3_key}")

# Main function
def main():
    # Generate and upload dummy employees data
    employees_file_name = 'employees.csv'
    generate_employees_data(employees_file_name)
    upload_to_s3(employees_file_name)

    # Generate and upload dummy transactions data
    transactions_file_name = 'transactions.csv'
    generate_transactions_data(transactions_file_name)
    upload_to_s3(transactions_file_name)

    # Generate and upload dummy accounts data
    accounts_file_name = 'accounts.csv'
    generate_accounts_data(accounts_file_name)
    upload_to_s3(accounts_file_name)

    # Generate and upload dummy department data
    department_file_name = 'department.csv'
    generate_department_data(department_file_name)
    upload_to_s3(department_file_name)

    # Generate and upload dummy position data
    position_file_name = 'position.csv'
    generate_position_data(position_file_name)
    upload_to_s3(position_file_name)

if __name__ == "__main__":
    main()
