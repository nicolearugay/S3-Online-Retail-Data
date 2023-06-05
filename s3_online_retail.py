# https://github.com/nicolearugay
# Uploaded 4/9/2023

import boto3
import pandas as pd
from io import StringIO


# Configure AWS credentials
AWS_ACCESS_KEY = 'insert access key'
AWS_SECRET_ACCESS_KEY = 'insert secret access key'
AWS_REGION = 'insert region'
S3_BUCKET_NAME = 'insert bucket name'


# Start the s3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)


# Upload a file to S3
file_path = "path/to/file"
file_name = 'insert file name'
s3_client.upload_file(file_path, S3_BUCKET_NAME, file_name)


# Download the CSV file from the S3 bucket
csv_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file_name)
csv_data = csv_obj['Body'].read().decode('utf-8')


# Load the CSV data into a pandas DataFrame
data = StringIO(csv_data)
df = pd.read_csv(data)


# Perform analysis using pandas
# Calculate the total quantity sold per product description
total_quantity_per_product = df.groupby('Description')['Quantity'].sum()


# Get top 10 most sold products
top_10_products = total_quantity_per_product.sort_values(ascending=False).head(10)
print("Top 10 most sold products:")
print(top_10_products)


# Calculate the total revenue per country
df['Revenue'] = df['Quantity'] * df['Price']
total_revenue_per_country = df.groupby('Country')['Revenue'].sum()


# Get the top 10 countries with the highest revenue
top_10_countries = total_revenue_per_country.sort_values(ascending=False).head(10)
print("\nTop 10 countries with the highest revenue:")
print(top_10_countries)


# Close the S3 client
s3_client.close()
