#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import zipfile
import os

# a) Data Extraction (data connection and extraction)


zip_file_path = './Case_Study_202309_Data.zip'
extract_dir = './Case_Study_Data/'

# Create extraction directory if it does not exist
os.makedirs(extract_dir, exist_ok=True)

# Extract the zip file
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(extract_dir)

# List the extracted files
extracted_files = os.listdir(extract_dir)
extracted_files


# In[82]:


## the code here is doing data 

from collections import defaultdict
from datetime import datetime
from pandasql import sqldf
import pandas as pd
import sqlite3
import chardet
import os
import re
from scipy import stats

# Function to detect file encoding
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(100000))
    return result['encoding']

def handle_bad_lines(data_dir, file, bad_lines):
    error_parts = bad_lines.split(': ')[1]
    #print(f'file: {file}, bad_lines: {error_parts}')
    error_file = 'bad_lines.csv'
    file_path = os.path.join(data_dir, error_file)
    with open(file_path, 'a') as f:
        for line_number in bad_lines:
            f.write(f'file: {file}, bad_lines: {error_parts}')
import os
import re
from collections import defaultdict
from datetime import datetime
# Function to find latest file per month
def extract_date_and_timestamp(filename):
    match = re.match(r"(\d{6})_Orders_(\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})", filename)
    if match:
        date_part = match.group(1)
        timestamp_part = match.group(2)
        return date_part, timestamp_part
    return None, None

def get_latest_files_by_month(directory):
    files = [file for file in os.listdir(directory) if file.endswith('.csv')]
    files_by_month = defaultdict(list)
    
    for file in files:
        date_part, timestamp_part = extract_date_and_timestamp(file)
        if date_part and timestamp_part:
            files_by_month[date_part].append((file, timestamp_part))
    
    latest_files = []
    for date_part, file_timestamps in files_by_month.items():
        latest_file = max(file_timestamps, key=lambda x: datetime.strptime(x[1], '%Y_%m_%d_%H_%M_%S'))
        latest_files.append(latest_file[0])
    
    return latest_files

 
data_dir = './Case_Study_Data/Case_Study_Data_For_Share'

# Use lateste file Of month if there is multiple file for the same Month
latest_files = get_latest_files_by_month(data_dir)



# Loading data from CSV
common_header = ['Row ID','Order ID','Order Date','Ship Date','Ship Mode','Customer ID','Customer Name','Segment','Country','City','State','Postal Code','Region','Product ID','Category','Sub-Category','Product Name','Sales','Quantity','Discount','Profit','FileName']
data_frames = []
for file in latest_files:
    file_path = os.path.join(data_dir, file)
    encoding = detect_encoding(file_path)

    try:
        df = pd.read_csv(file_path, encoding=encoding,sep='|',engine='python')
        df['FileName'] =file
        df = df.reindex(columns=common_header)
        data_frames.append(df)
    except pd.errors.ParserError as e:
        ##e) Master data qualifying (defining golden records for key master data elements) using filename
        handle_bad_lines(data_dir, file, str(e))
        continue
combined_df = pd.concat(data_frames, ignore_index=True)


#Create DateDimensionsions
def generate_date_dimension(start_date, end_date):
    date_range = pd.date_range(start_date, end_date)
    date_dimension_data = []
    
    for date in date_range:
        year = date.year
        quarter = (date.month - 1) // 3 + 1
        month = date.month
        day = date.day
        month_name = date.strftime('%B')
        
        date_dimension_data.append({
            'DateID': int(date.strftime('%Y%m%d')),
            'FullDate': date,
            'Year': year,
            'Quarter': quarter,
            'Month': month,
            'Day': day,
            'MonthName': month_name
        })
    
    return pd.DataFrame(date_dimension_data)

DateDimension = generate_date_dimension('2019-01-01', '2022-12-31')
#DateDimension.to_csv('DateDimension.csv', index=False)

CustomerDimension = combined_df[['Customer ID' , 'Customer Name' ,'Segment']].drop_duplicates().reset_index(drop=True)
#CustomerDimension.to_csv('CustomerDimension.csv', index=False)

ProductDimension = combined_df[['Product ID' , 'Category' , 'Sub-Category' , 'Product Name']].drop_duplicates().reset_index(drop=True)
#ProductDimension.to_csv('ProductDimension.csv', index=False)

OrderDimension = combined_df[['Order ID' , 'Ship Mode']].drop_duplicates().reset_index(drop=True)
#OrderDimension.to_csv('OrderDimension.csv', index=False)

#Extract LocationDimension and generating ID
LocationDimension = combined_df[['Country' , 'City' , 'State' , 'Postal Code' , 'Region']].drop_duplicates().reset_index(drop=True)
LocationDimension['LocationID'] = LocationDimension.index + 1
LocationColumns = ['Country' , 'City' , 'State' , 'Postal Code' , 'Region']
LocationDimension = LocationDimension[['LocationID'] + LocationColumns]
LocationDimension.to_csv('LocationDimension.csv', index=False)

combined_df = combined_df.merge(LocationDimension, on=LocationColumns, how='left')

#combined_df.to_csv('combined_df1.csv', index=False)

############

#b) Data Profiling (data examination and typification) 
inconsistent_quantity_records = combined_df[combined_df['Quantity'] <= 0]
inconsistent_rofit_records = combined_df[combined_df['Profit'] < 0]
 
combined_df = combined_df[combined_df['Quantity'] > 0]

#c) Data Cleansing (data cleansing in terms of inconsistencies)
#original_df = combined_df.copy()
date_columns = ['Ship Date', 'Order Date']

# Function to detect inconsistent date formats

def detect_inconsistent_dates(df, columns):
    inconsistent_indices = []
    for col in columns:
        for i, date in enumerate(df[col]):
            try:
                # Check if date matches the correct format
                pd.to_datetime(date, format='%d-%m-%Y', errors='raise')
            except ValueError:
                # If it doesn't match, mark it as inconsistent
                inconsistent_indices.append(i)
    return df.iloc[inconsistent_indices].drop_duplicates()

# Identify and store inconsistent records
inconsistent_date_records = detect_inconsistent_dates(combined_df, date_columns)

# Export inconsistent records to Excel file
with pd.ExcelWriter('inconsistent_records_Final.xlsx') as writer:
    if not inconsistent_date_records.empty:
        inconsistent_date_records.to_excel(writer, sheet_name='Inconsistent Dates', index=False)
    if not inconsistent_quantity_records.empty:
        inconsistent_quantity_records.to_excel(writer, sheet_name='Inconsistent Quantities', index=False)
    if not inconsistent_rofit_records.empty:
        inconsistent_rofit_records.to_excel(writer, sheet_name='Inconsistent Profit', index=False)

 
# Function to standardize date format
def standardize_date_format(date):
    formats = ['%d-%m-%Y', '%m/%d/%Y', '%Y-%m-%d']
    for fmt in formats:
        try:
            return pd.to_datetime(date, format=fmt).strftime('%d-%m-%Y')
        except ValueError:
            continue
    return date  # If all formats fail, return the original date

# Standardize dates in the DataFrame
for col in date_columns:
    combined_df[col] = combined_df[col].apply(standardize_date_format)
for col in date_columns:
    combined_df[col] = pd.to_datetime(combined_df[col], format='%d-%m-%Y')    
    
    
###########



combined_df['DeliveryTime'] = (combined_df['Ship Date'] - combined_df['Order Date']).dt.days

# Merge combined_df with DateDimension on Ship Date to get Ship Date ID
combined_df = pd.merge(combined_df, DateDimension[['DateID', 'FullDate']], left_on='Ship Date', right_on='FullDate', how='left')
combined_df.rename(columns={'DateID': 'ShipDateID'}, inplace=True)
combined_df.drop(['FullDate'], axis=1, inplace=True)

# Merge combined_df with DateDimension on Order Date to get Order Date ID
combined_df = pd.merge(combined_df, DateDimension[['DateID', 'FullDate']], left_on='Order Date', right_on='FullDate', how='left')
combined_df.rename(columns={'DateID': 'OrderDateID'}, inplace=True)
combined_df.drop(['FullDate'], axis=1, inplace=True)
combined_df['Price']=(combined_df['Sales'] / combined_df['Quantity'])

SalesFact = combined_df[['Row ID', 'Order ID', 'Customer ID', 'Product ID','OrderDateID', 'ShipDateID','DeliveryTime', 'LocationID', 'Sales', 'Quantity', 'Discount', 'Profit','Price']].drop_duplicates().reset_index(drop=True)
#SalesFact.to_csv('SalesFact.csv', index=False)

print ('Done')
#print (combined_df)


# In[59]:


#f) Data Loading in Data Marts
get_ipython().system('pip install pandas sqlalchemy')
from sqlalchemy import create_engine
engine = create_engine('sqlite:///data_warehouse.db')

#create tables
DateDimension.to_sql('DateDimension', con=engine, if_exists='replace', index=False)
ProductDimension.to_sql('ProductDimension', con=engine, if_exists='replace', index=False)
CustomerDimension.to_sql('CustomerDimension', con=engine, if_exists='replace', index=False)
OrderDimension.to_sql('OrderDimension', con=engine, if_exists='replace', index=False)
LocationDimension.to_sql('LocationDimension', con=engine, if_exists='replace', index=False)
SalesFact.to_sql('SalesFact', con=engine, if_exists='replace', index=False)


# In[81]:


# Sample Of KPIs

'''query_total_sales = "SELECT * AS TotalSales FROM SalesFact"
total_sales = engine.execute(query_total_sales).scalar()
print (total_sales)'''

#Average Price for Best-Selling Products
query = """
    SELECT "Product Id",AVG(sf.Price) AS AvgPrice
    FROM SalesFact sf
    WHERE sf."Product Id" IN (
        SELECT "Product Id"
        FROM (
            SELECT "Product Id", COUNT(*) AS product_count
            FROM SalesFact
            GROUP BY "Product Id"
            ORDER BY product_count DESC
            LIMIT 10
        ) top_products
    )
    group by "Product Id"
"""


# Execute the query and store the results in a DataFrame
query_result = pd.read_sql(query, engine)
print('Average Price for Best-Selling Products')
print(query_result)



#Sales Dynamics by States

query = """
    SELECT L.State, SUM(s.Sales) AS TotalSales
    FROM SalesFact s
    JOIN LocationDimension L ON s.LocationID = L.LocationID
    GROUP BY L.State
"""
query_result = pd.read_sql(query, engine)
print('Sales Dynamics by States')
print(query_result)




#Best Customers in Segments
query = """
SELECT "Customer ID", Segment, "Customer Name", TotalSales
FROM (
    SELECT 
        C."Customer ID",
        C.Segment,
        C."Customer Name",
        SUM(S.Sales + S.Profit) AS TotalSales,
        ROW_NUMBER() OVER (PARTITION BY C.Segment ORDER BY SUM(S.Sales + S.Profit) DESC) AS CustomerRank
    FROM SalesFact S
    JOIN CustomerDimension C ON S."Customer ID" = C."Customer ID"
    GROUP BY C."Customer ID", C.Segment, C."Customer Name"
) ranked_customers
WHERE CustomerRank <= 5
ORDER BY Segment, TotalSales DESC;


"""
#query = "select * from CustomerDimension"
query_result = pd.read_sql(query, engine)
print('Best Customers in Segments')
print(query_result)


# In[90]:


query = """
    SELECT  * from OrderDimension a"""
query_result = pd.read_sql(query, engine)
query_result.to_csv('OrderDimension.csv', index=False)

query = """
    SELECT  * from DateDimension a"""
query_result = pd.read_sql(query, engine)
query_result.to_csv('DateDimension.csv', index=False)

query = """
    SELECT  * from ProductDimension a"""
query_result = pd.read_sql(query, engine)
query_result.to_csv('ProductDimension.csv', index=False)

query = """
    SELECT  * from LocationDimension a"""
query_result = pd.read_sql(query, engine)
query_result.to_csv('LocationDimension.csv', index=False)


query = """
    SELECT  * from SalesFact a"""
query_result = pd.read_sql(query, engine)
query_result.to_csv('SalesFact.csv', index=False)


# In[106]:


query = """SELECT 
    'SalesFact' AS TableName,
    COUNT(*) AS RowCount,
    COUNT(DISTINCT RowID) AS DistinctPrimaryKeys,
    COUNT(DISTINCT "RowId") AS DistinctRowIds
FROM SalesFact"""
query_result = pd.read_sql(query, engine)
print (query_result)


query = """SELECT 
    'DateDimension' AS TableName,
    COUNT(*) AS RowCount,
    COUNT(DISTINCT DateID) AS DistinctPrimaryKeys,
    COUNT(DISTINCT RowId) AS DistinctRowIds
FROM DateDimension"""
query_result = pd.read_sql(query, engine)
print (query_result)



query = """SELECT 
    'OrderDimension' AS TableName,
    COUNT(*) AS RowCount,
    COUNT(DISTINCT "Order ID") AS DistinctPrimaryKeys,
    COUNT(DISTINCT RowId) AS DistinctRowIds
FROM OrderDimension"""
query_result = pd.read_sql(query, engine)
print (query_result)



query = """SELECT 
    'CustomerDimension' AS TableName,
    COUNT(*) AS RowCount,
    COUNT(DISTINCT "Customer ID") AS DistinctPrimaryKeys,
    COUNT(DISTINCT RowId) AS DistinctRowIds
FROM CustomerDimension"""
query_result = pd.read_sql(query, engine)
print (query_result)




query = """SELECT 
    'LocationDimension' AS TableName,
    COUNT(*) AS RowCount,
    COUNT(DISTINCT "LocationID") AS DistinctPrimaryKeys,
    COUNT(DISTINCT RowId) AS DistinctRowIds
FROM LocationDimension"""
query_result = pd.read_sql(query, engine)
print (query_result)




query = """SELECT 
    'ProductDimension' AS TableName,
    COUNT(*) AS RowCount,
    COUNT(DISTINCT "Product ID") AS DistinctPrimaryKeys,
    COUNT(DISTINCT RowId) AS DistinctRowIds
FROM ProductDimension"""
query_result = pd.read_sql(query, engine)
print (query_result)


# In[108]:


query = """SELECT 
    'SalesFact' AS TableName,
    COUNT(*) AS RowCount,
    COUNT(DISTINCT RowID) AS DistinctPrimaryKeys,
    COUNT(DISTINCT "RowId") AS DistinctRowIds
FROM SalesFact
union
SELECT 
    'DateDimension' AS TableName,
    COUNT(*) AS RowCount,
    COUNT(DISTINCT DateID) AS DistinctPrimaryKeys,
    COUNT(DISTINCT RowId) AS DistinctRowIds
FROM DateDimension
union 
SELECT 
    'OrderDimension' AS TableName,
    COUNT(*) AS RowCount,
    COUNT(DISTINCT "Order ID") AS DistinctPrimaryKeys,
    COUNT(DISTINCT RowId) AS DistinctRowIds
FROM OrderDimension
union
SELECT 
    'CustomerDimension' AS TableName,
    COUNT(*) AS RowCount,
    COUNT(DISTINCT "Customer ID") AS DistinctPrimaryKeys,
    COUNT(DISTINCT RowId) AS DistinctRowIds
FROM CustomerDimension
union
SELECT 
    'LocationDimension' AS TableName,
    COUNT(*) AS RowCount,
    COUNT(DISTINCT "LocationID") AS DistinctPrimaryKeys,
    COUNT(DISTINCT RowId) AS DistinctRowIds
FROM LocationDimension
union
SELECT 
    'ProductDimension' AS TableName,
    COUNT(*) AS RowCount,
    COUNT(DISTINCT "Product ID") AS DistinctPrimaryKeys,
    COUNT(DISTINCT RowId) AS DistinctRowIds
FROM ProductDimension"""
query_result = pd.read_sql(query, engine)
#print (query_result)
query_result.to_csv('Task_6_2_Data_Marts_Rows.csv', index=False)


# In[ ]:




