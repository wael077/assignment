is started whith  Data Extraction (data connection and extraction) from path
and then Create extraction directory.
then  Extract the zip file
and List the extracted files
After that starting loading date by imorting required libraries like pandas
to help reading files , created Function to detect file encoding
and to  handle bad lines
then  Function to find latest file per month
then start to ingest data from csv files
Use lateste file Of month if there is multiple file for the same Month
then Create dimension and fact dataframes 
then Data Profiling (data examination and typification) 
created Func 
Identify and store inconsistent records
 Export inconsistent records to Excel file
 Function to standardize date format
 Data Loading in Data Marts
create tables based on data frames 
Execute the query and store the results in a DataFrame





