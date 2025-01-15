import mysql.connector as mc
import matplotlib.pyplot as plt
import pandas as pd
import statistics
from sqlalchemy import create_engine

# Establish a connection to your MySQL database
engine = create_engine('mysql+mysqlconnector://root:root@localhost/springdb')

# Customer Tables
sql_query = "SELECT * FROM customers "
# "SELECT * FROM department LIMIT 4,2"


# Convert MYSql data into a pandas DataFrame
df_customers = pd.read_sql_query(sql_query, engine)
#
# # Display the DataFrame
print(df_customers)

# Product Table
sql_query = "SELECT * FROM products "
# "SELECT * FROM department LIMIT 4,2"

# Convert MYSql data into a pandas DataFrame
df_products = pd.read_sql_query(sql_query, engine)

# Display the DataFrame
print(df_products)

# sql_query = "SELECT * FROM sales"
sql_query = "SELECT * FROM salescleaned"
# "SELECT * FROM department LIMIT 4,2"

# Convert MYSql data into a pandas DataFrame
df_sales = pd.read_sql_query(sql_query, engine)

# Display the DataFrame
# print(df_sales)
# # Check for Database Structure
# print(df_customers.info())
# print(df_customers.describe())
# print(df_products.info())
# print(df_products.describe())
# print(df_sales.info())
# print(df_sales.describe())

# Data Cleaning

# Customers  Data Cleaning

# Forward fill missing values in the entire DataFrame
df_customers.ffill(inplace=True)

# Display the updated DataFrame
print(df_customers)

df_Noduplicates = (df_customers.drop_duplicates())
print(df_Noduplicates)

# Display the original DataFrame
print("Original DataFrame:")
print(df_customers)

# Products  Data Cleaning

# Forward fill missing values in the entire DataFrame
df_products.ffill(inplace=True)

# Display the updated DataFrame
print(df_products)

print(df_products.info())

df_products_Noduplicates = df_products.drop_duplicates()
print(df_products_Noduplicates)

# Display the original DataFrame
print("Original DataFrame:")
print(df_products)

# Sales Data Cleaning
# Forward fill missing values in the entire DataFrame
df_sales.ffill(inplace=True)

# Display the updated DataFrame
# print(df_sales)
#
# print(df_sales.info())

df_sales_Noduplicates = df_sales.drop_duplicates()
# print(df_sales_Noduplicates)
#
# Display the original DataFrame
print("Original DataFrame:")
print(df_sales)

# Write the DataFrame to the MySQL table
df_customers.to_sql(name="customercleaned", con=engine, if_exists='replace', index=False, schema=None)
df_products.to_sql(name="productcleaned", con=engine, if_exists='replace', index=False, schema=None)
df_sales.to_sql(name="salescleaned", con=engine, if_exists='replace', index=False, schema=None)

# Descriptive Statistics
Total_revenue = df_sales["total_amount"].sum()
Average_revenue = df_sales["total_amount"].mean()
Average_qty = df_sales["quantity"].mean()
Total_qty = df_sales["quantity"].sum()

print("***************************Descriptive Statistics********************************")
print("Total Revenue", Total_revenue)
print("Average Revenue", Average_revenue)
print("Average Qty", Average_qty)
print("Total Qty :", Total_qty)

Total_Category = df_products["category"].count()
Average_price = df_products["price"].mean()

print("Total Category:", Total_Category)
print("Average Price:", Average_price)

print("***************************Descriptive Statistics********************************")

# Visualization of Descriptive Statistics

plt.bar(['Total Revenue',  'Average Revenue'],
        [Total_revenue, Average_revenue],
        color=['blue', 'green'])
plt.title("Descriptive Statisitcs of Product Sales")
plt.xlabel("Stats Parameters")
plt.ylabel("Values")
plt.show()

plt.bar(['Average Order Value', 'Total Category','Average Price'],
        [Average_qty, Total_Category, Average_price],
        color=['blue', 'green', 'yellow'])
plt.title("Descriptive Statisitcs of Product Sales")
plt.xlabel("Stats Parameters")
plt.ylabel("Values")
plt.show()

total_amount = df_sales.groupby('customer_id')['total_amount'].sum()
print(total_amount)


# total_product_amount = product.df.groupby['product_id']sum('total_amount')
total_amount = df_products.groupby('product_id')['price'].sum()
print(total_amount)

total_customers = df_sales.groupby('customer_id')['total_amount'].sum()
top_customers = total_customers.nlargest(3)
print(top_customers)

top_customers = df_sales.groupby('customer_id')['total_amount'].sum().nlargest(3)
print(top_customers)

top_products = df_sales.groupby('product_id')['total_amount'].sum().nlargest(3)
print(top_products)

# Plotting the bar graph for Top 3 Customers based on their spendings.
colors = ['purple','yellow', 'green']
plt.bar(top_customers.index.astype(str), top_customers.values, color=colors)
plt.title("Top Customers by Total Spending")
plt.xlabel("Customer ID")
plt.ylabel("Total Spending")
plt.xticks(rotation=45)
plt.show()

# Plotting the bar graph for Top 3 Products based on their sales.
colors = ['orange','pink', 'red']
plt.bar(top_products.index.astype(str), top_products.values, color=colors)
plt.title("Top Products by Total Sales")
plt.xlabel("Product ID")
plt.ylabel("Total Sales")
plt.xticks(rotation=45)
plt.show()

# Plotting the line graph for Top 3 Customers based on their spendings.
colors = ['purple']
plt.plot(top_customers.index.astype(str), top_customers.values, color=colors[0], marker='o', label='Top Customers')
plt.title("Top Customers by Total Spending")
plt.xlabel("Customer ID")
plt.ylabel("Total Spending")
plt.xticks(rotation=45)
plt.legend()
plt.grid(True)
plt.show()

# Plotting the line graph for Top 3 products based on their sales.
colors = ['red']
plt.plot(top_products.index.astype(str), top_products.values, color=colors[0], marker='o', label='Top Products')
plt.title("Top Products by Total Sales")
plt.xlabel("Product ID")
plt.ylabel("Total Sales")
plt.xticks(rotation=45)
plt.legend()
plt.grid(True)
plt.show()

# **************Time based Analysis*********************

# Convert 'sale_date' column to datetime
df_sales['sale_date'] = pd.to_datetime(df_sales['sale_date'])

# Set 'sale_date' column as index
df_sales.set_index('sale_date', inplace=True)
df_sales.groupby('product_id')['quantity'].sum()
# Aggregate sales data based on different time intervals
monthly_sales = df_sales.resample('M').sum()
print(monthly_sales)
y = monthly_sales['total_amount']
plt.title("Monthly Sales")
plt.plot(y, color='red')
plt.show()

weekly_sales = df_sales.resample('W').sum()
print(weekly_sales)
y = weekly_sales['total_amount']
plt.title("Weekly Sales")
plt.plot(y, color='blue')
plt.show()
plt.title("Weekly Sales using Histogram")
plt.hist(y, color='blue')
plt.show()

Yearly_sales = df_sales.resample('Y').sum()
print(Yearly_sales)
y = Yearly_sales['total_amount']
plt.title("Yearly Sales")
plt.plot(y, color='blue')
plt.show()
plt.bar(y.index.year, y, color='pink')
plt.title("Yearly Sales")
plt.xlabel("Total Sales Amount")
plt.ylabel("Year")
plt.show()

daily_sales = df_sales.resample('D').sum()
print(daily_sales)
y = daily_sales['total_amount']
plt.title("Daily Sales")
plt.plot(y, color='blue')
plt.show()
plt.bar(y.index.year, y, color='blue')
plt.title("Daily Sales")
plt.xlabel("Total Sales Amount")
plt.ylabel("Daily")
plt.show()

# ***************************************************

df_sales.groupby('customer_id')['total_amount'].sum()
# Aggregate sales data based on different time intervals
monthly_sales = df_sales.resample('M').sum()
print(monthly_sales)
y = monthly_sales['total_amount']
plt.title("Aggregate sales data based on monthly intervals")
plt.plot(y, color='black')
plt.show()

df_sales.groupby('customer_id')['total_amount'].sum()
# Aggregate sales data based on different time intervals
Yearly_sales = df_sales.resample('Y').sum()
print(Yearly_sales)
plt.title("Aggregate sales data based on Yearly intervals")
y = Yearly_sales ['total_amount']
plt.plot(y, color='green')
plt.show()


# Plotting total sales amount over months
plt.figure(figsize=(10, 6))
plt.plot(monthly_sales.index, monthly_sales['total_amount'], marker='o', color='b')
plt.title('Monthly Sales Trends')
plt.xlabel('Month')
plt.ylabel('Total Sales Amount')
plt.xticks(rotation=45)
plt.grid(True)
plt.show()

# Assuming monthly_sales is already calculated
# Calculate rolling average and standard deviation
rolling_mean = monthly_sales['total_amount'].rolling(window=30).mean()
rolling_std = monthly_sales['total_amount'].rolling(window=30).std()

# Identify peak sales periods based on z-score threshold
threshold = 2  # Adjust threshold as needed
peak_periods = monthly_sales[(monthly_sales['total_amount'] - rolling_mean).abs() > threshold * rolling_std]

# Visualize monthly sales with rolling mean and standard deviation
plt.figure(figsize=(12, 6))
plt.plot(monthly_sales.index, monthly_sales['total_amount'], label='Monthly Sales', color='blue')
plt.plot(monthly_sales.index, rolling_mean, label='30-Day Rolling Mean', color='green')
plt.fill_between(monthly_sales.index, rolling_mean - rolling_std, rolling_mean + rolling_std, color='lightgray', alpha=0.5, label='Rolling Std')
plt.scatter(peak_periods.index, peak_periods['total_amount'], color='red', label='Peak Sales Periods')
plt.title('Monthly Sales Trends with Peak Sales Periods')
plt.xlabel('Date')
plt.ylabel('Total Sales Amount')
plt.legend()
plt.xticks(rotation=45)
plt.show()

# Assuming df_sales contains sales data with a product category column and a sales amount column
# Group by product category and calculate total sales amount
category_sales = df_sales.groupby('product_id')['total_amount'].sum()

# Calculate average sales amount per category
average_sales = df_sales.groupby('product_id')['total_amount'].mean()

# Calculate number of transactions per category
transaction_count = df_sales.groupby('product_id').size()

# Visualize sales performance by product category
plt.figure(figsize=(12, 6))

# Bar chart for total sales amount
plt.subplot(3, 1, 1)
category_sales.plot(kind='bar', color='blue')
plt.title('Total Sales Amount by Product Category')
plt.xlabel('Product_id')
plt.ylabel('Total Sales Amount')

# Bar chart for average sales amount
plt.subplot(3, 1, 2)
average_sales.plot(kind='bar', color='green')
plt.title('Average Sales Amount by Product Category')
plt.xlabel('Product_id')
plt.ylabel('Average Sales Amount')

# Bar chart for number of transactions
plt.subplot(3, 1, 3)
transaction_count.plot(kind='bar', color='orange')
plt.title('Number of Transactions by Product Id')
plt.xlabel('Product_id')
plt.ylabel('Number of Transactions')

plt.tight_layout()
plt.show()

