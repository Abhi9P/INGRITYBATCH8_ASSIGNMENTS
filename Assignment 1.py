import pandas as pd
from collections import Counter


x = r"C:\Users\abhin\Downloads\dataset1.csv"

# Read the CSV file
df = pd.read_csv(x)

# Display the first few rows of the DataFrame
print("=== FIRST FEW ROWS OF THE DATA ===")
print(df.head())
print("\n" + "="*45)

## Category-Based Analysis:
print("\n=== CATEGORY-BASED ANALYSIS ===")

# 1. Find the category with the highest average rating
highest_avg_rating = df.groupby('Category')['Rating'].mean().idxmax()
print(f"\n1. Category with the Highest Average Rating: {highest_avg_rating}")

# 2. Find the total stock available for each category
total_stock_by_category = df.groupby('Category')['Stock'].sum().reset_index()
print("\n2. Total Stock Available for Each Category:")
print(total_stock_by_category)

# 3. Create a new column 'Final_Price'
df['Final_Price'] = df['Price'] - (df['Price'] * df['Discount'] / 100)

# 4. Compare 'Price', 'Discount', and 'Final_Price'
print("\n3. Comparison of 'Price', 'Discount', and 'Final_Price':")
print(df[['Product_ID', 'Price', 'Discount', 'Final_Price']].head())

# 5. Find the top 3 most discounted products (Simple Method)
top_discounted_products = df.sort_values('Discount', ascending=False).head(3)
print("\n4. Top 3 Most Discounted Products:")
print(top_discounted_products[['Product_ID', 'Category', 'Discount', 'Final_Price']])

print("\n" + "="*50)

## Supplier Analysis:
print("\n=== SUPPLIER ANALYSIS ===")

# 6. Find the supplier with the highest average price of products
highest_avg_price_supplier = df.groupby('Supplier')['Price'].mean().idxmax()
print(f"\n1. Supplier with the Highest Average Price of Products: {highest_avg_price_supplier}")

# 7. Find the total number of unique suppliers
unique_suppliers_count = df['Supplier'].nunique()
print(f"\n2. Total Number of Unique Suppliers: {unique_suppliers_count}")

print("\n" + "="*50)

## Collections Question - Using collections.Counter
print("\n=== CATEGORY COUNTS USING Counter ===")

# 8. Count the occurrences of each category
category_counts = Counter(df['Category'])
print("\n1. Category Counts:")
for category, count in category_counts.items():
    print(f"   {category}: {count}")

# 9. Find the most common category
most_common_category = category_counts.most_common(1)[0][0]
print(f"\n2. Most Common Category: {most_common_category}")


