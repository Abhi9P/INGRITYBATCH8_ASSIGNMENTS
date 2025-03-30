# Assignment 2 - Data Processing

import pandas as pd
import numpy as np
from collections import defaultdict, Counter
import itertools
from datetime import datetime, timedelta

# Read the CSV file
x = r'C:\Ingrity Training\python\day 1 new\dataset2.csv'
df = pd.read_csv(x)

# =========================================================
# 1. Advanced Data Cleaning (Pandas)
# =========================================================

# 1.1 Remove duplicate rows
print('Removing duplicate rows...')
df.drop_duplicates(keep='first', inplace=True)

# 1.2 Fix Product typos
product_map = {
    'ProdA': 'Product A',
    'Product A': 'Product A',
    'ProdB': 'Product B',
    'Product B': 'Product B'
}
df['Product'] = df['Product'].map(product_map).fillna(df['Product'])

# 1.3 Validate numericals
print('Validating numerical columns...')
df['Quantity'] = df['Quantity'].apply(lambda x: max(1, x))  # Replace negative Quantity with 1
df = df[df['Price'] >= 0]  # Drop rows with negative Price

# 1.4 Fix Category inconsistencies
category_map = {
    'Electronics': 'Electronics',
    'Electornics': 'Electronics',
    'Electronis': 'Electronics'
}
df['Category'] = df['Category'].map(category_map).fillna(df['Category'])

# 1.5 Impute missing Regions using most common region for each CustomerID
print('Imputing missing regions...')
customer_region = df.groupby('CustomerID')['Region'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan)
df['Region'] = df.apply(lambda row: customer_region[row['CustomerID']] if pd.isna(row['Region']) else row['Region'], axis=1)

# 1.6 Create IsPromo flag from PromoCode
print('Creating IsPromo flag...')
df['IsPromo'] = df['PromoCode'].apply(lambda x: 1 if pd.notna(x) else 0)

print('Data Cleaning Completed! Displaying first few rows...')
print(df.head())

# =========================================================
# 2. Complex DateTime Operations
# =========================================================

print('Converting OrderDate to UTC datetime...')
def convert_date(value):
    try:
        if isinstance(value, (float, int)):
            return datetime.utcfromtimestamp(value)
        else:
            return pd.to_datetime(value, dayfirst=True)  # Assume day-first format
    except Exception:
        return np.nan

df['OrderDate'] = df['OrderDate'].apply(convert_date)

# Drop rows with invalid OrderDate
df = df.dropna(subset=['OrderDate'])

# 2.2 Calculate order processing time (Returns happen 7 days after order)
df['ReturnDate'] = df['OrderDate'] + timedelta(days=7)

print('DateTime Processing Completed! Displaying first few rows...')
print(df.head())

# =========================================================
# 3. Advanced Collections & Optimization
# =========================================================

# 3.1 Nested dictionary - CustomerID: {'total_spent': X, 'favorite_category': Y}
print('Building nested dictionary for customers...')
customer_summary = {}
grouped = df.groupby('CustomerID')
for customer_id, group in grouped:
    total_spent = (group['Quantity'] * group['Price']).sum()
    favorite_category = group['Category'].mode().iloc[0] if not group['Category'].mode().empty else None
    customer_summary[customer_id] = {'total_spent': total_spent, 'favorite_category': favorite_category}

# 3.2 Track return rates by region
print('Tracking return rates by region...')
return_rates = defaultdict(float)
for region, group in df.groupby('Region'):
    total_orders = len(group)
    total_returns = group['ReturnFlag'].sum()
    return_rates[region] = total_returns / total_orders if total_orders > 0 else 0

# 3.3 Find most common promo code sequence
print('Finding most common promo code sequence...')
promo_sequences = df['PromoCode'].dropna().tolist()
common_sequence = Counter(itertools.pairwise(promo_sequences)).most_common(1)

# 3.4 Optimize memory usage by downcasting numericals
print('Optimizing memory usage...')
df['Quantity'] = pd.to_numeric(df['Quantity'], downcast='integer')
df['Price'] = pd.to_numeric(df['Price'], downcast='float')

df.info()

# =========================================================
# 4. Bonus Section
# =========================================================

# 4.1 UDF to flag "suspicious orders" (multiple returns + high value)
def flag_suspicious(row):
    if row['ReturnFlag'] == 1 and row['Price'] > 1000:
        return 1
    return 0

df['SuspiciousFlag'] = df.apply(flag_suspicious, axis=1)

# 4.2 Calculate rolling 7-day average sales
print('Calculating rolling 7-day average sales...')
df = df.sort_values(by='OrderDate')
df['Rolling7DaySales'] = df['Price'].rolling(window=7, min_periods=1).mean()

print('Final Data Processing Completed! Displaying first few rows...')
print(df.head())
