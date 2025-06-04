import pandas as pd
import numpy as np

print("Loading cleaned datasets...")
# Load the partially cleaned datasets
orders = pd.read_csv('olist_orders_dataset_cleaned.csv')
reviews = pd.read_csv('olist_order_reviews_dataset_cleaned.csv')
products = pd.read_csv('olist_products_dataset_cleaned.csv')

print("Fixing product dataset remaining nulls...")
# Fix remaining nulls in products dataset
for col in ['product_name_lenght', 'product_description_lenght', 'product_photos_qty']:
    median_value = products[col].median()
    print(f"Filling {col} nulls with median value: {median_value}")
    products[col] = products[col].fillna(median_value)

print("Handling date fields in orders dataset...")
# Convert date strings to proper datetime objects
date_columns = [
    'order_purchase_timestamp', 'order_approved_at',
    'order_delivered_carrier_date', 'order_delivered_customer_date',
    'order_estimated_delivery_date'
]

for col in date_columns:
    orders[col] = pd.to_datetime(orders[col], errors='coerce')

print("Handling time-based calculations more robustly...")
# More robust time-based calculations with null handling
# Only calculate processing time for approved orders
approved_mask = orders['order_approved_at'].notnull()
orders.loc[approved_mask, 'processing_time_hours'] = (
    (orders.loc[approved_mask, 'order_approved_at'] -
     orders.loc[approved_mask, 'order_purchase_timestamp']).dt.total_seconds() / 3600
)

# Only calculate shipping time for delivered orders
delivered_mask = (orders['order_delivered_carrier_date'].notnull() &
                  orders['order_delivered_customer_date'].notnull())
orders.loc[delivered_mask, 'shipping_time_days'] = (
    (orders.loc[delivered_mask, 'order_delivered_customer_date'] -
     orders.loc[delivered_mask, 'order_delivered_carrier_date']).dt.total_seconds() / (3600*24)
)

# Only calculate delivery delay for delivered orders
orders.loc[orders['order_delivered_customer_date'].notnull(), 'delivery_delay_days'] = (
    (orders.loc[orders['order_delivered_customer_date'].notnull(), 'order_delivered_customer_date'] -
     orders.loc[orders['order_delivered_customer_date'].notnull(), 'order_estimated_delivery_date']).dt.total_seconds() / (3600*24)
)

# Fill remaining nulls in calculated columns with appropriate values
orders['processing_time_hours'] = orders['processing_time_hours'].fillna(-1)  # -1 indicates not processed yet
orders['shipping_time_days'] = orders['shipping_time_days'].fillna(-1)  # -1 indicates not shipped/delivered yet
orders['delivery_delay_days'] = orders['delivery_delay_days'].fillna(0)  # 0 indicates no delay calculated yet

# Update late delivery flag
orders['is_late_delivery'] = orders['delivery_delay_days'] > 0

print("Creating additional useful features...")
# Add order status categories for better filtering in Tableau
# Create a simplified status field
def simplify_status(status):
    if status in ['delivered']:
        return 'Completed'
    elif status in ['shipped', 'processing']:
        return 'In Progress'
    elif status in ['canceled', 'unavailable']:
        return 'Canceled'
    else:
        return 'Other'

orders['status_category'] = orders['order_status'].apply(simplify_status)

# Add delivery time field (total days from purchase to delivery)
delivered_orders_mask = orders['order_delivered_customer_date'].notnull()
orders.loc[delivered_orders_mask, 'total_delivery_time_days'] = (
    (orders.loc[delivered_orders_mask, 'order_delivered_customer_date'] -
     orders.loc[delivered_orders_mask, 'order_purchase_timestamp']).dt.total_seconds() / (3600*24)
)
orders['total_delivery_time_days'] = orders['total_delivery_time_days'].fillna(-1)

print("Enhancing the reviews dataset...")
# Replace nulls in review text fields with empty strings (better for text analysis)
reviews['review_comment_title'] = reviews['review_comment_title'].fillna('')
reviews['review_comment_message'] = reviews['review_comment_message'].fillna('')

# Add a review score category for easier visualization
def categorize_review(score):
    if score <= 2:
        return 'Negative'
    elif score == 3:
        return 'Neutral'
    else:  # 4 or 5
        return 'Positive'

reviews['review_category'] = reviews['review_score'].apply(categorize_review)

print("Saving fully cleaned datasets...")
# Save the updated files
orders.to_csv('olist_orders_dataset_final.csv', index=False)
reviews.to_csv('olist_order_reviews_dataset_final.csv', index=False)
products.to_csv('olist_products_dataset_final.csv', index=False)

print("Data cleaning complete!")