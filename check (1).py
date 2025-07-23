import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# List of all dataset file paths
dataset_paths = [
    'olist_customers_dataset.csv',
    'olist_geolocation_dataset.csv',
    'olist_order_items_dataset.csv',
    'olist_order_payments_dataset.csv',
    'olist_order_reviews_dataset.csv',
    'olist_orders_dataset.csv',
    'olist_products_dataset.csv',
    'olist_sellers_dataset.csv',
    'product_category_name_translation.csv'
]


# Function to check nulls and display summary
def check_nulls(file_path):
    """Analyze missing values in a dataset and display a summary."""
    try:
        # Load the dataset
        df = pd.read_csv(file_path)

        print(f"\n{'=' * 50}")
        print(f"DATASET: {file_path}")
        print(f"{'=' * 50}")
        print(f"Shape: {df.shape}")

        # Check for missing values
        null_counts = df.isnull().sum()
        null_percent = (df.isnull().sum() / len(df)) * 100

        # Create a summary dataframe for nulls
        null_info = pd.DataFrame({
            'Column': null_counts.index,
            'Missing Values': null_counts.values,
            'Missing Percentage': null_percent.values
        })

        # Sort by number of missing values in descending order
        null_info = null_info.sort_values('Missing Values', ascending=False)

        # Only show columns with missing values
        null_info_with_nulls = null_info[null_info['Missing Values'] > 0]

        if len(null_info_with_nulls) > 0:
            print("\nColumns with missing values:")
            print(null_info_with_nulls)
        else:
            print("\nNo missing values found in any column!")

        # Display dtypes
        print("\nData Types:")
        for col, dtype in df.dtypes.items():
            print(f"{col}: {dtype}")

        return null_info

    except Exception as e:
        print(f"Error analyzing {file_path}: {e}")
        return None


# Check nulls for all datasets
null_summaries = {}
for dataset in dataset_paths:
    null_summaries[dataset] = check_nulls(dataset)

# Create a consolidated summary of missing values across all datasets
print("\n\n")
print("=" * 80)
print("CONSOLIDATED SUMMARY OF MISSING VALUES")
print("=" * 80)

for dataset, null_info in null_summaries.items():
    if null_info is not None:
        missing_columns = null_info[null_info['Missing Values'] > 0]
        if len(missing_columns) > 0:
            print(f"\n{dataset}:")
            for _, row in missing_columns.iterrows():
                print(f"  - {row['Column']}: {row['Missing Values']} missing values ({row['Missing Percentage']:.2f}%)")