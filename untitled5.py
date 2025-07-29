# -*- coding: utf-8 -*-
"""
Supermarket Sales ETL Data Pipeline
Created on July 29, 2025
Author: Sharu
"""
# EXTRACT STAGE
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from datetime import datetime

# Set paths
input_path = r"C:\\Users\\user\\Downloads\\archive (8)\\supermarket_sales.csv"
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

# Load data
df = pd.read_csv(input_path)
print("Data Loaded Successfully")
print(df.head(5))
# TRANSFORM STAGE
# Cleaning column names
df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
# Save cleaned data
today = datetime.today().strftime('%Y-%m-%d')
cleaned_csv_path = f"{output_dir}/cleaned_sales.csv"
df.to_csv(cleaned_csv_path, index=False)
print(f"Cleaned data saved to {cleaned_csv_path}")


# -----------------------------------
#  LOAD STAGE – Analysis + Visuals
# -----------------------------------

# Which gender is more profitable?
males = df[df['gender'] == 'Male']['gross_income'].sum()
females = df[df['gender'] == 'Female']['gross_income'].sum()
gross = [males, females]

# Plot pie chart
plt.figure(figsize=(7, 7))
plt.pie(gross, labels=['Male','Female'], colors=['yellow','green'], startangle=90)
plt.title("Gross Income by Gender")
plt.axis('equal')
plt.savefig(f"{output_dir}/gender_income_pie.png")
plt.close()

# Profitable branch
branch_income = df.groupby('branch')['gross_income'].sum()
print("--- Most Profitable Branch ---")
print(branch_income.idxmax(), "=", branch_income.max())

# Profitable product line
product_income = df.groupby('product_line')['gross_income'].sum()
print("--- Most Profitable Product Line ---")
print(product_income.idxmax(), "=", product_income.max())

# High rating by customer type
print("--- Average Customer Rating by Type ---")
print(df.groupby('customer_type')['rating'].mean())

# Lowest performing branch and tax paid
lowest_branch = branch_income.idxmin()
lowest_branch_tax = df[df['branch'] == lowest_branch]['tax_5%'].sum()
print(f"Lowest Branch: {lowest_branch}, Tax Paid: {lowest_branch_tax}")

# Bar chart - Product income
product_income.plot(kind='bar', color='teal', figsize=(8,5))
plt.title("Gross Income by Product Line")
plt.ylabel("Gross Income")
plt.xticks(rotation=45)
plt.grid(axis='y')
plt.tight_layout()
plt.savefig(f"{output_dir}/product_income_bar.png")
plt.close()

# Bar chart - Customer rating
df.groupby('customer_type')['rating'].mean().plot(kind='bar', color='purple')
plt.title("Average Rating by Customer Type")
plt.ylabel("Rating")
plt.ylim(0, 10)
plt.grid(axis='y')
plt.savefig(f"{output_dir}/customer_rating_bar.png")
plt.close()

print("All graphs and outputs saved to 'output' folder.")
