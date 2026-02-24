import pandas as pd
import random
import numpy as np
from faker import Faker
from datetime import datetime, timedelta

# Initialisierung
fake = Faker()

# ---------------------------------------------------------
# 1. Generierung der Kundendaten (Users Table)
# ---------------------------------------------------------
countries = {
    "North America": ["USA", "Canada", "Mexico"],
    "Europe": ["Germany", "France", "UK", "Italy", "Spain"],
    "Asia": ["China", "India", "Japan", "South Korea"],
    "Australia/Oceania": ["Australia", "New Zealand"],
}

num_users = 5000
users_data = []

for user_id in range(1001, 1001 + num_users):
    first_name = fake.first_name()
    last_name = fake.last_name()
    gender = random.choice(["Male", "Female", "Non-binary"])
    age = random.randint(18, 75)
    country = random.choice(list(countries.keys()))
    country_name = random.choice(countries[country])
    email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(['gmail.com', 'yahoo.com', 'outlook.com'])}"
    signup_date = f"{random.randint(2018, 2024)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
    total_spent = round(random.uniform(10, 5000), 2)
    is_premium_member = random.choices([True, False], weights=[30, 70])[0]
    
    users_data.append([user_id, first_name, last_name, gender, age, country_name, email, signup_date, total_spent, is_premium_member])

users_df = pd.DataFrame(users_data, columns=["User_ID", "First_Name", "Last_Name", "Gender", "Age", "Country", "Email", "Signup_Date", "Total_Spent", "Premium_Member"])

# ---------------------------------------------------------
# 2. Generierung der Produktdaten (Products Table)
# ---------------------------------------------------------
product_categories = {
    "Tops": ["T-Shirt", "Hoodie"],
    "Bottoms": ["Jeans", "Shorts"],
    "Outerwear": ["Jacket", "Coat"],
    "Footwear": ["Sneakers", "Boots"],
    "Accessories": ["Hat", "Sunglasses"]
}
brands = ["Nike", "Adidas", "Zara", "Gucci", "Prada"]
materials = ["Cotton", "Leather", "Denim"]

num_products = 1000
products_data = []

for product_id in range(2001, 2001 + num_products):
    category = random.choice(list(product_categories.keys()))
    product_name = f"{random.choice(['Red', 'Blue', 'Black'])} {random.choice(product_categories[category])}"
    brand = random.choice(brands)
    price = round(random.uniform(10, 1000), 2)
    stock = random.randint(0, 500)
    discount = random.choices([0, round(random.uniform(5, 50), 2)], weights=[60, 40])[0]
    final_price = round(price * (1 - discount/100), 2)
    is_luxury = brand in ["Gucci", "Prada"]
    
    products_data.append([product_id, product_name, category, brand, price, stock, discount, final_price, is_luxury])

products_df = pd.DataFrame(products_data, columns=["Product_ID", "Product_Name", "Category", "Brand", "Price", "Stock", "Discount", "Final_Price", "Luxury_Brand"])

# ---------------------------------------------------------
# 3. Generierung der Bestelldaten (Orders Table)
# ---------------------------------------------------------
num_orders = 8000
order_statuses = ["Completed", "Pending", "Cancelled", "Returned"]
status_weights = [80, 10, 5, 5] 
start_date = datetime(2019, 1, 1)
end_date = datetime(2024, 12, 31)

orders_data = []
order_ids = range(3001, 3001 + num_orders)

for order_id in order_ids:
    user_id = random.choice(users_df["User_ID"].tolist())
    product_id = random.choice(products_df["Product_ID"].tolist())
    quantity = random.randint(1, 5)
    
    product_row = products_df[products_df["Product_ID"] == product_id].iloc[0]
    price_per_unit = product_row["Final_Price"]
    total_price = round(price_per_unit * quantity, 2)
    
    order_status = random.choices(order_statuses, weights=status_weights)[0]
    is_returned = order_status == "Returned"
    returns = quantity if is_returned else 0
    return_rate = round(returns / quantity, 2) if is_returned else 0.0
    
    conversion_rate = round(random.uniform(0.2, 0.9), 2) if price_per_unit < 200 else round(random.uniform(0.1, 0.7), 2)
    
    random_days = random.randint(0, (end_date - start_date).days)
    order_date = start_date + timedelta(days=random_days)
    timestamp = order_date.strftime("%Y-%m-%d %H:%M:%S")
    
    orders_data.append([order_id, user_id, product_id, quantity, total_price, order_status, conversion_rate, returns, return_rate, timestamp])

orders_df = pd.DataFrame(orders_data, columns=["Order_ID", "User_ID", "Product_ID", "Quantity", "Total_Price", "Order_Status", "Conversion_Rate", "Returns", "Return_Rate", "Timestamp"])

# Datenexport als CSV
# users_df.to_csv('users.csv', index=False)
# products_df.to_csv('products.csv', index=False)
# orders_df.to_csv('orders.csv', index=False)
print("Data generation complete.")