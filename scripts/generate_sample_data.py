"""Generate sample ecommerce data with intentional quality issues for testing."""

import os
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "sample")


def generate_customers(n=500):
    segments = ["Consumer", "Corporate", "Home Office"]
    customers = []
    for i in range(1, n + 1):
        customer = {
            "customer_id": f"CUST-{i:04d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "address": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "US",
            "segment": random.choice(segments),
            "signup_date": fake.date_between(
                start_date=datetime(2022, 1, 1),
                end_date=datetime(2024, 12, 31),
            ).isoformat(),
        }
        # Inject ~3% null emails
        if random.random() < 0.03:
            customer["email"] = None
        # Inject ~3% null phones
        if random.random() < 0.03:
            customer["phone"] = None
        customers.append(customer)
    return customers


def generate_products(n=100):
    categories = {
        "Electronics": ["Laptops", "Phones", "Tablets", "Headphones", "Cameras"],
        "Clothing": ["Shirts", "Pants", "Dresses", "Jackets", "Shoes"],
        "Home": ["Furniture", "Kitchenware", "Decor", "Bedding", "Lighting"],
        "Sports": ["Fitness", "Outdoor", "Team Sports", "Water Sports", "Cycling"],
        "Books": ["Fiction", "Non-Fiction", "Technical", "Children", "Comics"],
    }
    brands = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE",
              "BrandF", "BrandG", "BrandH", "BrandI", "BrandJ"]
    suppliers = ["SupplierX", "SupplierY", "SupplierZ", "SupplierW"]
    products = []
    for i in range(1, n + 1):
        category = random.choice(list(categories.keys()))
        sub_category = random.choice(categories[category])
        unit_price = round(random.uniform(10, 2000), 2)
        cost_price = round(unit_price * random.uniform(0.4, 0.8), 2)
        products.append({
            "product_id": f"PROD-{i:04d}",
            "product_name": f"{fake.word().capitalize()} {sub_category}",
            "category": category,
            "sub_category": sub_category,
            "brand": random.choice(brands),
            "unit_price": unit_price,
            "cost_price": cost_price,
            "weight": round(random.uniform(0.1, 50.0), 2),
            "supplier": random.choice(suppliers),
        })
    return products


def generate_orders(n=5000, num_customers=500, num_products=100):
    orders = []
    for i in range(1, n + 1):
        order_date = fake.date_between(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 12, 31),
        )
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(10, 2000), 2)
        discount_pct = round(random.uniform(0, 0.3), 2)
        tax_pct = round(random.uniform(0.05, 0.15), 2)

        order = {
            "order_id": f"ORD-{i:05d}",
            "customer_id": f"CUST-{random.randint(1, num_customers):04d}",
            "product_id": f"PROD-{random.randint(1, num_products):04d}",
            "order_date": order_date.isoformat(),
            "quantity": quantity,
            "unit_price": unit_price,
            "discount_pct": discount_pct,
            "tax_pct": tax_pct,
        }

        # Inject ~1% negative quantities (quality issue)
        if random.random() < 0.01:
            order["quantity"] = -random.randint(1, 5)

        # Inject ~1% out-of-range discounts
        if random.random() < 0.01:
            order["discount_pct"] = round(random.uniform(1.1, 2.0), 2)

        orders.append(order)

    # Inject ~2% duplicate rows
    num_dupes = int(n * 0.02)
    for _ in range(num_dupes):
        orders.append(random.choice(orders).copy())

    random.shuffle(orders)
    return orders


def generate_payments(orders):
    methods = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer"]
    card_types = ["Visa", "MasterCard", "Amex", "Discover"]
    statuses = ["Completed", "Pending", "Failed"]
    payments = []
    seen_order_ids = set()

    for order in orders:
        oid = order["order_id"]
        if oid in seen_order_ids:
            continue
        seen_order_ids.add(oid)

        method = random.choice(methods)
        order_date = datetime.fromisoformat(order["order_date"])
        payment_date = order_date + timedelta(days=random.randint(0, 2))

        payments.append({
            "payment_id": f"PAY-{len(payments) + 1:05d}",
            "order_id": oid,
            "payment_method": method,
            "card_type": random.choice(card_types) if "Card" in method else None,
            "card_number": fake.credit_card_number() if "Card" in method else None,
            "payment_status": random.choices(statuses, weights=[85, 10, 5])[0],
            "payment_date": payment_date.isoformat(),
            "transaction_id": fake.uuid4(),
        })
    return payments


def generate_shipping(orders):
    carriers = ["FedEx", "UPS", "USPS", "DHL"]
    modes = ["Standard", "Express", "Same Day"]
    statuses = ["Delivered", "In Transit", "Returned"]
    shipments = []
    seen_order_ids = set()

    for order in orders:
        oid = order["order_id"]
        if oid in seen_order_ids:
            continue
        seen_order_ids.add(oid)

        order_date = datetime.fromisoformat(order["order_date"])
        ship_date = order_date + timedelta(days=random.randint(0, 3))
        mode = random.choice(modes)
        if mode == "Same Day":
            delivery_days = 1
        elif mode == "Express":
            delivery_days = random.randint(1, 3)
        else:
            delivery_days = random.randint(3, 14)
        delivery_date = ship_date + timedelta(days=delivery_days)

        shipments.append({
            "shipping_id": f"SHIP-{len(shipments) + 1:05d}",
            "order_id": oid,
            "carrier": random.choice(carriers),
            "shipping_mode": mode,
            "tracking_number": fake.bothify(text="??########"),
            "ship_date": ship_date.isoformat(),
            "delivery_date": delivery_date.isoformat(),
            "shipping_cost": round(random.uniform(3.0, 50.0), 2),
            "status": random.choices(statuses, weights=[80, 12, 8])[0],
        })
    return shipments


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("Generating sample ecommerce data...")

    # Customers -> JSON
    customers = generate_customers()
    pd.DataFrame(customers).to_json(
        os.path.join(OUTPUT_DIR, "customers.json"), orient="records", lines=True
    )
    print(f"  customers.json: {len(customers)} records")

    # Products -> CSV
    products = generate_products()
    pd.DataFrame(products).to_csv(
        os.path.join(OUTPUT_DIR, "products.csv"), index=False
    )
    print(f"  products.csv: {len(products)} records")

    # Orders -> CSV
    orders = generate_orders()
    pd.DataFrame(orders).to_csv(
        os.path.join(OUTPUT_DIR, "orders.csv"), index=False
    )
    print(f"  orders.csv: {len(orders)} records (includes dupes)")

    # Payments -> Parquet
    payments = generate_payments(orders)
    pd.DataFrame(payments).to_parquet(
        os.path.join(OUTPUT_DIR, "payments.parquet"), index=False
    )
    print(f"  payments.parquet: {len(payments)} records")

    # Shipping -> JSON
    shipping = generate_shipping(orders)
    pd.DataFrame(shipping).to_json(
        os.path.join(OUTPUT_DIR, "shipping.json"), orient="records", lines=True
    )
    print(f"  shipping.json: {len(shipping)} records")

    print("Sample data generation complete!")


if __name__ == "__main__":
    main()
