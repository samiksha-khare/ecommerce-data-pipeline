from src.transformation.dim_customer import build_dim_customer
from src.transformation.dim_product import build_dim_product
from src.transformation.dim_date import build_dim_date
from src.transformation.dim_shipping import build_dim_shipping
from src.transformation.dim_payment import build_dim_payment
from src.transformation.fact_orders import build_fact_orders


def test_dim_customer(spark):
    data = [
        ("C001", "John", "Doe", "john@test.com", "1234567890",
         "123 Main St", "NYC", "NY", "US", "Consumer", "2023-01-15"),
    ]
    cols = ["customer_id", "first_name", "last_name", "email", "phone",
            "address", "city", "state", "country", "segment", "signup_date"]
    df = spark.createDataFrame(data, cols)
    result = build_dim_customer(df)
    assert "customer_key" in result.columns
    assert result.count() == 1


def test_dim_product(spark):
    data = [("P001", "Widget", "Electronics", "Gadgets", "BrandA",
             99.99, 50.0, 1.5, "SupplierX")]
    cols = ["product_id", "product_name", "category", "sub_category",
            "brand", "unit_price", "cost_price", "weight", "supplier"]
    df = spark.createDataFrame(data, cols)
    result = build_dim_product(df)
    assert "product_key" in result.columns
    assert result.count() == 1


def test_dim_date(spark):
    data = [("ORD-001", "2023-01-01"), ("ORD-002", "2023-01-05")]
    df = spark.createDataFrame(data, ["order_id", "order_date"])
    result = build_dim_date(spark, df)
    assert result.count() == 5  # Jan 1-5
    assert "year" in result.columns
    assert "is_weekend" in result.columns


def test_dim_shipping(spark):
    data = [("S001", "ORD-001", "FedEx", "Standard", "TR123",
             "2023-01-02", "2023-01-05", 15.0, "Delivered")]
    cols = ["shipping_id", "order_id", "carrier", "shipping_mode",
            "tracking_number", "ship_date", "delivery_date", "shipping_cost", "status"]
    df = spark.createDataFrame(data, cols)
    result = build_dim_shipping(df)
    assert "shipping_key" in result.columns
    assert "delivery_days" in result.columns


def test_dim_payment(spark):
    data = [("PAY-001", "ORD-001", "Credit Card", "Visa", "4111111111111234",
             "Completed", "2023-01-01", "txn-001")]
    cols = ["payment_id", "order_id", "payment_method", "card_type",
            "card_number", "payment_status", "payment_date", "transaction_id"]
    df = spark.createDataFrame(data, cols)
    result = build_dim_payment(df)
    assert "payment_key" in result.columns


def test_fact_orders(spark):
    # Build minimal dimensions
    cust_data = [("C001", "John", "Doe", "j@t.com", "123", "addr",
                  "NYC", "NY", "US", "Consumer", "2023-01-01")]
    cust_cols = ["customer_id", "first_name", "last_name", "email", "phone",
                 "address", "city", "state", "country", "segment", "signup_date"]
    dim_c = build_dim_customer(spark.createDataFrame(cust_data, cust_cols))

    prod_data = [("P001", "Widget", "Electronics", "Gadgets", "BrandA",
                  99.99, 50.0, 1.5, "SupplierX")]
    prod_cols = ["product_id", "product_name", "category", "sub_category",
                 "brand", "unit_price", "cost_price", "weight", "supplier"]
    dim_p = build_dim_product(spark.createDataFrame(prod_data, prod_cols))

    order_data = [("ORD-001", "C001", "P001", "2023-01-01", 2, 99.99, 0.1, 0.08)]
    order_cols = ["order_id", "customer_id", "product_id", "order_date",
                  "quantity", "unit_price", "discount_pct", "tax_pct"]
    orders_df = spark.createDataFrame(order_data, order_cols)

    dim_d = build_dim_date(spark, orders_df)

    ship_data = [("S001", "ORD-001", "FedEx", "Standard", "TR123",
                  "2023-01-02", "2023-01-05", 15.0, "Delivered")]
    ship_cols = ["shipping_id", "order_id", "carrier", "shipping_mode",
                 "tracking_number", "ship_date", "delivery_date", "shipping_cost", "status"]
    dim_s = build_dim_shipping(spark.createDataFrame(ship_data, ship_cols))

    pay_data = [("PAY-001", "ORD-001", "Credit Card", "Visa", "4111111111111234",
                 "Completed", "2023-01-01", "txn-001")]
    pay_cols = ["payment_id", "order_id", "payment_method", "card_type",
                "card_number", "payment_status", "payment_date", "transaction_id"]
    dim_pay = build_dim_payment(spark.createDataFrame(pay_data, pay_cols))

    fact = build_fact_orders(orders_df, dim_c, dim_p, dim_d, dim_s, dim_pay)
    assert fact.count() == 1
    assert "net_amount" in fact.columns
    assert "customer_key" in fact.columns
