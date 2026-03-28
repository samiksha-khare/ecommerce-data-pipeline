from src.transformation.dim_customer import build_dim_customer
from src.transformation.dim_product import build_dim_product
from src.transformation.dim_date import build_dim_date
from src.transformation.dim_shipping import build_dim_shipping
from src.transformation.dim_payment import build_dim_payment
from src.transformation.fact_orders import build_fact_orders
from src.kpi.revenue_kpis import RevenueKPIs
from src.kpi.customer_kpis import CustomerKPIs
from src.kpi.product_kpis import ProductKPIs


def _build_test_tables(spark):
    """Build a minimal star schema for KPI testing."""
    cust_data = [
        ("C001", "John", "Doe", "j@t.com", "123", "addr", "NYC", "NY", "US",
         "Consumer", "2023-01-01"),
        ("C002", "Jane", "Doe", "x@t.com", "456", "addr2", "LA", "CA", "US",
         "Corporate", "2023-06-15"),
    ]
    cust_cols = ["customer_id", "first_name", "last_name", "email", "phone",
                 "address", "city", "state", "country", "segment", "signup_date"]
    dim_c = build_dim_customer(spark.createDataFrame(cust_data, cust_cols))

    prod_data = [
        ("P001", "Widget", "Electronics", "Gadgets", "BrandA", 100.0, 50.0, 1.5, "SupX"),
        ("P002", "Shirt", "Clothing", "Tops", "BrandB", 40.0, 20.0, 0.3, "SupY"),
    ]
    prod_cols = ["product_id", "product_name", "category", "sub_category",
                 "brand", "unit_price", "cost_price", "weight", "supplier"]
    dim_p = build_dim_product(spark.createDataFrame(prod_data, prod_cols))

    order_data = [
        ("ORD-001", "C001", "P001", "2023-01-10", 2, 100.0, 0.1, 0.08),
        ("ORD-002", "C001", "P002", "2023-01-15", 1, 40.0, 0.0, 0.08),
        ("ORD-003", "C002", "P001", "2023-02-01", 3, 100.0, 0.2, 0.10),
    ]
    order_cols = ["order_id", "customer_id", "product_id", "order_date",
                  "quantity", "unit_price", "discount_pct", "tax_pct"]
    orders_df = spark.createDataFrame(order_data, order_cols)
    dim_d = build_dim_date(spark, orders_df)

    ship_data = [
        ("S001", "ORD-001", "FedEx", "Standard", "TR1", "2023-01-11", "2023-01-14", 15.0, "Delivered"),
        ("S002", "ORD-002", "UPS", "Express", "TR2", "2023-01-16", "2023-01-17", 20.0, "Delivered"),
        ("S003", "ORD-003", "DHL", "Standard", "TR3", "2023-02-02", "2023-02-06", 12.0, "Delivered"),
    ]
    ship_cols = ["shipping_id", "order_id", "carrier", "shipping_mode",
                 "tracking_number", "ship_date", "delivery_date", "shipping_cost", "status"]
    dim_s = build_dim_shipping(spark.createDataFrame(ship_data, ship_cols))

    pay_data = [
        ("PAY-001", "ORD-001", "Credit Card", "Visa", "4111111111111234", "Completed", "2023-01-10", "t1"),
        ("PAY-002", "ORD-002", "PayPal", None, None, "Completed", "2023-01-15", "t2"),
        ("PAY-003", "ORD-003", "Debit Card", "MasterCard", "5500000000001111", "Completed", "2023-02-01", "t3"),
    ]
    pay_cols = ["payment_id", "order_id", "payment_method", "card_type",
                "card_number", "payment_status", "payment_date", "transaction_id"]
    dim_pay = build_dim_payment(spark.createDataFrame(pay_data, pay_cols))

    fact = build_fact_orders(orders_df, dim_c, dim_p, dim_d, dim_s, dim_pay)

    return {
        "dim_customer": dim_c,
        "dim_product": dim_p,
        "dim_date": dim_d,
        "dim_shipping": dim_s,
        "dim_payment": dim_pay,
        "fact_orders": fact,
    }


def test_revenue_kpis(spark):
    tables = _build_test_tables(spark)
    kpis = RevenueKPIs().compute(tables)
    assert "kpi_total_revenue" in kpis
    assert "kpi_revenue_by_month" in kpis
    assert "kpi_aov" in kpis
    total = kpis["kpi_total_revenue"].collect()[0]
    assert total.total_revenue > 0


def test_customer_kpis(spark):
    tables = _build_test_tables(spark)
    kpis = CustomerKPIs().compute(tables)
    assert "kpi_customer_lifetime_value" in kpis
    assert "kpi_churn_rate" in kpis
    assert "kpi_repeat_purchase_rate" in kpis


def test_product_kpis(spark):
    tables = _build_test_tables(spark)
    kpis = ProductKPIs().compute(tables)
    assert "kpi_top_products_by_revenue" in kpis
    assert "kpi_product_profit_margin" in kpis
    assert "kpi_category_contribution" in kpis
