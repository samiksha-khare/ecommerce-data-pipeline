# Ecommerce Data Pipeline — Architecture & Data Flow

---

## End-to-End Pipeline Flow

```
  ┌─────────────────────────────────────────────────────────────────────────┐
  │                        RAW DATA SOURCES                                 │
  │                                                                         │
  │   orders.csv        customers.json    products.csv                      │
  │   (5100 rows)       (500 rows)        (100 rows)                        │
  │   payments.parquet  shipping.json                                       │
  │   (5000 rows)       (5000 rows)                                         │
  └──────────────────────────────┬──────────────────────────────────────────┘
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  STAGE 1 — INGESTION                                                    │
  │                                                                         │
  │         ReaderFactory.create(format) ──► BaseReader (ABC)               │
  │                                                                         │
  │      ┌──────────┐       ┌──────────┐       ┌───────────────┐           │
  │      │CSVReader │       │JSONReader│       │ParquetReader  │           │
  │      └──────────┘       └──────────┘       └───────────────┘           │
  │                   Raw Spark DataFrames                                  │
  └──────────────────────────────┬──────────────────────────────────────────┘
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  STAGE 2 — DATA QUALITY                                                 │
  │                                                                         │
  │    QualityCheckRegistry ──► QualityEngine  (config-driven, sequential) │
  │                                                                         │
  │   ┌──────────────────┐  ┌──────────┐  ┌───────────┐  ┌─────────────┐  │
  │   │DeduplicationCheck│  │NullCheck │  │RangeCheck │  │NonNegative  │  │
  │   └──────────────────┘  └──────────┘  └───────────┘  │Check        │  │
  │                                                        └─────────────┘  │
  │   orders:  5100 → 4875  (100 dupes + 66 negatives + 59 out-of-range)   │
  │                   Cleaned Spark DataFrames                              │
  └──────────────────────────────┬──────────────────────────────────────────┘
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  STAGE 3 — STAR SCHEMA TRANSFORMATION                                   │
  │                                                                         │
  │                    StarSchemaBuilder.build()                            │
  │                                                                         │
  │  ┌─────────────┐  ┌─────────────┐  ┌────────────────────────────────┐  │
  │  │dim_customer │  │dim_product  │  │dim_date                        │  │
  │  │─────────────│  │─────────────│  │────────────────────────────────│  │
  │  │customer_key │  │product_key  │  │ Generated from order date range│  │
  │  │customer_id  │  │product_id   │  │ year, quarter, month,          │  │
  │  │first_name   │  │product_name │  │ week_of_year, day_of_week,     │  │
  │  │last_name    │  │category     │  │ day_name, is_weekend           │  │
  │  │email  ████  │  │sub_category │  └────────────────────────────────┘  │
  │  │phone  ████  │  │brand        │                                      │
  │  │address████  │  │unit_price   │  ┌──────────────┐  ┌─────────────┐  │
  │  │city, state  │  │cost_price   │  │dim_shipping  │  │dim_payment  │  │
  │  │segment      │  │weight       │  │──────────────│  │─────────────│  │
  │  │age_group    │  │supplier     │  │shipping_key  │  │payment_key  │  │
  │  └─────────────┘  └─────────────┘  │carrier       │  │pay_method   │  │
  │                                    │shipping_mode │  │card_type    │  │
  │  ████ = PII field (masked later)   │delivery_days │  │card_num ████│  │
  │                                    │shipping_cost │  │pay_status   │  │
  │                                    └──────────────┘  └─────────────┘  │
  │                                                                         │
  │  ┌─────────────────────────────────────────────────────────────────┐   │
  │  │                        fact_orders  (4875 rows)                 │   │
  │  │─────────────────────────────────────────────────────────────────│   │
  │  │  customer_key FK   product_key FK   date_key FK                 │   │
  │  │  shipping_key FK   payment_key FK                               │   │
  │  │  Measures: quantity, unit_price, total_amount,                  │   │
  │  │            discount_amount, tax_amount, net_amount              │   │
  │  │  Grain: one row per order line item                             │   │
  │  └─────────────────────────────────────────────────────────────────┘   │
  └──────────────────────────────┬──────────────────────────────────────────┘
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  STAGE 4 — PII MASKING                                                  │
  │                                                                         │
  │       PIIMasker  ◄── pii_masking_config.yaml  ◄── MaskingStrategyFactory│
  │                                                                         │
  │  ┌──────────────────┬──────────────┬──────────────────────────────────┐ │
  │  │ Table            │ Column       │ Strategy → Result                │ │
  │  ├──────────────────┼──────────────┼──────────────────────────────────┤ │
  │  │ dim_customer     │ email        │ PartialEmailMask  "sa****@g.com" │ │
  │  │ dim_customer     │ phone        │ PartialPhoneMask  "****5678"     │ │
  │  │ dim_customer     │ address      │ RedactMask        "[REDACTED]"   │ │
  │  │ dim_payment      │ card_number  │ HashPartialMask   "****4321"     │ │
  │  └──────────────────┴──────────────┴──────────────────────────────────┘ │
  └──────────────┬──────────────────────────────────┬────────────────────────┘
                 │                                  │
                 ▼                                  ▼
  ┌──────────────────────────────┐  ┌──────────────────────────────────────┐
  │  STAGE 5 — WAREHOUSE WRITE   │  │  STAGE 6 & 7 — KPI COMPUTE & WRITE  │
  │                              │  │                                      │
  │  output/warehouse/ (parquet) │  │  KPIEngine → BaseKPI subclasses      │
  │  ├── dim_customer/           │  │                                      │
  │  ├── dim_product/            │  │  REVENUE KPIs (6)                    │
  │  ├── dim_date/               │  │  ├─ total_revenue                    │
  │  ├── dim_shipping/           │  │  ├─ revenue_by_month                 │
  │  ├── dim_payment/            │  │  ├─ revenue_growth  (MoM %)          │
  │  └── fact_orders/            │  │  ├─ revenue_by_category              │
  │                              │  │  ├─ revenue_by_segment               │
  └──────────────────────────────┘  │  └─ aov  (avg order value)          │
                                    │                                      │
                                    │  CUSTOMER KPIs (5)                   │
                                    │  ├─ customer_lifetime_value          │
                                    │  ├─ customer_acquisition             │
                                    │  ├─ churn_rate  (90-day window)      │
                                    │  ├─ repeat_purchase_rate             │
                                    │  └─ avg_orders_per_customer          │
                                    │                                      │
                                    │  PRODUCT KPIs (5)                    │
                                    │  ├─ top_products_by_revenue          │
                                    │  ├─ top_products_by_quantity         │
                                    │  ├─ product_profit_margin            │
                                    │  ├─ category_contribution            │
                                    │  └─ avg_discount_by_category         │
                                    │                                      │
                                    │  output/kpis/  (16 parquet tables)   │
                                    └──────────────────────────────────────┘
```

---

## Data Flow — Field Level

```
  orders.csv           customers.json        products.csv
  ──────────           ──────────────        ────────────
  order_id       ─┐   customer_id  ──────►  product_id  ────────────────┐
  customer_id  ───┼─►  first_name            product_name               │
  product_id   ─┐ │   last_name              category                   │
  order_date   ─┼─┼─►  email    ──► MASK     sub_category               │
  quantity       │ │   phone    ──► MASK     brand                      │
  unit_price     │ │   address  ──► REDACT   unit_price                 │
  discount_pct   │ │   city, state           cost_price                 │
  tax_pct        │ │   segment               weight, supplier           │
                 │ │                                                     │
  payments.parquet│ └──────────────► dim_customer                       │
  ────────────── │                        │                             │
  payment_id     │                        │     dim_product ◄───────────┘
  order_id  ─────┼──────────────────────┐ │         │
  pay_method     │                      │ │         │
  card_type      │                      │ │         │
  card_num ──► MASK                     │ │    dim_date (generated from
  pay_status     │                      │ │    min/max order date range)
  pay_date  ─────┼──────────────────────┼─┼─────────┤
  transaction_id │                      │ │         │
                 │       ┌──────────────┼─┼─────────┼───── dim_payment
  shipping.json  │       │              │ │         │
  ─────────────  │       │              │ │    dim_shipping ◄────────────┐
  shipping_id    │       │              │ │         │                    │
  order_id  ─────┼───────┼──────────────┼─┼─────────┼────────────────────┘
  carrier        │       │              │ │         │
  ship_date      │       │              ▼ ▼         ▼
  delivery_date  │       │    ┌─────────────────────────────────────┐
  shipping_cost  │       │    │            fact_orders              │
  status ────────┘       │    │─────────────────────────────────────│
                         │    │  customer_key  product_key          │
                         └───►│  date_key      shipping_key         │
                              │  payment_key                        │
                              │  quantity      unit_price           │
                              │  total_amount  discount_amount      │
                              │  tax_amount    net_amount           │
                              └──────────────────┬──────────────────┘
                                                 │
                           ┌─────────────────────┼─────────────────────┐
                           ▼                     ▼                     ▼
                    RevenueKPIs           CustomerKPIs          ProductKPIs
                    ───────────           ────────────          ───────────
                    joins dim_date        joins dim_customer     joins dim_product
                    joins dim_product     joins dim_date
                    joins dim_customer
```

---

## Component Architecture

```
  src/main.py — PipelineOrchestrator
  │  Sequences all stages. Depends only on abstractions.
  │
  ├── src/utils/config_loader.py     Reads YAML configs
  ├── src/utils/logger.py            Centralized logging
  ├── src/spark_session.py           Singleton SparkSession (one per process)
  │
  ├── src/ingestion/
  │   ├── base_reader.py             ABC: read(spark, path) → DataFrame
  │   ├── csv_reader.py              CSV with header + inferSchema
  │   ├── json_reader.py             JSON (line-delimited)
  │   ├── parquet_reader.py          Parquet
  │   └── reader_factory.py          Factory: "csv"/"json"/"parquet" → reader
  │
  ├── src/quality/
  │   ├── base_check.py              ABC: apply(df) → df
  │   ├── deduplication_check.py     dropDuplicates on configurable subset
  │   ├── null_check.py              dropna on configurable columns
  │   ├── range_check.py             filter column between min and max
  │   ├── non_negative_check.py      filter all listed columns >= 0
  │   ├── check_registry.py          Factory + registry for checks
  │   └── quality_engine.py          Runs checks sequentially, logs counts
  │
  ├── src/transformation/
  │   ├── dim_customer.py            Adds customer_key, age_group
  │   ├── dim_product.py             Adds product_key
  │   ├── dim_date.py                Generates full date spine from order range
  │   ├── dim_shipping.py            Adds shipping_key, computes delivery_days
  │   ├── dim_payment.py             Adds payment_key
  │   ├── fact_orders.py             Joins all dims, computes net_amount
  │   └── star_schema_builder.py     Orchestrates all dim/fact builds
  │
  ├── src/privacy/
  │   ├── masking_strategy.py        ABC + 4 strategies + MaskingStrategyFactory
  │   └── pii_masker.py              Config-driven: applies strategies per table/col
  │
  └── src/kpi/
      ├── base_kpi.py                ABC: compute(tables) → dict[name, DataFrame]
      ├── revenue_kpis.py            6 revenue KPIs
      ├── customer_kpis.py           5 customer KPIs
      ├── product_kpis.py            5 product KPIs
      └── kpi_engine.py              Iterates all BaseKPI subclasses
```

---

## Star Schema

```
                          ┌──────────────┐
                          │   dim_date   │
                          │──────────────│
                          │ date_key  PK │
                          │ full_date    │
                          │ year         │
                          │ quarter      │
                          │ month        │
                          │ month_name   │
                          │ week_of_year │
                          │ day_name     │
                          │ is_weekend   │
                          └──────┬───────┘
                                 │
  ┌──────────────┐       ┌───────┴──────────────┐       ┌─────────────┐
  │ dim_customer │       │      fact_orders      │       │ dim_product │
  │──────────────│       │───────────────────────│       │─────────────│
  │ customer_key │◄──────│ customer_key  FK      │──────►│ product_key │
  │ customer_id  │       │ product_key   FK      │       │ product_id  │
  │ first_name   │       │ date_key      FK      │       │ product_name│
  │ last_name    │       │ shipping_key  FK      │       │ category    │
  │ email  ████  │       │ payment_key   FK      │       │ sub_category│
  │ phone  ████  │       │ order_id              │       │ brand       │
  │ address████  │       │ quantity              │       │ unit_price  │
  │ city, state  │       │ unit_price            │       │ cost_price  │
  │ country      │       │ total_amount          │       │ weight      │
  │ segment      │       │ discount_amount       │       │ supplier    │
  │ signup_date  │       │ tax_amount            │       └─────────────┘
  │ age_group    │       │ net_amount            │
  └──────────────┘       └──────────┬────────────┘
   ████ = PII masked                │
                         ┌──────────┴──────────┐
                         │                     │
                  ┌──────┴────────┐    ┌───────┴───────┐
                  │ dim_shipping  │    │  dim_payment   │
                  │───────────────│    │───────────────│
                  │ shipping_key  │    │ payment_key   │
                  │ carrier       │    │ payment_method│
                  │ shipping_mode │    │ card_type     │
                  │ tracking_num  │    │ card_num ████ │
                  │ ship_date     │    │ pay_status    │
                  │ delivery_date │    │ pay_date      │
                  │ delivery_days │    │ transaction_id│
                  │ shipping_cost │    └───────────────┘
                  │ status        │
                  └───────────────┘
```

---

## Design Patterns

```
  ┌──────────────────────────────────────────────────────────────────────┐
  │  FACTORY                 STRATEGY               SINGLETON            │
  │  ───────                 ────────               ─────────            │
  │  ReaderFactory           QualityEngine runs     SparkSessionManager  │
  │  QualityCheckRegistry    pluggable checks as    ensures one Spark    │
  │  MaskingStrategyFactory  interchangeable        session per process  │
  │                          strategies                                  │
  └──────────────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────────────┐
  │  SOLID                                                               │
  │  S  Each file has one job (csv_reader reads, null_check filters)     │
  │  O  Add reader/check/KPI: new file + one registry line, no edits    │
  │  L  All readers/checks/KPIs swap via their abstract base            │
  │  I  BaseReader.read() | BaseQualityCheck.apply() | BaseKPI.compute() │
  │  D  Orchestrator depends on abstractions; factories wire concretes   │
  └──────────────────────────────────────────────────────────────────────┘
```
