# Ecommerce Data Pipeline — Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              PIPELINE ORCHESTRATOR (main.py)                            │
│                         PipelineOrchestrator.run() — sequences all stages               │
└──────────────────────────────────┬──────────────────────────────────────────────────────┘
                                   │
          ┌────────────────────────┼─────────────────────────┐
          ▼                        ▼                         ▼
  ┌───────────────┐      ┌─────────────────┐       ┌─────────────────┐
  │ ConfigLoader  │      │ SparkSession    │       │     Logger      │
  │ (YAML files)  │      │ (Singleton)     │       │  (centralized)  │
  └───────┬───────┘      └────────┬────────┘       └─────────────────┘
          │                       │
          ▼                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                         │
│  ╔═══════════════════════════════════════════════════════════════════════════════════╗   │
│  ║  STAGE 1: DATA INGESTION                                                        ║   │
│  ║                                                                                  ║   │
│  ║  ┌──────────────────────────────────────────────────────────┐                    ║   │
│  ║  │              ReaderFactory (Factory Pattern)             │                    ║   │
│  ║  │         ReaderFactory.create(format) → BaseReader        │                    ║   │
│  ║  └────┬──────────────┬──────────────┬───────────────────────┘                    ║   │
│  ║       │              │              │                                            ║   │
│  ║       ▼              ▼              ▼                                            ║   │
│  ║  ┌──────────┐  ┌──────────┐  ┌──────────────┐                                   ║   │
│  ║  │CSVReader │  │JSONReader│  │ParquetReader │    ◄── All extend BaseReader (ABC) ║   │
│  ║  └────┬─────┘  └────┬─────┘  └──────┬───────┘                                   ║   │
│  ║       │              │               │                                           ║   │
│  ║       ▼              ▼               ▼                                           ║   │
│  ║  ┌──────────┐  ┌──────────┐  ┌──────────────┐                                   ║   │
│  ║  │orders.csv│  │customers │  │payments      │                                    ║   │
│  ║  │products  │  │ .json    │  │ .parquet     │                                    ║   │
│  ║  │ .csv     │  │shipping  │  │              │                                    ║   │
│  ║  │          │  │ .json    │  │              │                                    ║   │
│  ║  └──────────┘  └──────────┘  └──────────────┘                                   ║   │
│  ╚═══════════════════════════════════════════════════════════════════════════════════╝   │
│                                        │                                                │
│                                        ▼                                                │
│  ╔═══════════════════════════════════════════════════════════════════════════════════╗   │
│  ║  STAGE 2: DATA QUALITY CHECKS                                                   ║   │
│  ║                                                                                  ║   │
│  ║  ┌──────────────────────────────────────────────────────────┐                    ║   │
│  ║  │         QualityCheckRegistry (Factory Pattern)           │                    ║   │
│  ║  │    QualityCheckRegistry.create(name) → BaseQualityCheck  │                    ║   │
│  ║  └──────────────────────┬───────────────────────────────────┘                    ║   │
│  ║                         │                                                        ║   │
│  ║           ┌─────────────┼─────────────┬──────────────┐                           ║   │
│  ║           ▼             ▼             ▼              ▼                            ║   │
│  ║  ┌──────────────┐ ┌──────────┐ ┌───────────┐ ┌──────────────┐                   ║   │
│  ║  │Deduplication │ │NullCheck │ │RangeCheck │ │NonNegative   │  ◄── All extend   ║   │
│  ║  │   Check      │ │          │ │           │ │   Check      │   BaseQualityCheck ║   │
│  ║  └──────────────┘ └──────────┘ └───────────┘ └──────────────┘      (ABC)        ║   │
│  ║                         │                                                        ║   │
│  ║                         ▼                                                        ║   │
│  ║           ┌─────────────────────────────────┐                                    ║   │
│  ║           │  QualityEngine                  │                                    ║   │
│  ║           │  (runs checks sequentially,     │                                    ║   │
│  ║           │   logs row counts before/after) │                                    ║   │
│  ║           └─────────────────────────────────┘                                    ║   │
│  ╚═══════════════════════════════════════════════════════════════════════════════════╝   │
│                                        │                                                │
│                                        ▼                                                │
│  ╔═══════════════════════════════════════════════════════════════════════════════════╗   │
│  ║  STAGE 3: STAR SCHEMA TRANSFORMATION                                            ║   │
│  ║                                                                                  ║   │
│  ║                  ┌─────────────────────────────┐                                 ║   │
│  ║                  │    StarSchemaBuilder         │                                 ║   │
│  ║                  │   (orchestrates all builds)  │                                 ║   │
│  ║                  └──────────────┬───────────────┘                                ║   │
│  ║                                 │                                                ║   │
│  ║       ┌──────────┬──────────┬───┴────┬────────────┐                              ║   │
│  ║       ▼          ▼          ▼        ▼            ▼                              ║   │
│  ║ ┌───────────┐┌────────┐┌────────┐┌──────────┐┌──────────┐                       ║   │
│  ║ │dim_       ││dim_    ││dim_    ││dim_      ││dim_      │                       ║   │
│  ║ │customer   ││product ││date    ││shipping  ││payment   │                       ║   │
│  ║ │(500 rows) ││(100)   ││(730)   ││(5000)    ││(5000)    │                       ║   │
│  ║ └─────┬─────┘└───┬────┘└───┬────┘└────┬─────┘└────┬─────┘                       ║   │
│  ║       │          │         │          │           │                              ║   │
│  ║       └──────────┴─────┬───┴──────────┴───────────┘                              ║   │
│  ║                        ▼                                                         ║   │
│  ║              ┌─────────────────────┐                                             ║   │
│  ║              │    fact_orders      │                                              ║   │
│  ║              │    (4875 rows)      │                                              ║   │
│  ║              │                     │                                              ║   │
│  ║              │  FK: customer_key   │                                              ║   │
│  ║              │  FK: product_key    │                                              ║   │
│  ║              │  FK: date_key       │                                              ║   │
│  ║              │  FK: shipping_key   │                                              ║   │
│  ║              │  FK: payment_key    │                                              ║   │
│  ║              │                     │                                              ║   │
│  ║              │  Measures:          │                                              ║   │
│  ║              │  quantity,          │                                              ║   │
│  ║              │  unit_price,        │                                              ║   │
│  ║              │  total_amount,      │                                              ║   │
│  ║              │  discount_amount,   │                                              ║   │
│  ║              │  tax_amount,        │                                              ║   │
│  ║              │  net_amount         │                                              ║   │
│  ║              └─────────────────────┘                                              ║   │
│  ╚═══════════════════════════════════════════════════════════════════════════════════╝   │
│                                        │                                                │
│                                        ▼                                                │
│  ╔═══════════════════════════════════════════════════════════════════════════════════╗   │
│  ║  STAGE 4: PII MASKING                                                            ║   │
│  ║                                                                                  ║   │
│  ║  ┌──────────────────────────────────────────────────────────────┐                ║   │
│  ║  │  PIIMasker (config-driven via pii_masking_config.yaml)      │                ║   │
│  ║  │                                                              │                ║   │
│  ║  │  MaskingStrategyFactory (Factory Pattern)                    │                ║   │
│  ║  └──────┬───────────────┬───────────────┬───────────────┬───────┘                ║   │
│  ║         ▼               ▼               ▼               ▼                        ║   │
│  ║  ┌─────────────┐ ┌─────────────┐ ┌───────────┐ ┌───────────────┐               ║   │
│  ║  │PartialEmail │ │PartialPhone│ │  Redact   │ │ HashPartial   │               ║   │
│  ║  │   Mask      │ │   Mask     │ │  Mask     │ │    Mask       │               ║   │
│  ║  │             │ │            │ │           │ │               │               ║   │
│  ║  │"sa****@    "│ │"****5678"  │ │"[REDACTED]│ │"****4321"    │               ║   │
│  ║  │  gmail.com" │ │            │ │           │ │               │               ║   │
│  ║  └─────────────┘ └─────────────┘ └───────────┘ └───────────────┘               ║   │
│  ║                                                                                  ║   │
│  ║  Applied to: dim_customer (email, phone, address)                               ║   │
│  ║               dim_payment  (card_number)                                         ║   │
│  ╚═══════════════════════════════════════════════════════════════════════════════════╝   │
│                                        │                                                │
│                          ┌─────────────┴─────────────┐                                  │
│                          ▼                           ▼                                   │
│  ╔══════════════════════════════════╗  ╔════════════════════════════════════════════╗    │
│  ║  STAGE 5: WRITE WAREHOUSE       ║  ║  STAGE 6 & 7: KPI COMPUTATION & WRITE     ║    │
│  ║                                  ║  ║                                            ║    │
│  ║  output/warehouse/               ║  ║  ┌────────────────────────────────────┐    ║    │
│  ║  ├── dim_customer/  (parquet)    ║  ║  │         KPIEngine                  │    ║    │
│  ║  ├── dim_product/               ║  ║  │  (iterates BaseKPI subclasses)     │    ║    │
│  ║  ├── dim_date/                  ║  ║  └────┬──────────┬──────────┬─────────┘    ║    │
│  ║  ├── dim_shipping/              ║  ║       ▼          ▼          ▼              ║    │
│  ║  ├── dim_payment/               ║  ║ ┌───────────┐┌────────┐┌──────────┐       ║    │
│  ║  └── fact_orders/               ║  ║ │ Revenue   ││Customer││ Product  │       ║    │
│  ║                                  ║  ║ │  KPIs    ││  KPIs  ││  KPIs   │       ║    │
│  ╚══════════════════════════════════╝  ║ │(6 tables)││(5)     ││(5)      │       ║    │
│                                        ║ └───────────┘└────────┘└──────────┘       ║    │
│                                        ║                                            ║    │
│                                        ║  output/kpis/ (16 parquet tables)          ║    │
│                                        ║  ├── kpi_total_revenue/                    ║    │
│                                        ║  ├── kpi_revenue_by_month/                 ║    │
│                                        ║  ├── kpi_revenue_growth/                   ║    │
│                                        ║  ├── kpi_revenue_by_category/              ║    │
│                                        ║  ├── kpi_revenue_by_segment/               ║    │
│                                        ║  ├── kpi_aov/                              ║    │
│                                        ║  ├── kpi_customer_lifetime_value/          ║    │
│                                        ║  ├── kpi_customer_acquisition/             ║    │
│                                        ║  ├── kpi_churn_rate/                       ║    │
│                                        ║  ├── kpi_repeat_purchase_rate/             ║    │
│                                        ║  ├── kpi_avg_orders_per_customer/          ║    │
│                                        ║  ├── kpi_top_products_by_revenue/          ║    │
│                                        ║  ├── kpi_top_products_by_quantity/         ║    │
│                                        ║  ├── kpi_product_profit_margin/            ║    │
│                                        ║  ├── kpi_category_contribution/            ║    │
│                                        ║  └── kpi_avg_discount_by_category/         ║    │
│                                        ╚════════════════════════════════════════════╝    │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘


══════════════════════════════════════════════════════════════
                    DESIGN PATTERNS APPLIED
══════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────────┐
│                         SOLID PRINCIPLES                                    │
│                                                                             │
│  S — Single Responsibility                                                  │
│      Each module has exactly one job.                                       │
│      CSVReader only reads CSV. NullCheck only removes nulls.                │
│      main.py only orchestrates — contains no transformation logic.          │
│                                                                             │
│  O — Open/Closed                                                            │
│      New reader? → create class + register in ReaderFactory.                │
│      New quality check? → create class + register in QualityCheckRegistry.  │
│      New KPI? → create class extending BaseKPI. Zero changes to existing.   │
│                                                                             │
│  L — Liskov Substitution                                                    │
│      All readers interchangeable via BaseReader interface.                  │
│      All checks interchangeable via BaseQualityCheck interface.             │
│      All KPIs interchangeable via BaseKPI interface.                        │
│                                                                             │
│  I — Interface Segregation                                                  │
│      BaseReader: read()  |  BaseQualityCheck: apply()  |  BaseKPI: compute()│
│      Minimal interfaces — no class forced to implement unused methods.      │
│                                                                             │
│  D — Dependency Inversion                                                   │
│      Orchestrator depends on abstractions (ABC), not concrete classes.      │
│      Factories serve as composition root wiring concretes from config.      │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                         DESIGN PATTERNS                                     │
│                                                                             │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────────┐  │
│  │  FACTORY PATTERN  │  │ STRATEGY PATTERN │  │   SINGLETON PATTERN     │  │
│  │                   │  │                  │  │                          │  │
│  │  ReaderFactory    │  │  QualityEngine   │  │  SparkSessionManager    │  │
│  │  QualityCheck     │  │  runs pluggable  │  │  ensures ONE Spark      │  │
│  │   Registry        │  │  checks as       │  │  session across the     │  │
│  │  MaskingStrategy  │  │  interchangeable │  │  entire pipeline        │  │
│  │   Factory         │  │  strategies      │  │                          │  │
│  └──────────────────┘  └──────────────────┘  └──────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘


══════════════════════════════════════════════════════════════
                    STAR SCHEMA (DATA WAREHOUSE)
══════════════════════════════════════════════════════════════

                        ┌──────────────┐
                        │  dim_date    │
                        │──────────────│
                        │ date_key (PK)│
                        │ full_date    │
                        │ year         │
                        │ quarter      │
                        │ month        │
                        │ month_name   │
                        │ week_of_year │
                        │ day_of_week  │
                        │ day_name     │
                        │ is_weekend   │
                        └──────┬───────┘
                               │
  ┌───────────────┐     ┌──────┴────────────┐     ┌────────────────┐
  │ dim_customer  │     │   fact_orders     │     │  dim_product   │
  │───────────────│     │───────────────────│     │────────────────│
  │ customer_key  │◄────│ customer_key (FK) │────►│ product_key    │
  │ customer_id   │     │ product_key  (FK) │     │ product_id     │
  │ first_name    │     │ date_key     (FK) │     │ product_name   │
  │ last_name     │     │ shipping_key (FK) │     │ category       │
  │ email ████    │     │ payment_key  (FK) │     │ sub_category   │
  │ phone ████    │     │ order_id         │     │ brand          │
  │ address ████  │     │ quantity         │     │ unit_price     │
  │ city          │     │ unit_price       │     │ cost_price     │
  │ state         │     │ total_amount     │     │ weight         │
  │ country       │     │ discount_amount  │     │ supplier       │
  │ segment       │     │ tax_amount       │     └────────────────┘
  │ signup_date   │     │ net_amount       │
  │ age_group     │     └──────┬────────────┘
  └───────────────┘            │
             ████ = PII masked │
                    ┌──────────┴──────────┐
                    │                     │
             ┌──────┴────────┐     ┌──────┴───────┐
             │ dim_shipping  │     │ dim_payment   │
             │───────────────│     │───────────────│
             │ shipping_key  │     │ payment_key   │
             │ shipping_id   │     │ payment_id    │
             │ carrier       │     │ payment_method│
             │ shipping_mode │     │ card_type     │
             │ tracking_num  │     │ card_number████
             │ ship_date     │     │ payment_status│
             │ delivery_date │     │ payment_date  │
             │ delivery_days │     │ transaction_id│
             │ shipping_cost │     └───────────────┘
             │ status        │
             └───────────────┘
```
