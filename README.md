# dbt Sales Analytics Platform

![GitHub stars](https://img.shields.io/github/stars/TheBoogieman/dbt-sales-analytics?style=social)
![GitHub forks](https://img.shields.io/github/forks/TheBoogieman/dbt-sales-analytics?style=social)

A production-grade analytics engineering project demonstrating modern data transformation practices using dbt (data build tool).

## 🎯 Project Overview

This project transforms raw e-commerce sales data into actionable business intelligence through a well-architected data pipeline. Built following industry best practices for scalability, testability, and maintainability.

### Business Questions Answered

1. **Revenue Analysis**: Monthly revenue trends by category with payment method breakdown
2. **Customer Segmentation**: Tiered classification based on lifetime value (High/Medium/Low)
3. **Payment Optimization**: Deep-dive into payment method preferences and performance
4. **Operational Flags**: Automated detection of orders requiring review (discount/shipping anomalies)
5. **Seasonal Patterns**: Quarter-over-quarter growth analysis with volatility metrics
6. **Daily Revenue**: Flexible date-range reporting with parameterized filtering

## 🏗️ Architecture

### Medallion Architecture (Bronze → Silver → Gold)
```
📁 Source Data (CSV)
    ↓
📊 STAGING LAYER (4 models)
    • Data cleaning & standardization
    • Type casting and validation
    • Source: stg_customer, stg_product, stg_product_category, stg_sales_fact
    ↓
🔗 INTERMEDIATE LAYER (2 models)
    • Business logic & joins
    • Reusable building blocks
    • Source: int_sales_enriched, int_customer_aggregates
    ↓
📈 MARTS LAYER (6 models)
    • Business-facing analytics
    • Optimized for reporting
    • Marts: revenue_analysis, customer_segmentation, payment_analysis, 
             order_flags, seasonal_patterns, daily_revenue
```

### Key Design Decisions

| Layer | Materialization | Rationale |
|-------|----------------|-----------|
| Staging | View | Lightweight, always fresh data |
| Intermediate | Table | Expensive joins computed once |
| Marts | Table | Query performance for BI tools |

## 🛠️ Technical Stack

- **dbt-core**: 1.8.0
- **Python**: 3.11
- **Database**: DuckDB (development), scalable to Snowflake/BigQuery/Redshift
- **Version Control**: Git
- **Testing**: 100+ automated data quality tests

## 📂 Project Structure
```
sales_analytics/
├── models/
│   ├── staging/           # Clean & standardize raw data
│   │   ├── _sources.yml
│   │   ├── stg_customer.sql
│   │   ├── stg_product.sql
│   │   ├── stg_product_category.sql
│   │   └── stg_sales_fact.sql
│   ├── intermediate/      # Joins & business logic
│   │   ├── int_customer_aggregates.sql
│   │   └── int_sales_enriched.sql
│   └── marts/             # Business-facing analytics
│       ├── mart_customer_segmentation.sql
│       ├── mart_daily_revenue.sql
│       ├── mart_order_flags.sql
│       ├── mart_payment_method_analysis.sql
│       ├── mart_revenue_analysis.sql
│       └── mart_seasonal_patterns.sql
├── macros/
│   ├── filter_date_range.sql    # Parameterized date filtering
│   └── read_csv_source.sql      # DuckDB CSV reader
├── tests/                 # Custom business logic tests
├── dbt_project.yml
└── README.md
```

## 🚀 Getting Started

### Prerequisites

- Python 3.11+
- dbt-core 1.8+
- DuckDB

### Installation

1. **Clone the repository**
```bash
   git clone https://github.com/TheBoogieman/dbt-sales-analytics.git
   cd dbt-sales-analytics
```

2. **Create virtual environment**
```bash
   python -m venv venv
   source venv/Scripts/activate  # Windows Git Bash
   # source venv/bin/activate     # Mac/Linux
```

3. **Install dependencies**
```bash
   pip install dbt-core==1.8.0 dbt-duckdb
```

4. **Run dbt**
```bash
   dbt run      # Build all models
   dbt test     # Run all tests
   dbt docs generate && dbt docs serve  # View documentation
```

## 📊 Key Features

### Advanced SQL Techniques
- **Window Functions**: LAG for QoQ growth, ROW_NUMBER for ranking
- **Statistical Calculations**: Standard deviation, coefficient of variation
- **Complex Joins**: 4-table star schema with referential integrity
- **Aggregations**: Multi-level GROUP BY with PARTITION BY

### Data Quality & Testing
- **100+ Automated Tests**: Generic tests (unique, not_null, relationships) + custom business logic validation
- **Null Handling**: Explicit COALESCE with audit trails
- **Edge Case Management**: Zero-quantity orders, missing data scenarios
- **Reconciliation Tests**: Revenue totals validated across layers

### Reusability & Maintainability
- **DRY Principle**: Complex joins written once in intermediate layer
- **Parameterized Macros**: Dynamic date filtering with Jinja templating
- **Comprehensive Documentation**: Every model and column documented with business context
- **Version Controlled**: Full Git history with meaningful commits

## 📈 Sample Insights

Based on the sample dataset:

- **Customer Distribution**: 41 customers across 3 value tiers (High/Medium/Low)
- **Revenue Analysis**: 100 orders totaling $X across Y product categories
- **Payment Preferences**: Credit Card leads with Z% of transactions
- **Flagged Orders**: N orders require review for high discounts or shipping costs
- **Seasonal Trends**: QoQ growth rates ranging from -X% to +Y%

## 🎯 Business Impact

- **Customer Targeting**: Identify high-value customers (≥$1000 LTV) for retention campaigns
- **Payment Optimization**: Data-driven negotiation with payment processors
- **Fraud Detection**: Automated flagging of anomalous orders
- **Inventory Planning**: Seasonal pattern analysis for stock optimization
- **Revenue Forecasting**: Time-series trends with statistical volatility measures

## 🔄 Scalability

**Current**: Handles 100 orders on local DuckDB  
**Production-Ready**: Architecture supports billions of rows with:
- Incremental materialization (process only new data)
- Cloud data warehouse integration (Snowflake, BigQuery, Redshift)
- CI/CD pipelines for automated testing
- Orchestration via Airflow/Dagster

**No code changes required** - only configuration adjustments.

## 🧪 Testing Strategy

### Test Pyramid
```
         /\
        /  \       Custom Business Logic Tests
       /____\      - Payment percentages sum to 100%
      /      \     - Revenue reconciliation across layers
     /        \    - Threshold validation (discount, shipping)
    /__________\   
   /            \  Generic Schema Tests
  /______________\ - unique, not_null, accepted_values
                   - relationships (foreign keys)
```

### Run Tests
```bash
dbt test                              # All tests
dbt test --select staging            # Staging layer only
dbt test --select mart_revenue_analysis  # Specific mart
```

## 📖 Documentation

Full project documentation available via dbt docs:
```bash
dbt docs generate
dbt docs serve
```

Opens interactive documentation with:
- Data lineage diagrams (DAG visualization)
- Column-level descriptions
- Model dependencies
- Test results

## 🎓 What I Learned

- **dbt Best Practices**: Layered architecture, materialization strategies, testing frameworks
- **Advanced SQL**: Window functions, CTEs, statistical calculations
- **Data Modeling**: Star schema design, slowly changing dimensions, fact/dimension tables
- **Jinja Templating**: Dynamic SQL generation, parameterized macros
- **Production Engineering**: Code quality, version control, documentation standards

## 🔮 Future Enhancements

- [ ] Implement incremental models for large-scale fact tables
- [ ] Add data freshness monitoring and alerting
- [ ] Create dbt exposures for downstream BI dashboards
- [ ] Integrate with CI/CD (GitHub Actions for automated testing)
- [ ] Add data observability with elementary or re_data
- [ ] Implement slowly changing dimensions (Type 2 SCD)

## 🤝 Contributing

This is a portfolio project, but suggestions and feedback are welcome! Feel free to:
- Open issues for questions or suggestions
- Fork the repo to experiment with the code
- Share ideas for additional analyses

## 📝 License

MIT License - feel free to use this project as a learning resource or template for your own dbt projects.

## 👤 Author

**Your Name**
- GitHub: [@TheBoogieman](https://github.com/TheBoogieman)
- LinkedIn: [LinkedIn](https://www.linkedin.com/in/ayd%C4%B1n-aksoy-138714106/)

---

*Built with ❤️ using dbt, demonstrating production-grade analytics engineering practices.*
